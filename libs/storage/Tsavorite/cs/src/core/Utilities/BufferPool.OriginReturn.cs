// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
#define CHECK_FREE      // disabled by default in Release due to overhead; must match BufferPool.cs
#endif

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>
    /// Standalone byte-budget accounting for the origin-return backend. Referenced directly by the pool and by
    /// every permit-holding buffer/shard so a late permit release (e.g. from a retired shard's finalizer)
    /// remains valid after the pool is closed. The counter is only touched on a buffer's cacheable birth and
    /// on its permanent death — never on the hot local/cross-thread/depot transitions.
    /// </summary>
    internal sealed class BudgetState
    {
        internal readonly long budgetBytes;
        private long usedBytes;

        internal BudgetState(long budgetBytes) => this.budgetBytes = budgetBytes;

        /// <summary>Reserve <paramref name="bytes"/> against the budget; false if it would exceed the cap.</summary>
        internal bool TryReserve(long bytes)
        {
            while (true)
            {
                var cur = Volatile.Read(ref usedBytes);
                var next = cur + bytes;
                if (next > budgetBytes)
                    return false;
                if (Interlocked.CompareExchange(ref usedBytes, next, cur) == cur)
                    return true;
            }
        }

        /// <summary>Release a previously reserved <paramref name="bytes"/>.</summary>
        internal void Release(long bytes) => Interlocked.Add(ref usedBytes, -bytes);

        internal long Used => Volatile.Read(ref usedBytes);
    }

    /// <summary>
    /// One <c>(pool, thread, size-class)</c> cache. Owner-only fields (<see cref="localHead"/> etc.) are touched
    /// without atomics by the owning thread; <see cref="crossThreadHead"/> is a lock-free, multi-producer/
    /// single-consumer stack that other (IO-completion) threads push onto and the owner batch-claims. The owner-hot
    /// <see cref="localHead"/> and the producer-hot <see cref="crossThreadHead"/> are placed on separate 64-byte
    /// cache lines via <see cref="StructLayoutAttribute"/>(<see cref="LayoutKind.Explicit"/>) so their concurrent
    /// mutation does not false-share. Explicit field offsets are required: a reference-type class ignores
    /// <see cref="LayoutKind.Sequential"/> and groups reference fields together (so interleaved padding fields do
    /// NOT separate them). When the owning shard retires, <see cref="crossThreadHead"/> is set to the
    /// <see cref="SectorAlignedBufferPool.Sealed"/> sentinel so late foreign Returns reroute to the depot instead
    /// of stranding.
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    internal sealed class Bucket
    {
        // Owner-only cache line [0, 64): mutated without atomics only by the owning thread's Get/Return, alongside
        // the immutable identity fields it also reads on those paths.
        [FieldOffset(0)] internal SectorAlignedMemory localHead;
        [FieldOffset(8)] internal int localCount;
        [FieldOffset(16)] internal long localBytes;
        [FieldOffset(24)] internal ThreadShard owner;
        [FieldOffset(32)] internal int sizeClass;
        [FieldOffset(40)] internal long classCapacityBytes;

        // Cross-thread cache line [64, 128): CAS-mutated by foreign (IO-completion) producers, isolated on its own
        // line. The trailing pad reserves the rest of the line so the head does not false-share with an adjacent
        // heap object.
        [FieldOffset(64)] internal SectorAlignedMemory crossThreadHead;
#pragma warning disable CS0169 // tail padding reserves the remainder of the cross-thread cache line
        [FieldOffset(120)] private long tailPad;
#pragma warning restore CS0169
    }

    /// <summary>
    /// Per <c>(pool, thread)</c> shard owning one <see cref="Bucket"/> per size-class, and the byte ceiling those
    /// buckets share (see <see cref="localByteCap"/>). Finalizable so that a thread which dies with buffers still
    /// cached releases those buffers' budget permits (there is no thread-exit callback). Self-clears its heavy
    /// references on seal so a lingering thread-static slot on another thread roots nothing large.
    /// </summary>
    internal sealed class ThreadShard
    {
        internal const int Alive = 0;
        internal const int Sealed = 1;

        internal Bucket[] buckets;
        internal SectorAlignedBufferPool pool;      // identity check for a recycled thread-static slot; nulled on seal
        internal int state;                          // Alive / Sealed
        internal int drainedOnce;                    // CAS 0->1: arbitrates explicit seal vs. finalization (drain at most once)

        /// <summary>Bytes currently parked on this shard's owner-local chains; owner-only, no atomics.</summary>
        internal long localBytes;

        /// <summary>Number of size classes with a non-empty owner-local chain; owner-only, no atomics.</summary>
        internal int activeClasses;

        /// <summary>Ceiling for <see cref="localBytes"/>, shared across this shard's size classes.</summary>
        internal readonly long localByteCap;

        internal ThreadShard(SectorAlignedBufferPool pool, int numClasses, int sectorSize, int[] classCaps, long localByteCap, bool bornSealed)
        {
            this.pool = pool;
            this.localByteCap = localByteCap;
            state = bornSealed ? Sealed : Alive;
            buckets = new Bucket[numClasses];
            for (var c = 0; c < numClasses; c++)
            {
                var bucket = new Bucket
                {
                    owner = this,
                    sizeClass = c,
                    classCapacityBytes = (long)classCaps[c] * sectorSize
                };
                if (bornSealed)
                    bucket.crossThreadHead = SectorAlignedBufferPool.Sealed;   // late foreign Returns reroute to depot
                buckets[c] = bucket;
            }
        }

        /// <summary>
        /// Finalizer: the owning thread is dead and no in-flight buffer roots this shard, so release the budget
        /// permits of the buffers it still holds. Arbitrated with explicit sealing via <see cref="drainedOnce"/>.
        /// </summary>
        ~ThreadShard()
        {
            if (Interlocked.CompareExchange(ref drainedOnce, 1, 0) != 0)
                return;
            var localBuckets = buckets;
            if (localBuckets is null)
                return;
            for (var c = 0; c < localBuckets.Length; c++)
            {
                var bucket = localBuckets[c];
                if (bucket is null)
                    continue;
                var chain = Interlocked.Exchange(ref bucket.crossThreadHead, SectorAlignedBufferPool.Sealed);
                if (!ReferenceEquals(chain, SectorAlignedBufferPool.Sealed))
                    SectorAlignedBufferPool.ReleaseChainPermits(chain);
                SectorAlignedBufferPool.ReleaseChainPermits(bucket.localHead);
                bucket.localHead = null;
            }
            buckets = null;
        }
    }

    /// <summary>
    /// Pool-owned, per-size-class, lightly-striped overflow depot. A cold path (only hit when a thread's local
    /// and cross-thread lists are both empty), so a per-stripe lock is fine and sidesteps ABA entirely. The
    /// close flag is consulted under the same lock as push, so there is no lifecycle-check-then-enqueue race.
    /// </summary>
    /// <remarks>
    /// Deliberately a monitor-guarded <see cref="Stack{T}"/> rather than <c>ConcurrentStack</c>:
    /// <list type="bullet">
    /// <item><b>Atomic close.</b> <see cref="Close"/> sets <c>closed</c> and drains under the one lock, so a push
    /// can never land in a stripe that has already been drained. Lock-free, that push would strand the buffer's
    /// byte permit for the life of the pool, since nothing revisits a closed stripe.</item>
    /// <item><b>Bounded capacity.</b> <c>ConcurrentStack</c> has no bounded form, so <c>cap</c> would need a
    /// separate interlocked counter that can drift from the actual contents. The cap is what bounds how much
    /// memory threads can park cross-thread, so it has to be enforced atomically with the push.</item>
    /// <item><b>No per-push allocation.</b> <c>ConcurrentStack.Push</c> allocates a link node per item; this pool
    /// exists to avoid exactly that. <see cref="Stack{T}"/> pushes into a pre-grown array.</item>
    /// </list>
    /// Contention is bounded by striping: the depot is spread <c>DepotStripes</c> ways, sized from the
    /// machine's processor count, is reached only on magazine overflow/underflow, and each critical section
    /// is O(1). A lock-free CAS on one shared head would reintroduce the cross-core ping-pong on a single
    /// cache line that this design removes.
    /// </remarks>
    internal sealed class DepotStripe
    {
        private readonly Stack<SectorAlignedMemory> items = new();
        private readonly int cap;
        private bool closed;

        internal DepotStripe(int cap) => this.cap = cap;

        internal bool TryPush(SectorAlignedMemory page)
        {
            lock (items)
            {
                if (closed || items.Count >= cap)
                    return false;
                items.Push(page);
                return true;
            }
        }

        internal SectorAlignedMemory TryPop()
        {
            lock (items)
            {
                return items.Count > 0 ? items.Pop() : null;
            }
        }

        internal void Close(Action<SectorAlignedMemory> dropAndRelease)
        {
            lock (items)
            {
                closed = true;
                while (items.Count > 0)
                    dropAndRelease(items.Pop());
            }
        }
    }

    public sealed partial class SectorAlignedBufferPool
    {
        // ---- Size-class ladder (exact-tiny + coarse-linear + geometric) ---------------------------------------
        // Linear region: TinyExactClasses exact 1-sector-granularity classes (512 B, 1 KB) so the very common
        // small reads/keys stay tight, then StrideClasses coarse classes at LinearStrideSectors granularity up to
        // LinearTopSectors (rounding waste < one stride). Above that, 2 classes per doubling up to MaxPooledSectors
        // (<= 1.5x waste). Requests above MaxPooledSectors bypass the cache (allocate-on-Get, free-on-Return). Values
        // are sector counts; at the common 512 B sector this ladder spans 512 B .. 16 MB, so a record built on a
        // multi-MB inline value up to nearly the ~16 MB max (16 MB minus the record header + key overhead) is
        // pooled. A record at the absolute inline-value cap (0xFFFFFE bytes) plus key + header rounds just past
        // 16 MB and falls into the bypass path, which is intentional and safe (allocate-on-Get, free-on-Return) —
        // such maximum-size records are rare and not worth parking a >16 MB buffer per thread-class.
        private const int TinyExactClasses = 2;         // exact 512 B, 1 KB
        private const int LinearStrideSectors = 4;      // 2 KB step at 512 B sectors
        private const int StrideClasses = 4;            // 2 / 4 / 6 / 8 KB classes
        private const int LinearClasses = TinyExactClasses + StrideClasses;         // 6
        private const int LinearTopSectors = LinearStrideSectors * StrideClasses;   // 16 (must be a power of two)
        private const int Log2LinearTop = 4;            // BitOperations.Log2(LinearTopSectors)
        private const int GeometricDoublings = 11;      // doublings above LinearTopSectors: 8 KB -> 16 MB
        private const int NumClasses = LinearClasses + 2 * GeometricDoublings;      // 28 (2 classes per doubling)
        private const int MaxPooledSectors = LinearTopSectors << GeometricDoublings; // 32768 (16 MB at 512 B sectors)

        // Local retention is bounded in bytes, per thread. Bytes are the resource the budget is denominated in;
        // a buffer count is not, since one size class's buffer is up to 512x another's, and the count a thread
        // needs is its in-flight IO pipeline depth, which is a property of the caller rather than of the pool.
        // A thread's ceiling is its equal slice of the sub-budget that local caching draws from, so the slices
        // of the expected concurrent threads sum to that sub-budget and no thread can retain enough to starve
        // the others. Within a thread the ceiling is shared across size classes and enforced only once the
        // thread reaches it: a thread whose traffic is a single class keeps the whole slice, and at the ceiling
        // a class below its equal share reclaims from the class furthest above its share (see TryMakeRoom).
        private const long MinThreadLocalBytes = 1L << 20;   // floor, so a small configured budget still caches
        private static readonly int DepotStripes = ConcurrencySharding.DepotStripeCount;   // power of two
        private static readonly int DepotStripeMask = DepotStripes - 1;
        private const int DepotStripeCap = 1024;        // buffers per depot stripe
        private const int InitialPoolShardRegistryCompactThreshold = 64;  // compact dead shard weak-references once the registry first exceeds this

        // Budget partitioning. The pool's total byte budget is split into two isolated sub-budgets so a burst of
        // large record/flush buffers (up to 16 MB each) cannot consume the whole budget and starve caching of the
        // hot small buffers (the common Get(sectorSize)/key/small-read path). A size class is "large" when its
        // capacity exceeds LargeTierMinBytes; small classes reserve against a fixed 1/SmallBudgetDivisor slice of
        // the total and large classes against the remainder.
        private const long LargeTierMinBytes = 256L << 10;  // classes with capacity > 256 KB draw from the large sub-budget
        private const int SmallBudgetDivisor = 4;           // small sub-budget = ManagedBudgetBytes / 4

        // Pool lifecycle.
        private const int PoolActive = 0;
        private const int PoolClosing = 1;
        private const int PoolClosed = 2;

        /// <summary>Sentinel installed in a <see cref="Bucket.crossThreadHead"/> when its owning shard retires. A
        /// claiming owner that observes it aborts (conditional CAS), and a pushing producer that observes it
        /// reroutes to the depot — the sentinel is never swapped out for null.</summary>
        internal static readonly SectorAlignedMemory Sealed = new();

        // ---- Shard indexing: the (pool, thread) -> ThreadShard relation -----------------------------------------
        //
        // Every thread that touches a pool gets its own ThreadShard for that pool, so live shards form a sparse
        // (pool x thread) matrix. Two indexes cover that matrix, one per axis, and slotIndex is the key joining
        // them:
        //
        //   t_shards           Thread-side index. [ThreadStatic], so each thread owns its array, indexed by the
        //                      POOL's slotIndex: t_shards[slotIndex] is the calling thread's shard for this pool.
        //                      The hot Get/Return lookup, so it is a bounds check plus an array read - no lock, no
        //                      allocation. References are STRONG: a shard and every buffer parked on its
        //                      owner-local chains stay rooted for the life of the thread, which is why teardown
        //                      detaches those chains rather than leaving them for the GC.
        //
        //   poolShardRegistry  Pool-side index. One list per pool holding a WEAK reference to each of that pool's
        //                      shards across all threads, guarded by poolShardRegistryLock and read only on cold
        //                      paths (teardown, diagnostics). Weak so that a shard whose thread has exited becomes
        //                      collectable and its finalizer returns the permits; dead entries are compacted
        //                      amortized (see CompactPoolShardRegistryLocked).
        //
        //   slotIndex          The join key: this pool's position within every thread's t_shards array, taken from
        //                      the process-wide free list below at construction and returned on Free.
        //                      Slots RECYCLE, so a thread's array may still hold a shard left by an earlier pool
        //                      at the same position; every read therefore confirms identity before trusting the
        //                      entry - against the pool (ReferenceEquals(shard.pool, this)) where the shard is
        //                      being resolved, or against the bucket's owner (ReferenceEquals(arr[slot], owner))
        //                      where a shard is already in hand. Recycling is also what bounds the array length by
        //                      the number of CONCURRENTLY-LIVE pools instead of by how many pools the process has
        //                      created over its lifetime.
        //
        // The two indexes are intentionally not symmetric in how they are cleared. A new shard is added to both,
        // but Free empties only the registry: the thread-side entry is left in place and repaired lazily, since
        // teardown nulls shard.pool and the owning thread's next Get fails the identity check and builds a fresh
        // (born-sealed) shard. Neither index is a source of truth for the other - the registry answers "which
        // shards belong to this pool", t_shards answers "which shard is mine".
        [ThreadStatic]
        private static ThreadShard[] t_shards;

        // Process-wide free-list of released slot indices (reused after a pool is freed).
        private static readonly object s_slotLock = new();
        private static readonly Stack<int> s_freeSlots = new();
        private static int s_slotHighWater;

        // ---- Per-pool origin-return state (set in InitOriginReturn) --------------------------------------------
        private int slotIndex;                            // this pool's position in every thread's t_shards array (see above)
        private BudgetState smallBudget;                  // isolated sub-budget for small classes (capacity <= LargeTierMinBytes)
        private BudgetState largeBudget;                  // isolated sub-budget for large (record/flush) classes
        private int firstLargeClass;                      // classes at or above this index draw from largeBudget
        private long threadLocalByteCap;                  // per-thread ceiling on locally-retained (small-class) bytes
        private int[] classCaps;                          // capacity in sectors per class
        private DepotStripe[] depot;                      // [NumClasses * DepotStripes]
        private List<WeakReference<ThreadShard>> poolShardRegistry;   // weak ref to each of this pool's shards, all threads (see above)
        private object poolShardRegistryLock;
        private int poolShardRegistryCompactThreshold;    // amortized dead-weak-reference compaction trigger (guarded by poolShardRegistryLock)
        private int poolState;                            // PoolActive / PoolClosing / PoolClosed
        private long totalManagedAllocations;             // test-only reuse-efficiency counter

        private void InitOriginReturn()
        {
            Debug.Assert(LinearTopSectors == (1 << Log2LinearTop), "LinearTopSectors must equal 1 << Log2LinearTop");
            Debug.Assert(LinearTopSectors == LinearStrideSectors * StrideClasses, "LinearTopSectors must equal LinearStrideSectors * StrideClasses");
            Debug.Assert(LinearClasses == TinyExactClasses + StrideClasses, "LinearClasses must equal TinyExactClasses + StrideClasses");
            slotIndex = AcquireSlot();
            var smallBudgetBytes = ManagedBudgetBytes / SmallBudgetDivisor;
            smallBudget = new BudgetState(smallBudgetBytes);
            largeBudget = new BudgetState(ManagedBudgetBytes - smallBudgetBytes);
            // Local caching parks only small classes, so a thread's slice is cut from the small sub-budget.
            threadLocalByteCap = Math.Max(smallBudgetBytes / ConcurrencySharding.ExpectedConcurrentThreads, MinThreadLocalBytes);
            classCaps = new int[NumClasses];
            firstLargeClass = NumClasses;
            for (var c = 0; c < NumClasses; c++)
            {
                classCaps[c] = ClassCapacitySectors(c);
                if (firstLargeClass == NumClasses && (long)classCaps[c] * sectorSize > LargeTierMinBytes)
                    firstLargeClass = c;
            }
            depot = new DepotStripe[NumClasses * DepotStripes];
            for (var i = 0; i < depot.Length; i++)
                depot[i] = new DepotStripe(DepotStripeCap);
            poolShardRegistry = new List<WeakReference<ThreadShard>>();
            poolShardRegistryLock = new object();
            poolShardRegistryCompactThreshold = InitialPoolShardRegistryCompactThreshold;
            poolState = PoolActive;
        }

        private static int AcquireSlot()
        {
            lock (s_slotLock)
                return s_freeSlots.Count > 0 ? s_freeSlots.Pop() : s_slotHighWater++;
        }

        private static void ReleaseSlot(int slot)
        {
            lock (s_slotLock)
                s_freeSlots.Push(slot);
        }

        // ---- Size-class math (pure) ----------------------------------------------------------------------------

        /// <summary>Map a sector count to a size class, or -1 to bypass the cache (request too large).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ClassOfSectors(int sectors)
        {
            if (sectors <= 0)
                sectors = 1;
            if (sectors <= TinyExactClasses)
                return sectors - 1;                                              // exact 512 B, 1 KB
            if (sectors <= LinearTopSectors)
                return TinyExactClasses + (sectors - 1) / LinearStrideSectors;   // 2 KB .. 8 KB (stride-rounded)
            if (sectors > MaxPooledSectors)
                return -1;
            // Geometric region: 2 classes per doubling — a midpoint (1.5 * 2^octave) and a top (2^(octave+1)) —
            // which halves worst-case over-allocation from 2x (one class per doubling) to 1.5x, hence the
            // 2 * GeometricDoublings term in NumClasses. At 512 B sectors these 22 classes are
            // 12/16/24/32/48/64/96/128/192/256/384/512/768 KB then 1/1.5/2/3/4/6/8/12/16 MB.
            var octave = BitOperations.Log2((uint)(sectors - 1));   // >= Log2LinearTop
            var mid = 3 << (octave - 1);                            // 1.5 * 2^octave
            var sub = sectors <= mid ? 0 : 1;
            return LinearClasses + 2 * (octave - Log2LinearTop) + sub;
        }

        /// <summary>Capacity of a size class in sectors.</summary>
        private static int ClassCapacitySectors(int cls)
        {
            if (cls < TinyExactClasses)
                return cls + 1;                                                  // 512 B, 1 KB
            if (cls < LinearClasses)
                return (cls - TinyExactClasses + 1) * LinearStrideSectors;       // 2 KB .. 8 KB
            var g = cls - LinearClasses;
            var octave = Log2LinearTop + (g >> 1);
            return (g & 1) == 0 ? (3 << (octave - 1)) : (1 << (octave + 1));
        }

        /// <summary>Select the isolated sub-budget a class reserves against: large (record/flush) classes draw
        /// from <see cref="largeBudget"/>, everything else from <see cref="smallBudget"/>, so large-buffer churn
        /// cannot exhaust the budget reserved for the hot small-buffer path.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private BudgetState BudgetFor(int cls) => cls >= firstLargeClass ? largeBudget : smallBudget;

        // ---- Get -----------------------------------------------------------------------------------------------

        private unsafe SectorAlignedMemory GetOriginReturn(int required_bytes, int requiredSize, bool clearOnReturn)
        {
            var sectors = sectorSizeShift >= 0 ? (requiredSize >> sectorSizeShift) : (requiredSize / sectorSize);
            var cls = ClassOfSectors(sectors);
            if (cls < 0 || Disabled)
            {
                RecordBypassAlloc();
                return AllocateUncached(required_bytes, requiredSize, clearOnReturn);
            }

            var shard = GetOrCreateShard();
            var bucket = shard.buckets[cls];

            // 1. owner-only local stack (no atomics)
            var page = bucket.localHead;
            if (page is not null)
            {
                bucket.localHead = page.next;
                bucket.localCount--;
                bucket.localBytes -= page.permitBytes;
                shard.localBytes -= page.permitBytes;
                if (bucket.localCount == 0)
                    shard.activeClasses--;
                RecordReuse(cls);
                return PrepareForRent(page, bucket, required_bytes, clearOnReturn);
            }

            // 2. claim the whole cross-thread chain in one seal-aware CAS, splice remainder into local
            var claimed = ClaimCrossThread(bucket);
            if (claimed is not null)
            {
                page = claimed;
                var rest = page.next;
                page.next = null;
                SpliceIntoLocal(bucket, rest);
                RecordReuse(cls);
                return PrepareForRent(page, bucket, required_bytes, clearOnReturn);
            }

            // 3. shared per-class depot
            page = DepotPop(cls);
            if (page is not null)
            {
                RecordReuse(cls);
                return PrepareForRent(page, bucket, required_bytes, clearOnReturn);
            }

            // 4. allocate (reserving a poolability permit against the byte budget)
            RecordAlloc(cls);
            return AllocateForBucket(bucket, cls, required_bytes, clearOnReturn);
        }

        /// <summary>Re-arm a cached buffer for a fresh rental: re-pin (unpin mode), lazy-clear a dirty tail, set
        /// rental fields, and tag the current owner bucket.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe SectorAlignedMemory PrepareForRent(SectorAlignedMemory page, Bucket bucket, int required_bytes, bool clearOnReturn)
        {
#if CHECK_FREE
            page.Free = false;
#endif
            if (unpinOnReturn)
            {
                page.handle = GCHandle.Alloc(page.buffer, GCHandleType.Pinned);
                page.aligned_pointer = (byte*)RoundUp(page.handle.AddrOfPinnedObject(), sectorSize);
                page.aligned_offset = (int)((long)page.aligned_pointer - page.handle.AddrOfPinnedObject());
            }
            if (clearOnReturn && page.isDirty)
            {
                Array.Clear(page.buffer, 0, page.buffer.Length);
                page.isDirty = false;
            }
            page.required_bytes = required_bytes;
            page.clearOnReturn = clearOnReturn;
            page.originBucket = bucket;
            page.next = null;
            return page;
        }

        private unsafe SectorAlignedMemory AllocateForBucket(Bucket bucket, int cls, int required_bytes, bool clearOnReturn)
        {
            var allocBytes = checked((classCaps[cls] + 1) * sectorSize);   // extra sector for internal alignment
            var page = BuildManaged(cls, allocBytes, required_bytes, clearOnReturn);
            page.originBucket = bucket;

            // Reserve a persistent poolability permit unless the pool/shard is closing or the budget is exhausted.
            var actual = (long)page.buffer.Length;
            var b = BudgetFor(cls);
            if (bucket.owner.state == ThreadShard.Alive && Volatile.Read(ref poolState) == PoolActive && b.TryReserve(actual))
            {
                page.cacheable = true;
                page.permitBytes = actual;
                page.permitReleased = 0;
                page.budget = b;
            }
            else
            {
                page.cacheable = false;
                page.permitBytes = 0;
            }
            Debug.Assert(bucket.classCapacityBytes >= RoundUp(required_bytes, sectorSize), "selected class capacity is smaller than the rounded request");
            return page;
        }

        /// <summary>Allocate a bypass buffer sized exactly to the (rounded) request; never enters the cache.</summary>
        private unsafe SectorAlignedMemory AllocateUncached(int required_bytes, int requiredSize, bool clearOnReturn)
        {
            var allocBytes = checked(requiredSize + sectorSize);
            var page = BuildManaged(0, allocBytes, required_bytes, clearOnReturn);
            page.cacheable = false;
            page.permitBytes = 0;
            page.originBucket = null;
            return page;
        }

        private unsafe SectorAlignedMemory BuildManaged(int level, int allocBytes, int required_bytes, bool clearOnReturn)
        {
            Interlocked.Increment(ref totalManagedAllocations);
            var page = new SectorAlignedMemory(level: level)
            {
                buffer = GC.AllocateArray<byte>(allocBytes, !unpinOnReturn)
            };
            if (unpinOnReturn)
                page.handle = GCHandle.Alloc(page.buffer, GCHandleType.Pinned);
            var pageAddr = (long)Unsafe.AsPointer(ref page.buffer[0]);
            page.aligned_pointer = (byte*)RoundUp(pageAddr, sectorSize);
            page.aligned_offset = (int)((long)page.aligned_pointer - pageAddr);
            page.required_bytes = required_bytes;
            // Freshly-allocated buffer from GC.AllocateArray is zero-init; isDirty stays false.
            page.clearOnReturn = clearOnReturn;
            page.pool = this;
            page.next = null;
            return page;
        }

        // ---- Return --------------------------------------------------------------------------------------------

        private void ReturnOriginReturn(SectorAlignedMemory page)
        {
            if (!page.cacheable)
            {
                // Bypass / budget-overflow buffer: never cached. Drop and release any permit (none for bypass).
#if CHECK_FREE
                page.Free = true;
#endif
                DropBuffer(page);
                return;
            }

#if CHECK_FREE
            page.Free = true;
#endif
            FinalizeForReturn(page);

            var bucket = page.originBucket;
            if (bucket is null)
            {
                DropBuffer(page);
                return;
            }

            // Large (record/flush) classes are shared globally via the striped depot, on both the owner and the
            // foreign return path. The per-thread tiers exploit a thread reusing the same size back-to-back; with
            // a wide value-size mix a thread rarely re-requests the same large class, so a large buffer parked on
            // its origin thread sits idle while the working set grows as (thread x class). One global working set
            // under the same byte budget serves every thread, and keeps large returns clear of the origin-shard
            // finalize race (the depot is pool-owned). Large-buffer ops are low-rate and the workload is
            // bandwidth-bound, so the striped-depot handoff costs ~nothing. Small classes keep the atomic-free
            // per-thread origin-return fast path below. If the depot is closed (pool teardown), drop the buffer and
            // release its permit.
            if (page.Level >= firstLargeClass)
            {
                if (!DepotPush(page.Level, page))
                    DropBuffer(page);
                return;
            }

            var owner = bucket.owner;

            // Small class, same-thread owner (this pool's shard is in our slot, still alive)? -> owner-only local
            // push, no atomics. On this path the bucket is reachable via page.originBucket and owned by this live
            // thread, so no finalizer race is possible and no GC.KeepAlive is needed.
            // The buffer already names its owning shard, so the thread-side lookup is compared against that owner
            // rather than against the pool: matching arr[slot] proves the owner is the calling thread's shard for
            // this pool, which subsumes the pool-identity check a slot recycle would otherwise require.
            var arr = t_shards;
            var slot = slotIndex;
            if (arr is not null && slot < arr.Length && ReferenceEquals(arr[slot], owner) && owner.state == ThreadShard.Alive)
            {
                PushLocal(bucket, page);
                return;
            }

            // Small class, foreign Return: publish onto the origin bucket's cross-thread stack; if sealed, spill to
            // depot; if the depot is closed, drop and release the permit. KeepAlive pins the owning shard across the
            // publish so a concurrent owner-side claim + finalize cannot collect it mid-push.
            if (!TryPushCrossThread(bucket, page))
            {
                if (!DepotPush(page.Level, page))
                    DropBuffer(page);
            }
            GC.KeepAlive(bucket);
        }

        /// <summary>Reset rental fields and honor the clear/unpin policy BEFORE the buffer is published to any
        /// list (the publishing CAS is the release; the owner's claim is the acquire).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void FinalizeForReturn(SectorAlignedMemory page)
        {
            page.available_bytes = 0;
            page.required_bytes = 0;
            page.valid_offset = 0;
            if (page.clearOnReturn)
            {
                Array.Clear(page.buffer, 0, page.buffer.Length);
                page.isDirty = false;
            }
            else
            {
                page.isDirty = true;
            }
            page.clearOnReturn = true;
            if (unpinOnReturn)
            {
                page.handle.Free();
                page.handle = default;
                page.aligned_pointer = null;
            }
            page.next = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PushLocal(Bucket bucket, SectorAlignedMemory page)
        {
            var shard = bucket.owner;
            var bytes = page.permitBytes;
            if (shard.localBytes + bytes > shard.localByteCap && !TryMakeRoom(shard, bucket, bytes))
            {
                if (!DepotPush(page.Level, page))
                    DropBuffer(page);
                return;
            }
            RetainLocal(shard, bucket, page, bytes);
        }

        /// <summary>Park a buffer on its bucket's owner-local chain and account for it on the owning shard.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void RetainLocal(ThreadShard shard, Bucket bucket, SectorAlignedMemory page, long bytes)
        {
            if (bucket.localCount == 0)
                shard.activeClasses++;
            page.next = bucket.localHead;
            bucket.localHead = page;
            bucket.localCount++;
            bucket.localBytes += bytes;
            shard.localBytes += bytes;
        }

        /// <summary>
        /// The thread is at its local byte ceiling. Admit <paramref name="bytes"/> for <paramref name="bucket"/>'s
        /// class only if that class holds less than an equal share of the ceiling, making room by spilling the
        /// class furthest above its share to the shared depot, where any thread can still reuse those buffers.
        /// Reached only at the ceiling, so a thread whose traffic is a single size class keeps the whole slice;
        /// under contention the outcome is max-min fair across the classes that thread is actually using.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private bool TryMakeRoom(ThreadShard shard, Bucket bucket, long bytes)
        {
            // The requester counts as active even when its chain is currently empty, so a starved class is not
            // shut out by the classes crowding it. A thread using a single class therefore gets share == cap and
            // fails here without scanning, which is the steady state once its chain is full.
            var active = bucket.localCount == 0 ? shard.activeClasses + 1 : shard.activeClasses;
            var share = shard.localByteCap / active;
            if (bucket.localBytes + bytes > share)
                return false;

            var buckets = shard.buckets;
            Bucket victim = null;
            while (shard.localBytes + bytes > shard.localByteCap)
            {
                if (victim is null || victim.localBytes <= share || victim.localHead is null)
                    victim = WorstOverShare(buckets, share);
                if (victim is null)
                    return false;
                SpillOneLocal(shard, victim);
            }
            return true;
        }

        /// <summary>The owner-local chain holding the most bytes above <paramref name="share"/>, or null if none is.</summary>
        private Bucket WorstOverShare(Bucket[] buckets, long share)
        {
            Bucket victim = null;
            var worst = 0L;
            for (var c = 0; c < firstLargeClass; c++)
            {
                var candidate = buckets[c];
                var over = candidate.localBytes - share;
                if (over > worst && candidate.localHead is not null)
                {
                    worst = over;
                    victim = candidate;
                }
            }
            return victim;
        }

        /// <summary>Move one buffer off an owner-local chain to the shared depot, dropping it if the depot is full.</summary>
        private void SpillOneLocal(ThreadShard shard, Bucket victim)
        {
            var page = victim.localHead;
            victim.localHead = page.next;
            victim.localCount--;
            victim.localBytes -= page.permitBytes;
            shard.localBytes -= page.permitBytes;
            if (victim.localCount == 0)
                shard.activeClasses--;
            if (!DepotPush(page.Level, page))
                DropBuffer(page);
        }

        private void SpliceIntoLocal(Bucket bucket, SectorAlignedMemory rest)
        {
            var shard = bucket.owner;
            var node = rest;
            while (node is not null && shard.localBytes + node.permitBytes <= shard.localByteCap)
            {
                var nx = node.next;
                RetainLocal(shard, bucket, node, node.permitBytes);
                node = nx;
            }
            while (node is not null)
            {
                var nx = node.next;
                node.next = null;
                if (!DepotPush(node.Level, node))
                    DropBuffer(node);
                node = nx;
            }
        }

        // ---- Lock-free cross-thread stack (multi-producer push, single-consumer bulk-claim; seal-aware) --------

        /// <summary>Producer push. Fails (returns false) when the bucket is sealed so the caller reroutes.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryPushCrossThread(Bucket bucket, SectorAlignedMemory page)
        {
            while (true)
            {
                var head = Volatile.Read(ref bucket.crossThreadHead);
                if (ReferenceEquals(head, Sealed))
                    return false;
                page.next = head;
                if (ReferenceEquals(Interlocked.CompareExchange(ref bucket.crossThreadHead, page, head), head))
                    return true;
            }
        }

        /// <summary>Owner claim of the whole chain via a seal-aware conditional CAS (swap head to null only when
        /// it is neither null nor the seal sentinel). This is not a CAS-pop, so there is no ABA.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static SectorAlignedMemory ClaimCrossThread(Bucket bucket)
        {
            while (true)
            {
                var head = Volatile.Read(ref bucket.crossThreadHead);
                if (head is null || ReferenceEquals(head, Sealed))
                    return null;
                if (ReferenceEquals(Interlocked.CompareExchange(ref bucket.crossThreadHead, null, head), head))
                    return head;
            }
        }

        // ---- Depot ---------------------------------------------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ThreadStripe() => Environment.CurrentManagedThreadId & DepotStripeMask;

        private bool DepotPush(int cls, SectorAlignedMemory page)
        {
            page.originBucket = null;    // migrating; the depot owns it now (its permit travels unchanged)
            page.next = null;
            return depot[cls * DepotStripes + ThreadStripe()].TryPush(page);
        }

        private SectorAlignedMemory DepotPop(int cls)
        {
            var baseIdx = cls * DepotStripes;
            var start = ThreadStripe();
            for (var i = 0; i < DepotStripes; i++)
            {
                var page = depot[baseIdx + ((start + i) & DepotStripeMask)].TryPop();
                if (page is not null)
                    return page;
            }
            return null;
        }

        // ---- Permit release / buffer drop ----------------------------------------------------------------------

        /// <summary>Release a buffer's byte-budget permit exactly once (guarded by the buffer's own flag), on its
        /// permanent drop.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ReleasePermit(SectorAlignedMemory page)
        {
            var b = page.budget;
            if (b is not null && page.permitBytes != 0 && Interlocked.CompareExchange(ref page.permitReleased, 1, 0) == 0)
                b.Release(page.permitBytes);
        }

        internal static void ReleaseChainPermits(SectorAlignedMemory head)
        {
            var node = head;
            while (node is not null)
            {
                var nx = node.next;
                ReleasePermit(node);
                node = nx;
            }
        }

        /// <summary>Permanently drop a buffer: free its pin handle (unpin mode), release its permit, unroot it.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void DropBuffer(SectorAlignedMemory page)
        {
            if (unpinOnReturn && page.handle.IsAllocated)
            {
                page.handle.Free();
                page.handle = default;
            }
            ReleasePermit(page);
            page.next = null;
            page.originBucket = null;
            page.buffer = null;
            page.aligned_pointer = null;
        }

        // ---- Shard resolution ----------------------------------------------------------------------------------

        /// <summary>Resolve the calling thread's shard for this pool, creating it on first touch. Reads the
        /// thread-side index (t_shards[slotIndex]); the pool-identity check rejects a shard left behind by an
        /// earlier pool that has since released this slot. See the shard-indexing notes above.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ThreadShard GetOrCreateShard()
        {
            var arr = t_shards;
            var slot = slotIndex;
            if (arr is not null && slot < arr.Length)
            {
                var s = arr[slot];
                if (s is not null && ReferenceEquals(s.pool, this))
                    return s;
            }
            return CreateShardSlow(slot);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private ThreadShard CreateShardSlow(int slot)
        {
            // Grow the thread-side array to cover this pool's slot, then register the new shard on the pool side so
            // teardown can reach it. Both indexes gain the shard here; only the registry is cleared by Free.
            var arr = t_shards;
            if (arr is null || slot >= arr.Length)
            {
                var newLen = arr is null ? Math.Max(slot + 1, 4) : Math.Max(slot + 1, arr.Length * 2);
                var na = new ThreadShard[newLen];
                if (arr is not null)
                    Array.Copy(arr, na, arr.Length);
                t_shards = na;
                arr = na;
            }

            ThreadShard shard;
            lock (poolShardRegistryLock)
            {
                var bornSealed = poolState != PoolActive;
                shard = new ThreadShard(this, NumClasses, sectorSize, classCaps, threadLocalByteCap, bornSealed);
                if (bornSealed)
                    shard.drainedOnce = 1;      // nothing cached; no drain, no permits held
                else
                {
                    // Compact dead weak-references before appending so a long-lived pool under thread churn keeps
                    // the registry bounded by (roughly) the live shard count rather than growing with every
                    // historical thread. Amortized O(1): compaction runs only when the list outgrows the last
                    // compacted size.
                    if (poolShardRegistry.Count >= poolShardRegistryCompactThreshold)
                        CompactPoolShardRegistryLocked();
                    poolShardRegistry.Add(new WeakReference<ThreadShard>(shard));
                }
            }
            arr[slot] = shard;
            return shard;
        }

        /// <summary>Remove dead (collected-shard) weak-references from the registry in place. Caller holds
        /// <see cref="poolShardRegistryLock"/>. Resets the compaction threshold to twice the surviving count so the next
        /// compaction is amortized against real growth.</summary>
        private void CompactPoolShardRegistryLocked()
        {
            var w = 0;
            for (var r = 0; r < poolShardRegistry.Count; r++)
            {
                if (poolShardRegistry[r].TryGetTarget(out _))
                    poolShardRegistry[w++] = poolShardRegistry[r];
            }
            if (w < poolShardRegistry.Count)
                poolShardRegistry.RemoveRange(w, poolShardRegistry.Count - w);
            poolShardRegistryCompactThreshold = Math.Max(InitialPoolShardRegistryCompactThreshold, poolShardRegistry.Count * 2);
        }

        // ---- Teardown ------------------------------------------------------------------------------------------

        private void FreeOriginReturn()
        {
            List<ThreadShard> live;
            lock (poolShardRegistryLock)
            {
                // Elect exactly one closer: only the caller that observes PoolActive proceeds. A second concurrent
                // (or later sequential) Free sees PoolClosing/PoolClosed and returns, so the slot is released once.
                if (poolState != PoolActive)
                    return;
                poolState = PoolClosing;    // stops new permit reservations and makes new shards born-sealed
                live = new List<ThreadShard>(poolShardRegistry.Count);
                foreach (var wr in poolShardRegistry)
                    if (wr.TryGetTarget(out var s))
                        live.Add(s);
                poolShardRegistry.Clear();
            }

            foreach (var shard in live)
                SealAndDrainShard(shard);

            if (depot is not null)
                foreach (var stripe in depot)
                    stripe.Close(DropBuffer);

            lock (poolShardRegistryLock)
                poolState = PoolClosed;

            ReleaseSlot(slotIndex);
        }

        private void SealAndDrainShard(ThreadShard shard)
        {
            if (Interlocked.CompareExchange(ref shard.drainedOnce, 1, 0) != 0)
                return;    // finalizer (or a prior drain) already handled it
            Interlocked.Exchange(ref shard.state, ThreadShard.Sealed);

            // Null the pool back-pointer BEFORE draining the buckets so an owner entering Get() fails the
            // thread-static identity check and builds a fresh born-sealed shard instead of popping from the buckets
            // being drained here. Combined with the Sealed state above (which already diverts owner Returns), this
            // narrows owner interference to a thread that is already past those checks.
            shard.pool = null;

            // The owner-private local stack (localHead/localCount/localBytes) is mutated WITHOUT atomics by the
            // owning thread's Get/Return. Only the shard owned by THIS (freeing) thread is provably quiescent here,
            // so only it may have its buffers dropped. For a shard owned by another (possibly still-active) thread,
            // dropping its local chain would race that owner's non-atomic pop/push — handing out a buffer whose
            // backing array was just nulled. Such a chain is instead detached and its permits released, WITHOUT
            // dropping the buffers: a buffer the owner is concurrently popping stays fully valid, while detaching
            // unroots the rest. Detaching is required, not merely tidy — the owning thread's thread-static slot
            // roots (shard -> buckets -> localHead -> buffers), so leaving the chain in place would pin this dead
            // pool's cached buffers for the remaining life of a long-lived (e.g. thread-pool) thread, invisibly to
            // the byte budget whose permits were just released.
            var arr = t_shards;
            var slot = slotIndex;
            var ownedByCurrentThread = arr is not null && slot < arr.Length && ReferenceEquals(arr[slot], shard);

            var buckets = shard.buckets;
            if (buckets is not null)
            {
                foreach (var bucket in buckets)
                {
                    if (bucket is null)
                        continue;
                    // Cross-thread chain: atomically claim it away from producers (they observe Sealed and reroute),
                    // then drop it — safe because the exchange is the single consumer of that chain.
                    var chain = Interlocked.Exchange(ref bucket.crossThreadHead, Sealed);
                    if (!ReferenceEquals(chain, Sealed))
                        DropChain(chain);

                    if (ownedByCurrentThread)
                    {
                        DropChain(bucket.localHead);
                        bucket.localHead = null;
                        bucket.localCount = 0;
                        bucket.localBytes = 0;
                    }
                    else
                    {
                        // Detach atomically with respect to other interlocked accesses, then release the permits of
                        // what we took. A racing owner can still resurrect part of the chain afterwards, because both
                        // its push (page.next = localHead; localHead = page) and its pop (localHead = page.next) are
                        // non-atomic read-then-write pairs that may straddle this exchange. Resurrected buffers stay
                        // fully valid; they are unrooted when the owner's next Get replaces the shard or the thread
                        // exits, and their permits are recovered by the finalizer (see below).
                        var localChain = Interlocked.Exchange(ref bucket.localHead, null);
                        bucket.localCount = 0;
                        bucket.localBytes = 0;
                        ReleaseChainPermits(localChain);
                    }
                }
                shard.localBytes = 0;
                shard.activeClasses = 0;
            }

            // Hand repair of any resurrected buffers back to ~ThreadShard by releasing drainedOnce: once this
            // (now pool-less) shard becomes unreachable, the finalizer sweeps whatever the owner put back and
            // releases those permits. Releasing a buffer's permit is CAS-guarded for exactly-once, so a second
            // sweep over already-released buffers is a no-op, and no finalizer can be running concurrently here
            // because Free still holds the shard reachable through its live-shard list. Without this the budget of
            // a pool freed under concurrent traffic would stay permanently short by the resurrected bytes.
            if (!ownedByCurrentThread)
                Volatile.Write(ref shard.drainedOnce, 0);

            // Do NOT null shard.buckets: a concurrent owner Get() may still be dereferencing buckets[cls]. The
            // per-bucket Sealed cross-thread tombstone is the lifecycle signal; keep buckets addressable.
        }

        private void DropChain(SectorAlignedMemory head)
        {
            var node = head;
            while (node is not null)
            {
                var nx = node.next;
                node.next = null;
                DropBuffer(node);
                node = nx;
            }
        }

        private void PrintOriginReturn()
        {
            List<ThreadShard> live;
            lock (poolShardRegistryLock)
            {
                live = new List<ThreadShard>(poolShardRegistry.Count);
                foreach (var wr in poolShardRegistry)
                    if (wr.TryGetTarget(out var s))
                        live.Add(s);
            }
            Console.WriteLine($"  origin-return pool: {live.Count} live shard(s), budget small {smallBudget.Used}/{smallBudget.budgetBytes} + large {largeBudget.Used}/{largeBudget.budgetBytes} bytes reserved");
        }

        // ---- Diagnostics (used by tests) -----------------------------------------------------------------------

        /// <summary>
        /// Optional, per-size-class allocation/reuse instrumentation for the origin-return pool, used by the
        /// KV.benchmark disk-stress harness to observe how new-buffer allocations stabilize while cache reuse
        /// grows, per size class, across threads. Counts are aggregated across every live pool in the process
        /// (a benchmark typically has one dominant read pool).
        /// <para>
        /// The recording call sites in the hot <c>Get</c> path are marked
        /// <see cref="System.Diagnostics.ConditionalAttribute"/> on the <c>BUFFER_POOL_STATS</c> compilation
        /// symbol, so a default build of Tsavorite emits <b>no instructions at all</b> for them — the shipping
        /// <c>Get</c> path is byte-for-byte identical whether or not this diagnostic exists. To collect stats,
        /// build the consuming project (e.g. KV.benchmark) with <c>-p:BufferPoolStats=true</c>, which defines the
        /// symbol for the transitively-built Tsavorite.core; then set <see cref="Enabled"/> before the workload.
        /// <see cref="Compiled"/> reports whether the current build actually recorded any counts.
        /// </para>
        /// </summary>
        public static class Stats
        {
            /// <summary>
            /// True only when Tsavorite.core was built with the <c>BUFFER_POOL_STATS</c> symbol
            /// (<c>-p:BufferPoolStats=true</c>); when false the <c>Get</c> path records nothing and
            /// <see cref="Enabled"/> has no effect. Consumers should warn if they requested stats but this is false.
            /// A non-const <c>readonly</c> so a consumer's <c>if (!Compiled)</c> guard is not flagged as
            /// unreachable code in the compiled-in build.
            /// </summary>
#if BUFFER_POOL_STATS
            public static readonly bool Compiled = true;
#else
            public static readonly bool Compiled = false;
#endif

            /// <summary>When true (and <see cref="Compiled"/>), every <c>Get</c> records a per-size-class allocation or reuse.</summary>
            public static bool Enabled;

            internal static readonly long[] classAllocs = new long[NumClasses];
            internal static readonly long[] classReuses = new long[NumClasses];
            internal static long bypassAllocs;

            /// <summary>Number of pooled size classes (the length of the snapshot arrays).</summary>
            public static int NumSizeClasses => NumClasses;

            /// <summary>Capacity, in sectors, of a pooled size class (multiply by the pool's sector size for bytes).</summary>
            public static int ClassCapacitySectors(int cls) => SectorAlignedBufferPool.ClassCapacitySectors(cls);

            /// <summary>New buffers allocated for requests above the pooled ceiling (or while the pool is disabled).</summary>
            public static long BypassAllocs => Interlocked.Read(ref bypassAllocs);

            /// <summary>Zero all counters (call at the start of a measurement window).</summary>
            public static void Reset()
            {
                Array.Clear(classAllocs);
                Array.Clear(classReuses);
                Interlocked.Exchange(ref bypassAllocs, 0);
            }

            /// <summary>Snapshot of per-class new-buffer allocation counts.</summary>
            public static long[] SnapshotAllocs() => (long[])classAllocs.Clone();

            /// <summary>Snapshot of per-class cache-reuse counts (local + cross-thread + depot hits).</summary>
            public static long[] SnapshotReuses() => (long[])classReuses.Clone();
        }

        [Conditional("BUFFER_POOL_STATS")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void RecordReuse(int cls)
        {
            if (Stats.Enabled)
                Interlocked.Increment(ref Stats.classReuses[cls]);
        }

        [Conditional("BUFFER_POOL_STATS")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void RecordAlloc(int cls)
        {
            if (Stats.Enabled)
                Interlocked.Increment(ref Stats.classAllocs[cls]);
        }

        [Conditional("BUFFER_POOL_STATS")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void RecordBypassAlloc()
        {
            if (Stats.Enabled)
                Interlocked.Increment(ref Stats.bypassAllocs);
        }

        /// <summary>Bytes currently reserved against the pool's budget (0 at full quiesce). Test-only.</summary>
        internal long ReservedBytes => (smallBudget?.Used ?? 0) + (largeBudget?.Used ?? 0);
        /// <summary>Bytes reserved against the small-class sub-budget. Test-only.</summary>
        internal long SmallReservedBytes => smallBudget?.Used ?? 0;
        /// <summary>Bytes reserved against the large-class sub-budget. Test-only.</summary>
        internal long LargeReservedBytes => largeBudget?.Used ?? 0;
        /// <summary>First size-class index that draws from the large sub-budget. Test-only.</summary>
        internal int FirstLargeClass => firstLargeClass;
        /// <summary>Per-thread ceiling on locally-retained small-class bytes. Test-only.</summary>
        internal long ThreadLocalByteCap => threadLocalByteCap;
        /// <summary>Bytes the calling thread holds on all of its owner-local chains. Test-only.</summary>
        internal long CallerLocalBytes => CallerShard()?.localBytes ?? 0;
        /// <summary>Bytes the calling thread holds on its owner-local chain for <paramref name="cls"/>. Test-only.</summary>
        internal long CallerLocalBytesForClass(int cls) => CallerShard()?.buckets[cls].localBytes ?? 0;

        /// <summary>The calling thread's shard for this pool, or null if it has none. Test-only.</summary>
        private ThreadShard CallerShard()
        {
            var arr = t_shards;
            var slot = slotIndex;
            if (arr is not null && slot < arr.Length)
            {
                var s = arr[slot];
                if (s is not null && ReferenceEquals(s.pool, this))
                    return s;
            }
            return null;
        }
        /// <summary>Total managed buffer allocations served by this pool (reuse-efficiency measure). Test-only.</summary>
        internal long TotalManagedAllocations => Interlocked.Read(ref totalManagedAllocations);
        /// <summary>Number of live shards registered with this pool. Test-only.</summary>
        internal int LiveShardCount
        {
            get
            {
                if (poolShardRegistry is null)
                    return 0;
                var n = 0;
                lock (poolShardRegistryLock)
                    foreach (var wr in poolShardRegistry)
                        if (wr.TryGetTarget(out _))
                            n++;
                return n;
            }
        }

        /// <summary>Current length of the calling thread's shard-slot array. Test-only (bounded-growth check).</summary>
        internal static int ThreadShardArrayLength => t_shards?.Length ?? 0;

        // ---- Size-class ladder test hooks ----------------------------------------------------------------------
        internal static int TestClassOfSectors(int sectors) => ClassOfSectors(sectors);
        internal static int TestClassCapacitySectors(int cls) => ClassCapacitySectors(cls);
        internal static int TestNumClasses => NumClasses;
        internal static int TestMaxPooledSectors => MaxPooledSectors;
        internal static int TestLinearTopSectors => LinearTopSectors;
        internal static int TestLinearStrideSectors => LinearStrideSectors;

        /// <summary>
        /// Byte offset between a <see cref="Bucket"/>'s owner-hot <c>localHead</c> and the producer-hot
        /// <c>crossThreadHead</c>. Test-only regression guard for the explicit cache-line separation
        /// (must stay >= 64 so the two heads never false-share).
        /// </summary>
        internal static nint TestBucketHeadCacheLineOffset()
        {
            var b = new Bucket();
            return Unsafe.ByteOffset(
                ref Unsafe.As<SectorAlignedMemory, byte>(ref b.localHead),
                ref Unsafe.As<SectorAlignedMemory, byte>(ref b.crossThreadHead));
        }
    }
}