// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
#define CHECK_FREE      // disabled by default in Release due to overhead
#endif
// #define CHECK_FOR_LEAKS // disabled by default due to overhead

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>
    /// Sector aligned memory allocator
    /// </summary>
    public sealed unsafe class SectorAlignedMemory
    {
        // Byte #31 is used to denote free (1) or in-use (0) page
        const int kFreeBitMask = 1 << 31;

        /// <summary>
        /// Actual buffer
        /// </summary>
        public byte[] buffer;

        /// <summary>
        /// Handle
        /// </summary>
        internal GCHandle handle;

        /// <summary>
        /// Offset for initial allocation alignment of the block; this is the offset from the first element of <see cref="buffer"/> to form <see cref="aligned_pointer"/>.
        /// This alignment is internal to <see cref="SectorAlignedMemory"/>, and ensures that callers see an aligned starting address.
        /// </summary>
        public int aligned_offset;

        /// <summary>
        /// Aligned pointer; initial allocation (the first element of <see cref="buffer"/>) plus <see cref="aligned_offset"/>
        /// This alignment is internal to <see cref="SectorAlignedMemory"/>, and ensures that callers see an aligned starting address.
        /// </summary>
        public byte* aligned_pointer;

        /// <summary>
        /// Valid offset for operations above <see cref="aligned_pointer"/>, to get their own desired alignment relative to our aligned starting address.
        /// This is set by the caller for operations such as file reading, which rounds down to the nearest sector size; this is the amount of that rounding down.
        /// Used by <see cref="GetValidPointer()"/>, which is <see cref="aligned_pointer"/> + <see cref="valid_offset"/>.
        /// </summary>
        public int valid_offset;

        /// <summary>
        /// Required (requested) bytes for the current operation: the unaligned number of bytes to read. There will always be at least this much usable space in the allocation.
        /// Use this when the original request size is needed.
        /// </summary>
        public int required_bytes;

        /// <summary>
        /// Available bytes after the operation is complete: the number of bytes actually read, e.g. aligned number of bytes requested. See <see cref="GetValidPointer()"/>.
        /// Use this to see if there are additional bytes over the original request (see <see cref="required_bytes"/>.
        /// </summary>
        public int available_bytes;

        /// <summary>
        /// Per-rental clear-on-return policy. The current renter set this at
        /// <see cref="SectorAlignedBufferPool.Get(int,bool)"/> time and the pool consults
        /// it on the matching <see cref="SectorAlignedBufferPool.Return(SectorAlignedMemory)"/>.
        /// Default <c>true</c> (the safe choice for any caller that may stage a partial
        /// write — the cleared tail forms the zero padding the device writes to disk).
        /// <para>
        /// Pass <c>false</c> only when the renter will fully overwrite the buffer's read
        /// region (e.g., O_DIRECT device-read destinations). Saves the per-Return memory
        /// bandwidth cost (~4-8 KB per pending read) that dominates the CPU profile on
        /// disk-bound benchmarks.
        /// </para>
        /// </summary>
        public bool clearOnReturn = true;

        /// <summary>
        /// Pool-internal: true when the buffer's tail (beyond <see cref="valid_offset"/> +
        /// <see cref="available_bytes"/>) MAY contain non-zero bytes from a previous rental
        /// that opted out of <see cref="clearOnReturn"/>. Cleared to false when the pool
        /// zeroes the buffer (either on the matching Return or lazily on the next default
        /// Get). Set to true on Return when the renter opted out.
        /// <para>
        /// Necessary because the buffer-pool slots are fungible: a buffer last rented by a
        /// device-read destination (opted out) may be dequeued next by a write-staging
        /// caller (default, expects zero tail). The lazy clear on default Get preserves the
        /// historical "Get returns a zero buffer" contract while still letting the read
        /// path skip the per-Return clear.
        /// </para>
        /// </summary>
        internal bool isDirty;

        /// <summary>
        /// Intrusive singly-linked list link. A buffer is in <b>exactly one</b> list at a time, so a single link
        /// serves every list it may join: the origin-return owner-local stack, the cross-thread return stack,
        /// and the pool depot. Cleared before a buffer is handed to a renter so a long-lived rental never roots a chain.
        /// </summary>
        internal SectorAlignedMemory next;

        /// <summary>
        /// Origin-return owner: the exact <c>(pool, thread, size-class)</c> bucket that owns this buffer's
        /// <b>current</b> rental. Set at <see cref="SectorAlignedBufferPool.Get(int,bool)"/>; the matching
        /// cross-thread <see cref="SectorAlignedBufferPool.Return(SectorAlignedMemory)"/> routes the buffer back
        /// to this bucket's owner thread rather than to the freeing thread.
        /// Cleared when the buffer migrates into the pool depot (so a depot entry never roots a retired shard).
        /// </summary>
        internal Bucket originBucket;

        /// <summary>
        /// Standalone byte-budget accounting object this buffer's poolability permit was reserved against.
        /// Held directly (not via <see cref="pool"/>) so the permit can be released even after the pool is
        /// closed (e.g. from a retired shard's finalizer). Null for non-cacheable / bypass buffers.
        /// </summary>
        internal BudgetState budget;

        /// <summary>
        /// Poolability-permit size in bytes reserved against <see cref="budget"/> when this buffer was first
        /// designated cacheable (at allocation). Persists unchanged while the buffer cycles through
        /// rented → local → cross-thread → depot states; released exactly once (guarded by
        /// <see cref="permitReleased"/>) when the buffer is permanently dropped. 0 when non-cacheable.
        /// </summary>
        internal long permitBytes;

        /// <summary>Exactly-once release guard for <see cref="permitBytes"/> (CAS 0 → 1). </summary>
        internal int permitReleased;

        /// <summary>True when this buffer holds a poolability permit and may re-enter the pool's caches on
        /// Return; false for bypass/overflow buffers that are dropped on Return.</summary>
        internal bool cacheable;

        private int level;
        internal int Level => level
#if CHECK_FREE
            & ~kFreeBitMask
#endif
            ;

        internal SectorAlignedBufferPool pool;

#if CHECK_FREE
        internal bool Free
        {
            get => (level & kFreeBitMask) != 0;
            set
            {
                if (value)
                {
                    if (Free)
                        throw new TsavoriteException("Attempting to return an already-free block");
                    this.level |= kFreeBitMask;
                }
                else
                {
                    if (!Free)
                        throw new TsavoriteException("Attempting to allocate an already-allocated block");
                    this.level &= ~kFreeBitMask;
                }
            }
        }
#endif // CHECK_FREE

        /// <summary>
        /// Default constructor
        /// </summary>
        public SectorAlignedMemory(int level = default)
        {
            this.level = level;
            // Assume ctor is called for allocation and leave Free unset
        }

        /// <summary>
        /// Create new instance of SectorAlignedMemory
        /// </summary>
        /// <param name="numRecords"></param>
        /// <param name="sectorSize"></param>
        public SectorAlignedMemory(int numRecords, int sectorSize)
        {
            const int recordSize = 1;
            required_bytes = numRecords * recordSize;
            int requiredSize = sectorSize + RoundUp(required_bytes, sectorSize);    // An additional sector size for the aligned_offset

            buffer = GC.AllocateArray<byte>(requiredSize, true);
            long bufferAddr = (long)Unsafe.AsPointer(ref buffer[0]);
            aligned_pointer = (byte*)((bufferAddr + (sectorSize - 1)) & ~((long)sectorSize - 1));
            aligned_offset = (int)((long)aligned_pointer - bufferAddr);
            // Assume ctor is called for allocation and leave Free unset
        }

        public unsafe (byte[] array, long offset) GetArrayAndUnalignedOffset(long alignedOffset)
        {
            long ptr = (long)Unsafe.AsPointer(ref buffer[0]);
            return (buffer, alignedOffset + ptr - (long)aligned_pointer);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            buffer = null;
#if CHECK_FREE
            this.Free = true;
#endif
        }

        /// <summary>
        /// Return
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return()
        {
            pool?.Return(this);
        }

        /// <summary>
        /// Get the total aligned memory capacity of the buffer
        /// </summary>
        public int AlignedTotalCapacity => buffer.Length - aligned_offset;

        /// <summary>
        /// Get the total valid memory capacity of the buffer
        /// </summary>
        public int ValidTotalCapacity => AlignedTotalCapacity - valid_offset;

        /// <summary>
        /// Get the total valid required (requested) capacity of the buffer
        /// </summary>
        public int RequiredCapacity => required_bytes - valid_offset;

        /// <summary>
        /// Get valid pointer (accounts for aligned padding plus any offset specified for the valid start of data)
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* GetValidPointer() => aligned_pointer + valid_offset;

        /// <summary>
        /// Get Span of entire allocated space after the valid pointer
        /// </summary>
        public Span<byte> TotalValidSpan => new(GetValidPointer(), ValidTotalCapacity);

        /// <summary>
        /// Get Span of entire allocated space after the aligned pointer (see <see cref="available_bytes"/>).
        /// </summary>
        public Span<byte> AvailableSpan => new(aligned_pointer, available_bytes);

        /// <summary>
        /// Get Span of entire allocated space after the valid pointer (see <see cref="valid_offset"/>).
        /// </summary>
        public Span<byte> AvailableValidSpan => new(GetValidPointer(), available_bytes - valid_offset);

        /// <summary>
        /// Returns the Span of requested space (see <see cref="required_bytes"/>).
        /// </summary>
        public Span<byte> RequiredValidSpan => new(GetValidPointer(), RequiredCapacity);

        /// <summary>
        /// ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format($"aligned: [offset {aligned_offset}, ptr {(long)aligned_pointer} = 0x{(long)aligned_pointer:X}];" +
                $" valid: [offset {valid_offset} ptr {(long)GetValidPointer()} = 0x{(long)GetValidPointer():X}];" +
                $" reqBytes {required_bytes}; availBytes {available_bytes}; cap {AlignedTotalCapacity}"
#if CHECK_FREE
                + $"; free {Free}"
#endif
                );
        }
    }

    /// <summary>
    /// A pool of sector-aligned memory buffers.
    /// <para>
    /// The default managed backend is <b>origin-return</b> and per-thread: a buffer is parked on the thread that
    /// <see cref="Get(int,bool)"/>s it, and a <see cref="Return(SectorAlignedMemory)"/> from a different
    /// (IO-completion) thread routes the buffer back to its originating thread rather than parking it on the
    /// freeing thread. Per-thread parking gives each thread contention-free reuse of its own buffers. Routing
    /// cross-thread returns back to origin is what keeps that parking from bloating: IO-completion threads free
    /// far more buffers than they allocate, so parking on the freeing thread would strand buffers there (never
    /// reused) while issuing threads keep allocating fresh. A per-pool byte budget caps the total retained bytes.
    /// Setting <see cref="UseOriginReturn"/> = false at startup routes the pool to a <see cref="LegacyBufferPool"/>
    /// instead: an array of shared queues (no per-thread parking) that is simpler but serializes every Get/Return
    /// on one queue's head per size class, so it does not scale with thread count.
    /// </para>
    /// </summary>
    public sealed partial class SectorAlignedBufferPool
    {
        /// <summary>
        /// Disable buffer pool.
        /// This static option should be enabled on program entry, and not modified once Tsavorite is instantiated.
        /// </summary>
        public static bool Disabled;

        /// <summary>
        /// Unpin objects when they are returned to the pool, so that we do not hold pinned objects long term.
        /// If set, we will unpin when objects are returned and re-pin when objects are returned from the pool.
        /// This static option should be enabled on program entry, and not modified once Tsavorite is instantiated.
        /// </summary>
        public static bool UnpinOnReturn;

        /// <summary>
        /// Selects the origin-return managed backend (default). When false, the pool routes to a
        /// <see cref="LegacyBufferPool"/>. Captured once per pool at construction (so a toggled static can
        /// never mix modes within a pool). Set at program entry only, mirroring <see cref="Disabled"/> /
        /// <see cref="UnpinOnReturn"/>.
        /// </summary>
        public static bool UseOriginReturn = true;

        /// <summary>
        /// Per-pool byte budget for the origin-return backend: the single hard bound on the total
        /// <b>reusable</b> (cacheable) bytes the pool will retain across all threads/classes. Captured once per
        /// pool at construction. An allocation that cannot reserve a permit against this budget is still served
        /// to the caller but marked non-cacheable and dropped on Return. Default 1 GiB per pool.
        /// </summary>
        public static long ManagedBudgetBytes = 1L << 30;

        /// <summary>Non-null only in legacy mode (<see cref="UseOriginReturn"/> = false); Get/Return/Free/Print
        /// route to it. Null selects the origin-return path. Captured once at construction.</summary>
        private readonly LegacyBufferPool legacy;

        /// <summary>Per-pool captured unpin-on-return policy (see <see cref="UnpinOnReturn"/>). Read once at
        /// construction so a buffer allocated under one pin policy is always returned under that policy.</summary>
        private readonly bool unpinOnReturn;

        private readonly int recordSize;
        private readonly int sectorSize;
        private readonly int sectorSizeShift;   // Log2(sectorSize) if a power of two, else -1 (fall back to division)
#if CHECK_FOR_LEAKS
        static int totalGets, totalReturns;
#endif

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="recordSize">Record size. May be 1 if allocations of different lengths will be made</param>
        /// <param name="sectorSize">Sector size, e.g. from log device</param>
        public SectorAlignedBufferPool(int recordSize, int sectorSize)
        {
            this.recordSize = recordSize;
            this.sectorSize = sectorSize;
            sectorSizeShift = BitOperations.IsPow2((uint)sectorSize) ? BitOperations.Log2((uint)sectorSize) : -1;

            // Capture the managed-mode and pin policy once so Get/Return route from immutable per-pool state:
            // a buffer allocated under one mode/pin-policy is always returned under that same mode.
            unpinOnReturn = UnpinOnReturn;

            if (UseOriginReturn)
                InitOriginReturn();
            else
                legacy = new LegacyBufferPool(recordSize, sectorSize, this, unpinOnReturn);
        }

        public void EnsureSize(ref SectorAlignedMemory page, int size)
        {
            if (page is null)
            {
                page = Get(size);
                return;
            }
            if (page.AlignedTotalCapacity < size)
            {
                page.Return();
                page = Get(size);
                return;
            }

            // Reusing the page, so ensure this is set correctly.
            page.required_bytes = size;
        }

        /// <summary>
        /// Return a <see cref="SectorAlignedMemory"/> to the pool. Zeros the backing
        /// buffer if <see cref="SectorAlignedMemory.clearOnReturn"/> is true (default);
        /// callers that rented via <see cref="Get(int,bool)"/> with
        /// <c>clearOnReturn: false</c> opt out of the per-Return zeroing. When opted
        /// out, the buffer is enqueued in a dirty state and the lazy clear is deferred
        /// to the next default <see cref="Get(int)"/> that dequeues it (preserving the
        /// historical "Get returns a zero buffer" contract for write-staging callers).
        /// </summary>
        /// <param name="page"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(SectorAlignedMemory page)
        {
#if CHECK_FOR_LEAKS
            System.Threading.Interlocked.Increment(ref totalReturns);
#endif

            if (legacy is not null)
            {
                legacy.Return(page);
                return;
            }

            ReturnOriginReturn(page);
        }

        /// <summary>
        /// Get buffer. Preserves the historical contract that the returned buffer is
        /// fully zeroed; lazy-clears the tail if the slot is dirty from a prior
        /// opted-out rental.
        /// </summary>
        /// <param name="numRecords"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SectorAlignedMemory Get(int numRecords) => Get(numRecords, clearOnReturn: true);

        /// <summary>
        /// Get buffer with an explicit <paramref name="clearOnReturn"/> policy that overrides
        /// the default (<c>true</c>). Pass <c>false</c> only when the caller will fully
        /// overwrite the buffer's read region (e.g., O_DIRECT device-read destinations) and
        /// no downstream consumer relies on the buffer's tail being zero-padded.
        /// <para>
        /// When <paramref name="clearOnReturn"/> is <c>true</c> (the default) and the
        /// dequeued slot is dirty from a prior opt-out, the buffer is zeroed before being
        /// handed to the caller — so the "Get returns a zero buffer" contract is preserved
        /// regardless of which previous renter handed it back.
        /// </para>
        /// </summary>
        /// <param name="numRecords"></param>
        /// <param name="clearOnReturn">Per-rent clear-on-return policy; carried by the
        /// returned <see cref="SectorAlignedMemory"/> and consulted on the next
        /// <see cref="Return(SectorAlignedMemory)"/>.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe SectorAlignedMemory Get(int numRecords, bool clearOnReturn)
        {
#if CHECK_FOR_LEAKS
            System.Threading.Interlocked.Increment(ref totalGets);
#endif

            if (legacy is not null)
                return legacy.Get(numRecords, clearOnReturn);

            int required_bytes = numRecords * recordSize;
            int requiredSize = RoundUp(required_bytes, sectorSize);
            return GetOriginReturn(required_bytes, requiredSize, clearOnReturn);
        }

        /// <summary>
        /// Free buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Free()
        {
#if CHECK_FOR_LEAKS
            System.Diagnostics.Debug.Assert(totalGets == totalReturns);
#endif
            if (legacy is not null)
            {
                legacy.Free();
                return;
            }

            FreeOriginReturn();
        }

        /// <summary>
        /// Print pool contents
        /// </summary>
        public void Print()
        {
            if (legacy is not null)
            {
                legacy.Print();
                return;
            }

            PrintOriginReturn();
        }
    }
}