// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Per-request descriptor carried through the in-memory IO path. Value-type so it can be
    /// stored in <see cref="SpscRing{T}"/> without heap allocation per request.
    /// </summary>
    internal unsafe struct IORequestLocalMemory
    {
        public void* srcAddress;
        public void* dstAddress;
        public uint bytes;
        public DeviceIOCompletionCallback callback;
        public object context;
        public long startTimestamp;     // Stopwatch ticks at enqueue, used for latency simulation.
    }

    /// <summary>
    /// In-process device backed by pinned RAM segments. Models a "fast IO device" without
    /// any kernel-syscall cost — submitters post requests to a per-thread SPSC ring, and a
    /// pool of dedicated IO processor threads drain rings and fire completion callbacks
    /// asynchronously (the same async-dispatch model used by io_uring per-thread rings and
    /// SPDK per-CPU NVMe queue pairs).
    ///
    /// Designed for unit tests and benchmarks that need to characterize the upper bound of
    /// Tsavorite/Garnet throughput without paying real disk cost.
    /// </summary>
    public sealed unsafe class LocalMemoryDevice : StorageDeviceBase
    {
        // Per-segment storage. segmentArrays[i] is null until first written; volatile-set in EnsureSegment.
        private readonly byte[][] segmentArrays;
        private readonly byte*[] segmentPtrs;
        private readonly int maxSegments;
        private readonly long sz_segment;

        // Latency simulation (Stopwatch ticks, NOT DateTime ticks).
        private readonly long latencyTimestampTicks;
        private readonly long oneMsTicks;
        private readonly bool latencyEnabled;

        // IO processor threads — one per submission ring. Each processor drains exactly
        // one ring (SPSC consumer side).
        private readonly int parallelism;
        private readonly Thread[] processors;
        private readonly SpscRing<IORequestLocalMemory>[] rings;

        // Per-submitter-thread ring routing. Each submitter thread caches a ring index on
        // first use. With per-thread routing each ring sees a single producer in the common
        // case (#submitter threads <= parallelism), making the ring effectively SPSC. When
        // more producers share a ring, SpscRing.Enqueue handles them natively via fetch-and-add
        // on its tail counter — no per-ring lock needed.
        //
        // The cache is [ThreadStatic] (one slot per thread, process-wide), so a thread that
        // submitted to another LocalMemoryDevice instance may carry that instance's index here.
        // Enqueue therefore validates the cached index against THIS instance's parallelism and
        // reassigns when it is out of range — a single unsigned compare on the hot path.
        [ThreadStatic]
        private static int t_ringIdxPlusOne;
        private int nextRingIdx;

        // Sentinel stored in t_ringIdxPlusOne on a processor (drain) thread (see ProcessorLoop). A
        // re-entrant Enqueue from within a completion callback on that thread then executes inline
        // rather than posting to the bounded ring only this thread drains — see Enqueue. Distinct from
        // "unset" (0) and any valid idx+1 (>= 1), so it is recognized with the same single TLS read.
        private const int ProcessorThreadRingIdx = int.MinValue;

        // When capacity is unspecified, cap the segment-pointer arrays at this many segments
        // (≈ 32 KB of overhead per array at 4096 entries). Segments themselves are still lazily
        // allocated, so this bounds the upper limit of data that can be stored, not the working
        // set. With the default 1 GB sz_segment this admits 4 TB of virtual address space.
        private const int DefaultMaxSegments = 4096;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="capacity">Maximum number of bytes this device can accommodate. Pass <see cref="Devices.CAPACITY_UNSPECIFIED"/> or any value &lt;= 0 to default to a large bounded capacity of <c>sz_segment * DefaultMaxSegments</c> (segments are still allocated lazily, so this only sizes the per-segment lookup arrays). When &gt; 0 it must be a multiple of <paramref name="sz_segment"/>.</param>
        /// <param name="sz_segment">Size in bytes of each segment.</param>
        /// <param name="parallelism">Number of dedicated IO processor threads (and rings). Must be &gt;= 1.</param>
        /// <param name="latencyUs">Per-IO simulated wall-clock latency in microseconds (0 = none). Microsecond
        /// granularity models modern low-latency devices (single-digit microsecond NVMe).</param>
        /// <param name="sector_size">Sector size for device (default 512).</param>
        /// <param name="ringCapacity">Per-ring slot count (power of two). Defaults to 4096.</param>
        /// <param name="fileName">Virtual path used as the device identifier.</param>
        public LocalMemoryDevice(long capacity, long sz_segment, int parallelism, int latencyUs = 0, uint sector_size = 512, int ringCapacity = 4096, string fileName = "/userspace/ram/storage")
            : base(fileName, sector_size, capacity > 0 ? capacity : checked(sz_segment * DefaultMaxSegments))
        {
            if (sz_segment <= 0) throw new ArgumentOutOfRangeException(nameof(sz_segment), "sz_segment must be > 0");
            if (sz_segment > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(sz_segment), "sz_segment must be <= int.MaxValue");
            if (parallelism < 1) throw new ArgumentOutOfRangeException(nameof(parallelism), "parallelism must be >= 1");
            if (latencyUs < 0) throw new ArgumentOutOfRangeException(nameof(latencyUs), "latencyUs must be >= 0");
            if (ringCapacity <= 0 || (ringCapacity & (ringCapacity - 1)) != 0)
                throw new ArgumentOutOfRangeException(nameof(ringCapacity), "ringCapacity must be a positive power of two");
            if (capacity > 0 && capacity % sz_segment != 0)
                throw new ArgumentException("capacity must be a multiple of sz_segment", nameof(capacity));

            // base.Capacity already holds the effective capacity (the caller's value when > 0, else the
            // lazily-backed sz_segment * DefaultMaxSegments default passed to the base ctor above).
            this.sz_segment = sz_segment;
            maxSegments = checked((int)(Capacity / sz_segment));
            this.parallelism = parallelism;

            latencyTimestampTicks = latencyUs > 0
                ? (long)((double)latencyUs * Stopwatch.Frequency / 1_000_000.0)
                : 0;
            latencyEnabled = latencyTimestampTicks > 0;
            oneMsTicks = Stopwatch.Frequency / 1000;

            segmentArrays = new byte[maxSegments][];
            segmentPtrs = new byte*[maxSegments];

            rings = new SpscRing<IORequestLocalMemory>[parallelism];
            processors = new Thread[parallelism];
            for (int i = 0; i < parallelism; i++)
            {
                rings[i] = new SpscRing<IORequestLocalMemory>(ringCapacity);
                int local = i;
                processors[i] = new Thread(() => ProcessorLoop(rings[local]))
                {
                    IsBackground = true,
                    Name = $"TsavoriteLocalMemIO-{local}",
                };
                processors[i].Start();
            }

            Debug.WriteLine($"LocalMemoryDevice: capacity={Capacity} segments={maxSegments} segSize={sz_segment} parallelism={parallelism} latencyUs={latencyUs} ringCapacity={ringCapacity}");
        }

        /// <summary>
        /// Lazily allocates the backing pinned array for <paramref name="segmentId"/>.
        /// Reads of unwritten segments are not permitted (caller must write first).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureSegment(int segmentId)
        {
            if ((uint)segmentId >= (uint)maxSegments)
                throw new ArgumentOutOfRangeException(nameof(segmentId), $"segmentId {segmentId} >= maxSegments {maxSegments} (capacity exhausted)");
            if (Volatile.Read(ref segmentArrays[segmentId]) != null) return;
            EnsureSegmentSlow(segmentId);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void EnsureSegmentSlow(int segmentId)
        {
            lock (segmentArrays)
            {
                if (segmentArrays[segmentId] != null) return;
                var arr = GC.AllocateArray<byte>((int)sz_segment, pinned: true);
                segmentPtrs[segmentId] = (byte*)Unsafe.AsPointer(ref arr[0]);
                Volatile.Write(ref segmentArrays[segmentId], arr);
            }
        }

        /// <inheritdoc />
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
        {
            if ((uint)segmentId >= (uint)maxSegments)
            {
                callback(2, 0, context); // ENOENT
                return;
            }
            var arr = Volatile.Read(ref segmentArrays[segmentId]);
            if (arr == null)
            {
                callback(2, 0, context);
                return;
            }
            if ((long)sourceAddress + readLength > sz_segment)
            {
                callback(22, 0, context); // EINVAL
                return;
            }
            Enqueue(segmentPtrs[segmentId] + sourceAddress, (void*)destinationAddress, readLength, callback, context);
        }

        /// <inheritdoc />
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            if ((uint)segmentId >= (uint)maxSegments)
                throw new ArgumentOutOfRangeException(nameof(segmentId), $"segmentId {segmentId} >= maxSegments {maxSegments} (capacity exhausted)");
            if ((long)destinationAddress + numBytesToWrite > sz_segment)
                throw new ArgumentException("write extends past segment boundary", nameof(numBytesToWrite));

            EnsureSegment(segmentId);
            HandleCapacity(segmentId + 1);

            Enqueue((void*)sourceAddress, segmentPtrs[segmentId] + destinationAddress, numBytesToWrite, callback, context);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Enqueue(void* src, void* dst, uint bytes, DeviceIOCompletionCallback callback, object context)
        {
            int idxPlusOne = t_ringIdxPlusOne;

            // Re-entrant submit from a completion callback running on a drain thread (e.g. Tsavorite's
            // synchronous flush-chain WriteAsync invoked from req.callback). Execute inline rather than
            // posting to the bounded ring this same thread drains — otherwise, if the ring is full, the
            // thread would spin forever in WaitForSlotEmpty waiting for itself to drain (self-deadlock).
            // No latency model on this rare internal path; exceptions propagate to the re-entrant caller.
            if (idxPlusOne == ProcessorThreadRingIdx)
            {
                Buffer.MemoryCopy(src, dst, bytes, bytes);
                callback(0, bytes, context);
                return;
            }

            // Validate against THIS instance's parallelism: a single unsigned compare catches both
            // the unset slot (-1 after the decrement) and a stale index cached while submitting to
            // another instance with larger parallelism. Either way we (re)assign a valid ring index.
            int idx = idxPlusOne - 1;
            if ((uint)idx >= (uint)parallelism)
                idx = AssignRingIdx();

            var req = new IORequestLocalMemory
            {
                srcAddress = src,
                dstAddress = dst,
                bytes = bytes,
                callback = callback,
                context = context,
                startTimestamp = latencyEnabled ? Stopwatch.GetTimestamp() : 0,
            };

            // SpscRing.Enqueue is MPSC-safe natively via fetch-and-add on its tail
            // counter — no per-ring lock needed even when ThreadPool flush threads
            // collide with worker threads on the same ring.
            rings[idx].Enqueue(req);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private int AssignRingIdx()
        {
            int seq = Interlocked.Increment(ref nextRingIdx) - 1;
            int idx = (int)((uint)seq % (uint)parallelism);
            t_ringIdxPlusOne = idx + 1;
            return idx;
        }

        private void ProcessorLoop(SpscRing<IORequestLocalMemory> ring)
        {
            // Mark this as a drain thread: a re-entrant Enqueue from within a completion callback below
            // executes inline instead of blocking on a ring only this thread can drain (see Enqueue).
            t_ringIdxPlusOne = ProcessorThreadRingIdx;

            while (!ring.IsStopped)
            {
                if (ring.TryDequeue(out var req))
                {
                    if (latencyEnabled)
                    {
                        // Release the IO at startTimestamp + latency. Sleep only while more than ~1ms
                        // remains (Thread.Sleep is too coarse for finer waits); busy-spin the sub-millisecond
                        // remainder so microsecond-scale latencies release close to on time.
                        long deadline = req.startTimestamp + latencyTimestampTicks;
                        while (true)
                        {
                            long remainingTicks = deadline - Stopwatch.GetTimestamp();
                            if (remainingTicks <= 0)
                                break;
                            if (remainingTicks > oneMsTicks)
                                Thread.Sleep(1);
                            else
                                Thread.SpinWait(16);
                        }
                    }

                    Buffer.MemoryCopy(req.srcAddress, req.dstAddress, req.bytes, req.bytes);
                    // Guard the callback inline (no helper — a try/catch blocks JIT inlining, so factoring
                    // this would add a call on the hot drain path). A faulty callback must not kill the
                    // dedicated drain thread: that would wedge the bounded ring (producers block forever).
                    try { req.callback(0, req.bytes, req.context); }
                    catch (Exception ex) { Debug.WriteLine($"LocalMemoryDevice completion callback threw: {ex}"); }
                }
                else
                {
                    // Brief spin to catch back-to-back work, then park on the ring's
                    // signal until the producer's next Enqueue wakes us, or Stop is
                    // called. WaitForWork reads ring._stopped directly (single L1 read,
                    // no virtual call) so shutdown is observed immediately.
                    ring.WaitForWork();
                }
            }
            // Drain remainder after Stop.
            while (ring.TryDequeue(out var req))
            {
                Buffer.MemoryCopy(req.srcAddress, req.dstAddress, req.bytes, req.bytes);
                try { req.callback(0, req.bytes, req.context); }
                catch (Exception ex) { Debug.WriteLine($"LocalMemoryDevice completion callback threw: {ex}"); }
            }
        }

        /// <inheritdoc />
        /// <remarks>
        /// Precondition: no IO request targeting <paramref name="segment"/> may be in flight. The backing
        /// array is pinned (no move) but nulling the last reference here makes it collectable, and a queued
        /// request holds only a raw pointer into it. Callers satisfy this because truncation only removes head
        /// segments below the begin address while IO targets the tail; a removed segment therefore has no
        /// outstanding requests.
        /// </remarks>
        public override void RemoveSegment(int segment)
        {
            if ((uint)segment >= (uint)maxSegments) return;
            lock (segmentArrays)
            {
                segmentArrays[segment] = null;
                segmentPtrs[segment] = null;
            }
        }

        /// <inheritdoc />
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            RemoveSegment(segment);
            callback(result);
        }

        /// <inheritdoc />
        public override void Dispose()
        {
            // Stop each ring (sets the ring's own _stopped flag with proper ordering
            // and signals the event). Parked processors observe _stopped on their
            // post-fence recheck and exit WaitForWork without blocking.
            for (int i = 0; i < rings.Length; i++)
                rings[i].Stop();
            for (int i = 0; i < processors.Length; i++)
                processors[i].Join();
        }

        /// <summary>Total RAM committed for segments (diagnostic).</summary>
        public long GetAllocatedBytes()
        {
            long total = 0;
            for (int i = 0; i < maxSegments; i++)
                if (Volatile.Read(ref segmentArrays[i]) != null)
                    total += sz_segment;
            return total;
        }
    }
}