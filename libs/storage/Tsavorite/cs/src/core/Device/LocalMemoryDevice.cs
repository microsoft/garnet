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
        private readonly bool latencyEnabled;

        // IO processor threads — one per submission ring. Each processor drains exactly
        // one ring (SPSC consumer side).
        private readonly int parallelism;
        private readonly Thread[] processors;
        private readonly SpscRing<IORequestLocalMemory>[] rings;

        // Per-submitter-thread ring routing. Each submitter thread is permanently assigned
        // a ring index on first use, ensuring each ring sees exactly one producer when
        // (#submitter threads ≤ #rings). This is the SPSC pattern used by io_uring per-thread
        // rings and SPDK per-CPU NVMe queue pairs. When (#submitter threads > #rings),
        // multiple producers map to the same ring; in that case the ring's MPSC discipline
        // is preserved via a single per-ring spin lock taken only on enqueue.
        [ThreadStatic]
        private static int t_ringIdxPlusOne;
        private int nextRingIdx;
        private readonly object[] ringEnqueueLocks;   // null entries when not contended.
        private volatile bool ringNeedsLock;          // set true if any producer collision occurs.

        private volatile bool terminated;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="capacity">Maximum number of bytes this device can accommodate. Must be &gt; 0 and a multiple of <paramref name="sz_segment"/>.</param>
        /// <param name="sz_segment">Size in bytes of each segment.</param>
        /// <param name="parallelism">Number of dedicated IO processor threads (and rings). Must be &gt;= 1.</param>
        /// <param name="latencyMs">Per-IO simulated wall-clock latency in milliseconds.</param>
        /// <param name="sector_size">Sector size for device (default 512).</param>
        /// <param name="ringCapacity">Per-ring slot count (power of two). Defaults to 4096.</param>
        /// <param name="fileName">Virtual path used as the device identifier.</param>
        public LocalMemoryDevice(long capacity, long sz_segment, int parallelism, int latencyMs = 0, uint sector_size = 512, int ringCapacity = 4096, string fileName = "/userspace/ram/storage")
            : base(fileName, sector_size, capacity)
        {
            if (capacity == Devices.CAPACITY_UNSPECIFIED) throw new ArgumentException("LocalMemoryDevice must have a finite capacity.", nameof(capacity));
            if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity), "capacity must be > 0");
            if (sz_segment <= 0) throw new ArgumentOutOfRangeException(nameof(sz_segment), "sz_segment must be > 0");
            if (sz_segment > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(sz_segment), "sz_segment must be <= int.MaxValue");
            if (capacity % sz_segment != 0) throw new ArgumentException("capacity must be a multiple of sz_segment", nameof(capacity));
            if (parallelism < 1) throw new ArgumentOutOfRangeException(nameof(parallelism), "parallelism must be >= 1");
            if (latencyMs < 0) throw new ArgumentOutOfRangeException(nameof(latencyMs), "latencyMs must be >= 0");
            if (ringCapacity <= 0 || (ringCapacity & (ringCapacity - 1)) != 0)
                throw new ArgumentOutOfRangeException(nameof(ringCapacity), "ringCapacity must be a positive power of two");

            this.sz_segment = sz_segment;
            maxSegments = checked((int)(capacity / sz_segment));
            this.parallelism = parallelism;

            latencyTimestampTicks = latencyMs > 0
                ? (long)((double)latencyMs * Stopwatch.Frequency / 1000.0)
                : 0;
            latencyEnabled = latencyTimestampTicks > 0;

            segmentArrays = new byte[maxSegments][];
            segmentPtrs = new byte*[maxSegments];

            rings = new SpscRing<IORequestLocalMemory>[parallelism];
            ringEnqueueLocks = new object[parallelism];
            processors = new Thread[parallelism];
            for (int i = 0; i < parallelism; i++)
            {
                rings[i] = new SpscRing<IORequestLocalMemory>(ringCapacity);
                ringEnqueueLocks[i] = new object();
                int local = i;
                processors[i] = new Thread(() => ProcessorLoop(rings[local]))
                {
                    IsBackground = true,
                    Name = $"TsavoriteLocalMemIO-{local}",
                };
                processors[i].Start();
            }

            Debug.WriteLine($"LocalMemoryDevice: capacity={capacity} segments={maxSegments} segSize={sz_segment} parallelism={parallelism} latencyMs={latencyMs} ringCapacity={ringCapacity}");
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
            int idx = t_ringIdxPlusOne - 1;
            if (idx < 0)
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

            if (!ringNeedsLock)
            {
                // Fast path: pure SPSC. Each thread is uniquely assigned a ring index
                // (round-robin via Interlocked.Increment on AssignRingIdx), so when
                // #submitter threads <= parallelism, each ring sees exactly one producer.
                rings[idx].Enqueue(req);
            }
            else
            {
                // Slow path: more producers than rings. Hold a per-ring lock to keep the
                // SPSC ring's producer-side invariant (single concurrent producer) intact.
                lock (ringEnqueueLocks[idx]) rings[idx].Enqueue(req);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private int AssignRingIdx()
        {
            int seq = Interlocked.Increment(ref nextRingIdx) - 1;
            int idx = (int)((uint)seq % (uint)parallelism);
            t_ringIdxPlusOne = idx + 1;
            // If round-robin assignment has wrapped past parallelism, multiple submitters
            // now share a ring — switch to the MPSC-via-lock slow path.
            if (seq >= parallelism)
                ringNeedsLock = true;
            return idx;
        }

        private void ProcessorLoop(SpscRing<IORequestLocalMemory> ring)
        {
            var spinner = new SpinWait();
            while (!terminated)
            {
                if (ring.TryDequeue(out var req))
                {
                    spinner.Reset();

                    if (latencyEnabled)
                    {
                        long deadline = req.startTimestamp + latencyTimestampTicks;
                        long now = Stopwatch.GetTimestamp();
                        while (now < deadline)
                        {
                            long remainingTicks = deadline - now;
                            long remainingMs = remainingTicks * 1000 / Stopwatch.Frequency;
                            if (remainingMs >= 2)
                                Thread.Sleep((int)remainingMs - 1);
                            else
                                Thread.SpinWait(64);
                            now = Stopwatch.GetTimestamp();
                        }
                    }

                    Buffer.MemoryCopy(req.srcAddress, req.dstAddress, req.bytes, req.bytes);
                    req.callback(0, req.bytes, req.context);
                }
                else
                {
                    spinner.SpinOnce();
                }
            }
            // Drain remainder after termination.
            while (ring.TryDequeue(out var req))
            {
                Buffer.MemoryCopy(req.srcAddress, req.dstAddress, req.bytes, req.bytes);
                req.callback(0, req.bytes, req.context);
            }
        }

        /// <inheritdoc />
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
            terminated = true;
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
