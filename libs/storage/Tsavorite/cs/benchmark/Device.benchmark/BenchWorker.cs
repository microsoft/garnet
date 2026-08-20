// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Threading;
using Tsavorite.core;

namespace Device.benchmark
{
    unsafe class BenchmarkOperation
    {
        public void* Buffer { get; private set; }

        public BenchmarkOperation(int sectorSize)
        {
            // Use _aligned_malloc for .NET Framework
            Buffer = NativeMemory.AlignedAlloc((nuint)sectorSize, (nuint)sectorSize);
            if (Buffer == null)
            {
                throw new OutOfMemoryException("Failed to allocate client-side aligned buffer.");
            }
        }

        public void FreeBuffer()
        {
            if (Buffer != null)
            {
                NativeMemory.AlignedFree(Buffer);
                Buffer = null;
            }
        }
    }

    class BenchWorker
    {
        readonly int batchSize, sectorSize;
        readonly Random threadRnd;
        readonly long fileSize;
        readonly byte[] expectedData;
        readonly IDevice device;
        readonly ManualResetEventSlim startEvent, timeUpEvent, doneEvent;
        readonly ConcurrentQueue<BenchmarkOperation> _benchmarkPool = new();

        // Per-worker (single-writer-from-processor-thread) counters. Avoid the cache-line
        // ping-pong of incrementing Program.totalCompletedOk on every callback when many
        // processor threads are active. Each worker's ops are routed to a stable single
        // processor by the device (per-thread SPSC routing), so the processor that writes
        // these counters is unique per worker.
        internal long localCompletedOk;
        internal long localErrors;
        internal readonly long[] localErrorCounts = new long[256];

        public BenchWorker(int batchSize, int threadId, int sectorSize, long fileSize, byte[] expectedData, IDevice device, ManualResetEventSlim startEvent, ManualResetEventSlim timeUpEvent, ManualResetEventSlim doneEvent)
        {
            this.batchSize = batchSize;
            this.sectorSize = sectorSize;
            this.fileSize = fileSize;
            this.expectedData = expectedData;
            this.device = device;
            this.startEvent = startEvent;
            this.timeUpEvent = timeUpEvent;
            this.doneEvent = doneEvent;
            this.threadRnd = new Random(threadId);
            for (int i = 0; i < batchSize; i++)
            {
                _benchmarkPool.Enqueue(new BenchmarkOperation(sectorSize));
            }
        }

        unsafe void Callback(uint errorCode, uint numBytes, object ctx, Exception ioException)
        {
#if DEBUG
            if (errorCode == 0)
            {
                var readSpan = new Span<byte>((void*)((BenchmarkOperation)ctx).Buffer, sectorSize);
                var expectedSpan = new Span<byte>(expectedData, 0, sectorSize);
                bool valid = readSpan.SequenceEqual(expectedSpan);

                if (!valid)
                {
                    Console.WriteLine($"Data mismatch");
                }
            }
#endif
            if (errorCode != 0)
            {
                // Per-worker counters: single processor thread writes these, so no
                // synchronization is needed during the run. Cross-thread visibility is
                // established when the submitter sets doneEvent (full memory barrier) and
                // the main thread reads after doneEvent.Wait().
                localErrors++;
                if (errorCode < (uint)localErrorCounts.Length)
                    localErrorCounts[errorCode]++;
            }
            else
            {
                localCompletedOk++;
            }
            _benchmarkPool.Enqueue((BenchmarkOperation)ctx);
        }

        public unsafe void Run()
        {
            long localTotalSubmitted = 0;
            try
            {
                // Wait for the start event to be signaled
                startEvent.Wait();

                BenchmarkOperation op;
                while (!timeUpEvent.IsSet)
                {
                    while (!_benchmarkPool.TryDequeue(out op))
                    {
                        // Pool empty: every buffer this worker owns is in flight. The device's
                        // dedicated completion-drainer threads refill the pool via Callback, so
                        // this drains only as a liveness fallback. It is rarely hit because the
                        // per-thread throttle caps in-flight well below batchSize. Draining after
                        // every submit would keep it on the hot path, where the single-event
                        // TryComplete serializes all submitters on context 0's kernel ring mutex.
                        device.TryComplete();
                        Thread.Yield();
                    }
                    long sectorCount = (long)(fileSize / sectorSize);
                    long sector = threadRnd.NextInt64(0, (long)sectorCount) * sectorSize;
                    long dest = (long)op.Buffer;
                    while (device.Throttle()) Thread.Yield();
                    localTotalSubmitted++;
                    device.ReadAsync((ulong)sector, (IntPtr)dest, (uint)sectorSize, Callback, op);
                }
            }
            finally
            {
                // Drain until every buffer this worker owns has returned, polling completions so
                // any reads submitted during the last iterations complete here rather than
                // stranding the shutdown wait.
                while (_benchmarkPool.Count < batchSize)
                {
                    device.TryComplete();
                    Thread.Yield();
                }
                // Authoritative throughput counter (successful ops only) is updated in the
                // callback. We also publish the per-thread submission count for diagnostics
                // (helps spot pathological submit/complete ratios under errors).
                _ = Interlocked.Add(ref Program.totalSubmitted, localTotalSubmitted);
                doneEvent.Set();
            }
        }
    }
}