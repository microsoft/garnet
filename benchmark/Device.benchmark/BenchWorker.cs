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

        unsafe void Callback(uint errorCode, uint numBytes, object ctx)
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
                // Hot-path: NEVER Console.WriteLine here. 100K+ errors/sec serial console
                // writes both falsify throughput numbers (errors get counted as "completed
                // fast") and slow the real path enough to compound the failure rate.
                // Aggregate per-code counts; Program prints them once after the run.
                Interlocked.Increment(ref Program.totalErrors);
                if (errorCode < (uint)Program.ErrorCounts.Length)
                    Interlocked.Increment(ref Program.ErrorCounts[errorCode]);
            }
            else
            {
                Interlocked.Increment(ref Program.totalCompletedOk);
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
                        Thread.Yield();
                        continue;
                    }
                    long sectorCount = (long)(fileSize / sectorSize);
                    long sector = threadRnd.NextInt64(0, (long)sectorCount) * sectorSize;
                    long dest = (long)op.Buffer;
                    while (device.Throttle()) Thread.Yield();
                    localTotalSubmitted++;
                    device.ReadAsync((ulong)sector, (IntPtr)dest, (uint)sectorSize, Callback, op);
                    device.TryComplete();
                }
            }
            finally
            {
                while (_benchmarkPool.Count < batchSize) Thread.Yield();
                // Authoritative throughput counter (successful ops only) is updated in the
                // callback. We also publish the per-thread submission count for diagnostics
                // (helps spot pathological submit/complete ratios under errors).
                _ = Interlocked.Add(ref Program.totalSubmitted, localTotalSubmitted);
                doneEvent.Set();
            }
        }
    }
}