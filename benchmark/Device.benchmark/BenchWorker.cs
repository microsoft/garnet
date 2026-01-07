// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.Runtime.InteropServices;
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
            var readSpan = new Span<byte>((void*)((BenchmarkOperation)ctx).Buffer, sectorSize);
            var expectedSpan = new Span<byte>(expectedData, 0, sectorSize);
            bool valid = readSpan.SequenceEqual(expectedSpan);

            if (!valid)
            {
                Console.WriteLine($"Data mismatch");
            }
#else
            // In Release builds, skip data validation for performance
#endif
            if (errorCode != 0)
            {
                Console.WriteLine($"I/O error: {errorCode}");
            }
            _benchmarkPool.Enqueue((BenchmarkOperation)ctx);
        }

        public unsafe void Run()
        {
            long localTotalOperations = 0;
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
                    localTotalOperations++;
                    device.ReadAsync((ulong)sector, (IntPtr)dest, (uint)sectorSize, Callback, op);
                    device.TryComplete();
                }
            }
            finally
            {
                while (_benchmarkPool.Count < batchSize) Thread.Yield();
                _ = Interlocked.Add(ref Program.totalOperations, localTotalOperations);
                doneEvent.Set();
            }
        }
    }
}