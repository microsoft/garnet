// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Tsavorite.core;

namespace Resp.benchmark
{
    class BenchWorker
    {
        readonly SemaphoreSlim semaphore;
        readonly int batchSize, sectorSize;
        readonly Random threadRnd;
        readonly byte[] buffer;
        readonly long alignedBufferPtr, bufferPtr, fileSize;
        readonly byte[] expectedData;
        readonly IDevice device;
        int completed;
        readonly ManualResetEventSlim startEvent, timeUpEvent, doneEvent;

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
            semaphore = new SemaphoreSlim(0);

            int bufferSize = sectorSize * batchSize;
            buffer = GC.AllocateArray<byte>(bufferSize + sectorSize, pinned: true);

            unsafe
            {
                bufferPtr = (long)Unsafe.AsPointer(ref buffer[0]);
                alignedBufferPtr = (bufferPtr + (sectorSize - 1)) & ~(sectorSize - 1);
            }
        }

        void Callback(uint errorCode, uint numBytes, object ctx)
        {
#if DEBUG
                int idx = (int)ctx;
                var readSpan = new Span<byte>(buffer, (int)(alignedBufferPtr - bufferPtr) + idx * sectorSize, sectorSize);
                var expectedSpan = new Span<byte>(expectedData, 0, sectorSize);
                bool valid = readSpan.SequenceEqual(expectedSpan);

                if (!valid)
                {
                    Console.WriteLine($"Data mismatch at batch index {idx}");
                }
#else
            // In Release builds, skip data validation for performance
#endif
            if (errorCode != 0)
            {
                Console.WriteLine($"I/O error: {errorCode}");
            }
            if (Interlocked.Increment(ref completed) == batchSize)
            {
                semaphore.Release();
            }
        }

        public unsafe void Run()
        {
            long localTotalOperations = 0;
            try
            {
                // Wait for the start event to be signaled
                startEvent.Wait();

                while (!timeUpEvent.IsSet)
                {
                    completed = 0;

                    for (int b = 0; b < batchSize; b++)
                    {
                        int _b = b;
                        long sectorCount = (long)(fileSize / sectorSize);
                        long sector = threadRnd.NextInt64(0, (long)sectorCount) * sectorSize;
                        long dest = alignedBufferPtr + b * sectorSize;
                        while (device.Throttle()) Thread.Yield();
                        device.ReadAsync((ulong)sector, (IntPtr)dest, (uint)sectorSize, Callback, _b);
                    }
                    semaphore.Wait();
                    localTotalOperations += batchSize;
                }
            }
            finally
            {
                _ = Interlocked.Add(ref Program.totalOperations, localTotalOperations);
                doneEvent.Set();
            }
        }
    }
}