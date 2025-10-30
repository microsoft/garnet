// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using CommandLine;
using Device.benchmark;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Resp.benchmark
{
    class Program
    {
        public static ILoggerFactory loggerFactory;

        static ILoggerFactory CreateLoggerFactory(Options opts)
        {
            return LoggerFactory.Create(builder =>
            {
                if (!opts.DisableConsoleLogger)
                {
                    builder.AddProvider(new BenchmarkLoggerProvider(Console.Out));
                }

                // Optional: Flush log output to file.
                if (opts.FileLogger != null)
                    builder.AddFile(opts.FileLogger);
                builder.SetMinimumLevel(opts.LogLevel);
            });
        }

        static void PrintBenchMarkSummary(Options opts)
        {
            Console.WriteLine("<<<<<<< Benchmark Configuration >>>>>>>>");
            Console.WriteLine($"Device Type: {opts.Device}");
            Console.WriteLine($"File Name: {opts.FileName}");
            Console.WriteLine($"File Size: {opts.FileSize}");
            Console.WriteLine($"Sector Size: {opts.SectorSize}");
            Console.WriteLine($"BatchSize: {string.Join(",", opts.BatchSize.ToList())}");
            Console.WriteLine($"RunTime: {opts.RunTime}");
            Console.WriteLine($"NumThreads: {string.Join(",", opts.NumThreads.ToList())}");
            ThreadPool.SetMinThreads(workerThreads: 1000, completionPortThreads: 1000);
            ThreadPool.GetMinThreads(out var minWorkerThreads, out var minCompletionPortThreads);
            Console.WriteLine($"minWorkerThreads: {minWorkerThreads}");
            Console.WriteLine($"minCompletionPortThreads: {minCompletionPortThreads}");
            Console.WriteLine("----------------------------------");
        }

        static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var opts = result.MapResult(o => o, xs => new Options());

            loggerFactory = CreateLoggerFactory(opts);

            PrintBenchMarkSummary(opts);
            
            SetupDeviceBenchmark(opts);
        }

        static byte[] ExpectedData;

        // Add a new field to signal time completion
        static ManualResetEventSlim timeUpEvent = new ManualResetEventSlim(false);

        // Add a new field to signal start event
        static ManualResetEventSlim startEvent = new ManualResetEventSlim(false);

        // Add a new field to collect total operations
        static long totalOperations = 0;

        static void SetupDeviceBenchmark(Options opts)
        {
            // Create disk file
            using var device = GetDevice(opts.Device, opts.FileName);

            // Remove IO throttle limit for benchmark
            device.ThrottleLimit = int.MaxValue;

            // Fill device with FileSize bytes of data using a larger temporary buffer
            FillDeviceWithTestData(device, opts);

            Console.WriteLine("Starting benchmark...");
            int[] numThreads = [.. opts.NumThreads];
            int[] batchSizes = [.. opts.BatchSize];

            foreach (var threads in numThreads)
            {
                foreach (var batchSize in batchSizes)
                {
                    Console.WriteLine($"Starting benchmark with {threads} threads and batch size {batchSize}");

                    Thread[] workers = new Thread[threads];
                    ManualResetEventSlim[] doneEvents = new ManualResetEventSlim[threads];
                    timeUpEvent.Reset();
                    startEvent.Reset();
                    Interlocked.Exchange(ref totalOperations, 0);

                    for (int t = 0; t < threads; t++)
                    {
                        doneEvents[t] = new ManualResetEventSlim(false);
                        int threadIndex = t;
                        workers[t] = new Thread(() => RunWorker(device, opts, batchSize, ExpectedData, startEvent, timeUpEvent, doneEvents[threadIndex]));
                        workers[t].IsBackground = true;
                        workers[t].Start();
                    }

                    // Let all threads start and wait on the event
                    Thread.Sleep(100); // Small delay to ensure all threads are waiting

                    // Start timer
                    var sw = System.Diagnostics.Stopwatch.StartNew();

                    // Signal all threads to start work at the same time
                    startEvent.Set();

                    // Wait for the benchmark duration
                    Thread.Sleep(opts.RunTime * 1000);

                    // Signal time is up
                    timeUpEvent.Set();

                    // Wait for all threads to finish
                    foreach (var done in doneEvents)
                        done.Wait();

                    sw.Stop();

                    long ops = Interlocked.Read(ref totalOperations);
                    double seconds = sw.Elapsed.TotalSeconds;
                    double throughput = ops / seconds;

                    Console.WriteLine($"Benchmark finished: {ops} operations in {seconds:F2} seconds, throughput: {throughput:F2} ops/sec");
                }
            }
        }

        static unsafe void RunWorker(IDevice device, Options opts, int batchSize, byte[] ExpectedData, ManualResetEventSlim startEvent, ManualResetEventSlim timeUpEvent, ManualResetEventSlim doneEvent)
        {
            var threadRnd = new Random(Guid.NewGuid().GetHashCode());
            int sectorSize = (int)device.SectorSize;
            int bufferSize = sectorSize * batchSize;

            // Allocate sector-aligned buffer using pinned allocation
            byte[] buffer = GC.AllocateArray<byte>(bufferSize + sectorSize, pinned: true);
            long addr = (long)Unsafe.AsPointer(ref buffer[0]);
            long alignedAddr = (addr + sectorSize - 1) & ~(sectorSize - 1);
            IntPtr alignedBufferPtr = new IntPtr(alignedAddr);

            var semaphore = new SemaphoreSlim(0);
            int completed = 0;

            void Callback(uint errorCode, uint numBytes, object ctx)
            {
#if DEBUG
                int idx = (int)ctx;
                var readSpan = new Span<byte>(buffer, (int)(alignedAddr - addr) + idx * sectorSize, sectorSize);
                var expectedSpan = new Span<byte>(ExpectedData, 0, sectorSize);
                bool valid = readSpan.SequenceEqual(expectedSpan);

                if (!valid)
                {
                    Console.WriteLine($"Data mismatch at batch index {idx}");
                }
#else
                // In Release builds, skip data validation for performance
#endif
                if (Interlocked.Increment(ref completed) == batchSize)
                {
                    semaphore.Release();
                }
            }

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
                        ulong sectorCount = (ulong)(opts.FileSize / sectorSize);
                        ulong sector = (ulong)threadRnd.NextInt64(0, (long)sectorCount) * (ulong)sectorSize;
                        IntPtr dest = IntPtr.Add(alignedBufferPtr, b * sectorSize);
                        device.ReadAsync(sector, dest, (uint)sectorSize, Callback, b);
                    }

                    semaphore.Wait();
                    localTotalOperations += batchSize;
                }
            }
            finally
            {
                Interlocked.Add(ref totalOperations, localTotalOperations);
                doneEvent.Set();
            }
        }

        static void FillDeviceWithTestData(IDevice device, Options opts)
        {
            Console.WriteLine("Filling device with test data...");
            ExpectedData = new byte[opts.SectorSize];
            var rnd = new Random();
            rnd.NextBytes(ExpectedData);

            const int tempBufferSectors = 1024;
            int sectorSize = opts.SectorSize;
            int tempBufferSize = tempBufferSectors * sectorSize;

            if (opts.FileSize % tempBufferSize != 0)
                throw new InvalidOperationException("FileSize must be a perfect multiple of the temporary buffer size (1024 sectors).");

            byte[] tempBuffer = new byte[tempBufferSize];
            for (int i = 0; i < tempBufferSectors; i++)
                Buffer.BlockCopy(ExpectedData, 0, tempBuffer, i * sectorSize, sectorSize);

            int totalBuffers = opts.FileSize / tempBufferSize;
            for (int i = 0; i < totalBuffers; i++)
                DeviceUtils.WriteInto(device, (ulong)(i * tempBufferSize), tempBuffer);

            GC.Collect();
            GC.WaitForFullGCComplete();
        }

        static IDevice GetDevice(DeviceType deviceType, string fileName) => deviceType switch
        {
            DeviceType.LocalStorage when OperatingSystem.IsWindows() => new LocalStorageDevice(fileName, true, true, true, -1, false, false, false),
            DeviceType.ManagedLocalStorage => new ManagedLocalStorageDevice(fileName, true, false, true, -1, false, false, false),
            DeviceType.RandomAccessLocalStorage => new RandomAccessLocalStorageDevice(fileName, true, true, true, -1, false, false, false),
            _ => throw new ArgumentOutOfRangeException()
        };
    }
}