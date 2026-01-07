// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using CommandLine;
using Tsavorite.core;

namespace Device.benchmark
{
    /// <summary>
    /// Example usage:
    /// Device.benchmark --file-name d:/data/file.dat --runtime 30 --threads 16 --batch-size 1024 --device-type Native
    /// </summary>
    class Program
    {
        // Expected data for verification
        static byte[] ExpectedData;

        // Signals to coordinate benchmark timing
        static readonly ManualResetEventSlim timeUpEvent = new ManualResetEventSlim(false);

        // Add a new field to signal start event
        static readonly ManualResetEventSlim startEvent = new ManualResetEventSlim(false);

        // Add a new field to collect total operations
        internal static long totalOperations = 0;

        static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var opts = result.MapResult(o => o, xs => new Options());

            PrintBenchMarkSummary(opts);
            SetupDeviceBenchmark(opts);
        }

        static void PrintBenchMarkSummary(Options opts)
        {
            Console.WriteLine("<<<<<<< Start Benchmark Configuration >>>>>>>>");
            Console.WriteLine($"Device Type: {opts.DeviceType}");
            Console.WriteLine($"File Name: {opts.FileName}");
            Console.WriteLine($"File Size: {opts.FileSize}");
            Console.WriteLine($"Sector Size: {opts.SectorSize}");
            Console.WriteLine($"Segment Size: {opts.SegmentSize}");
            Console.WriteLine($"Throttle Limit: {opts.ThrottleLimit}");
            Console.WriteLine($"Completion Threads: {opts.CompletionThreads}");
            Console.WriteLine($"Runtime: {opts.Runtime}");
            Console.WriteLine($"BatchSize: {string.Join(",", opts.BatchSize.ToList())}");
            Console.WriteLine($"NumThreads: {string.Join(",", opts.NumThreads.ToList())}");
            Console.WriteLine("<<<<<<< End Benchmark Configuration >>>>>>>>");
        }

        static void SetupDeviceBenchmark(Options opts)
        {
            // Set completion threads
            if (opts.DeviceType == DeviceType.Native && OperatingSystem.IsWindows())
            {
                var ct = opts.CompletionThreads > 0 ? opts.CompletionThreads : Environment.ProcessorCount;
                LocalStorageDevice.NumCompletionThreads = ct;
            }

            // Create disk file
            using var device = GetDevice(opts.DeviceType, opts.FileName);

            // Set IO throttle limit
            device.ThrottleLimit = opts.ThrottleLimit > 0 ? opts.ThrottleLimit : int.MaxValue;

            // Set segment size
            device.Initialize(opts.SegmentSize);

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
                    BenchWorker[] bworkers = new BenchWorker[threads];
                    for (int t = 0; t < threads; t++)
                    {
                        int threadIndex = t;

                        doneEvents[t] = new ManualResetEventSlim(false);
                        bworkers[t] = new BenchWorker(batchSize, t, opts.SectorSize, opts.FileSize, ExpectedData, device, startEvent, timeUpEvent, doneEvents[threadIndex]);
                        workers[t] = new Thread(() => bworkers[threadIndex].Run());
                        workers[t].IsBackground = false;
                        workers[t].Start();
                    }

                    // Let all threads start and wait on the event
                    Thread.Sleep(100); // Small delay to ensure all threads are waiting

                    // Start timer
                    var sw = Stopwatch.StartNew();

                    // Signal all threads to start work at the same time
                    startEvent.Set();

                    // Wait for the benchmark duration
                    Thread.Sleep(opts.Runtime * 1000);

                    // Signal time is up
                    timeUpEvent.Set();

                    // Wait for all threads to finish
                    foreach (var done in doneEvents)
                        done.Wait();

                    sw.Stop();

                    // Join all worker threads
                    foreach (var worker in workers)
                        worker.Join();

                    long ops = Interlocked.Read(ref totalOperations);
                    double seconds = sw.Elapsed.TotalSeconds;
                    double throughput = ops / seconds;

                    Console.WriteLine($"Benchmark finished: {ops} operations in {seconds:F2} seconds, throughput: {throughput:F2} ops/sec");
                    Thread.Sleep(2000);
                }
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
        }

        static IDevice GetDevice(DeviceType deviceType, string fileName) => deviceType switch
        {
            DeviceType.Native when OperatingSystem.IsWindows() => new LocalStorageDevice(fileName, true, true, true, -1, false, true, false),
            DeviceType.Native when OperatingSystem.IsLinux() => new NativeStorageDevice(fileName, true, true, -1, 1, null),
            DeviceType.FileStream => new ManagedLocalStorageDevice(fileName, true, false, true, -1, false, false, false),
            DeviceType.RandomAccess => new RandomAccessLocalStorageDevice(fileName, true, true, true, -1, false, false, false),
            _ => throw new ArgumentOutOfRangeException()
        };
    }
}