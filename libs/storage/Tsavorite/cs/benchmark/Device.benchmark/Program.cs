// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
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

        // ---- Run-phase counters. Reset between thread settings. -----------------------
        // totalSubmitted    = ops kicked off by workers (does not imply success).
        // totalCompletedOk  = ops whose callback returned errorCode == 0 (real throughput).
        // totalErrors       = ops whose callback returned errorCode != 0.
        // ErrorCounts[code] = per-code histogram (cap 256; rare codes outside that range
        //                     still contribute to totalErrors but not the histogram).
        // The old `totalOperations` field is kept only as an alias for totalSubmitted so
        // anyone scraping `... operations in ...` strings doesn't break, but the new
        // "throughput" field below is computed from totalCompletedOk.
        internal static long totalSubmitted = 0;
        internal static long totalCompletedOk = 0;
        internal static long totalErrors = 0;
        internal static readonly long[] ErrorCounts = new long[256];

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
            if (opts.DeviceType == DeviceType.LocalMemory)
            {
                Console.WriteLine($"Device Type: LocalMemory");
            }
            else
            {
                Console.WriteLine($"Device Type: {opts.DeviceType}");
                Console.WriteLine($"IO Backend: {opts.IoBackend}");
                Console.WriteLine($"File Name: {opts.FileName}");
            }
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
            using var device = GetDevice(opts);

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
                    Interlocked.Exchange(ref totalSubmitted, 0);
                    Interlocked.Exchange(ref totalCompletedOk, 0);
                    Interlocked.Exchange(ref totalErrors, 0);
                    for (int i = 0; i < ErrorCounts.Length; i++) Interlocked.Exchange(ref ErrorCounts[i], 0);
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

                    // Aggregate per-worker counters (single-writer per worker → no race).
                    long completedOk = 0, errors = 0;
                    for (int wi = 0; wi < bworkers.Length; wi++)
                    {
                        completedOk += Interlocked.Read(ref bworkers[wi].localCompletedOk);
                        errors += Interlocked.Read(ref bworkers[wi].localErrors);
                        for (int code = 0; code < ErrorCounts.Length; code++)
                            Interlocked.Add(ref ErrorCounts[code], Interlocked.Read(ref bworkers[wi].localErrorCounts[code]));
                    }
                    Interlocked.Exchange(ref totalCompletedOk, completedOk);
                    Interlocked.Exchange(ref totalErrors, errors);

                    long submitted = Interlocked.Read(ref totalSubmitted);
                    double seconds = sw.Elapsed.TotalSeconds;
                    double throughput = completedOk / seconds;

                    // Throughput is from SUCCESSFUL completions only (errored ops are NOT real
                    // IOPS). The submitted/error counts are reported alongside so a misleading
                    // number can be spotted instantly: a healthy run has errors=0 and
                    // submitted ≈ completedOk; a flooded run has errors > 0 (most often
                    // Status::IOError=4, kernel libaio EAGAIN — try a smaller --throttle-limit).
                    Console.WriteLine($"Benchmark finished: {completedOk} ok, {errors} err, {submitted} submitted in {seconds:F2} s, throughput: {throughput:F2} ops/sec");
                    if (errors > 0)
                    {
                        Console.Write("  error breakdown:");
                        for (int code = 0; code < ErrorCounts.Length; code++)
                        {
                            long c = Interlocked.Read(ref ErrorCounts[code]);
                            if (c > 0) Console.Write($" code{code}={c}");
                        }
                        Console.WriteLine();
                    }
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

            long totalBuffers = opts.FileSize / tempBufferSize;
            for (long i = 0; i < totalBuffers; i++)
                DeviceUtils.WriteInto(device, (ulong)(i * tempBufferSize), tempBuffer);
        }

        static IDevice GetDevice(Options opts)
        {
            if (opts.DeviceType == DeviceType.LocalMemory)
            {
                // Capacity must be >= FileSize (the benchmark fills FileSize bytes then reads randomly).
                // Capacity must be a multiple of segment size.
                long segSize = opts.SegmentSize;
                long capacity = opts.FileSize;
                if (capacity % segSize != 0)
                    capacity = ((capacity + segSize - 1) / segSize) * segSize;
                int parallelism = opts.CompletionThreads > 0 ? opts.CompletionThreads : Environment.ProcessorCount;
                Console.WriteLine($"[local-memory] capacity={capacity} segmentSize={segSize} parallelism(={CompletionThreadsLabel}={parallelism})");
                return Devices.CreateLogDevice(
                    logPath: null,
                    deviceType: DeviceType.LocalMemory,
                    capacity: capacity,
                    numCompletionThreads: parallelism,
                    localMemorySegmentSize: segSize);
            }

            var deviceType = opts.DeviceType;
            var fileName = opts.FileName;
            return deviceType switch
            {
                DeviceType.Native when OperatingSystem.IsWindows() => new LocalStorageDevice(fileName, true, true, true, -1, false, true, false),
                DeviceType.Native when OperatingSystem.IsLinux() => new NativeStorageDevice(
                    fileName,
                    deleteOnClose: true,
                    disableFileBuffering: true,
                    capacity: -1,
                    numCompletionThreads: opts.CompletionThreads > 0 ? opts.CompletionThreads : 1,
                    ioBackend: ParseBackend(opts.IoBackend),
                    logger: null),
                DeviceType.FileStream => new ManagedLocalStorageDevice(fileName, true, false, true, -1, false, false, false),
                DeviceType.RandomAccess => new RandomAccessLocalStorageDevice(fileName, true, true, true, -1, false, false, false),
                _ => throw new ArgumentOutOfRangeException()
            };
        }

        const string CompletionThreadsLabel = "completion-threads";

        static NativeStorageDevice.IoBackend ParseBackend(string s) => (s ?? "default").ToLowerInvariant() switch
        {
            "default" => NativeStorageDevice.IoBackend.Default,
            "libaio" => NativeStorageDevice.IoBackend.Libaio,
            "uring" => NativeStorageDevice.IoBackend.Uring,
            _ => throw new ArgumentException(
                $"Unknown --io-backend value '{s}'. Valid values: default, libaio, uring."),
        };
    }
}