// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Resp.benchmark
{
    public partial class ClusterBench
    {
        private void RunOffline()
        {
            // Prepare offline buffers
            Console.WriteLine("Preparing offline buffers...");
            Parallel.ForEach(providers, new ParallelOptions { MaxDegreeOfParallelism = LoadDataThreads },
                provider => provider.PrepareBuffers());

            var runTime = TimeSpan.FromSeconds(opts.RunTime == -1 ? int.MaxValue : opts.RunTime);
            var startSignal = new ManualResetEventSlim(false);

            Console.WriteLine($"Starting offline benchmark ({opts.RunTime}s, {shards.Length} shards x {opts.NumThreads.First()} workers/shard = {providers.Length} workers, batch={opts.BatchSize.First()})...");
            PrintOfflineHeader();

            var threads = new Thread[providers.Length];
            for (var i = 0; i < providers.Length; i++)
            {
                var p = providers[i];
                threads[i] = new Thread(() => p.RunOffline(startSignal, runTime));
                threads[i].Start();
            }

            // Start all workers simultaneously
            var sw = Stopwatch.StartNew();
            startSignal.Set();

            // Monitor and report metrics periodically
            MonitorOffline(sw, runTime, providers);

            // Signal all providers to stop
            foreach (var provider in providers)
                provider.Stop();

            // Wait for all threads to complete
            foreach (var t in threads)
                t.Join();

            // Final report
            PrintFinalReport(sw.Elapsed);
        }

        private void RunWorkerPoolOffline()
        {
            // Prepare offline buffers
            Console.WriteLine("Preparing offline buffers...");
            var allProviders = workers.SelectMany(w => w.Providers).ToArray();
            Parallel.ForEach(allProviders, new ParallelOptions { MaxDegreeOfParallelism = LoadDataThreads },
                provider => provider.PrepareBuffers());

            var runTime = TimeSpan.FromSeconds(opts.RunTime == -1 ? int.MaxValue : opts.RunTime);

            Console.WriteLine($"Starting offline benchmark (workers={workers.Length}, batch={opts.BatchSize.First()})...");
            PrintOfflineHeader();

            // Create cancellation token for workers
            using var cts = new CancellationTokenSource();

            // Start worker threads
            var threads = new Thread[workers.Length];
            for (var i = 0; i < workers.Length; i++)
            {
                var idx = i;
                var worker = workers[idx];
                threads[idx] = new Thread(() => worker.RunOffline(cts.Token))
                {
                    Name = $"Worker-{idx}"
                };
                threads[idx].Start();
            }

            // Start timing and monitor
            var sw = Stopwatch.StartNew();

            // Monitor and report metrics periodically
            MonitorOffline(sw, runTime, allProviders);

            // Signal all workers to stop
            cts.Cancel();

            // Wait for all threads to complete
            foreach (var t in threads)
                t.Join();

            // Final report
            PrintWorkerPoolFinalReport(sw.Elapsed);
        }

        private void MonitorOffline(Stopwatch sw, TimeSpan runTime, IEnumerable<ClientRequestProvider> allProviders)
        {
            long lastTotalOps = 0;
            long lastTotalBytes = 0;
            var reportInterval = TimeSpan.FromSeconds(1);
            var batchSize = opts.BatchSize.First();
            var keysPerOp = (opts.Op is OpType.MGET or OpType.MSET) ? batchSize : 1;
            var logicalBytesPerOp = Math.Max(opts.KeyLength, 8) + Math.Max(opts.ValueLength, 8);

            while (sw.Elapsed < runTime)
            {
                Thread.Sleep(reportInterval);

                long currentTotalOps = 0;
                long currentTotalBytes = 0;
                foreach (var provider in allProviders)
                {
                    currentTotalOps += provider.OpsCompleted * keysPerOp;
                    currentTotalBytes += provider.BytesSent;
                }

                var iterOps = currentTotalOps - lastTotalOps;
                var iterBytes = currentTotalBytes - lastTotalBytes;
                var tptKops = iterOps / reportInterval.TotalSeconds / 1000.0;
                var dataGBps = (iterOps * logicalBytesPerOp) / reportInterval.TotalSeconds / (1024.0 * 1024 * 1024);
                var wireGbps = iterBytes * 8.0 / reportInterval.TotalSeconds / 1_000_000_000.0;

                ReportOfflineIteration(currentTotalOps, iterOps, tptKops, dataGBps, wireGbps);
                lastTotalOps = currentTotalOps;
                lastTotalBytes = currentTotalBytes;
            }
        }

        private void PrintOfflineHeader()
        {
            var totalOpsHdr = "total_ops";
            var iterOpsHdr = "iter_ops";
            var tptHdr = "Kops/sec";
            var dataHdr = "data (GB/s)";
            var wireHdr = "wire (Gbps)";
            var header =
                $"{totalOpsHdr,15} | {iterOpsHdr,15} | {tptHdr,15} | {dataHdr,12} | {wireHdr,12}";
            var separator =
                $"{new string('-', 15)}-+-{new string('-', 15)}-+-{new string('-', 15)}-+-{new string('-', 12)}-+-{new string('-', 12)}";

            if (opts.DisableConsoleLogger && opts.FileLogger == null)
            {
                Console.WriteLine(header);
                Console.WriteLine(separator);
            }
            else
            {
                logger?.LogInformation("{msg}", header);
                logger?.LogInformation("{msg}", separator);
            }
        }

        private void ReportOfflineIteration(long totalOps, long iterOps, double tptKops, double dataGBps, double wireGbps)
        {
            var msg =
                $"{totalOps,15:N0} | {iterOps,15:N0} | {tptKops,15:N2} | {dataGBps,12:N3} | {wireGbps,12:N3}";

            if (opts.DisableConsoleLogger && opts.FileLogger == null)
                Console.WriteLine(msg);
            else
                logger?.LogInformation("{msg}", msg);
        }
    }
}
