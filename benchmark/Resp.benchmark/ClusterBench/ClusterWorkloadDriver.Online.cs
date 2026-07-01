// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using HdrHistogram;
using Microsoft.Extensions.Logging;

namespace Resp.benchmark
{
    public partial class ClusterBench
    {
        private void RunWorkerPoolOnline()
        {
            var runTime = TimeSpan.FromSeconds(opts.RunTime == -1 ? int.MaxValue : opts.RunTime);
            var startSignal = new ManualResetEventSlim(false);

            Console.WriteLine($"Starting worker pool online benchmark ({opts.RunTime}s, {workers.Length} workers × {shards.Length} shards = {workers.Length * shards.Length} providers, itp={opts.IntraThreadParallelism})...");
            PrintOnlineHeader();

            var threads = new Thread[workers.Length];
            for (var i = 0; i < workers.Length; i++)
            {
                var worker = workers[i];
                threads[i] = new Thread(() => worker.RunOnline(startSignal, runTime));
                threads[i].Start();
            }

            // Start all workers simultaneously
            var sw = Stopwatch.StartNew();
            startSignal.Set();

            // Monitor and report metrics periodically
            var allProviders = workers.SelectMany(w => w.Providers).ToArray();
            MonitorOnline(sw, runTime, allProviders);

            // Signal all providers to stop
            foreach (var provider in allProviders)
                provider.Stop();

            // Wait for all threads to complete
            foreach (var t in threads)
                t.Join();

            // Final report
            PrintWorkerPoolFinalReport(sw.Elapsed);
        }

        private void RunOnline()
        {
            var runTime = TimeSpan.FromSeconds(opts.RunTime == -1 ? int.MaxValue : opts.RunTime);
            var startSignal = new ManualResetEventSlim(false);

            Console.WriteLine($"Starting online benchmark ({opts.RunTime}s, {shards.Length} shards x {opts.NumThreads.First()} workers/shard = {providers.Length} workers, itp={opts.IntraThreadParallelism})...");
            PrintOnlineHeader();

            var threads = new Thread[providers.Length];
            for (var i = 0; i < providers.Length; i++)
            {
                var p = providers[i];
                threads[i] = new Thread(() => p.RunOnline(startSignal, runTime));
                threads[i].Start();
            }

            // Start all workers simultaneously
            var sw = Stopwatch.StartNew();
            startSignal.Set();

            // Monitor and report metrics periodically
            MonitorOnline(sw, runTime, providers);

            // Signal all providers to stop
            foreach (var provider in providers)
                provider.Stop();

            // Wait for all threads to complete
            foreach (var t in threads)
                t.Join();

            // Final report
            PrintFinalReport(sw.Elapsed);
        }

        private void MonitorOnline(Stopwatch sw, TimeSpan runTime, IEnumerable<ClientRequestProvider> allProviders)
        {
            long lastTotalOps = 0;
            var reportInterval = TimeSpan.FromSeconds(1);
            var summary = new LongHistogram(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, 2);
            var batchSize = opts.BatchSize.First();
            var keysPerOp = (opts.Op is OpType.MGET or OpType.MSET) ? batchSize : 1;

            while (sw.Elapsed < runTime)
            {
                Thread.Sleep(reportInterval);

                summary.Reset();
                long currentTotalOps = 0;
                foreach (var provider in allProviders)
                {
                    summary.Add(provider.Histogram);
                    currentTotalOps += provider.OpsCompleted * keysPerOp;
                }

                var iterOps = currentTotalOps - lastTotalOps;
                var tptKops = iterOps / reportInterval.TotalSeconds / 1000.0;

                ReportOnlineIteration(summary, currentTotalOps, iterOps, tptKops);
                lastTotalOps = currentTotalOps;
            }
        }

        private void PrintOnlineHeader()
        {
            string[] hdrs = ["min (us)", "5th (us)", "med (us)", "avg (us)", "95th (us)", "99th (us)", "99.9th (us)", "total_ops", "iter_ops", "Kops/sec"];
            var header = string.Join(" | ", hdrs.Select(h => $"{h,12}"));
            var separator = string.Join("-+-", Enumerable.Repeat(new string('-', 12), hdrs.Length));

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

        private void ReportOnlineIteration(LongHistogram summary, long totalOps, long iterOps, double tptKops)
        {
            string msg;
            if (summary.TotalCount > 0)
            {
                msg =
                    $"{Math.Round(summary.GetValueAtPercentile(0) / OutputScalingFactor.TimeStampToMicroseconds, 2),12} | " +
                    $"{Math.Round(summary.GetValueAtPercentile(5) / OutputScalingFactor.TimeStampToMicroseconds, 2),12} | " +
                    $"{Math.Round(summary.GetValueAtPercentile(50) / OutputScalingFactor.TimeStampToMicroseconds, 2),12} | " +
                    $"{Math.Round(summary.GetMean() / OutputScalingFactor.TimeStampToMicroseconds, 2),12} | " +
                    $"{Math.Round(summary.GetValueAtPercentile(95) / OutputScalingFactor.TimeStampToMicroseconds, 2),12} | " +
                    $"{Math.Round(summary.GetValueAtPercentile(99) / OutputScalingFactor.TimeStampToMicroseconds, 2),12} | " +
                    $"{Math.Round(summary.GetValueAtPercentile(99.9) / OutputScalingFactor.TimeStampToMicroseconds, 2),12} | " +
                    $"{totalOps,12:N0} | " +
                    $"{iterOps,12:N0} | " +
                    $"{Math.Round(tptKops, 2),12}";
            }
            else
            {
                msg =
                    $"{0,12} | {0,12} | {0,12} | {0,12} | {0,12} | {0,12} | {0,12} | " +
                    $"{totalOps,12:N0} | {iterOps,12:N0} | {Math.Round(tptKops, 2),12}";
            }

            if (opts.DisableConsoleLogger && opts.FileLogger == null)
                Console.WriteLine(msg);
            else
                logger?.LogInformation("{msg}", msg);
        }
    }
}