// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using HdrHistogram;
using Microsoft.Extensions.Logging;

namespace Resp.benchmark
{
    /// <summary>
    /// Orchestrates cluster-mode benchmarking by discovering topology,
    /// creating ClientRequestProviders per shard, and aggregating results.
    /// </summary>
    public class ClusterRequestProvider : IDisposable
    {
        readonly Options opts;
        readonly ILoggerFactory loggerFactory;
        readonly ILogger logger;

        ShardInfo[] shards;
        ClientRequestProvider[] providers;

        public ClusterRequestProvider(Options opts, ILoggerFactory loggerFactory = null)
        {
            this.opts = opts;
            this.loggerFactory = loggerFactory;
            this.logger = loggerFactory?.CreateLogger("ClusterBench");
        }

        /// <summary>
        /// Discover cluster topology and initialize providers.
        /// Creates NumThreads providers per shard.
        /// </summary>
        public void DiscoverTopology()
        {
            var topology = new ClusterTopology(opts);
            shards = topology.DiscoverPrimaryShards();

            Console.WriteLine($"Discovered {shards.Length} primary shard(s):");
            foreach (var shard in shards)
                Console.WriteLine($"  {shard}");

            var threadsPerShard = opts.NumThreads.First();
            var totalProviders = threadsPerShard * shards.Length;
            providers = new ClientRequestProvider[totalProviders];

            var idx = 0;
            for (var s = 0; s < shards.Length; s++)
            {
                for (var t = 0; t < threadsPerShard; t++)
                {
                    providers[idx] = new ClientRequestProvider(shards[s], opts, idx);
                    idx++;
                }
            }

            Console.WriteLine($"Created {totalProviders} ClientRequestProviders ({threadsPerShard} per shard)");
        }

        /// <summary>
        /// Load data into all shards in parallel.
        /// Each provider loads its portion of the key space.
        /// </summary>
        public void LoadData()
        {
            Console.WriteLine("Loading data into cluster...");
            var sw = Stopwatch.StartNew();

            var threads = new Thread[providers.Length];
            for (var i = 0; i < providers.Length; i++)
            {
                var p = providers[i];
                threads[i] = new Thread(() => p.LoadData());
                threads[i].Start();
            }

            foreach (var t in threads)
                t.Join();

            sw.Stop();
            Console.WriteLine($"Load complete in {sw.ElapsedMilliseconds}ms");
        }

        /// <summary>
        /// Run the benchmark (offline or online based on opts.Online).
        /// </summary>
        public void Run()
        {
            if (opts.Online)
                RunOnline();
            else
                RunOffline();
        }

        private void RunOffline()
        {
            Console.WriteLine("Preparing offline buffers...");
            foreach (var provider in providers)
                provider.PrepareOfflineBuffers();

            var runTime = TimeSpan.FromSeconds(opts.RunTime == -1 ? int.MaxValue : opts.RunTime);
            var startSignal = new ManualResetEventSlim(false);

            Console.WriteLine($"Starting offline benchmark ({opts.RunTime}s, {providers.Length} workers, batch={opts.BatchSize.First()})...");
            PrintHeader();

            var threads = new Thread[providers.Length];
            for (int i = 0; i < providers.Length; i++)
            {
                var p = providers[i];
                threads[i] = new Thread(() => p.RunOffline(startSignal, runTime));
                threads[i].Start();
            }

            // Start all workers simultaneously
            var sw = Stopwatch.StartNew();
            startSignal.Set();

            // Monitor and report metrics periodically
            MonitorAndReport(sw, runTime, threads);
        }

        private void RunOnline()
        {
            var runTime = TimeSpan.FromSeconds(opts.RunTime == -1 ? int.MaxValue : opts.RunTime);
            var startSignal = new ManualResetEventSlim(false);

            Console.WriteLine($"Starting online benchmark ({opts.RunTime}s, {providers.Length} workers, itp={opts.IntraThreadParallelism})...");
            PrintHeader();

            var threads = new Thread[providers.Length];
            for (int i = 0; i < providers.Length; i++)
            {
                var p = providers[i];
                threads[i] = new Thread(() => p.RunOnline(startSignal, runTime));
                threads[i].Start();
            }

            // Start all workers simultaneously
            var sw = Stopwatch.StartNew();
            startSignal.Set();

            // Monitor and report metrics periodically
            MonitorAndReport(sw, runTime, threads);
        }

        private void MonitorAndReport(Stopwatch sw, TimeSpan runTime, Thread[] threads)
        {
            long lastTotalOps = 0;
            var reportInterval = TimeSpan.FromSeconds(2);

            while (sw.Elapsed < runTime)
            {
                Thread.Sleep(reportInterval);

                long currentTotalOps = 0;
                foreach (var provider in providers)
                    currentTotalOps += provider.OpsCompleted;

                long iterOps = currentTotalOps - lastTotalOps;
                double elapsedSecs = reportInterval.TotalSeconds;
                double tptKops = iterOps / elapsedSecs / 1000.0;

                ReportIteration(currentTotalOps, iterOps, tptKops);
                lastTotalOps = currentTotalOps;
            }

            // Signal all providers to stop
            foreach (var provider in providers)
                provider.Stop();

            // Wait for all threads to complete
            foreach (var t in threads)
                t.Join();

            // Final report
            PrintFinalReport(sw.Elapsed);
        }

        private void PrintHeader()
        {
            const int pad = -15;
            var header =
                $"{"total_ops",pad}" +
                $"{"iter_ops",pad}" +
                $"{"tpt (Kops/sec)",pad}";

            if (opts.DisableConsoleLogger && opts.FileLogger == null)
                Console.WriteLine(header);
            else
                logger?.LogInformation("{msg}", header);
        }

        private void ReportIteration(long totalOps, long iterOps, double tptKops)
        {
            const int pad = -15;
            var msg =
                $"{totalOps,pad}" +
                $"{iterOps,pad}" +
                $"{tptKops:F2,pad}";

            if (opts.DisableConsoleLogger && opts.FileLogger == null)
                Console.WriteLine(msg);
            else
                logger?.LogInformation("{msg}", msg);
        }

        private void PrintFinalReport(TimeSpan totalElapsed)
        {
            Console.WriteLine();
            Console.WriteLine("========== CLUSTER BENCHMARK RESULTS ==========");
            Console.WriteLine();

            var summary = new LongHistogram(1, TimeStamp.Seconds(100), 2);
            long totalOps = 0;

            // Per-shard summary
            Console.WriteLine($"{"Shard",-8}{"Endpoint",-25}{"Threads",-10}{"Ops",-15}{"Ops/sec",-15}");
            Console.WriteLine(new string('-', 73));

            int threadsPerShard = opts.NumThreads.First();

            for (int s = 0; s < shards.Length; s++)
            {
                long shardOps = 0;
                for (int t = 0; t < threadsPerShard; t++)
                {
                    var provider = providers[s * threadsPerShard + t];
                    shardOps += provider.OpsCompleted;
                    summary.Add(provider.Histogram);
                }
                totalOps += shardOps;

                double shardOpsPerSec = shardOps / totalElapsed.TotalSeconds;
                Console.WriteLine($"{s,-8}{shards[s].Address + ":" + shards[s].Port,-25}{threadsPerShard,-10}{shardOps,-15}{shardOpsPerSec:N0,-15}");
            }

            Console.WriteLine(new string('-', 73));
            double totalOpsPerSec = totalOps / totalElapsed.TotalSeconds;
            Console.WriteLine($"{"Total",-8}{"",-25}{providers.Length,-10}{totalOps,-15}{totalOpsPerSec:N0,-15}");

            // Latency summary
            if (summary.TotalCount > 0)
            {
                Console.WriteLine();
                Console.WriteLine("Latency (batch-level):");
                Console.WriteLine($"  p50:   {summary.GetValueAtPercentile(50) / OutputScalingFactor.TimeStampToMicroseconds:F1} us");
                Console.WriteLine($"  p95:   {summary.GetValueAtPercentile(95) / OutputScalingFactor.TimeStampToMicroseconds:F1} us");
                Console.WriteLine($"  p99:   {summary.GetValueAtPercentile(99) / OutputScalingFactor.TimeStampToMicroseconds:F1} us");
                Console.WriteLine($"  p99.9: {summary.GetValueAtPercentile(99.9) / OutputScalingFactor.TimeStampToMicroseconds:F1} us");
                Console.WriteLine($"  avg:   {summary.GetMean() / OutputScalingFactor.TimeStampToMicroseconds:F1} us");
            }

            Console.WriteLine();
            Console.WriteLine($"Duration: {totalElapsed.TotalSeconds:F1}s");
            Console.WriteLine($"Total throughput: {totalOpsPerSec:N0} ops/sec ({totalOpsPerSec / 1000:N1} Kops/sec)");
            Console.WriteLine("================================================");
        }

        public void Dispose()
        {
            if (providers != null)
            {
                foreach (var provider in providers)
                    provider.Dispose();
            }
        }
    }
}
