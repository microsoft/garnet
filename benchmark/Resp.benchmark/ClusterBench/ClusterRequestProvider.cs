// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using HdrHistogram;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Resp.benchmark
{
    /// <summary>
    /// Orchestrates cluster-mode benchmarking by discovering topology,
    /// creating ClientRequestProviders per shard, and aggregating results.
    /// </summary>
    public class ClusterBench : IDisposable
    {
        static readonly long HISTOGRAM_LOWER_BOUND = 1;
        static readonly long HISTOGRAM_UPPER_BOUND = TimeStamp.Seconds(100);

        readonly Options opts;
        readonly ILoggerFactory loggerFactory;
        readonly ILogger logger;

        PrimaryInfo[] shards;
        ClientRequestProvider[] providers;

        public ClusterBench(Options opts, ILoggerFactory loggerFactory = null)
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
            ValidateReplicaReadOptions();

            var clusterManager = new ClusterManager(opts);
            shards = clusterManager.DiscoverPrimaryShards();

            var threadsPerShard = opts.NumThreads.First();
            var totalProviders = threadsPerShard * shards.Length;
            providers = new ClientRequestProvider[totalProviders];

            var idx = 0;
            for (var s = 0; s < shards.Length; s++)
            {
                for (var t = 0; t < threadsPerShard; t++)
                {
                    providers[idx] = new ClientRequestProvider(shards[s], opts, idx, t);
                    idx++;
                }
            }

            PrintConfiguration(threadsPerShard, totalProviders);
        }

        /// <summary>
        /// Validate --allow-replica-reads flag value.
        /// </summary>
        private void ValidateReplicaReadOptions()
        {
            if (opts.AllowReplicaReads < -1 || opts.AllowReplicaReads > 100)
            {
                throw new Exception($"Invalid --allow-replica-reads value: {opts.AllowReplicaReads}. Valid range is -1 (disabled) or 0-100 (percentage).");
            }
        }

        private void PrintConfiguration(int threadsPerShard, int totalProviders)
        {
            var mode = opts.Online ? "Online" : "Offline";
            var tls = opts.EnableTLS ? "Yes" : "No";
            var skipLoad = opts.SkipLoad ? "Yes" : "No";
            var itp = opts.IntraThreadParallelism;
            var batch = opts.BatchSize.First();

            // Count total replicas
            var totalReplicas = shards.Sum(s => s.Replicas.Count);
            var replicaReads = opts.AllowReplicaReads >= 0 ? $"{opts.AllowReplicaReads}%" : "Disabled";

            Console.WriteLine();
            Console.WriteLine("=========== Cluster Benchmark Configuration ===========");
            Console.WriteLine($"{"Mode: " + mode,-28}{"Client: " + opts.Client,-28}");
            Console.WriteLine($"{"Op: " + opts.Op,-28}{"Threads: " + threadsPerShard + " (per shard)",-28}");
            Console.WriteLine($"{"DB Size: " + opts.DbSize,-28}{"ITP: " + itp,-28}");
            Console.WriteLine($"{"Key/Val: " + opts.KeyLength + "/" + opts.ValueLength + " B",-28}{"Batch: " + batch,-28}");
            Console.WriteLine($"{"Runtime: " + opts.RunTime + "s",-28}{"TLS: " + tls,-28}");
            Console.WriteLine($"{"Shards: " + shards.Length,-28}{"Workers: " + totalProviders,-28}");
            Console.WriteLine($"{"Skip Load: " + skipLoad,-28}{"Auth: " + (string.IsNullOrEmpty(opts.Auth) ? "No" : "Yes"),-28}");
            Console.WriteLine($"{"Replicas: " + totalReplicas,-28}{"Replica Reads: " + replicaReads,-28}");
            Console.WriteLine("=======================================================");
            Console.WriteLine();

            // Topology table
            Console.WriteLine($"  {"Shard",-7}| {"Endpoint",-23}| {"Slots",-7}| {"     Range",-17}| {"Replicas",-10}| {"Prefix",-10}");
            Console.WriteLine($"  {new string('-', 7)}+{new string('-', 24)}+{new string('-', 8)}+{new string('-', 18)}+{new string('-', 11)}+{new string('-', 11)}");

            for (int s = 0; s < shards.Length; s++)
            {
                var shard = shards[s];
                var endpoint = $"{shard.Address}:{shard.Port}";
                var range = FormatSlotRanges(shard.SlotRanges);
                var prefix = providers[s * threadsPerShard].KeyPrefix;
                var replicaCount = shard.Replicas.Count;
                Console.WriteLine($"  {s,-7}| {endpoint,-23}| {shard.TotalSlots,-7}| {range,-17}| {replicaCount,-10}| {prefix,-10}");
                
                // Print replica endpoints underneath if they exist
                foreach (var replica in shard.Replicas)
                {
                    var replicaEndpoint = $"  {replica.Address}:{replica.Port}";
                    Console.WriteLine($"  {"replica",-7}| {replicaEndpoint,-23}|");
                }
            }

            Console.WriteLine();

            // Warning if replica reads enabled but no replicas found
            if (opts.AllowReplicaReads > 0 && totalReplicas == 0)
            {
                Console.WriteLine("  [WARNING] --allow-replica-reads is enabled but no replicas discovered.");
                Console.WriteLine("            All operations will execute on primaries.");
                Console.WriteLine();
            }
        }

        private static string FormatSlotRanges(List<(int Start, int End)> ranges)
        {
            if (ranges.Count == 1)
                return $"[{ranges[0].Start,5},{ranges[0].End,5}]";

            if (ranges.Count == 2)
                return $"[{ranges[0].Start,5},{ranges[0].End,5}],[{ranges[1].Start,5},{ranges[1].End,5}]";

            return $"[{ranges[0].Start,5},{ranges[0].End,5}],...,[{ranges[^1].Start,5},{ranges[^1].End,5}]";
        }

        /// <summary>
        /// Load data into all shards in parallel.
        /// Each provider loads its portion of the key space.
        /// </summary>
        public void LoadData()
        {
            Console.WriteLine(" Loading keys...");
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

            // Per-shard summary
            int threadsPerShard = opts.NumThreads.First();
            long totalKeys = 0;
            int maxEndpointLen = shards.Max(s => $"{s.Address}:{s.Port}".Length);

            for (int s = 0; s < shards.Length; s++)
            {
                long shardKeys = 0;
                for (int t = 0; t < threadsPerShard; t++)
                    shardKeys += providers[s * threadsPerShard + t].KeysLoaded;
                totalKeys += shardKeys;

                var endpoint = $"{shards[s].Address}:{shards[s].Port}";
                Console.WriteLine($"   Loaded {shardKeys} keys to {endpoint.PadLeft(maxEndpointLen)}");
            }

            Console.WriteLine($" Total: {totalKeys} keys ({shards.Length} shards, {providers.Length} threads) in {sw.ElapsedMilliseconds}ms");
            Console.WriteLine();

            // Validate DBSIZE if under 1MB threshold
            ValidateDBSize(totalKeys);
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
                provider.PrepareBuffers();

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
            MonitorAndReportOffline(sw, runTime, threads);
        }

        private void RunOnline()
        {
            var runTime = TimeSpan.FromSeconds(opts.RunTime == -1 ? int.MaxValue : opts.RunTime);
            var startSignal = new ManualResetEventSlim(false);

            Console.WriteLine($"Starting online benchmark ({opts.RunTime}s, {shards.Length} shards x {opts.NumThreads.First()} workers/shard = {providers.Length} workers, itp={opts.IntraThreadParallelism})...");
            PrintOnlineHeader();

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
            MonitorAndReportOnline(sw, runTime, threads);
        }

        private void MonitorAndReportOnline(Stopwatch sw, TimeSpan runTime, Thread[] threads)
        {
            long lastTotalOps = 0;
            var reportInterval = TimeSpan.FromSeconds(2);
            var summary = new LongHistogram(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, 2);

            while (sw.Elapsed < runTime)
            {
                Thread.Sleep(reportInterval);

                // Aggregate histograms from all providers
                summary.Reset();
                long currentTotalOps = 0;
                foreach (var provider in providers)
                {
                    summary.Add(provider.Histogram);
                    currentTotalOps += provider.OpsCompleted;
                }

                long iterOps = currentTotalOps - lastTotalOps;
                double tptKops = iterOps / reportInterval.TotalSeconds / 1000.0;

                ReportOnlineIteration(summary, currentTotalOps, iterOps, tptKops);
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

        private void MonitorAndReportOffline(Stopwatch sw, TimeSpan runTime, Thread[] threads)
        {
            long lastTotalOps = 0;
            long lastTotalBytes = 0;
            var reportInterval = TimeSpan.FromSeconds(2);
            var logicalBytesPerOp = opts.KeyLength + opts.ValueLength;

            while (sw.Elapsed < runTime)
            {
                Thread.Sleep(reportInterval);

                long currentTotalOps = 0;
                long currentTotalBytes = 0;
                foreach (var provider in providers)
                {
                    currentTotalOps += provider.OpsCompleted;
                    currentTotalBytes += provider.BytesSent;
                }

                long iterOps = currentTotalOps - lastTotalOps;
                long iterBytes = currentTotalBytes - lastTotalBytes;
                double tptKops = iterOps / reportInterval.TotalSeconds / 1000.0;
                double dataGBps = (iterOps * logicalBytesPerOp) / reportInterval.TotalSeconds / (1024.0 * 1024 * 1024);
                double wireGBps = iterBytes / reportInterval.TotalSeconds / (1024.0 * 1024 * 1024);

                ReportOfflineIteration(currentTotalOps, iterOps, tptKops, dataGBps, wireGBps);
                lastTotalOps = currentTotalOps;
                lastTotalBytes = currentTotalBytes;
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

        private void PrintOfflineHeader()
        {
            var totalOpsHdr = "total_ops";
            var iterOpsHdr = "iter_ops";
            var tptHdr = "Kops/sec";
            var dataHdr = "data (GB/s)";
            var wireHdr = "wire (GB/s)";
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

        private void ReportOfflineIteration(long totalOps, long iterOps, double tptKops, double dataGBps, double wireGBps)
        {
            var msg =
                $"{totalOps,15:N0} | {iterOps,15:N0} | {tptKops,15:N2} | {dataGBps,12:N3} | {wireGBps,12:N3}";

            if (opts.DisableConsoleLogger && opts.FileLogger == null)
                Console.WriteLine(msg);
            else
                logger?.LogInformation("{msg}", msg);
        }

        /// <summary>
        /// Validate that DBSIZE matches expected key count per shard.
        /// Only runs if key-count * key-length * val-length is under 1MB threshold.
        /// </summary>
        private void ValidateDBSize(long totalKeysLoaded)
        {
            // Check 1MB threshold
            long estimatedDataSize = (long)opts.DbSize * opts.KeyLength * opts.ValueLength;
            if (estimatedDataSize > 1024 * 1024)
            {
                Console.WriteLine($" Skipping DBSIZE validation (estimated data size {estimatedDataSize / (1024.0 * 1024):F2} MB > 1 MB threshold)");
                Console.WriteLine();
                return;
            }

            Console.WriteLine(" Validating DBSIZE per shard...");
            int threadsPerShard = opts.NumThreads.First();
            bool allValid = true;

            for (int s = 0; s < shards.Length; s++)
            {
                long expectedKeys = 0;
                for (int t = 0; t < threadsPerShard; t++)
                    expectedKeys += providers[s * threadsPerShard + t].KeysLoaded;

                try
                {
                    using var redis = ConnectionMultiplexer.Connect(
                        BenchUtils.GetConfig(shards[s].Address, shards[s].Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost, allowAdmin: true));

                    var db = redis.GetDatabase(0);
                    var actualKeys = (long)db.Execute("DBSIZE");

                    var endpoint = $"{shards[s].Address}:{shards[s].Port}";
                    if (actualKeys == expectedKeys)
                    {
                        Console.WriteLine($"   [OK] {endpoint.PadRight(25)} DBSIZE={actualKeys} (expected={expectedKeys})");
                    }
                    else
                    {
                        Console.WriteLine($"   [FAIL] {endpoint.PadRight(25)} DBSIZE={actualKeys} (expected={expectedKeys}) [MISMATCH]");
                        allValid = false;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"   [ERROR] {shards[s].Address}:{shards[s].Port} - DBSIZE failed: {ex.Message}");
                    allValid = false;
                }
            }

            if (allValid)
                Console.WriteLine(" All shards validated successfully!");
            else
                Console.WriteLine(" WARNING: Some shards have key count mismatches!");

            Console.WriteLine();
        }

        private void PrintFinalReport(TimeSpan totalElapsed)
        {
            Console.WriteLine();
            Console.WriteLine("========== CLUSTER BENCHMARK RESULTS ==========");
            Console.WriteLine();

            var summary = new LongHistogram(1, TimeStamp.Seconds(100), 2);
            long totalOps = 0;
            long totalBytes = 0;

            // Per-shard summary
            Console.WriteLine($"{"Shard",-8}{"Endpoint",-25}{"Threads",-10}{"Ops",-15}{"Ops/sec",-15}");
            Console.WriteLine(new string('-', 73));

            int threadsPerShard = opts.NumThreads.First();

            for (int s = 0; s < shards.Length; s++)
            {
                long shardOps = 0;
                long shardBytes = 0;
                for (int t = 0; t < threadsPerShard; t++)
                {
                    var provider = providers[s * threadsPerShard + t];
                    shardOps += provider.OpsCompleted;
                    shardBytes += provider.BytesSent;
                    summary.Add(provider.Histogram);
                }
                totalOps += shardOps;
                totalBytes += shardBytes;

                double shardOpsPerSec = shardOps / totalElapsed.TotalSeconds;
                Console.WriteLine($"{s,-8}{shards[s].Address + ":" + shards[s].Port,-25}{threadsPerShard,-10}{shardOps,-15}{shardOpsPerSec,-15:N0}");
            }

            Console.WriteLine(new string('-', 73));
            double totalOpsPerSec = totalOps / totalElapsed.TotalSeconds;
            Console.WriteLine($"{"Total",-8}{"",-25}{providers.Length,-10}{totalOps,-15}{totalOpsPerSec,-15:N0}");

            // Latency summary
            if (summary.TotalCount > 0)
            {
                Console.WriteLine();
                var p50 = summary.GetValueAtPercentile(50) / OutputScalingFactor.TimeStampToMicroseconds;
                var p95 = summary.GetValueAtPercentile(95) / OutputScalingFactor.TimeStampToMicroseconds;
                var p99 = summary.GetValueAtPercentile(99) / OutputScalingFactor.TimeStampToMicroseconds;
                var p999 = summary.GetValueAtPercentile(99.9) / OutputScalingFactor.TimeStampToMicroseconds;
                var avg = summary.GetMean() / OutputScalingFactor.TimeStampToMicroseconds;
                Console.WriteLine($"Latency (us): p50={p50:F1}  p95={p95:F1}  p99={p99:F1}  p99.9={p999:F1}  avg={avg:F1}");
            }

            // Throughput summary
            var logicalBytesPerOp = opts.KeyLength + opts.ValueLength;
            double dataGBps = (totalOps * logicalBytesPerOp) / totalElapsed.TotalSeconds / (1024.0 * 1024 * 1024);
            double wireGBps = totalBytes / totalElapsed.TotalSeconds / (1024.0 * 1024 * 1024);

            Console.WriteLine();
            Console.WriteLine($"Duration: {totalElapsed.TotalSeconds:F1}s");
            Console.WriteLine($"Total throughput: {totalOpsPerSec:N0} ops/sec ({totalOpsPerSec / 1000:N1} Kops/sec)");
            Console.WriteLine($"Data throughput:  {dataGBps:N3} GB/sec (logical: key={opts.KeyLength}B + val={opts.ValueLength}B = {logicalBytesPerOp}B/op)");
            Console.WriteLine($"Wire throughput:  {wireGBps:N3} GB/sec (RESP bytes sent)");

            // Report hit rates from INFO STATS
            ReportHitRates();

            Console.WriteLine("================================================");
        }

        /// <summary>
        /// Query INFO STATS from each shard and report garnet_hit_rate.
        /// </summary>
        private void ReportHitRates()
        {
            Console.WriteLine();
            Console.WriteLine("Hit Rates (from INFO STATS):");

            double totalHitRate = 0;
            int validShards = 0;

            for (int s = 0; s < shards.Length; s++)
            {
                try
                {
                    using var redis = ConnectionMultiplexer.Connect(
                        BenchUtils.GetConfig(shards[s].Address, shards[s].Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost, allowAdmin: true));

                    var server = redis.GetServer(new System.Net.IPEndPoint(System.Net.IPAddress.Parse(shards[s].Address), shards[s].Port));
                    var info = server.Info("STATS");

                    // Parse garnet_hit_rate from info
                    double hitRate = 0.0;
                    foreach (var section in info)
                    {
                        foreach (var kvp in section)
                        {
                            if (kvp.Key == "garnet_hit_rate")
                            {
                                double.TryParse(kvp.Value, out hitRate);
                                break;
                            }
                        }
                    }

                    var endpoint = $"{shards[s].Address}:{shards[s].Port}";
                    Console.WriteLine($"  {endpoint.PadRight(25)} garnet_hit_rate: {hitRate:F2}");

                    totalHitRate += hitRate;
                    validShards++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"  {shards[s].Address}:{shards[s].Port} - INFO STATS failed: {ex.Message}");
                }
            }

            if (validShards > 0)
            {
                double avgHitRate = totalHitRate / validShards;
                Console.WriteLine($"  {"Average:",-25} {avgHitRate:F2}");
            }
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
