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
    public partial class ClusterBench : IDisposable
    {
        const int LoadDataThreads = 8;

        static readonly long HISTOGRAM_LOWER_BOUND = 1;
        static readonly long HISTOGRAM_UPPER_BOUND = TimeStamp.Seconds(100);

        readonly Options opts;
        readonly ILoggerFactory loggerFactory;
        readonly ILogger logger;

        PrimaryInfo[] shards;
        ClientRequestProvider[] providers;  // Used in sharded mode
        Worker[] workers;                   // Used in worker pool mode

        public ClusterBench(Options opts, ILoggerFactory loggerFactory = null)
        {
            this.opts = opts;
            this.loggerFactory = loggerFactory;
            this.logger = loggerFactory?.CreateLogger("ClusterBench");
        }

        /// <summary>
        /// Discover cluster topology and initialize providers based on architecture mode.
        ///
        /// Sharded architecture (default):
        ///   - Creates (threads-per-shard × shard-count) providers
        ///   - Each provider serves one shard with primary + replica connections
        ///
        /// Worker pool architecture (--worker-pool):
        ///   - Creates (thread-count) workers
        ///   - Each worker maintains providers for ALL shards
        ///   - Workers randomly distribute operations across shards
        ///
        /// Round-robin assignment example:
        ///   - Shard with 2 replicas and 6 clients:
        ///     Client 0 → Replica 0, Client 1 → Replica 1, Client 2 → Replica 0,
        ///     Client 3 → Replica 1, Client 4 → Replica 0, Client 5 → Replica 1
        ///
        ///   - Each client knows which replica endpoint to use for routing read operations
        ///   - The --replica-read-percent percentage determines how often reads go to replicas
        ///   - Across all clients for a shard, approximately X% of read operations go to replicas
        /// </summary>
        public void DiscoverTopology()
        {
            ValidateReplicaReadOptions();

            var clusterManager = new ClusterManager(opts);
            shards = clusterManager.DiscoverPrimaryShards();

            if (opts.Pool)
            {
                CreateWorkerPool();
            }
            else
            {
                CreateShardedProviders();
            }
        }

        /// <summary>
        /// Creates worker pool architecture: fixed number of workers, each maintaining providers for all shards.
        /// Workers randomly select shards for each operation.
        /// Total providers = worker-count × shard-count
        /// Total connections = providers × (1 + replicas-per-shard)
        /// </summary>
        private void CreateWorkerPool()
        {
            var workerCount = opts.NumThreads.First();
            workers = new Worker[workerCount];

            for (var w = 0; w < workerCount; w++)
            {
                workers[w] = new Worker(w, shards, opts);
            }

            // Calculate totals for display
            var totalProviders = workerCount * shards.Length;
            var totalReplicas = shards.Sum(s => s.Replicas.Count);
            var avgReplicasPerShard = shards.Length > 0 ? totalReplicas / shards.Length : 0;
            var workersWithReplicas = workers.Sum(w => w.Providers.Count(p => p.HasReplica));
            var totalConnections = totalProviders + workersWithReplicas;

            PrintWorkerPoolConfiguration(workerCount, totalProviders, totalConnections, workersWithReplicas);
        }

        /// <summary>
        /// Creates sharded architecture: threads-per-shard providers.
        /// Each provider serves one shard exclusively.
        /// Total providers = threads-per-shard × shard-count
        /// Total connections = providers × (1 + replicas-per-shard)
        /// </summary>
        private void CreateShardedProviders()
        {
            var threadsPerShard = opts.NumThreads.First();
            var totalProviders = threadsPerShard * shards.Length;
            providers = new ClientRequestProvider[totalProviders];

            var idx = 0;
            for (var s = 0; s < shards.Length; s++)
            {
                var shard = shards[s];
                var replicaCount = shard.Replicas.Count;

                for (var t = 0; t < threadsPerShard; t++)
                {
                    // Round-robin replica assignment: worker t gets replica (t % replicaCount)
                    // If no replicas exist, assignedReplica will be null
                    // If replicas exist, always assign them (they will serve reads based on --replica-read-percent)
                    ReplicaInfo assignedReplica = null;
                    if (replicaCount > 0)
                    {
                        assignedReplica = shard.Replicas[t % replicaCount];
                    }

                    providers[idx] = new ClientRequestProvider(shard, assignedReplica, opts, idx, t);
                    idx++;
                }
            }

            PrintConfiguration(threadsPerShard, totalProviders);
        }

        /// <summary>
        /// Validates replica read percentage option.
        /// Valid range: 0-100 (percentage of reads sent to replicas).
        /// If replicas exist, they always serve reads - this just controls the percentage.
        /// </summary>
        private void ValidateReplicaReadOptions()
        {
            if (opts.ReplicaOpsPercent < 0 || opts.ReplicaOpsPercent > 100)
            {
                throw new Exception($"Invalid --replica-read-percent value: {opts.ReplicaOpsPercent}. Valid range is 0-100 (percentage).");
            }
        }

        private void PrintConfiguration(int threadsPerShard, int totalProviders)
        {
            var mode = opts.Online ? "Online" : "Offline";
            var pool = opts.Pool ? "Yes" : "No";
            var tls = opts.EnableTLS ? "Yes" : "No";
            var skipLoad = opts.SkipLoad ? "Yes" : "No";
            var itp = opts.IntraThreadParallelism;
            var batch = opts.BatchSize.First();

            // Count total replicas and workers with replica assignments
            var totalReplicas = shards.Sum(s => s.Replicas.Count);
            var workersWithReplicas = providers.Count(p => p.HasReplica);
            var replicaReads = $"{opts.ReplicaOpsPercent}%";
            var totalConnections = totalProviders + workersWithReplicas; // primary + replica connections

            Console.WriteLine();
            Console.WriteLine("=========== Cluster Benchmark Configuration ===========");
            Console.WriteLine($"{"Mode: " + mode,-28}{"Client: " + opts.Client,-28}");
            Console.WriteLine($"{"Pool: " + pool,-28}{"Op: " + opts.Op,-28}");
            Console.WriteLine($"{"Threads: " + threadsPerShard + " (per shard)",-28}{"ITP: " + itp,-28}");
            Console.WriteLine($"{"DB Size: " + opts.DbSize,-28}{"Batch: " + batch,-28}");
            Console.WriteLine($"{"Key Length: " + opts.KeyLength,-28}{"Value Length: " + opts.ValueLength,-28}");
            Console.WriteLine($"{"Runtime: " + opts.RunTime + "s",-28}{"TLS: " + tls,-28}");
            Console.WriteLine($"{"Shards: " + shards.Length,-28}{"Connections: " + totalConnections + " (threads x shards" + (workersWithReplicas > 0 ? " + replicas)" : ")"),-28}");
            Console.WriteLine($"{"Skip Load: " + skipLoad,-28}{"Auth: " + (string.IsNullOrEmpty(opts.Auth) ? "No" : "Yes"),-28}");
            Console.WriteLine($"{"Replicas: " + totalReplicas,-28}{"Replica Reads: " + replicaReads,-28}");
            Console.WriteLine($"{"Workers: " + totalProviders,-28}{"Broadcast: " + (opts.Broadcast ? "Yes" : "No"),-28}");
            Console.WriteLine("=======================================================");
            Console.WriteLine();

            // Topology table
            Console.WriteLine($"  {"Role",-10}| {"Endpoint",-23}| {"Slots",-7}| {"     Range",-17}| {"Replicas",-10}| {"Prefix",-10}");
            Console.WriteLine($"  {new string('-', 10)}+{new string('-', 24)}+{new string('-', 8)}+{new string('-', 18)}+{new string('-', 11)}+{new string('-', 11)}");

            for (int s = 0; s < shards.Length; s++)
            {
                var shard = shards[s];
                var endpoint = $"{shard.Address}:{shard.Port}";
                var range = FormatSlotRanges(shard.SlotRanges);
                var prefix = providers[s * threadsPerShard].KeyPrefix;
                var replicaCount = shard.Replicas.Count;

                // Print primary with "primary" label
                Console.WriteLine($"  {"primary",-10}| {endpoint,-23}| {shard.TotalSlots,-7}| {range,-17}| {replicaCount,-10}| {prefix,-10}");

                // Print replicas with proper alignment
                foreach (var replica in shard.Replicas)
                {
                    var replicaEndpoint = $"{replica.Address}:{replica.Port}";
                    Console.WriteLine($"  {"replica",-10}| {replicaEndpoint,-23}| {"",-7}| {"",-17}| {"",-10}| {"",-10}");
                }
            }

            Console.WriteLine();

            // Info message about replica assignment (only if replicas exist)
            if (totalReplicas > 0)
            {
                Console.WriteLine($"  [INFO] Replica read routing: ~{opts.ReplicaOpsPercent}% of read operations per shard will target replicas.");
                Console.WriteLine($"         Round-robin assignment: Each client assigned to one of {totalReplicas / shards.Length} replica(s) per shard.");
                Console.WriteLine($"         Clients with replicas: {workersWithReplicas}/{totalProviders}");
                Console.WriteLine();
            }
        }

        private void PrintWorkerPoolConfiguration(int workerCount, int totalProviders, int totalConnections, int workersWithReplicas)
        {
            var mode = opts.Online ? "Online" : "Offline";
            var pool = "Yes";
            var tls = opts.EnableTLS ? "Yes" : "No";
            var skipLoad = opts.SkipLoad ? "Yes" : "No";
            var itp = opts.IntraThreadParallelism;
            var batch = opts.BatchSize.First();

            // Count total replicas
            var totalReplicas = shards.Sum(s => s.Replicas.Count);
            var replicaReads = $"{opts.ReplicaOpsPercent}%";

            Console.WriteLine();
            Console.WriteLine("=========== Cluster Benchmark Configuration ===========");
            Console.WriteLine($"{"Mode: " + mode,-28}{"Client: " + opts.Client,-28}");
            Console.WriteLine($"{"Pool: " + pool,-28}{"Op: " + opts.Op,-28}");
            Console.WriteLine($"{"Threads: " + workerCount,-28}{"ITP: " + itp,-28}");
            Console.WriteLine($"{"DB Size: " + opts.DbSize,-28}{"Batch: " + batch,-28}");
            Console.WriteLine($"{"Key Length: " + opts.KeyLength,-28}{"Value Length: " + opts.ValueLength,-28}");
            Console.WriteLine($"{"Runtime: " + opts.RunTime + "s",-28}{"TLS: " + tls,-28}");
            Console.WriteLine($"{"Shards: " + shards.Length,-28}{"Connections: " + totalConnections + " (threads x shards" + (workersWithReplicas > 0 ? " + replicas)" : ")"),-28}");
            Console.WriteLine($"{"Skip Load: " + skipLoad,-28}{"Auth: " + (string.IsNullOrEmpty(opts.Auth) ? "No" : "Yes"),-28}");
            Console.WriteLine($"{"Replicas: " + totalReplicas,-28}{"Replica Reads: " + replicaReads,-28}");
            Console.WriteLine($"{"Workers: " + workerCount,-28}{"Broadcast: " + (opts.Broadcast ? "Yes" : "No"),-28}");
            Console.WriteLine("=======================================================");
            Console.WriteLine();

            // Connection count warning for large configurations
            if (totalConnections > 10000)
            {
                Console.WriteLine($"  [WARNING] High connection count ({totalConnections:N0} connections)");
                Console.WriteLine($"            Ensure ulimit is sufficient: ulimit -n {totalConnections * 2}");
                Console.WriteLine();
            }

            // Topology table (same as sharded mode)
            Console.WriteLine($"  {"Role",-10}| {"Endpoint",-23}| {"Slots",-7}| {"     Range",-17}| {"Replicas",-10}");
            Console.WriteLine($"  {new string('-', 10)}+{new string('-', 24)}+{new string('-', 8)}+{new string('-', 18)}+{new string('-', 11)}");

            for (var s = 0; s < shards.Length; s++)
            {
                var shard = shards[s];
                var endpoint = $"{shard.Address}:{shard.Port}";
                var range = FormatSlotRanges(shard.SlotRanges);
                var replicaCount = shard.Replicas.Count;

                // Print primary with "primary" label
                Console.WriteLine($"  {"primary",-10}| {endpoint,-23}| {shard.TotalSlots,-7}| {range,-17}| {replicaCount,-10}");

                // Print replicas with proper alignment
                foreach (var replica in shard.Replicas)
                {
                    var replicaEndpoint = $"{replica.Address}:{replica.Port}";
                    Console.WriteLine($"  {"replica",-10}| {replicaEndpoint,-23}| {"",-7}| {"",-17}| {"",-10}");
                }
            }

            Console.WriteLine();

            // Info message about worker pool mode
            if (totalReplicas > 0)
            {
                Console.WriteLine($"  [INFO] Worker pool mode: {workerCount} workers, each handling all {shards.Length} shards.");
                Console.WriteLine($"         Operations randomly distributed across shards.");
                Console.WriteLine($"         Replica read routing: ~{opts.ReplicaOpsPercent}% of reads per shard target replicas.");
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
        /// <summary>
        /// Load data into the cluster. Supports both sharded and worker pool architectures.
        /// In worker pool mode, only the first worker's providers are used to load data
        /// to avoid duplicate loading.
        /// </summary>
        public void LoadData()
        {
            Console.WriteLine(" Loading keys...");
            var sw = Stopwatch.StartNew();

            if (opts.Pool)
            {
                // Worker pool mode: Use first worker's providers to load data (one per shard)
                var firstWorker = workers[0];
                var loadThreads = new Thread[shards.Length];

                for (var s = 0; s < shards.Length; s++)
                {
                    var provider = firstWorker.GetProvider(s);
                    loadThreads[s] = new Thread(() => provider.LoadData());
                    loadThreads[s].Start();
                }

                foreach (var t in loadThreads)
                    t.Join();

                // Aggregate loaded keys from first worker's providers
                long totalKeys = 0;
                var maxEndpointLen = shards.Max(sh => $"{sh.Address}:{sh.Port}".Length);

                for (var s = 0; s < shards.Length; s++)
                {
                    var provider = firstWorker.GetProvider(s);
                    var shardKeys = provider.KeysLoaded;
                    totalKeys += shardKeys;

                    var endpoint = $"{shards[s].Address}:{shards[s].Port}";
                    Console.WriteLine($"   Loaded {shardKeys} keys to {endpoint.PadLeft(maxEndpointLen)}");
                }

                Console.WriteLine($" Total: {totalKeys} keys ({shards.Length} shards, worker pool mode) in {sw.ElapsedMilliseconds}ms");
                Console.WriteLine();

                // Validate DBSIZE if under 1MB threshold
                ValidateDBSize(totalKeys);
            }
            else
            {
                // Sharded mode: Parallelize across LoadDataThreads
                Parallel.ForEach(providers, new ParallelOptions { MaxDegreeOfParallelism = LoadDataThreads },
                    p => p.LoadData());

                sw.Stop();

                // Per-shard summary
                var threadsPerShard = opts.NumThreads.First();
                long totalKeys = 0;
                var maxEndpointLen = shards.Max(s => $"{s.Address}:{s.Port}".Length);

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
        }

        /// <summary>
        /// Run the benchmark (offline or online based on opts.Online).
        /// </summary>
        public void Run()
        {
            if (opts.Pool)
            {
                // Worker pool architecture
                if (opts.Online)
                    RunWorkerPoolOnline();
                else
                    RunWorkerPoolOffline();
            }
            else
            {
                // Sharded architecture
                if (opts.Online)
                    RunOnline();
                else
                    RunOffline();
            }
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
            bool allValid = true;

            for (int s = 0; s < shards.Length; s++)
            {
                long expectedKeys = 0;

                if (opts.Pool)
                {
                    // Worker pool mode: Only worker[0] loaded data
                    expectedKeys = workers[0].GetProvider(s).KeysLoaded;
                }
                else
                {
                    // Sharded mode: Sum across all threads for this shard
                    int threadsPerShard = opts.NumThreads.First();
                    for (int t = 0; t < threadsPerShard; t++)
                        expectedKeys += providers[s * threadsPerShard + t].KeysLoaded;
                }

                try
                {
                    var config = BenchUtils.GetConfig(shards[s].Address, shards[s].Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost, allowAdmin: true);
                    config.AbortOnConnectFail = false;
                    using var redis = ConnectionMultiplexer.Connect(config);

                    var server = redis.GetServer(shards[s].Address, shards[s].Port);
                    var actualKeys = server.DatabaseSize();

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
            long totalPrimaryOps = 0;
            long totalReplicaOps = 0;
            long totalBytesSent = 0;
            long totalBytesReceived = 0;

            // Per-shard summary
            Console.WriteLine($"{"Shard",-8}{"Endpoint",-25}{"Threads",-10}{"Ops",-15}{"Ops/sec",-15}");
            Console.WriteLine(new string('-', 73));

            var threadsPerShard = opts.NumThreads.First();
            var batchSize = opts.BatchSize.First();
            var keysPerOp = (opts.Op is OpType.MGET or OpType.MSET) ? batchSize : 1;

            for (var s = 0; s < shards.Length; s++)
            {
                long shardOps = 0;
                long shardBytesSent = 0;
                long shardBytesRcvd = 0;
                for (var t = 0; t < threadsPerShard; t++)
                {
                    var provider = providers[s * threadsPerShard + t];
                    shardOps += provider.OpsCompleted * keysPerOp;
                    shardBytesSent += provider.BytesSent;
                    shardBytesRcvd += provider.BytesReceived;
                    totalPrimaryOps += provider.PrimaryOps * keysPerOp;
                    totalReplicaOps += provider.ReplicaOps * keysPerOp;
                    summary.Add(provider.Histogram);
                }
                totalOps += shardOps;
                totalBytesSent += shardBytesSent;
                totalBytesReceived += shardBytesRcvd;

                var shardOpsPerSec = shardOps / totalElapsed.TotalSeconds;
                Console.WriteLine($"{s,-8}{shards[s].Address + ":" + shards[s].Port,-25}{threadsPerShard,-10}{shardOps,-15}{shardOpsPerSec,-15:N0}");
            }

            Console.WriteLine(new string('-', 73));
            var totalOpsPerSec = totalOps / totalElapsed.TotalSeconds;
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
            var logicalBytesPerOp = Math.Max(opts.KeyLength, 8) + Math.Max(opts.ValueLength, 8);
            var dataGBps = (totalOps * logicalBytesPerOp) / totalElapsed.TotalSeconds / (1024.0 * 1024 * 1024);
            var totalWireBytes = totalBytesSent + totalBytesReceived;
            var wireGBps = totalWireBytes / totalElapsed.TotalSeconds / (1024.0 * 1024 * 1024);

            Console.WriteLine();
            Console.WriteLine($"Duration: {totalElapsed.TotalSeconds:F1}s");
            Console.WriteLine($"Total throughput: {totalOpsPerSec:N0} ops/sec ({totalOpsPerSec / 1000:N1} Kops/sec)");
            Console.WriteLine($"Data throughput:  {dataGBps:N3} GB/sec (logical: key={opts.KeyLength}B + val={opts.ValueLength}B = {logicalBytesPerOp}B/op)");
            Console.WriteLine($"Wire throughput:  {wireGBps:N3} GB/sec (RESP sent: {totalBytesSent:N0} B, received: {totalBytesReceived:N0} B)");

            // Show replica metrics when replicas are configured in the cluster
            if (totalOps > 0 && shards.Any(s => s.Replicas.Count > 0))
            {
                var actualReplicaPercent = totalOps > 0 ? (totalReplicaOps * 100.0) / totalOps : 0.0;
                var primaryOpsPerSec = totalPrimaryOps / totalElapsed.TotalSeconds;
                var replicaOpsPerSec = totalReplicaOps / totalElapsed.TotalSeconds;

                // Determine operation types for display
                string primaryOpType = opts.Op.ToString();
                string replicaOpType = opts.Op.ToString();

                // If mixed workload (write op with replicas and percentage > 0), show read operation type
                if (opts.ReplicaOpsPercent > 0 && OperationClassifier.IsWriteOperation(opts.Op))
                {
                    var readOp = OperationClassifier.GetCorrespondingReadOperation(opts.Op);
                    if (readOp.HasValue)
                        replicaOpType = readOp.Value.ToString();
                }

                Console.WriteLine($"Replica routing: {actualReplicaPercent:F1}% actual (target: {opts.ReplicaOpsPercent}% for reads)");
                Console.WriteLine($"Primary throughput:     {primaryOpsPerSec:N0} ops/sec ({primaryOpsPerSec / 1000:N1} Kops/sec) ({primaryOpType})");
                Console.WriteLine($"Replica throughput:     {replicaOpsPerSec:N0} ops/sec ({replicaOpsPerSec / 1000:N1} Kops/sec) ({replicaOpType})");
            }

            // Report hit rates from INFO STATS
            ReportHitRates();

            Console.WriteLine("================================================");
        }

        private void PrintWorkerPoolFinalReport(TimeSpan totalElapsed)
        {
            Console.WriteLine();
            Console.WriteLine("========== WORKER POOL BENCHMARK RESULTS ==========");
            Console.WriteLine();

            var summary = new LongHistogram(1, TimeStamp.Seconds(100), 2);
            long totalOps = 0;
            long totalPrimaryOps = 0;
            long totalReplicaOps = 0;
            long totalBytesSent = 0;
            long totalBytesReceived = 0;

            // Determine keysPerOp first for table display
            var batchSize = opts.BatchSize.First();
            var keysPerOp = (opts.Op is OpType.MGET or OpType.MSET) ? batchSize : 1;

            // Per-worker summary
            Console.WriteLine($"{"Worker",-10}{"Providers",-12}{"Total Ops",-15}{"Primary Ops",-15}{"Replica Ops",-15}");
            Console.WriteLine(new string('-', 67));

            foreach (var worker in workers)
            {
                var metrics = worker.GetMetrics();
                totalOps += metrics.TotalOperations;
                totalPrimaryOps += metrics.PrimaryOperations;
                totalReplicaOps += metrics.ReplicaOperations;

                // Aggregate histograms and bytes
                foreach (var provider in worker.Providers)
                {
                    summary.Add(provider.Histogram);
                    totalBytesSent += provider.BytesSent;
                    totalBytesReceived += provider.BytesReceived;
                }

                // Display key counts (multiply by keysPerOp for MGET/MSET)
                Console.WriteLine($"{metrics.WorkerId,-10}{worker.ProviderCount,-12}{metrics.TotalOperations * keysPerOp,-15}{metrics.PrimaryOperations * keysPerOp,-15}{metrics.ReplicaOperations * keysPerOp,-15}");
            }

            Console.WriteLine(new string('-', 67));
            // Multiply by keysPerOp before displaying totals
            Console.WriteLine($"{"Total",-10}{workers.Length * shards.Length,-12}{totalOps * keysPerOp,-15}{totalPrimaryOps * keysPerOp,-15}{totalReplicaOps * keysPerOp,-15}");

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

            // Throughput summary - multiply totalOps by keysPerOp to get total keys
            totalOps *= keysPerOp;
            totalPrimaryOps *= keysPerOp;
            totalReplicaOps *= keysPerOp;
            var totalOpsPerSec = totalOps / totalElapsed.TotalSeconds;

            var logicalBytesPerOp = Math.Max(opts.KeyLength, 8) + Math.Max(opts.ValueLength, 8);  // Per key (SlotKeyGenerator enforces min 8)
            var dataGBps = (totalOps * logicalBytesPerOp) / totalElapsed.TotalSeconds / (1024.0 * 1024 * 1024);
            var totalWireBytes = totalBytesSent + totalBytesReceived;
            var wireGBps = totalWireBytes / totalElapsed.TotalSeconds / (1024.0 * 1024 * 1024);

            Console.WriteLine();
            Console.WriteLine($"Duration: {totalElapsed.TotalSeconds:F1}s");
            Console.WriteLine($"Total throughput: {totalOpsPerSec:N0} ops/sec ({totalOpsPerSec / 1000:N1} Kops/sec)");
            Console.WriteLine($"Data throughput:  {dataGBps:N3} GB/sec (logical: key={opts.KeyLength}B + val={opts.ValueLength}B = {logicalBytesPerOp}B/op)");
            Console.WriteLine($"Wire throughput:  {wireGBps:N3} GB/sec (RESP sent: {totalBytesSent:N0} B, received: {totalBytesReceived:N0} B)");
            Console.WriteLine($"Workers: {workers.Length}, Shards: {shards.Length}, Providers: {workers.Length * shards.Length}");

            // Show replica metrics when replicas are configured in the cluster
            if (totalOps > 0 && shards.Any(s => s.Replicas.Count > 0))
            {
                var actualReplicaPercent = totalOps > 0 ? (totalReplicaOps * 100.0) / totalOps : 0.0;
                var primaryOpsPerSec = totalPrimaryOps / totalElapsed.TotalSeconds;
                var replicaOpsPerSec = totalReplicaOps / totalElapsed.TotalSeconds;

                // Determine operation types for display
                string primaryOpType = opts.Op.ToString();
                string replicaOpType = opts.Op.ToString();

                // If mixed workload (write op with replicas and percentage > 0), show read operation type
                if (opts.ReplicaOpsPercent > 0 && OperationClassifier.IsWriteOperation(opts.Op))
                {
                    var readOp = OperationClassifier.GetCorrespondingReadOperation(opts.Op);
                    if (readOp.HasValue)
                        replicaOpType = readOp.Value.ToString();
                }

                Console.WriteLine($"Replica routing: {actualReplicaPercent:F1}% actual (target: {opts.ReplicaOpsPercent}% for reads)");
                Console.WriteLine($"Primary throughput:     {primaryOpsPerSec:N0} ops/sec ({primaryOpsPerSec / 1000:N1} Kops/sec) ({primaryOpType})");
                Console.WriteLine($"Replica throughput:     {replicaOpsPerSec:N0} ops/sec ({replicaOpsPerSec / 1000:N1} Kops/sec) ({replicaOpType})");
            }

            // Report hit rates from INFO STATS
            ReportHitRates();

            Console.WriteLine("====================================================");
        }

        /// <summary>
        /// Query INFO STATS from each shard and report garnet_hit_rate.
        /// </summary>
        private void ReportHitRates()
        {
            Console.WriteLine();
            Console.WriteLine("Hit Rates (from INFO STATS):");

            double totalHitRate = 0;
            var validShards = 0;

            for (var s = 0; s < shards.Length; s++)
            {
                try
                {
                    using var redis = ConnectionMultiplexer.Connect(
                        BenchUtils.GetConfig(shards[s].Address, shards[s].Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost, allowAdmin: true));

                    var server = redis.GetServer(new System.Net.IPEndPoint(System.Net.IPAddress.Parse(shards[s].Address), shards[s].Port));
                    var info = server.Info("STATS");

                    // Parse garnet_hit_rate from info
                    var hitRate = 0.0;
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
                var avgHitRate = totalHitRate / validShards;
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