// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Resp.benchmark
{
    /// <summary>
    /// Worker maintains providers for all shards and randomly distributes operations across them.
    /// Each worker is independent and runs on its own thread.
    /// Total providers = worker-count × shard-count
    /// Total connections = providers × (1 + replicas-per-shard)
    /// </summary>
    public class Worker
    {
        private readonly ClientRequestProvider[] providers;
        private readonly Random rng;
        private readonly Options opts;
        private readonly int workerId;

        /// <summary>
        /// Creates a new worker with providers for all shards.
        /// Each provider maintains primary + replica connections for its shard.
        /// </summary>
        /// <param name="id">Unique worker ID</param>
        /// <param name="shards">All cluster shards</param>
        /// <param name="opts">Benchmark options</param>
        public Worker(int id, PrimaryInfo[] shards, Options opts)
        {
            this.workerId = id;
            this.opts = opts;
            this.rng = new Random(id); // Seeded with worker ID for reproducibility

            // Create one provider per shard
            providers = new ClientRequestProvider[shards.Length];
            for (var s = 0; s < shards.Length; s++)
            {
                var shard = shards[s];

                // Round-robin replica assignment within this worker's providers
                // If shard has 2 replicas and we create providers [0,1,2,3] for that shard,
                // they get replicas [0,1,0,1]
                ReplicaInfo assignedReplica = null;
                if (shard.Replicas.Count > 0)
                {
                    assignedReplica = shard.Replicas[s % shard.Replicas.Count];
                }

                // Provider index is (workerId * shardCount + shardIndex) for global uniqueness
                var providerIndex = (id * shards.Length) + s;

                providers[s] = new ClientRequestProvider(shard, assignedReplica, opts, providerIndex, id);
            }
        }

        /// <summary>
        /// Randomly selects a provider from the worker's provider pool.
        /// Each call is independent - distribution is probabilistic over time.
        /// </summary>
        /// <returns>A randomly selected provider</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ClientRequestProvider SelectRandomProvider()
        {
            var index = rng.Next(providers.Length);
            return providers[index];
        }

        /// <summary>
        /// Online mode: continuously generate and execute operations until time expires.
        /// Operations are distributed randomly across all shards.
        /// In pipeline mode, requests are sent without waiting for responses;
        /// pending responses are completed when the provider is revisited.
        ///
        /// Key difference from sharded mode:
        /// - Worker drives the workload loop (not individual providers)
        /// - Each iteration selects a random provider
        /// - Provider handles single-operation execution with primary/replica routing
        /// </summary>
        /// <param name="startSignal">Signal to synchronize start across all workers</param>
        /// <param name="runTime">How long to run</param>
        public void RunOnline(ManualResetEventSlim startSignal, TimeSpan runTime)
        {
            // Wait for all workers to be ready
            startSignal.Wait();

            var sw = Stopwatch.StartNew();

            if (opts.Pipeline)
            {
                // Pipeline mode: broadcast a request to every provider (shard),
                // then complete pending on all of them.
                while (sw.Elapsed < runTime)
                {
                    // Send phase: issue one request to each shard
                    for (var i = 0; i < providers.Length; i++)
                        providers[i].SendSingleOnlineOperation();

                    // Complete phase: wait for all responses
                    for (var i = 0; i < providers.Length; i++)
                        providers[i].CompletePendingAndRecordOnlineMetrics();
                }
            }
            else
            {
                while (sw.Elapsed < runTime)
                {
                    var provider = SelectRandomProvider();
                    provider.ExecuteSingleOnlineOperation();
                }
            }
        }

        /// <summary>
        /// Offline mode: execute pre-generated batches until completion.
        /// Batches are distributed randomly across all shards.
        /// In pipeline mode, broadcasts a batch to every provider then completes all.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        public void RunOffline(CancellationToken token)
        {
            if (opts.Pipeline)
            {
                // Pipeline mode: broadcast a batch to every provider (shard),
                // then complete pending on all of them.
                while (!token.IsCancellationRequested)
                {
                    // Send phase: issue one batch to each shard
                    for (var i = 0; i < providers.Length; i++)
                        providers[i].SendSingleOfflineBatch();

                    // Complete phase: wait for all responses
                    for (var i = 0; i < providers.Length; i++)
                        providers[i].CompletePendingAndRecordOfflineMetrics();
                }
            }
            else
            {
                while (!token.IsCancellationRequested)
                {
                    var provider = SelectRandomProvider();
                    provider.ExecuteSingleOfflineBatch();
                }
            }
        }

        /// <summary>
        /// Aggregates metrics from all providers owned by this worker.
        /// </summary>
        /// <returns>Aggregated worker metrics</returns>
        public WorkerMetrics GetMetrics()
        {
            long totalOps = 0;
            long totalPrimaryOps = 0;
            long totalReplicaOps = 0;

            foreach (var provider in providers)
            {
                // These properties need to be added to ClientRequestProvider
                totalPrimaryOps += provider.PrimaryOps;
                totalReplicaOps += provider.ReplicaOps;
            }

            totalOps = totalPrimaryOps + totalReplicaOps;

            return new WorkerMetrics
            {
                WorkerId = workerId,
                TotalOperations = totalOps,
                PrimaryOperations = totalPrimaryOps,
                ReplicaOperations = totalReplicaOps
            };
        }

        /// <summary>
        /// Gets the provider for a specific shard index.
        /// Used for LoadData coordination (Phase 5).
        /// </summary>
        /// <param name="shardIndex">Index of the shard</param>
        /// <returns>Provider for that shard</returns>
        public ClientRequestProvider GetProvider(int shardIndex)
        {
            return providers[shardIndex];
        }

        /// <summary>
        /// Number of providers owned by this worker (equals shard count).
        /// </summary>
        public int ProviderCount => providers.Length;

        /// <summary>
        /// Gets all providers owned by this worker.
        /// </summary>
        public ClientRequestProvider[] Providers => providers;
    }

    /// <summary>
    /// Aggregated metrics for a single worker.
    /// </summary>
    public struct WorkerMetrics
    {
        public int WorkerId { get; set; }
        public long TotalOperations { get; set; }
        public long PrimaryOperations { get; set; }
        public long ReplicaOperations { get; set; }
    }
}