// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
            for (int s = 0; s < shards.Length; s++)
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
                int providerIndex = id * shards.Length + s;

                providers[s] = new ClientRequestProvider(shard, assignedReplica, opts, providerIndex, id);
            }
        }

        /// <summary>
        /// Randomly selects a provider from the worker's provider pool.
        /// Each call is independent - distribution is probabilistic over time.
        /// </summary>
        /// <returns>A randomly selected provider</returns>
        private ClientRequestProvider SelectRandomProvider()
        {
            int index = rng.Next(providers.Length);
            return providers[index];
        }

        /// <summary>
        /// Online mode: continuously generate and execute operations until cancellation.
        /// Operations are distributed randomly across all shards.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        public void RunOnline(CancellationToken token)
        {
            throw new NotImplementedException("Phase 2: Online mode implementation");
        }

        /// <summary>
        /// Offline mode: execute pre-generated batches until completion.
        /// Batches are distributed randomly across all shards.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        public void RunOffline(CancellationToken token)
        {
            throw new NotImplementedException("Phase 3: Offline mode implementation");
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