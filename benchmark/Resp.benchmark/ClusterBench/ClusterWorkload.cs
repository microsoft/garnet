// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Resp.benchmark
{
    /// <summary>
    /// Encapsulates pre-generated request buffers for mixed write/read workload in offline mode.
    /// Used when replicas exist and --replica-ops-percent > 0 with write operations.
    /// 
    /// Execution model: Choose ONE request per iteration based on ReadUseReplica flag:
    ///   - ReadUseReplica[i] = true  → execute ReplicaRequests[i] (read) to replica
    ///   - ReadUseReplica[i] = false → execute PrimaryRequests[i] (write) to primary
    /// </summary>
    public struct ClusterWorkload
    {
        /// <summary>
        /// Requests executed when ReadUseReplica[i] = false (writes: SET, MSET, etc.).
        /// Always routes to primary.
        /// </summary>
        public Request[] PrimaryRequests;

        /// <summary>
        /// Requests executed when ReadUseReplica[i] = true (reads: GET, MGET, etc. for THE SAME KEYS).
        /// Routes to replica.
        /// </summary>
        public Request[] ReplicaRequests;

        /// <summary>
        /// Per-iteration routing decision: which request array to use.
        /// True = execute ReplicaRequests[i] to replica
        /// False = execute PrimaryRequests[i] to primary
        /// Computed during PrepareBuffers() based on --replica-ops-percent.
        /// </summary>
        public bool[] ReadUseReplica;
    }

    /// <summary>
    /// Immutable, read-only offline workload buffers shared by every
    /// <see cref="ClientRequestProvider"/> that targets the same shard.
    ///
    /// The request byte buffers depend only on the shard's slot range and the benchmark
    /// options, and are never mutated during the run, so a single instance can be safely
    /// shared across all workers/providers of a shard. This keeps benchmark memory
    /// proportional to shard-count rather than provider-count (worker-count × shard-count),
    /// which matters when running with many workers and a large --dbsize.
    ///
    /// Because <see cref="ClusterWorkload.ReadUseReplica"/> is computed once per shard
    /// (rather than once per provider), replica routing decisions are shared across providers.
    /// The overall read distribution still approximates --replica-ops-percent.
    /// </summary>
    public struct SharedOfflineWorkload
    {
        /// <summary>Pre-generated request buffers (primary + optional replica) for the shard.</summary>
        public ClusterWorkload Workload;

        /// <summary>Number of pre-generated batches in the shared collection. Each worker
        /// randomly selects a batch in [0, BatchCount) per iteration, so concurrent workers
        /// targeting the same shard touch different keys at any instant despite sharing buffers.</summary>
        public int BatchCount;

        /// <summary>LightClient connection buffer size needed to hold the largest request.</summary>
        public int OfflineBufferSize;
    }
}