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
}
