// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    /// <summary>
    /// Recovery status
    /// </summary>
    public enum RecoveryStatus : byte
    {
        /// <summary>
        /// No recovery
        /// </summary>
        NoRecovery,
        /// <summary>
        /// Recovery at initilization
        /// </summary>
        InitializeRecover,
        /// <summary>
        /// Recovery at cluster replicate
        /// </summary>
        ClusterReplicate,
        /// <summary>
        /// Recovery at cluster failover
        /// </summary>
        ClusterFailover,
        /// <summary>
        /// Recovery at replicaof no one
        /// </summary>
        ReplicaOfNoOne
    }
}