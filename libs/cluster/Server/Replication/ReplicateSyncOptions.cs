// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster.Server.Replication
{
    /// <summary>
    /// Holds options for initiating a replica sync between the current node and another node.
    /// </summary>
    /// <param name="NodeId">Id of node to replicate.</param>
    /// <param name="Background">If sync should occur on a background task.</param>
    /// <param name="Force">Force adding the current node as a Replica of <paramref name="NodeId"/>.  If <paramref name="TryAddReplica"/> is false, this is ignored.</param>
    /// <param name="TryAddReplica">Try to add the current node as a Replica of <paramref name="NodeId"/>.  If <paramref name="Force"/> is false, this can fail.</param>
    /// <param name="AllowReplicaResetOnFailure">If sync fails, reset the current node to a Primary.</param>
    /// <param name="UpgradeLock">If set, a <see cref="RecoveryStatus.ReadRole"/> read lock can be upgraded to a write lock.  After sync, the lock will be reset to a read lock.</param>
    internal readonly record struct ReplicateSyncOptions(string NodeId, bool Background, bool Force, bool TryAddReplica, bool AllowReplicaResetOnFailure, bool UpgradeLock)
    {
    }
}