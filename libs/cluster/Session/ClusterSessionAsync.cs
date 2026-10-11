// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using Garnet.cluster.Server.Replication;

namespace Garnet.cluster
{
    // Not declared unsafe: an async method body cannot sit in an unsafe context, and the unsafe modifier
    // on a partial type declaration applies only to the part that carries it.
    internal sealed partial class ClusterSession
    {
        /// <summary>
        /// Waits for the cluster to settle on a new config epoch.
        /// </summary>
        async ValueTask BumpEpochAsync()
            => _ = await UnsafeBumpAndWaitForEpochTransitionAsync().ConfigureAwait(false);

        /// <summary>
        /// Starts a migration task and records whether it took.
        /// </summary>
        /// <param name="mSession">The migration session to start.</param>
        async ValueTask StartMigrationAsync(MigrateSession mSession)
        {
            var (success, errorMessage) = await mSession.TryStartMigrationTaskAsync().ConfigureAwait(false);
            SetPendingOutcome(success, errorMessage);
        }

        /// <summary>
        /// Makes this replica a primary. The recovery lock is already held on entry and is released here.
        /// </summary>
        async ValueTask ReplicaOfNoOneAsync()
        {
            try
            {
                clusterProvider.clusterManager.TryResetReplica();
                clusterProvider.replicationManager.TryUpdateForFailover();

                clusterProvider.replicationManager.ResetReplicaReplayDriverStore();
                _ = await UnsafeBumpAndWaitForEpochTransitionAsync().ConfigureAwait(false);

                await clusterProvider.storeWrapper.SuspendReplicaOnlyTasksAsync().ConfigureAwait(false);
                clusterProvider.storeWrapper.StartPrimaryTasks();
            }
            finally
            {
                clusterProvider.replicationManager.EndRecovery(RecoveryStatus.NoRecovery, downgradeLock: false);
            }
        }

        /// <summary>
        /// Attaches this node to a primary and records whether the sync succeeded.
        /// </summary>
        /// <param name="syncOpts">Options describing the primary to replicate and how to sync.</param>
        async ValueTask ReplicateAsync(ReplicateSyncOptions syncOpts)
        {
            var (success, errorMessage) = await (clusterProvider.serverOptions.ReplicaDisklessSync ?
                clusterProvider.replicationManager.TryReplicateDisklessSyncAsync(this, syncOpts) :
                clusterProvider.replicationManager.TryReplicateDiskbasedSyncAsync(this, syncOpts)).ConfigureAwait(false);

            clusterProvider.storeWrapper.StartReplicaTasks();
            SetPendingOutcome(success, errorMessage);
        }
    }
}