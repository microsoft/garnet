// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.cluster.Server.Replication;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

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

        /// <summary>
        /// Attaches this node to a primary on behalf of CLUSTER REPLICATE, which -- unlike REPLICAOF -- leaves
        /// the replica background tasks to be started by the sync itself.
        /// </summary>
        /// <param name="syncOpts">Options describing the primary to replicate and how to sync.</param>
        async ValueTask ClusterReplicateAsync(ReplicateSyncOptions syncOpts)
        {
            var (success, errorMessage) = await (clusterProvider.serverOptions.ReplicaDisklessSync ?
                clusterProvider.replicationManager.TryReplicateDisklessSyncAsync(this, syncOpts) :
                clusterProvider.replicationManager.TryReplicateDiskbasedSyncAsync(this, syncOpts)).ConfigureAwait(false);

            SetPendingOutcome(success, errorMessage);
        }

        /// <summary>
        /// Begins serving a disk-based sync to a replica and records whether it took.
        /// </summary>
        /// <param name="replicaNodeId">Node id of the replica being synced.</param>
        /// <param name="replicaAssignedPrimaryId">Node id the replica believes is its primary.</param>
        /// <param name="replicaCheckpointEntry">The replica's current checkpoint.</param>
        /// <param name="replicaAofBeginAddress">The replica's AOF begin address.</param>
        /// <param name="replicaAofTailAddress">The replica's AOF tail address.</param>
        async ValueTask BeginDiskbasedSyncAsync(
            string replicaNodeId,
            string replicaAssignedPrimaryId,
            CheckpointEntry replicaCheckpointEntry,
            AofAddress replicaAofBeginAddress,
            AofAddress replicaAofTailAddress)
        {
            var (success, errorMessage) = await clusterProvider.replicationManager.TryBeginDiskbasedSyncAsync(
                replicaNodeId, replicaAssignedPrimaryId, replicaCheckpointEntry, replicaAofBeginAddress, replicaAofTailAddress).ConfigureAwait(false);

            SetPendingOutcome(success, errorMessage);
        }

        /// <summary>
        /// Hands the recovery of this replica from the checkpoint the primary sent it to the thread pool, and
        /// answers with the replication offset it reached.
        /// </summary>
        /// <param name="recoverStoreFromToken">Whether to recover the store from the received checkpoint tokens.</param>
        /// <param name="replayAOFMap">Non-zero when the local AOF is replayed over the recovered store.</param>
        /// <param name="primaryReplicaId">Replication id of the primary being synced from.</param>
        /// <param name="checkpoint">The checkpoint the primary sent.</param>
        /// <param name="beginAddress">AOF begin address to initialize the log with.</param>
        /// <param name="tailAddress">AOF address the recovered store reaches.</param>
        async ValueTask BeginReplicaRecoverAsync(
            bool recoverStoreFromToken,
            ulong replayAOFMap,
            string primaryReplicaId,
            CheckpointEntry checkpoint,
            AofAddress beginAddress,
            AofAddress tailAddress)
        {
            var (replicationOffset, errorMessage) = await clusterProvider.replicationManager.TryReplicaDiskbasedRecoveryAsync(
                recoverStoreFromToken, replayAOFMap, primaryReplicaId, checkpoint, beginAddress, tailAddress).ConfigureAwait(false);
            SetPendingAofAddress(replicationOffset, errorMessage);
        }

        /// <summary>
        /// Begins a diskless sync against the replica described by the sync metadata, and answers with the
        /// address the caller should resume from.
        /// </summary>
        /// <param name="syncMetadata">Metadata describing the peer requesting the attach.</param>
        /// <param name="replicationOffset">Address to answer with when the attach succeeds.</param>
        async ValueTask BeginDisklessSyncAsync(SyncMetadata syncMetadata, AofAddress replicationOffset)
        {
            var (_, errorMessage) = await clusterProvider.replicationManager.TryBeginDisklessSyncAsync(syncMetadata).ConfigureAwait(false);
            SetPendingAofAddress(replicationOffset, errorMessage);
        }

        /// <summary>
        /// Waits until this replica has caught up to the given primary replication offset, and answers with
        /// the offset it reached.
        /// </summary>
        /// <param name="primaryReplicationOffset">The primary offset to catch up to.</param>
        async ValueTask WaitForReplicationOffsetAsync(AofAddress primaryReplicationOffset)
        {
            var offset = await clusterProvider.replicationManager.WaitForReplicationOffsetAsync(primaryReplicationOffset).ConfigureAwait(false);
            SetPendingAofAddress(offset);
        }

        /// <summary>
        /// Waits for the primary's stream to be applied, or for background replay to catch up with it, and then for the
        /// pulse that this makes applicable. Gives up the replay that <see cref="ProcessPrimaryStreamAsync"/> took, on whichever
        /// thread the wait ends on.
        /// </summary>
        /// <param name="driver">The replay driver of the sublog that the stream belongs to.</param>
        /// <param name="replaying">The wait to complete.</param>
        /// <param name="pulseFollows">Whether a pulse that has become applicable is applied once the wait completes.</param>
        /// <param name="ownsReplay">Whether the replay taken by <see cref="ProcessPrimaryStreamAsync"/> is to be given up.</param>
        async ValueTask FinishReplayAsync(ReplicaReplayDriver driver, ValueTask replaying, bool pulseFollows, bool ownsReplay)
        {
            try
            {
                await replaying.ConfigureAwait(false);
                if (pulseFollows)
                    await driver.ThrottleAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ProcessPrimaryStream");
                throw new GarnetException(ex.Message, ex, LogLevel.Warning, clientResponse: false);
            }
            finally
            {
                if (ownsReplay)
                    driver.SuspendReplay();
            }
        }

        /// <summary>
        /// Waits for every sublog of the primary to have connected. A timeout is not an error: the primary
        /// streams on once this replica answers, whether or not the rest of the sublogs arrived in time.
        /// </summary>
        /// <param name="sublogsInitialized">Completes once the replay drivers of all sublogs are initialized.</param>
        async ValueTask AwaitSublogsAsync(ValueTask<bool> sublogsInitialized)
            => _ = await sublogsInitialized.ConfigureAwait(false);
    }
}