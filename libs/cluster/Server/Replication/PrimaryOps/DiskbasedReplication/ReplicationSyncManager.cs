// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationSyncManager
    {
        /// <summary>
        /// Disk-based sync flow: acquires an existing checkpoint and broadcasts file data to all attached replicas.
        /// Does NOT pause checkpoints — avoids deadlock risk from on-demand checkpoint while paused.
        /// </summary>
        async Task DiskbasedSyncFlowAsync()
        {
            CheckpointEntry localEntry = null;
            try
            {
                // Acquire checkpoint entry with reader-lock and create AofSyncDrivers per session
                localEntry = await AcquireCheckpointEntryForBatchAsync().ConfigureAwait(false);

                // Broadcast checkpoint data to all active sessions
                await BroadcastCheckpointAsync(localEntry).ConfigureAwait(false);

                // Finalize: signal replicas to recover and start AOF sync
                await FinalizeReplicaRecoverAsync(localEntry).ConfigureAwait(false);
            }
            finally
            {
                localEntry?.RemoveReader();
            }
        }

        /// <summary>
        /// Acquires a checkpoint entry with reader-lock for the batch.
        /// Creates AofSyncDrivers for each session (which own the GarnetClientSession connections).
        /// </summary>
        async Task<CheckpointEntry> AcquireCheckpointEntryForBatchAsync()
        {
            CheckpointEntry cEntry;
            var iteration = 0;
            var numOdcAttempts = 0;
            const int maxOdcAttempts = 2;

            while (true)
            {
                logger?.LogInformation("AcquireCheckpointEntryForBatch iteration {iteration}", iteration);
                iteration++;

                var lastSaveTime = ClusterProvider.storeWrapper.lastSaveTime;

                // Retrieve latest checkpoint and lock it from deletion
                if (!ClusterProvider.replicationManager.TryGetLatestCheckpointEntryFromMemory(out cEntry))
                {
                    logger?.LogWarning("Could not acquire lock for existing checkpoint, retrying.");
                    await Task.Yield();
                    continue;
                }

                var startAofAddress = cEntry.GetMinAofCoveredAddress();

                // Validate checkpoint metadata
                var validMetadata = ValidateCheckpointForBatch(cEntry);

                if (ClusterProvider.serverOptions.OnDemandCheckpoint &&
                    (startAofAddress.AnyLesser(ClusterProvider.replicationManager.AofSyncDriverStore.TruncatedUntil) || !validMetadata))
                {
                    if (numOdcAttempts >= maxOdcAttempts && ClusterProvider.AllowDataLoss)
                    {
                        logger?.LogWarning("Failed to acquire checkpoint after {numOdcAttempts} on-demand checkpoint attempts.", numOdcAttempts);
                    }
                    else
                    {
                        cEntry.RemoveReader();
                        numOdcAttempts++;
                        logger?.LogInformation("Taking on-demand checkpoint for batch, attempt {numOdcAttempts}.", numOdcAttempts);
                        await ClusterProvider.storeWrapper.TakeOnDemandCheckpointAsync(lastSaveTime).ConfigureAwait(false);
                        await Task.Yield();
                        continue;
                    }
                }

                break;
            }

            // Register AofSyncDrivers for each session (creates GCS connections)
            var minServiceableAofAddress = cEntry.GetMinAofCoveredAddress();
            if (!ClusterProvider.replicationManager.AofSyncDriverStore.TryAddReplicationDrivers(Sessions, ref minServiceableAofAddress))
                throw new GarnetException("Failed to register AOF sync drivers for disk-based broadcast batch");

            // Connect all sessions
            for (var i = 0; i < NumSessions; i++)
            {
                if (!IsActive(i)) continue;
                try
                {
                    await Sessions[i].ConnectAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Sessions[i]?.SetStatus(SyncStatus.FAILED, ex.Message);
                    Sessions[i] = null;
                }
            }

            return cEntry;
        }

        /// <summary>
        /// Validates the checkpoint metadata against the first valid session's requirements.
        /// </summary>
        bool ValidateCheckpointForBatch(CheckpointEntry cEntry)
        {
            if (cEntry == null) return false;

            try
            {
                return ClusterProvider.replicationManager.TryAcquireSettledMetadataForMainStore(cEntry, out _, out _);
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Broadcasts checkpoint data to all active sessions using SnapshotTransmissionDriver in broadcast mode.
        /// Uses the GarnetClientSession instances owned by each ReplicaSyncSession's AofSyncDriver.
        /// </summary>
        async Task BroadcastCheckpointAsync(CheckpointEntry localEntry)
        {
            // Collect active GCS instances from sessions' AofSyncDrivers
            var gcsSessions = new List<GarnetClientSession>();
            var sessionIndices = new List<int>();

            for (var i = 0; i < NumSessions; i++)
            {
                if (!IsActive(i)) continue;
                gcsSessions.Add(Sessions[i].AofSyncDriver.PrimaryClient);
                sessionIndices.Add(i);
            }

            if (gcsSessions.Count == 0)
                throw new GarnetException("No active sessions for checkpoint broadcast");

            // Build snapshot readers
            if (!ClusterProvider.replicationManager.TryAcquireSettledMetadataForMainStore(localEntry, out var hlog_size, out var index_size))
                throw new GarnetException("Failed to validate checkpoint metadata for broadcast");

            var tsavoriteSnapshotReader = new TsavoriteSnapshotReader(
                ClusterProvider, localEntry, hlog_size, index_size,
                ClusterProvider.storeWrapper.serverOptions.ReplicaSyncTimeout, logger);

            List<ISnapshotReader> snapshotReaders = [tsavoriteSnapshotReader];

            if (ClusterProvider.storeWrapper.serverOptions.EnableRangeIndexPreview)
            {
                snapshotReaders.Add(new RangeIndexSnapshotReader(
                    ClusterProvider.storeWrapper.RangeIndexManager,
                    localEntry.metadata.storeHlogToken,
                    ClusterProvider.storeWrapper.serverOptions.ReplicaSyncTimeout, logger));
            }

            try
            {
                using var driver = new SnapshotTransmissionDriver(
                    snapshotReaders,
                    gcsSessions.ToArray(),
                    ClusterProvider.storeWrapper.serverOptions.ReplicaSyncTimeout,
                    logger);

                await driver.SendCheckpointAsync(cts.Token).ConfigureAwait(false);

                // Mark any sessions that failed during broadcast
                for (var i = 0; i < sessionIndices.Count; i++)
                {
                    if (!driver.IsSessionActive(i))
                    {
                        var idx = sessionIndices[i];
                        Sessions[idx]?.SetStatus(SyncStatus.FAILED, "Broadcast transmission failed");
                        Sessions[idx] = null;
                    }
                }
            }
            finally
            {
                foreach (var reader in snapshotReaders)
                    reader.Dispose();
            }
        }

        /// <summary>
        /// Sends recover command to each replica and starts AOF sync per-replica.
        /// Uses the GarnetClientSession from each session's AofSyncDriver.
        /// </summary>
        async Task FinalizeReplicaRecoverAsync(CheckpointEntry localEntry)
        {
            for (var i = 0; i < NumSessions; i++)
            {
                if (!IsActive(i)) continue;

                try
                {
                    var session = Sessions[i];
                    var gcs = session.AofSyncDriver.PrimaryClient;
                    var recoverFromRemote = true;
                    var checkpointAofBeginAddress = localEntry.GetMinAofCoveredAddress();
                    var beginAddress = checkpointAofBeginAddress;

                    var sameHistory2 = string.IsNullOrEmpty(ClusterProvider.replicationManager.PrimaryReplId2) &&
                        ClusterProvider.replicationManager.PrimaryReplId2.Equals(session.replicaAssignedPrimaryId);

                    var sameMainStoreCheckpointHistory = !string.IsNullOrEmpty(session.ReplicaCheckpointEntry?.metadata.storePrimaryReplId) &&
                        session.ReplicaCheckpointEntry.metadata.storePrimaryReplId.Equals(localEntry.metadata.storePrimaryReplId);

                    var replayAOFMap = ClusterProvider.storeWrapper.appendOnlyFile.ComputeAofSyncReplayAddress(
                        recoverFromRemote,
                        sameMainStoreCheckpointHistory,
                        sameHistory2,
                        ClusterProvider.replicationManager.ReplicationOffset2,
                        session.ReplicaAofBeginAddress,
                        session.ReplicaAofTailAddress,
                        beginAddress,
                        ref checkpointAofBeginAddress);

                    // Signal replica to recover
                    var resp = await gcs.ExecuteClusterBeginReplicaRecover(
                        recoverFromRemote,
                        replayAOFMap,
                        ClusterProvider.replicationManager.PrimaryReplId,
                        localEntry.ToByteArray(),
                        beginAddress.Span,
                        checkpointAofBeginAddress.Span)
                        .WaitAsync(ClusterProvider.storeWrapper.serverOptions.ReplicaSyncTimeout, cts.Token)
                        .ConfigureAwait(false);

                    var syncFromAofAddress = AofAddress.FromString(resp);

                    // Validate AOF data integrity
                    var possibleAofDataLoss = ClusterProvider.serverOptions.UseAofNullDevice ||
                        (ClusterProvider.serverOptions.FastAofTruncate && !ClusterProvider.serverOptions.OnDemandCheckpoint);
                    ClusterProvider.storeWrapper.appendOnlyFile.DataLossCheck(possibleAofDataLoss, syncFromAofAddress, logger);

                    // Start AOF sync for this replica (driver already registered in AcquireCheckpointEntryForBatchAsync)
                    if (!ClusterProvider.replicationManager.TryConnectToReplica(session.RemoteNodeId, ref syncFromAofAddress, session.AofSyncDriver, out _))
                        throw new GarnetException($"Failed connecting to replica {session.RemoteNodeId} for aofSync");
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "FinalizeReplicaRecover failed for session {index}", i);
                    Sessions[i]?.SetStatus(SyncStatus.FAILED, ex.Message);
                    Sessions[i] = null;
                }
            }
        }
    }
}