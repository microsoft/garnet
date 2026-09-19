// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class ReplicaSyncSession(
        StoreWrapper storeWrapper,
        ClusterProvider clusterProvider,
        AofAddress replicaAofBeginAddress,
        AofAddress replicaAofTailAddress,
        SyncMetadata replicaSyncMetadata = null,
        CancellationToken token = default,
        string replicaNodeId = null,
        string replicaAssignedPrimaryId = null,
        CheckpointEntry replicaCheckpointEntry = null,
        ILogger logger = null) : IDisposable
    {
        readonly StoreWrapper storeWrapper = storeWrapper;
        readonly ClusterProvider clusterProvider = clusterProvider;
        public readonly SyncMetadata replicaSyncMetadata = replicaSyncMetadata;
        readonly CancellationToken token = token;
        readonly CancellationTokenSource cts = new();
        readonly SemaphoreSlim signalCompletion = new(0);

        public readonly string replicaNodeId = replicaNodeId;
        public readonly string replicaAssignedPrimaryId = replicaAssignedPrimaryId;
        private readonly AofAddress replicaAofBeginAddress = replicaAofBeginAddress;
        private readonly AofAddress replicaAofTailAddress = replicaAofTailAddress;

        private readonly CheckpointEntry replicaCheckpointEntry = replicaCheckpointEntry;

        private readonly ILogger logger = logger;

        public string errorMsg = default;

        const int validateMetadataMaxRetryCount = 10;

        public void Dispose()
        {
            AofSyncDriver?.DisposeClient();
            AofSyncDriver = null;
            cts.Cancel();
            cts.Dispose();
            signalCompletion?.Dispose();
        }

        private bool ValidateMetadata(
            CheckpointEntry localEntry,
            out long index_size,
            out LogFileInfo hlog_size,
            out bool skipLocalMainStoreCheckpoint)
        {
            hlog_size = default;
            index_size = -1L;

            // Local and remote checkpoints are of same history if both of the following hold
            // 1. There is a checkpoint available at remote node
            // 2. Remote and local checkpoints contain the same PrimaryReplId
            var sameMainStoreCheckpointHistory = !string.IsNullOrEmpty(replicaCheckpointEntry.metadata.storePrimaryReplId) && replicaCheckpointEntry.metadata.storePrimaryReplId.Equals(localEntry.metadata.storePrimaryReplId);
            // We will not send the latest local checkpoint if any of the following hold
            // 1. Local node does not have any checkpoints
            // 2. Local checkpoint is of same version and history as the remote checkpoint
            skipLocalMainStoreCheckpoint = localEntry.metadata.storeHlogToken == default || (sameMainStoreCheckpointHistory && localEntry.metadata.storeVersion == replicaCheckpointEntry.metadata.storeVersion);

            // Acquire metadata for main store
            // If failed then this checkpoint is not usable because it is corrupted
            if (!skipLocalMainStoreCheckpoint && !clusterProvider.replicationManager.TryAcquireSettledMetadataForMainStore(localEntry, out hlog_size, out index_size))
                return false;

            return true;
        }

        /// <summary>
        /// Start sending the latest checkpoint to replica
        /// </summary>
        public async Task<bool> SendCheckpointAsync()
        {
            errorMsg = default;
            var current = clusterProvider.clusterManager.CurrentConfig;
            var (address, port) = current.GetWorkerAddressFromNodeId(replicaNodeId);

            if (address == null || port == -1)
            {
                errorMsg = $"PRIMARY-ERR don't know about replicaId: {replicaNodeId}";
                logger?.LogError("{errorMsg}", errorMsg);
                return false;
            }

            GarnetClientSession gcs = new(
                new IPEndPoint(IPAddress.Parse(address), port),
                clusterProvider.replicationManager.GetRSSNetworkBufferSettings,
                clusterProvider.replicationManager.GetNetworkPool,
                tlsOptions: clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                authUsername: clusterProvider.ClusterUsername,
                authPassword: clusterProvider.ClusterPassword,
                clientName: nameof(ReplicaSyncSession.SendCheckpointAsync),
                logger: logger);
            CheckpointLease<CheckpointEntry> checkpointLease = null;
            AofRetentionLease aofRetentionLease = null;
            AofSyncDriver aofSyncDriver = null;

            try
            {
                logger?.LogInformation("Replica replicaId:{replicaId} requesting checkpoint replicaStoreVersion:{replicaStoreVersion}",
                    replicaNodeId, replicaCheckpointEntry.metadata.storeVersion);

                logger?.LogInformation("Attempting to acquire checkpoint");

                (checkpointLease, aofRetentionLease) = await AcquireCheckpointEntryAsync().ConfigureAwait(false);
                var localEntry = checkpointLease.Value;
                logger?.LogInformation("Checkpoint search completed");

                await gcs.ConnectAsync((int)storeWrapper.serverOptions.ReplicaSyncTimeout.TotalMilliseconds, cts.Token).ConfigureAwait(false);

                var index_size = -1L;
                var hlog_size = default(LogFileInfo);
                var skipLocalMainStoreCheckpoint = false;
                var retryCount = validateMetadataMaxRetryCount;
                while (!ValidateMetadata(localEntry, out index_size, out hlog_size, out skipLocalMainStoreCheckpoint))
                {
                    logger?.LogError("Failed to validate metadata. Retrying....");
                    await Task.Yield();
                    if (retryCount-- <= 0)
                        throw new GarnetException("Failed to validate metadata!");
                }

                #region sendStoresSnapshotData
                if (!skipLocalMainStoreCheckpoint)
                {
                    logger?.LogInformation("Sending main store checkpoint {version} {storeHlogToken} {storeIndexToken} to replica", localEntry.metadata.storeVersion, localEntry.metadata.storeHlogToken, localEntry.metadata.storeIndexToken);

                    var checkpointFileProvider = new ClusterCheckpointFileTransferProvider(
                        clusterProvider.serverOptions,
                        clusterProvider.ReplicationLogCheckpointManager);
                    using var checkpointTransmissionDriver = new SnapshotTransmissionDriver(gcs, storeWrapper.serverOptions.ReplicaSyncTimeout, logger);
                    checkpointTransmissionDriver.AddReader(new TsavoriteSnapshotReader(checkpointFileProvider, localEntry, hlog_size, index_size, storeWrapper.serverOptions.ReplicaSyncTimeout, logger));

                    // Add RangeIndex files if RI is enabled
                    if (storeWrapper.serverOptions.EnableRangeIndexPreview)
                    {
                        checkpointTransmissionDriver.AddReader(new RangeIndexSnapshotReader(
                            clusterProvider.rangeIndexManager,
                            localEntry.metadata.storeHlogToken,
                            hlog_size.hybridLogFileStartAddress,
                            hlog_size.hybridLogFileEndAddress,
                            logger));
                    }

                    await checkpointTransmissionDriver.SendCheckpointAsync(cts.Token).ConfigureAwait(false);
                }
                #endregion

                #region startAofSync
                var recoverFromRemote = !skipLocalMainStoreCheckpoint;
                var checkpointAofBeginAddress = localEntry.GetMinAofCoveredAddress();
                var beginAddress = checkpointAofBeginAddress;
                var sameHistory2 = string.IsNullOrEmpty(clusterProvider.replicationManager.PrimaryReplId2) && clusterProvider.replicationManager.PrimaryReplId2.Equals(replicaAssignedPrimaryId);

                // Calculate replay AOF range
                var sameMainStoreCheckpointHistory = !string.IsNullOrEmpty(replicaCheckpointEntry.metadata.storePrimaryReplId) && replicaCheckpointEntry.metadata.storePrimaryReplId.Equals(localEntry.metadata.storePrimaryReplId);
                var replayAOFMap = clusterProvider.storeWrapper.appendOnlyFile.ComputeAofSyncReplayAddress(
                    recoverFromRemote,
                    sameMainStoreCheckpointHistory,
                    sameHistory2,
                    clusterProvider.replicationManager.ReplicationOffset2,
                    replicaAofBeginAddress,
                    replicaAofTailAddress,
                    beginAddress,
                    ref checkpointAofBeginAddress);

                // Signal replica to recover from local/remote checkpoint
                // Make replica replayAOF if needed and replay from provided beginAddress to RecoveredReplication Address
                var resp = await gcs.ExecuteClusterBeginReplicaRecover(
                    !skipLocalMainStoreCheckpoint,
                    replayAOFMap,
                    clusterProvider.replicationManager.PrimaryReplId,
                    localEntry.ToByteArray(),
                    beginAddress.Span,
                    checkpointAofBeginAddress.Span).WaitAsync(storeWrapper.serverOptions.ReplicaSyncTimeout, cts.Token).ConfigureAwait(false);
                var syncFromAofAddress = AofAddress.FromString(resp);

                // Assert that AOF address the replica will be requesting can be served, except in case of:
                // Possible AOF data loss: { using null AOF device } OR { main memory replication AND no on-demand checkpoints }
                var possibleAofDataLoss = clusterProvider.serverOptions.UseAofNullDevice ||
                    (clusterProvider.serverOptions.FastAofTruncate && !clusterProvider.serverOptions.OnDemandCheckpoint);
                clusterProvider.storeWrapper.appendOnlyFile.DataLossCheck(possibleAofDataLoss, syncFromAofAddress, logger);

                // Check what happens if we fail after recovery and start AOF stream
                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Replication_Fail_Before_Background_AOF_Stream_Task_Start);

                // We have already added the iterator for the covered address above but replica might request an address
                // that is ahead of the covered address so we should start streaming from that address in order not to
                // introduce duplicate insertions.
                if (!clusterProvider.replicationManager.AofSyncDriverStore.TryAddReplicationDriver(replicaNodeId, ref syncFromAofAddress, out aofSyncDriver))
                    throw new GarnetException("Failed trying to try update replication task");
                if (!clusterProvider.replicationManager.TryConnectToReplica(replicaNodeId, ref syncFromAofAddress, aofSyncDriver, out _))
                    throw new GarnetException("Failed connecting to replica for aofSync");
                #endregion
            }
            catch (Exception ex)
            {
                if (checkpointLease != null)
                    logger?.LogCheckpointEntry(LogLevel.Error, "Error at attaching", checkpointLease.Value);
                else
                    logger?.LogError("Error at attaching: {ex}", ex.Message);

                if (aofSyncDriver != null)
                    _ = clusterProvider.replicationManager.AofSyncDriverStore.TryRemove(aofSyncDriver);
                errorMsg = ex.Message;// this is error sent to remote client
                return false;
            }
            finally
            {
                // At this point the replica has received the most recent checkpoint data
                // and recovered from it so primary can release and delete it safely
                aofRetentionLease?.Dispose();
                checkpointLease?.Dispose();
                gcs.Dispose();
            }
            return true;
        }

        private async Task<(CheckpointLease<CheckpointEntry>, AofRetentionLease)> AcquireCheckpointEntryAsync()
        {
            CheckpointLease<CheckpointEntry> checkpointLease;
            AofRetentionLease aofRetentionLease;
            CheckpointEntry cEntry;

            // This loop tries to provide the following two guarantees
            // 1. Lease the latest checkpoint to prevent deletion before it is sent to the replica.
            // 2. Guard against AOF truncation between checkpoint selection and live streaming.
            var iteration = 0;
            var numOdcAttempts = 0;
            const int maxOdcAttempts = 2;

            while (true)
            {
                cts.Token.ThrowIfCancellationRequested();
                logger?.LogInformation("AcquireCheckpointEntry iteration {iteration}", iteration);
                iteration++;

                checkpointLease = null;
                aofRetentionLease = null;
                cEntry = default;

                // Acquire startSaveTime to identify if an external task might have taken the checkpoint for us
                // This is only useful for MainMemoryReplication where we might have multiple replicas attaching
                // We want to share the on-demand checkpoint and ensure that only one replica should succeed when calling TakeOnDemandCheckpoint
                var lastSaveTime = storeWrapper.lastSaveTime;

                var exceptionInjected = ExceptionInjectionHelper.TriggerCondition(ExceptionInjectionType.Replication_Acquire_Checkpoint_Entry_Fail_Condition);

                // Retrieve latest checkpoint and lock it from deletion operations
                var addedReader = !exceptionInjected && clusterProvider.replicationManager.TryAcquireLatestCheckpoint(out checkpointLease);

                if (!addedReader)
                {
                    // Fail to acquire lock, could mean that a writer might be trying to delete
                    logger?.LogWarning("Could not acquire lock for existing checkpoint, retrying.");

                    // Go back to re-acquire the latest checkpoint
                    await Task.Yield();
                    continue;
                }
                var transferLeases = false;
                try
                {
                    cEntry = checkpointLease.Value;

#if DEBUG
                    // Only on Debug mode
                    await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Replication_Wait_After_Checkpoint_Acquisition).ConfigureAwait(false);
#endif

                    // Calculate the minimum start address covered by this checkpoint
                    var startAofAddress = cEntry.GetMinAofCoveredAddress();

                    // If there is possible AOF data loss and we need to take an on-demand checkpoint,
                    // then take the checkpoint before registering the retention lease.
                    var validMetadata = ValidateMetadata(cEntry, out _, out _, out _);
                    if (clusterProvider.serverOptions.OnDemandCheckpoint &&
                        (startAofAddress.AnyLesser(clusterProvider.replicationManager.AofSyncDriverStore.TruncatedUntil) || !validMetadata))
                    {
                        if (numOdcAttempts >= maxOdcAttempts && clusterProvider.AllowDataLoss)
                        {
                            logger?.LogWarning("Failed to acquire checkpoint after {numOdcAttempts} on-demand checkpoint attempts. Possible data loss, startAofAddress:{startAofAddress} < truncatedUntil:{truncatedUntil}.", numOdcAttempts, startAofAddress, clusterProvider.replicationManager.AofSyncDriverStore.TruncatedUntil);
                        }
                        else
                        {
                            numOdcAttempts++;
                            logger?.LogInformation("Taking on-demand checkpoint, attempt {numOdcAttempts}.", numOdcAttempts);
                            await storeWrapper.TakeOnDemandCheckpointAsync(lastSaveTime).ConfigureAwait(false);
                            await Task.Yield();
                            continue;
                        }
                    }

                    // Pin the checkpoint boundary before releasing the checkpoint lease. Registration
                    // is serialized with AOF truncation, closing the snapshot-to-stream race.
                    if (clusterProvider.replicationManager.AofSyncDriverStore.TryAcquireRetention(startAofAddress, out aofRetentionLease))
                    {
                        transferLeases = true;
                        return (checkpointLease, aofRetentionLease);
                    }
                }
                finally
                {
                    if (!transferLeases)
                    {
                        aofRetentionLease?.Dispose();
                        checkpointLease.Dispose();
                    }
                }

                // Go back to re-acquire checkpoint
                await Task.Yield();
            }
        }
    }
}