// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
        SectorAlignedBufferPool bufferPool = null;
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
            bufferPool?.Free();
        }

        public bool ValidateMetadata(
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
        public async Task<bool> SendCheckpoint()
        {
            errorMsg = default;
            var storeCkptManager = clusterProvider.ReplicationLogCheckpointManager;
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
                clientName: nameof(ReplicaSyncSession.SendCheckpoint),
                logger: logger);
            CheckpointEntry localEntry = default;
            AofSyncDriver aofSyncDriver = null;

            try
            {
                logger?.LogInformation("Replica replicaId:{replicaId} requesting checkpoint replicaStoreVersion:{replicaStoreVersion}",
                    replicaNodeId, replicaCheckpointEntry.metadata.storeVersion);

                logger?.LogInformation("Attempting to acquire checkpoint");
                (localEntry, aofSyncDriver) = await AcquireCheckpointEntry();
                logger?.LogInformation("Checkpoint search completed");

                gcs.Connect((int)storeWrapper.serverOptions.ReplicaSyncTimeout.TotalMilliseconds, cts.Token);

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

                    // 1. send hlog file segments
                    if (clusterProvider.serverOptions.EnableStorageTier && hlog_size.hybridLogFileEndAddress > PageHeader.Size)
                    {
                        //send hlog file segments and object file segments
                        await SendFileSegments(gcs, localEntry.metadata.storeHlogToken, CheckpointFileType.STORE_HLOG, hlog_size.hybridLogFileStartAddress, hlog_size.hybridLogFileEndAddress);
                        if (hlog_size.hasSnapshotObjects)
                            await SendFileSegments(gcs, localEntry.metadata.storeHlogToken, CheckpointFileType.STORE_HLOG_OBJ, hlog_size.hybridLogObjectFileStartAddress, hlog_size.hybridLogObjectFileEndAddress);
                    }

                    // 2.Send index file segments
                    await SendFileSegments(gcs, localEntry.metadata.storeIndexToken, CheckpointFileType.STORE_INDEX, 0, index_size);

                    // 3. Send snapshot files
                    if (hlog_size.snapshotFileEndAddress > PageHeader.Size)
                    {
                        //send snapshot file segments and object file segments
                        await SendFileSegments(gcs, localEntry.metadata.storeHlogToken, CheckpointFileType.STORE_SNAPSHOT, 0, hlog_size.snapshotFileEndAddress);
                        if (hlog_size.hasSnapshotObjects)
                            await SendFileSegments(gcs, localEntry.metadata.storeHlogToken, CheckpointFileType.STORE_SNAPSHOT_OBJ, 0, hlog_size.snapshotObjectFileEndAddress);
                    }

                    // 4. Send delta log segments
                    var dlog_size = hlog_size.deltaLogTailAddress;
                    await SendFileSegments(gcs, localEntry.metadata.storeHlogToken, CheckpointFileType.STORE_DLOG, 0, dlog_size);

                    // 5.Send index metadata
                    await SendCheckpointMetadata(gcs, storeCkptManager, CheckpointFileType.STORE_INDEX, localEntry.metadata.storeIndexToken);

                    // 6. Send snapshot metadata
                    await SendCheckpointMetadata(gcs, storeCkptManager, CheckpointFileType.STORE_SNAPSHOT, localEntry.metadata.storeHlogToken);
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
                    beginAddress.ToByteArray(),
                    checkpointAofBeginAddress.ToByteArray()).WaitAsync(storeWrapper.serverOptions.ReplicaSyncTimeout, cts.Token).ConfigureAwait(false);
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
                if (localEntry != null)
                    logger?.LogCheckpointEntry(LogLevel.Error, "Error at attaching", localEntry);
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
                localEntry?.RemoveReader();
                gcs.Dispose();
            }
            return true;
        }

        private async Task<(CheckpointEntry, AofSyncDriver)> AcquireCheckpointEntry()
        {
            AofSyncDriver aofSyncDriver;
            CheckpointEntry cEntry;

            // This loop tries to provide the following two guarantees
            // 1. Retrieve latest checkpoint and lock it to prevent deletion before it is send to the replica
            // 2. Guard against truncation of AOF in between the retrieval of the checkpoint metadata and start of the aofSyncTask
            var iteration = 0;
            var numOdcAttempts = 0;
            const int maxOdcAttempts = 2;
            while (true)
            {
                logger?.LogInformation("AcquireCheckpointEntry iteration {iteration}", iteration);
                iteration++;

                aofSyncDriver = null;
                cEntry = default;

                // Acquire startSaveTime to identify if an external task might have taken the checkpoint for us
                // This is only useful for MainMemoryReplication where we might have multiple replicas attaching
                // We want to share the on-demand checkpoint and ensure that only one replica should succeed when calling TakeOnDemandCheckpoint
                var lastSaveTime = storeWrapper.lastSaveTime;

                var exceptionInjected = ExceptionInjectionHelper.TriggerCondition(ExceptionInjectionType.Replication_Acquire_Checkpoint_Entry_Fail_Condition);

                // Retrieve latest checkpoint and lock it from deletion operations
                var addedReader = !exceptionInjected && clusterProvider.replicationManager.TryGetLatestCheckpointEntryFromMemory(out cEntry);

                if (!addedReader)
                {
                    // Fail to acquire lock, could mean that a writer might be trying to delete
                    logger?.LogWarning("Could not acquire lock for existing checkpoint, retrying.");

                    // Go back to re-acquire the latest checkpoint
                    await Task.Yield();
                    continue;
                }

#if DEBUG
                // Only on Debug mode
                await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Replication_Wait_After_Checkpoint_Acquisition);
#endif

                // Calculate the minimum start address covered by this checkpoint
                var startAofAddress = cEntry.GetMinAofCoveredAddress();

                // If there is possible AOF data loss and we need to take an on-demand checkpoint,
                // then we should take the checkpoint before we register the sync task, because
                // TryAddReplicationTask is guaranteed to return true in this scenario.
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
                        cEntry.RemoveReader();
                        numOdcAttempts++;
                        logger?.LogInformation("Taking on-demand checkpoint, attempt {numOdcAttempts}.", numOdcAttempts);
                        await storeWrapper.TakeOnDemandCheckpoint(lastSaveTime);
                        await Task.Yield();
                        continue;
                    }
                }

                // Enqueue AOF sync task with startAofAddress to prevent future AOF truncations
                // and check if truncation has happened in between retrieving the latest checkpoint and enqueuing the aofSyncTask
                if (clusterProvider.replicationManager.AofSyncDriverStore.TryAddReplicationDriver(replicaNodeId, ref startAofAddress, out aofSyncDriver))
                    break;

                // Unlock last checkpoint because associated startAofAddress is no longer available
                cEntry.RemoveReader();

                // Go back to re-acquire checkpoint
                await Task.Yield();
            }

            return (cEntry, aofSyncDriver);
        }

        private async Task SendCheckpointMetadata(GarnetClientSession gcs, GarnetClusterCheckpointManager ckptManager, CheckpointFileType fileType, Guid fileToken)
        {
            var retryCount = validateMetadataMaxRetryCount;
            while (true)
            {
                try
                {
                    logger?.LogInformation("<Begin sending checkpoint metadata {fileToken} {fileType}", fileToken, fileType);
                    var checkpointMetadata = Array.Empty<byte>();
                    if (fileToken != default)
                    {
                        switch (fileType)
                        {
                            case CheckpointFileType.STORE_SNAPSHOT:
                                checkpointMetadata = ckptManager.GetLogCheckpointMetadata(fileToken, null, true, -1);
                                break;
                            case CheckpointFileType.STORE_INDEX:
                                checkpointMetadata = ckptManager.GetIndexCheckpointMetadata(fileToken);
                                break;
                        }
                    }

                    var resp = await gcs.ExecuteClusterSendCheckpointMetadata(fileToken.ToByteArray(), (int)fileType, checkpointMetadata).WaitAsync(storeWrapper.serverOptions.ReplicaSyncTimeout, cts.Token).ConfigureAwait(false);
                    if (!resp.Equals("OK"))
                    {
                        logger?.LogError("Primary error at SendCheckpointMetadata {resp}", resp);
                        throw new Exception($"Primary error at SendCheckpointMetadata {resp}");
                    }

                    logger?.LogInformation("<Complete sending checkpoint metadata {fileToken} {fileType}", fileToken, fileType);
                    break; // Exit loop if metadata sent successfully
                }
                catch (Exception ex)
                {
                    logger?.LogError("SendCheckpointMetadata Error: {msg}", ex.Message);
                    if (retryCount-- <= 0)
                        throw new Exception("Max retry attempts reached for checkpoint metadata sending.");
                }
                await Task.Yield();
            }
        }

        private async Task SendFileSegments(GarnetClientSession gcs, Guid token, CheckpointFileType type, long startAddress, long endAddress, int batchSize = 1 << 17)
        {
            var fileTokenBytes = token.ToByteArray();
            var device = clusterProvider.replicationManager.GetInitializedSegmentFileDevice(token, type);

            Debug.Assert(device != null);
            var (shouldInitialize, segmentSizeBits) = ReplicationManager.ShouldInitialize(type, clusterProvider.serverOptions);
            if (shouldInitialize)
                batchSize = (int)Math.Min(batchSize, 1L << segmentSizeBits);
            string resp;

            logger?.LogInformation("<Begin sending checkpoint file segments {guid} {type} {startAddress} {endAddress} {batchSize}", token, type, startAddress, endAddress, batchSize);
            try
            {
                while (startAddress < endAddress)
                {
                    var num_bytes = startAddress + batchSize < endAddress ? batchSize : (int)(endAddress - startAddress);
                    var (pbuffer, readBytes) = await ReadInto(device, (ulong)startAddress, num_bytes).ConfigureAwait(false);

                    resp = await gcs.ExecuteClusterSendCheckpointFileSegment(fileTokenBytes, (int)type, startAddress, pbuffer.GetSlice(readBytes)).
                        WaitAsync(storeWrapper.serverOptions.ReplicaSyncTimeout, cts.Token).ConfigureAwait(false);
                    if (!resp.Equals("OK"))
                    {
                        logger?.LogError("Primary error at SendFileSegments {type} {resp}", type, resp);
                        throw new Exception($"Primary error at SendFileSegments {type} {resp}");
                    }
                    pbuffer.Return();
                    startAddress += readBytes;
                }

                // Send last empty package to indicate end of transmission and let replica dispose IDevice
                resp = await gcs.ExecuteClusterSendCheckpointFileSegment(fileTokenBytes, (int)type, startAddress, []).
                    WaitAsync(storeWrapper.serverOptions.ReplicaSyncTimeout, cts.Token).ConfigureAwait(false);
                if (!resp.Equals("OK"))
                {
                    logger?.LogError("Primary error at SendFileSegments {type} {resp}", type, resp);
                    throw new Exception($"Primary error at SendFileSegments {type} {resp}");
                }
            }
            finally
            {
                device.Dispose();
            }
            logger?.LogInformation("<Complete sending checkpoint file segments {guid} {type} {startAddress} {endAddress}", token, type, startAddress, endAddress);
        }

        /// <summary>
        /// Note: will read potentially more data (based on sector alignment)
        /// </summary>
        /// <param name="device"></param>
        /// <param name="address"></param>
        /// <param name="size"></param>
        /// <param name="segmentId"></param>
        private async Task<(SectorAlignedMemory, int)> ReadInto(IDevice device, ulong address, int size, int segmentId = -1)
        {
            bufferPool ??= new SectorAlignedBufferPool(1, (int)device.SectorSize);

            long numBytesToRead = size;
            numBytesToRead = (numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1);

            var pbuffer = bufferPool.Get((int)numBytesToRead);
            unsafe
            {
                if (segmentId == -1)
                    device.ReadAsync(address, (IntPtr)pbuffer.aligned_pointer, (uint)numBytesToRead, IOCallback, null);
                else
                    device.ReadAsync(segmentId, address, (IntPtr)pbuffer.aligned_pointer, (uint)numBytesToRead, IOCallback, null);
            }
            _ = await signalCompletion.WaitAsync(storeWrapper.serverOptions.ReplicaSyncTimeout, cts.Token).ConfigureAwait(false);
            return (pbuffer, (int)numBytesToRead);
        }

        private unsafe void IOCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                var errorMessage = Tsavorite.core.Utility.GetCallbackErrorMessage(errorCode, numBytes, context);
                logger?.LogError("[ReplicaSyncSession] OverlappedStream GetQueuedCompletionStatus error: {errorCode} msg: {errorMessage}", errorCode, errorMessage);
            }
            _ = signalCompletion.Release();
        }
    }

    internal static unsafe class SectorAlignedMemoryExtensions
    {
        public static Span<byte> GetSlice(this SectorAlignedMemory pbuffer, int length)
        {
            return new Span<byte>(pbuffer.aligned_pointer, length);
        }
    }
}