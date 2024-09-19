// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class ReplicaSyncSession(StoreWrapper storeWrapper, ClusterProvider clusterProvider, string remoteNodeId, string remote_primary_replid, CheckpointEntry remoteEntry, long replicaAofBeginAddress, long replicaAofTailAddress, ILogger logger = null) : IDisposable
    {
        readonly StoreWrapper storeWrapper = storeWrapper;
        readonly ClusterProvider clusterProvider = clusterProvider;
        readonly CancellationTokenSource ctsCheckpointRetrievalSession = new();
        private SectorAlignedBufferPool bufferPool = null;
        private readonly SemaphoreSlim semaphore = new(0);

        public readonly string remoteNodeId = remoteNodeId;
        public readonly string remote_primary_replid = remote_primary_replid;
        private readonly long replicaAofBeginAddress = replicaAofBeginAddress;
        private readonly long replicaAofTailAddress = replicaAofTailAddress;

        private readonly CheckpointEntry remoteEntry = remoteEntry;

        private readonly ILogger logger = logger;

        public string errorMsg = default;

        public void Dispose()
        {
            ctsCheckpointRetrievalSession.Cancel();
            ctsCheckpointRetrievalSession.Dispose();
            semaphore?.Dispose();
            bufferPool?.Free();
        }

        /// <summary>
        /// Start sending the latest checkpoint to replica
        /// </summary>
        public async Task<bool> SendCheckpoint()
        {
            errorMsg = default;
            var retryCount = 0;
            var storeCkptManager = clusterProvider.GetReplicationLogCheckpointManager(StoreType.Main);
            var objectStoreCkptManager = clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object);
            var current = clusterProvider.clusterManager.CurrentConfig;
            var (address, port) = current.GetWorkerAddressFromNodeId(remoteNodeId);

            if (address == null || port == -1)
            {
                errorMsg = $"PRIMARY-ERR don't know about replicaId: {remoteNodeId}";
                logger?.LogError("{errorMsg}", errorMsg);
                return false;
            }

            GarnetClientSession gcs = new(address, port, clusterProvider.replicationManager.GetNetworkBuffers, tlsOptions: clusterProvider.serverOptions.TlsOptions?.TlsClientOptions, authUsername: clusterProvider.ClusterUsername, authPassword: clusterProvider.ClusterPassword, logger: logger);
            CheckpointEntry localEntry = default;
            AofSyncTaskInfo aofSyncTaskInfo = null;

            try
            {
                logger?.LogInformation("Replica replicaId:{replicaId} requesting checkpoint replicaStoreVersion:{replicaStoreVersion} replicaObjectStoreVersion:{replicaObjectStoreVersion}",
                    remoteNodeId, remoteEntry.storeVersion, remoteEntry.objectStoreVersion);
                gcs.Connect((int)clusterProvider.clusterManager.GetClusterTimeout().TotalMilliseconds);

            retry:
                logger?.LogInformation("Attempting to acquire checkpoint");
                AcquireCheckpointEntry(out localEntry, out aofSyncTaskInfo);
                logger?.LogInformation("Checkpoint search completed");

                var primary_replId = clusterProvider.replicationManager.PrimaryReplId;
                var primary_replId2 = clusterProvider.replicationManager.PrimaryReplId2;

                // If the replica does not have a checkpoint we will have to send the local if it exists
                // Else we need to compare the checkpoint versions if replica comes from the same history as this primary
                var canCompareMainStoreCheckpoint = string.IsNullOrEmpty(remoteEntry.storePrimaryReplId) || remoteEntry.storePrimaryReplId.Equals(localEntry.storePrimaryReplId);
                var canCompareObjectStoreCheckpoint = string.IsNullOrEmpty(remoteEntry.objectStorePrimaryReplId) || remoteEntry.objectStorePrimaryReplId.Equals(localEntry.objectStorePrimaryReplId);

                // We can skip sending the local checkpoint if it is of same history and version. Remote checkpoints with greater version will be ovewritten
                var skipSendingMainStore = localEntry.storeHlogToken == default || (canCompareMainStoreCheckpoint && localEntry.storeVersion == remoteEntry.storeVersion);
                var skipSendingObjectStore = clusterProvider.serverOptions.DisableObjects || localEntry.objectStoreHlogToken == default || (canCompareObjectStoreCheckpoint && localEntry.objectStoreVersion == remoteEntry.objectStoreVersion);

                LogFileInfo hlog_size = default;
                long index_size = -1;
                if (!skipSendingMainStore)
                {
                    // Try to acquire metadata because checkpoint might not have completed and we have to spinWait
                    // TODO: maybe try once and then go back to acquire new checkpoint or limit retries to avoid getting stuck
                    if (!clusterProvider.replicationManager.TryAcquireSettledMetadataForMainStore(localEntry, out hlog_size, out index_size))
                    {
                        localEntry.RemoveReader();
                        _ = Thread.Yield();
                        if (retryCount++ > 10)
                            throw new GarnetException("Attaching replica maximum retry count reached!");
                        goto retry;
                    }
                }

                LogFileInfo obj_hlog_size = default;
                long obj_index_size = -1;
                if (!skipSendingObjectStore)
                {
                    // Try to acquire metadata because checkpoint might not have completed and we have to spinWait
                    // TODO: maybe try once and then go back to acquire new checkpoint or limit retries to avoid getting stuck
                    if (!clusterProvider.replicationManager.TryAcquireSettledMetadataForObjectStore(localEntry, out obj_hlog_size, out obj_index_size))
                    {
                        localEntry.RemoveReader();
                        _ = Thread.Yield();
                        if (retryCount++ > 10)
                            throw new GarnetException("Attaching replica maximum retry count reached!");
                        goto retry;
                    }
                }

                if (!skipSendingMainStore)
                {
                    logger?.LogInformation("Sending main store checkpoint {version} {storeHlogToken} {storeIndexToken} to replica", localEntry.storeVersion, localEntry.storeHlogToken, localEntry.storeIndexToken);

                    // 1. send hlog file segments
                    if (clusterProvider.serverOptions.EnableStorageTier && hlog_size.hybridLogFileEndAddress > 64)
                        await SendFileSegments(gcs, localEntry.storeHlogToken, CheckpointFileType.STORE_HLOG, hlog_size.hybridLogFileStartAddress, hlog_size.hybridLogFileEndAddress);

                    // 2.Send index file segments
                    //var index_size = storeWrapper.store.GetIndexFileSize(localEntry.storeIndexToken);
                    await SendFileSegments(gcs, localEntry.storeIndexToken, CheckpointFileType.STORE_INDEX, 0, index_size);

                    // 3. Send snapshot file segments
                    await SendFileSegments(gcs, localEntry.storeHlogToken, CheckpointFileType.STORE_SNAPSHOT, 0, hlog_size.snapshotFileEndAddress);

                    // 4. Send delta log segments
                    var dlog_size = hlog_size.deltaLogTailAddress;
                    await SendFileSegments(gcs, localEntry.storeHlogToken, CheckpointFileType.STORE_DLOG, 0, dlog_size);

                    // 5.Send index metadata
                    await SendCheckpointMetadata(gcs, storeCkptManager, CheckpointFileType.STORE_INDEX, localEntry.storeIndexToken);

                    // 6. Send snapshot metadata
                    await SendCheckpointMetadata(gcs, storeCkptManager, CheckpointFileType.STORE_SNAPSHOT, localEntry.storeHlogToken);
                }

                if (!skipSendingObjectStore)
                {
                    logger?.LogInformation("Sending object store checkpoint {version} {objectStoreHlogToken} {objectStoreIndexToken} to replica", localEntry.objectStoreVersion, localEntry.objectStoreHlogToken, localEntry.objectStoreIndexToken);

                    // 1. send hlog file segments
                    if (clusterProvider.serverOptions.EnableStorageTier && obj_hlog_size.hybridLogFileEndAddress > 24)
                    {
                        //send object hlog file segments
                        await SendFileSegments(gcs, localEntry.objectStoreHlogToken, CheckpointFileType.OBJ_STORE_HLOG, obj_hlog_size.hybridLogFileStartAddress, obj_hlog_size.hybridLogFileEndAddress);

                        var hlogSegmentCount = ((obj_hlog_size.hybridLogFileEndAddress - obj_hlog_size.hybridLogFileStartAddress) >> clusterProvider.serverOptions.ObjectStoreSegmentSizeBits()) + 1;
                        await SendObjectFiles(gcs, localEntry.objectStoreHlogToken, CheckpointFileType.OBJ_STORE_HLOG_OBJ, (int)hlogSegmentCount);
                    }

                    // 2. Send object store snapshot files
                    if (obj_hlog_size.snapshotFileEndAddress > 24)
                    {
                        //send snapshot file segments
                        await SendFileSegments(gcs, localEntry.objectStoreHlogToken, CheckpointFileType.OBJ_STORE_SNAPSHOT, 0, obj_hlog_size.snapshotFileEndAddress);

                        //send snapshot.obj file segments
                        var snapshotSegmentCount = (obj_hlog_size.snapshotFileEndAddress >> clusterProvider.serverOptions.ObjectStoreSegmentSizeBits()) + 1;
                        await SendObjectFiles(gcs, localEntry.objectStoreHlogToken, CheckpointFileType.OBJ_STORE_SNAPSHOT_OBJ, (int)snapshotSegmentCount);
                    }

                    // 3. Send object store index file segments
                    if (obj_index_size > 0)
                        await SendFileSegments(gcs, localEntry.objectStoreIndexToken, CheckpointFileType.OBJ_STORE_INDEX, 0, obj_index_size);

                    // 4. Send object store delta file segments
                    var obj_dlog_size = obj_hlog_size.deltaLogTailAddress;
                    if (obj_dlog_size > 0)
                        await SendFileSegments(gcs, localEntry.objectStoreHlogToken, CheckpointFileType.OBJ_STORE_DLOG, 0, obj_dlog_size);

                    // 5. Send object store index metadata
                    await SendCheckpointMetadata(gcs, objectStoreCkptManager, CheckpointFileType.OBJ_STORE_INDEX, localEntry.objectStoreIndexToken);

                    // 6. Send object store snapshot metadata
                    await SendCheckpointMetadata(gcs, objectStoreCkptManager, CheckpointFileType.OBJ_STORE_SNAPSHOT, localEntry.objectStoreHlogToken);
                }

                var recoverFromRemote = !skipSendingMainStore || !skipSendingObjectStore;
                var replayAOF = false;
                var RecoveredReplicationOffset = localEntry.GetMinAofCoveredAddress();
                var beginAddress = RecoveredReplicationOffset;
                if (!recoverFromRemote)
                {
                    // If replica is ahead of this primary it will force itself to forget and start syncing from RecoveredReplicationOffset
                    if (replicaAofBeginAddress > ReplicationManager.kFirstValidAofAddress && replicaAofBeginAddress > RecoveredReplicationOffset)
                    {
                        logger?.LogInformation(
                            "ReplicaSyncSession: replicaAofBeginAddress {replicaAofBeginAddress} > PrimaryCheckpointRecoveredReplicationOffset {RecoveredReplicationOffset}, cannot use remote AOF",
                            replicaAofBeginAddress, RecoveredReplicationOffset);
                    }
                    else
                    {
                        // Tail address cannot be behind the recovered address since above we checked replicaAofBeginAddress and it appears after RecoveredReplicationOffset
                        // unless we are performing MainMemoryReplication
                        // TODO: shouldn't we use the remote cEntry's tail address here since replica will recover to that?
                        if (replicaAofTailAddress < RecoveredReplicationOffset && !clusterProvider.serverOptions.MainMemoryReplication)
                        {
                            logger?.LogCritical("ReplicaSyncSession replicaAofTail {replicaAofTailAddress} < canServeFromAofAddress {RecoveredReplicationOffset}", replicaAofTailAddress, RecoveredReplicationOffset);
                            throw new Exception($"ReplicaSyncSession replicaAofTail {replicaAofTailAddress} < canServeFromAofAddress {RecoveredReplicationOffset}");
                        }

                        // If we are behind this primary we need to decide until where to replay
                        var replayUntilAddress = replicaAofTailAddress;
                        // Replica tail is further ahead than committed address of primary
                        if (storeWrapper.appendOnlyFile.CommittedUntilAddress < replayUntilAddress)
                        {
                            replayUntilAddress = storeWrapper.appendOnlyFile.CommittedUntilAddress;
                        }

                        // Replay only if records not included in checkpoint
                        if (replayUntilAddress > RecoveredReplicationOffset)
                        {
                            logger?.LogInformation("ReplicaSyncSession: have to replay remote AOF from {beginAddress} until {untilAddress}", beginAddress, replayUntilAddress);
                            replayAOF = true;
                            // Bound replayUntilAddress to ReplicationOffset2 to avoid replaying divergent history only if connecting replica was attached to old primary
                            if (!string.IsNullOrEmpty(clusterProvider.replicationManager.PrimaryReplId2) &&
                                clusterProvider.replicationManager.PrimaryReplId2.Equals(remote_primary_replid) &&
                                replayUntilAddress > clusterProvider.replicationManager.ReplicationOffset2)
                                replayUntilAddress = clusterProvider.replicationManager.ReplicationOffset2;
                            RecoveredReplicationOffset = replayUntilAddress;
                        }
                    }
                }

                // Signal replica to recover from local/remote checkpoint
                // Make replica replayAOF if needed and replay from provided beginAddress to ReocoveredReplication Address
                var resp = await gcs.ExecuteBeginReplicaRecover(
                    !skipSendingMainStore,
                    !skipSendingObjectStore,
                    replayAOF,
                    primary_replId,
                    localEntry.ToByteArray(),
                    beginAddress,
                    RecoveredReplicationOffset).ConfigureAwait(false);
                var syncFromAofAddress = long.Parse(resp);

                // Assert that AOF address the replica will be requesting can be served, except in case of:
                // Possible AOF data loss: { using null AOF device } OR { main memory replication AND no on-demand checkpoints }
                var possibleAofDataLoss = clusterProvider.serverOptions.UseAofNullDevice ||
                    (clusterProvider.serverOptions.MainMemoryReplication && !clusterProvider.serverOptions.OnDemandCheckpoint);

                if (!possibleAofDataLoss)
                {
                    if (syncFromAofAddress < storeWrapper.appendOnlyFile.BeginAddress)
                    {
                        logger?.LogError("syncFromAofAddress: {syncFromAofAddress} < beginAofAddress: {storeWrapper.appendOnlyFile.BeginAddress}", syncFromAofAddress, storeWrapper.appendOnlyFile.BeginAddress);
                        var tailEntry = clusterProvider.replicationManager.GetLatestCheckpointEntryFromMemory();
                        logger?.LogError("tailEntry:{tailEntry}", tailEntry.GetCheckpointEntryDump());
                        tailEntry.RemoveReader();
                        throw new Exception("Failed syncing because replica requested truncated AOF address");
                    }
                }
                else // possible AOF data loss
                {
                    if (syncFromAofAddress < storeWrapper.appendOnlyFile.BeginAddress)
                    {
                        logger?.LogWarning("AOF truncated, unsafe attach: syncFromAofAddress: {syncFromAofAddress} < beginAofAddress: {storeWrapper.appendOnlyFile.BeginAddress}", syncFromAofAddress, storeWrapper.appendOnlyFile.BeginAddress);
                        logger?.LogWarning("{cEntryDump}", localEntry.GetCheckpointEntryDump());
                    }
                }

                // We have already added the iterator for the covered address above but replica might request an address
                // that is ahead of the covered address so we should start streaming from that address in order not to
                // introduce duplicate insertions.
                if (!clusterProvider.replicationManager.TryAddReplicationTask(remoteNodeId, syncFromAofAddress, out aofSyncTaskInfo))
                    throw new GarnetException("Failed trying to try update replication task");
                if (!clusterProvider.replicationManager.TryConnectToReplica(remoteNodeId, syncFromAofAddress, aofSyncTaskInfo, out _))
                    throw new GarnetException("Failed connecting to replica for aofSync");
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "acquiredEntry: {cEntryDump}", localEntry.GetCheckpointEntryDump());
                if (aofSyncTaskInfo != null) _ = clusterProvider.replicationManager.TryRemoveReplicationTask(aofSyncTaskInfo);
                errorMsg = "ERR " + ex.Message;// this is error sent to remote client
                return false;
            }
            finally
            {
                // At this point the replica has received the most recent checkpoint data
                // and recovered from it so primary can release and delete it safely
                localEntry.RemoveReader();
                gcs.Dispose();
            }
            return true;
        }

        public void AcquireCheckpointEntry(out CheckpointEntry cEntry, out AofSyncTaskInfo aofSyncTaskInfo)
        {
            // Possible AOF data loss: { using null AOF device } OR { main memory replication AND no on-demand checkpoints }
            var possibleAofDataLoss = clusterProvider.serverOptions.UseAofNullDevice ||
                (clusterProvider.serverOptions.MainMemoryReplication && !clusterProvider.serverOptions.OnDemandCheckpoint);

            aofSyncTaskInfo = null;

            // This loop tries to provide the following two guarantees
            // 1. Retrieve latest checkpoint and lock it to prevent deletion before it is send to the replica
            // 2. Guard against truncation of AOF in between the retrieval of the checkpoint metadata and start of the aofSyncTask
            while (true)
            {
                // Acquire startSaveTime to identify if an external task might have taken the checkpoint for us
                // This is only useful for MainMemoryReplication where we might have multiple replicas attaching
                // We want to share the on-demand checkpoint and ensure that only one replica should succeed when calling TakeOnDemandCheckpoint
                var lastSaveTime = storeWrapper.lastSaveTime;

                // Retrieve latest checkpoint and lock it from deletion operations
                cEntry = clusterProvider.replicationManager.GetLatestCheckpointEntryFromMemory();

                // Break early if main-memory-replication on and do not wait for OnDemandCheckpoint
                // We do this to avoid waiting indefinetely for a checkpoint that will never be taken
                if (clusterProvider.serverOptions.MainMemoryReplication && !clusterProvider.serverOptions.OnDemandCheckpoint)
                {
                    logger?.LogWarning("MainMemoryReplication: OnDemandCheckpoint is turned off, skipping valid checkpoint acquisition.");
                    break;
                }

                // Calculate the minimum start address covered by this checkpoint
                var startAofAddress = cEntry.GetMinAofCoveredAddress();

                // If there is possible AOF data loss and we need to take an on-demand checkpoint,
                // then we should take the checkpoint before we register the sync task, because
                // TryAddReplicationTask is guaranteed to return true in this scenario.
                if (possibleAofDataLoss && clusterProvider.serverOptions.OnDemandCheckpoint && startAofAddress < clusterProvider.replicationManager.AofTruncatedUntil)
                {
                    cEntry.RemoveReader();
                    storeWrapper.TakeOnDemandCheckpoint(lastSaveTime).ConfigureAwait(false).GetAwaiter().GetResult();
                    cEntry = clusterProvider.replicationManager.GetLatestCheckpointEntryFromMemory();
                    startAofAddress = cEntry.GetMinAofCoveredAddress();
                }

                // Enqueue AOF sync task with startAofAddress to prevent future AOF truncations
                // and check if truncation has happened in between retrieving the latest checkpoint and enqueuing the aofSyncTask
                if (clusterProvider.replicationManager.TryAddReplicationTask(remoteNodeId, startAofAddress, out aofSyncTaskInfo))
                    break;

                // Unlock last checkpoint because associated startAofAddress is no longer available
                cEntry.RemoveReader();

                // Take on demand checkpoint if main memory replication is enabled
                if (clusterProvider.serverOptions.OnDemandCheckpoint)
                    storeWrapper.TakeOnDemandCheckpoint(lastSaveTime).ConfigureAwait(false).GetAwaiter().GetResult();

                Thread.Yield();
            }
        }

        private async Task SendCheckpointMetadata(GarnetClientSession gcs, ReplicationLogCheckpointManager ckptManager, CheckpointFileType fileType, Guid fileToken)
        {
            logger?.LogInformation("<Begin sending checkpoint metadata {fileToken} {fileType}", fileToken, fileType);
            var checkpointMetadata = Array.Empty<byte>();
            if (fileToken != default)
            {
                switch (fileType)
                {
                    case CheckpointFileType.STORE_SNAPSHOT:
                    case CheckpointFileType.OBJ_STORE_SNAPSHOT:
                        var pageSizeBits = fileType == CheckpointFileType.STORE_SNAPSHOT ? clusterProvider.serverOptions.PageSizeBits() : clusterProvider.serverOptions.ObjectStorePageSizeBits();
                        using (var deltaFileDevice = ckptManager.GetDeltaLogDevice(fileToken))
                        {
                            if (deltaFileDevice is not null)
                            {
                                deltaFileDevice.Initialize(-1);
                                if (deltaFileDevice.GetFileSize(0) > 0)
                                {
                                    var deltaLog = new DeltaLog(deltaFileDevice, pageSizeBits, -1);
                                    deltaLog.InitializeForReads();
                                    checkpointMetadata = ckptManager.GetLogCheckpointMetadata(fileToken, deltaLog, true, -1, withoutCookie: false);
                                    break;
                                }
                            }
                        }
                        checkpointMetadata = ckptManager.GetLogCheckpointMetadata(fileToken, null, false, -1, withoutCookie: false);
                        break;
                    case CheckpointFileType.STORE_INDEX:
                    case CheckpointFileType.OBJ_STORE_INDEX:
                        checkpointMetadata = ckptManager.GetIndexCheckpointMetadata(fileToken);
                        break;
                }
            }

            var resp = await gcs.ExecuteSendCkptMetadata(fileToken.ToByteArray(), (int)fileType, checkpointMetadata).ConfigureAwait(false);
            if (!resp.Equals("OK"))
            {
                logger?.LogError("Primary error at SendCheckpointMetadata {resp}", resp);
                throw new Exception($"Primary error at SendCheckpointMetadata {resp}");
            }

            logger?.LogInformation("<Complete sending checkpoint metadata {fileToken} {fileType}", fileToken, fileType);
        }

        private async Task SendFileSegments(GarnetClientSession gcs, Guid token, CheckpointFileType type, long startAddress, long endAddress, int batchSize = 1 << 17)
        {
            var fileTokenBytes = token.ToByteArray();
            var device = clusterProvider.replicationManager.GetInitializedSegmentFileDevice(token, type);
            logger?.LogInformation("<Begin sending checkpoint file segments {guid} {type} {startAddress} {endAddress}", token, type, startAddress, endAddress);

            Debug.Assert(device != null);
            batchSize = !ReplicationManager.ShouldInitialize(type) ?
                batchSize : (int)Math.Min(batchSize, 1L << clusterProvider.serverOptions.SegmentSizeBits());
            string resp;
            try
            {
                while (startAddress < endAddress)
                {
                    var num_bytes = startAddress + batchSize < endAddress ?
                        batchSize :
                        (int)(endAddress - startAddress);
                    var (pbuffer, readBytes) = ReadInto(device, (ulong)startAddress, num_bytes);

                    resp = await gcs.ExecuteSendFileSegments(fileTokenBytes, (int)type, startAddress, pbuffer.GetSlice(readBytes)).ConfigureAwait(false);
                    if (!resp.Equals("OK"))
                    {
                        logger?.LogError("Primary error at SendFileSegments {type} {resp}", type, resp);
                        throw new Exception($"Primary error at SendFileSegments {type} {resp}");
                    }
                    pbuffer.Return();
                    startAddress += readBytes;
                }

                // Send last empty package to indicate end of transmission and let replica dispose IDevice
                resp = await gcs.ExecuteSendFileSegments(fileTokenBytes, (int)type, startAddress, []).ConfigureAwait(false);
                if (!resp.Equals("OK"))
                {
                    logger?.LogError("Primary error at SendFileSegments {type} {resp}", type, resp);
                    throw new Exception($"Primary error at SendFileSegments {type} {resp}");
                }
            }
            catch (Exception ex)
            {
                logger?.LogError("SendFileSegments Error: {msg}", ex.Message);
            }
            finally
            {
                device.Dispose();
            }
            logger?.LogInformation("<Complete sending checkpoint file segments {guid} {type} {startAddress} {endAddress}", token, type, startAddress, endAddress);
        }

        private async Task SendObjectFiles(GarnetClientSession gcs, Guid token, CheckpointFileType type, int segmentCount, int batchSize = 1 << 17)
        {
            var fileTokenBytes = token.ToByteArray();
            IDevice device = null;
            string resp;
            try
            {
                for (var segment = 0; segment < segmentCount; segment++)
                {
                    device = clusterProvider.replicationManager.GetInitializedSegmentFileDevice(token, type);
                    Debug.Assert(device != null);
                    device.Initialize(-1);
                    var size = device.GetFileSize(segment);
                    var startAddress = 0L;

                    while (startAddress < size)
                    {
                        var num_bytes = startAddress + batchSize < size ? batchSize : (int)(size - startAddress);
                        var (pbuffer, readBytes) = ReadInto(device, (ulong)startAddress, num_bytes, segment);

                        resp = await gcs.ExecuteSendFileSegments(fileTokenBytes, (int)type, startAddress, pbuffer.GetSlice(readBytes), segment).ConfigureAwait(false);
                        if (!resp.Equals("OK"))
                        {
                            logger?.LogError("Primary error at SendFileSegments {type} {resp}", type, resp);
                            throw new Exception($"Primary error at SendFileSegments {type} {resp}");
                        }

                        pbuffer.Return();
                        startAddress += readBytes;
                    }

                    resp = await gcs.ExecuteSendFileSegments(fileTokenBytes, (int)type, 0L, []).ConfigureAwait(false);
                    if (!resp.Equals("OK"))
                    {
                        logger?.LogError("Primary error at SendFileSegments {type} {resp}", type, resp);
                        throw new Exception($"Primary error at SendFileSegments {type} {resp}");
                    }
                    device.Dispose();
                    device = null;
                }
            }
            catch (Exception ex)
            {
                logger?.LogError("SendFileSegments Error: {msg}", ex.Message);
            }
            finally
            {
                device?.Dispose();
            }
        }

        /// <summary>
        /// Note: will read potentially more data (based on sector alignment)
        /// </summary>
        /// <param name="device"></param>
        /// <param name="address"></param>
        /// <param name="size"></param>
        /// <param name="segmentId"></param>
        private unsafe (SectorAlignedMemory, int) ReadInto(IDevice device, ulong address, int size, int segmentId = -1)
        {
            bufferPool ??= new SectorAlignedBufferPool(1, (int)device.SectorSize);

            long numBytesToRead = size;
            numBytesToRead = ((numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = bufferPool.Get((int)numBytesToRead);
            if (segmentId == -1)
                device.ReadAsync(address, (IntPtr)pbuffer.aligned_pointer, (uint)numBytesToRead, IOCallback, null);
            else
                device.ReadAsync(segmentId, address, (IntPtr)pbuffer.aligned_pointer, (uint)numBytesToRead, IOCallback, null);
            semaphore.Wait();
            return (pbuffer, (int)numBytesToRead);
        }

        private unsafe void IOCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                var errorMessage = new Win32Exception((int)errorCode).Message;
                logger.LogError("[Primary] OverlappedStream GetQueuedCompletionStatus error: {errorCode} msg: {errorMessage}", errorCode, errorMessage);
            }
            semaphore.Release();
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