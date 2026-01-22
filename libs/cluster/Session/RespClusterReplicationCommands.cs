// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.cluster.Server.Replication;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        /// <summary>
        /// Implements CLUSTER REPLICAS command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterReplicas(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 argument
            if (parseState.Count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var nodeId = parseState.GetString(0);
            var replicas = clusterProvider.clusterManager.ListReplicas(nodeId, clusterProvider);

            while (!RespWriteUtils.TryWriteArrayLength(replicas.Count, ref dcurr, dend))
                SendAndReset();

            foreach (var replica in replicas)
            {
                while (!RespWriteUtils.TryWriteAsciiBulkString(replica, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER REPLICATE command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterReplicate(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting 1 or 2 arguments
            if (parseState.Count is < 1 or > 2)
            {
                invalidParameters = true;
                return true;
            }

            var background = true;
            var nodeId = parseState.GetString(0);

            if (parseState.Count > 1)
            {
                var backgroundFlagSpan = parseState.GetArgSliceByRef(1).ReadOnlySpan;

                if (backgroundFlagSpan.EqualsUpperCaseSpanIgnoringCase("SYNC"u8))
                    background = false;
                else if (backgroundFlagSpan.EqualsUpperCaseSpanIgnoringCase("ASYNC"u8))
                    background = true;
                else
                {
                    while (!RespWriteUtils.TryWriteError(
                               $"ERR Invalid CLUSTER REPLICATE FLAG ({Encoding.ASCII.GetString(backgroundFlagSpan)}) not valid",
                               ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            if (!clusterProvider.serverOptions.EnableAOF)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                ReplicateSyncOptions syncOpts = new(
                    nodeId,
                    Background: background,
                    Force: false,
                    TryAddReplica: true,
                    AllowReplicaResetOnFailure: true,
                    UpgradeLock: false
                );
                var success = clusterProvider.serverOptions.ReplicaDisklessSync ?
                    clusterProvider.replicationManager.TryReplicateDisklessSync(this, syncOpts, out var errorMessage) :
                    clusterProvider.replicationManager.TryReplicateDiskbasedSync(this, syncOpts, out errorMessage);

                if (success)
                {
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER appendlog command (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.client.GarnetClientSession.ExecuteClusterAppendLog"/>
        private bool NetworkClusterAppendLog(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 6 arguments (6-th argument is AOF page parsed later)
            if (parseState.Count != 6)
            {
                invalidParameters = true;
                return true;
            }

            var nodeId = parseState.GetString(0);

            if (!parseState.TryGetInt(1, out var physicalSublogIdx) ||
                !parseState.TryGetLong(2, out var previousAddress) ||
                !parseState.TryGetLong(3, out var currentAddress) ||
                !parseState.TryGetLong(4, out var nextAddress))
            {
                logger?.LogError("{str}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER));
                return true;
            }

            // This is an initialization message
            if (previousAddress == -1 && currentAddress == -1 && nextAddress == -1)
            {
                if (clusterProvider.replicationManager.InitializeReplicaReplayGroup(physicalSublogIdx, networkSender))
                    replicaReplayDriverStore = clusterProvider.replicationManager.ReplicaReplayDriverStore;
                else
                    throw new GarnetException($"[physicalSublogIdx: {physicalSublogIdx}] Received initialization message but ReplicaReplayDriver is already initialized!", LogLevel.Warning, clientResponse: false);
                return true;
            }

            var sbRecord = parseState.GetArgSliceByRef(5);
            var currentConfig = clusterProvider.clusterManager.CurrentConfig;
            var localRole = currentConfig.LocalNodeRole;
            var primaryId = currentConfig.LocalNodePrimaryId;
            if (localRole != NodeRole.REPLICA)
            {
                throw new GarnetException("aofsync node not a replica", LogLevel.Error, clientResponse: false);
            }
            else if (!primaryId.Equals(nodeId))
            {
                throw new GarnetException($"aofsync node replicating {primaryId}", LogLevel.Error, clientResponse: false);
            }
            else
            {
                IsReplicating = true;

                ProcessPrimaryStream(physicalSublogIdx, sbRecord.ToPointer(), sbRecord.Length,
                    previousAddress, currentAddress, nextAddress);
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER initiate_replica_sync command (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.client.GarnetClientSession.ExecuteClusterInitiateReplicaSync"/>
        private bool NetworkClusterInitiateReplicaSync(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 5 arguments
            if (parseState.Count != 5)
            {
                invalidParameters = true;
                return true;
            }

            var replicaNodeId = parseState.GetString(0);
            var replicaAssignedPrimaryId = parseState.GetString(1);
            var checkpointEntryBytes = parseState.GetArgSliceByRef(2).ToArray();

            var replicaAofBeginAddress = AofAddress.FromByteArray(parseState.GetArgSliceByRef(3).ToArray());
            var replicaAofTailAddress = AofAddress.FromByteArray(parseState.GetArgSliceByRef(4).ToArray());

            var replicaCheckpointEntry = CheckpointEntry.FromByteArray(checkpointEntryBytes);

            if (!clusterProvider.replicationManager.TryBeginDiskSync(
                replicaNodeId,
                replicaAssignedPrimaryId,
                replicaCheckpointEntry,
                replicaAofBeginAddress,
                replicaAofTailAddress,
                out var errorMessage))
            {
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implement CLUSTER send_ckpt_metadata command (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.client.GarnetClientSession.ExecuteClusterSendCheckpointMetadata"/>
        private bool NetworkClusterSendCheckpointMetadata(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 3 arguments
            if (parseState.Count != 3)
            {
                invalidParameters = true;
                return true;
            }

            var fileTokenBytes = parseState.GetArgSliceByRef(0).ReadOnlySpan;

            if (!parseState.TryGetInt(1, out var fileTypeInt))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var checkpointMetadata = parseState.GetArgSliceByRef(2).ToArray();

            var fileToken = new Guid(fileTokenBytes);
            var fileType = (CheckpointFileType)fileTypeInt;
            clusterProvider.replicationManager.ProcessCheckpointMetadata(fileToken, fileType, checkpointMetadata);
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER send_ckpt_file_segment command (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.client.GarnetClientSession.ExecuteClusterSendCheckpointFileSegment"/>
        private bool NetworkClusterSendCheckpointFileSegment(out bool invalidParameters)
        {
            invalidParameters = false;

            if (parseState.Count != 5)
            {
                invalidParameters = true;
                return true;
            }

            var fileTokenBytes = parseState.GetArgSliceByRef(0).ReadOnlySpan;

            if (!parseState.TryGetInt(1, out var ckptFileTypeInt) ||
                !parseState.TryGetLong(2, out var startAddress) ||
                !parseState.TryGetInt(4, out var segmentId))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var data = parseState.GetArgSliceByRef(3).ReadOnlySpan;

            var fileToken = new Guid(fileTokenBytes);
            var ckptFileType = (CheckpointFileType)ckptFileTypeInt;

            // Commenting due to high verbosity
            // logger?.LogTrace("send_ckpt_file_segment {fileToken} {ckptFileType} {startAddress} {dataLength}", fileToken, ckptFileType, startAddress, data.Length);
            clusterProvider.replicationManager.recvCheckpointHandler.ProcessFileSegments(segmentId, fileToken, ckptFileType, startAddress, data);
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER begin_replica_recover (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.client.GarnetClientSession.ExecuteClusterBeginReplicaRecover"/>
        private bool NetworkClusterBeginReplicaRecover(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 6 arguments
            if (parseState.Count != 6)
            {
                invalidParameters = true;
                return true;
            }

            if (!parseState.TryGetBool(0, out var recoverStoreFromToken) || !parseState.TryGetLong(1, out var replayAOFMap))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_BOOLEAN, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var primaryReplicaId = parseState.GetString(2);
            var checkpointEntryBytes = parseState.GetArgSliceByRef(3).ToArray();

            var beginAddress = AofAddress.FromByteArray(parseState.GetArgSliceByRef(4).ToArray());
            var tailAddress = AofAddress.FromByteArray(parseState.GetArgSliceByRef(5).ToArray());

            var entry = CheckpointEntry.FromByteArray(checkpointEntryBytes);
            var replicationOffset = clusterProvider.replicationManager.TryReplicaDiskbasedRecovery(
                recoverStoreFromToken,
                (ulong)replayAOFMap,
                primaryReplicaId,
                entry,
                beginAddress,
                ref tailAddress,
                out var errorMessage);

            if (errorMessage.IsEmpty)
            {
                while (!RespWriteUtils.TryWriteAsciiBulkString(replicationOffset.ToString(), ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER attach_sync command (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.client.GarnetClientSession.ExecuteClusterAttachSync"/>
        private bool NetworkClusterAttachSync(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 arguments
            if (parseState.Count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var checkpointEntryBytes = parseState.GetArgSliceByRef(0).ToArray();
            var syncMetadata = SyncMetadata.FromByteArray(checkpointEntryBytes);

            ReadOnlySpan<byte> errorMessage;
            var replicationOffset = AofAddress.Create(clusterProvider.serverOptions.AofPhysicalSublogCount, -1);
            if (syncMetadata.originNodeRole == NodeRole.REPLICA)
                _ = clusterProvider.replicationManager.TryBeginDisklessSync(syncMetadata, out errorMessage);
            else
                replicationOffset = clusterProvider.replicationManager.TryReplicaDisklessRecovery(syncMetadata, out errorMessage);

            if (errorMessage.IsEmpty)
            {
                while (!RespWriteUtils.TryWriteAsciiBulkString(replicationOffset.ToString(), ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER SYNC
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.cluster.SnapshotIteratorManager"/>
        /// <seealso cref="T:Garnet.cluster.ReplicaSyncSession.TryWriteRecordSpan"/>        
        /// <seealso cref="T:Garnet.client.GarnetClientSession.SetClusterSyncHeader"/>
        private bool NetworkClusterSync(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 2 arguments
            if (parseState.Count != 2)
            {
                invalidParameters = true;
                return true;
            }

            var payload = parseState.GetArgSliceByRef(1);
            var payloadPtr = payload.ToPointer();
            var lastParam = parseState.GetArgSliceByRef(parseState.Count - 1);
            var payloadEndPtr = lastParam.ToPointer() + lastParam.Length;

            var recordCount = *(int*)payloadPtr;
            var i = 0;
            payloadPtr += 4;

            TrackImportProgress(recordCount, recordCount == 0);
            var storeWrapper = clusterProvider.storeWrapper;
            var transientObjectIdMap = storeWrapper.store.Log.TransientObjectIdMap;

            DiskLogRecord diskLogRecord = default;
            try
            {
                while (i < recordCount)
                {
                    if (!RespReadUtils.GetSerializedRecordSpan(out var recordSpan, ref payloadPtr, payloadEndPtr))
                        return false;

                    diskLogRecord = DiskLogRecord.Deserialize(recordSpan, storeWrapper.GarnetObjectSerializer, transientObjectIdMap, storeWrapper.storeFunctions);
                    _ = basicGarnetApi.SET(in diskLogRecord);
                    diskLogRecord.Dispose();
                    i++;
                }
            }
            catch
            {
                // Dispose the diskLogRecord if there was an exception in SET
                diskLogRecord.Dispose();
                throw;
            }

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER FLUSHALL
        /// </summary>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.cluster.ReplicaSyncSession.IssueFlushAllAsync"/>
        private bool NetworkClusterFlushAll(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            // Flush all keys
            clusterProvider.storeWrapper.FlushAllDatabases(unsafeTruncateLog: false);

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// Implements CLUSTER_SHARDED_LOG_KEY_SEQUENCE_VECTOR
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.client.GarnetClientSession.ExecuteClusterShardedLogKeySequenceVector"/>
        private bool NetworkShardedLogKeySequenceVector(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var maxKeySeqNumVector = clusterProvider.storeWrapper.appendOnlyFile.readConsistencyManager.GetSublogMaxKeySequenceNumber();
            while (!RespWriteUtils.TryWriteAsciiBulkString(maxKeySeqNumVector.ToString(), ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}