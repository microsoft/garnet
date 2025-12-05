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
        /// Implements CLUSTER reserve command (only for internode use).
        /// 
        /// Allows for pre-migration reservation of certain resources.
        /// 
        /// For now, this is only used for Vector Sets.
        /// </summary>
        private bool NetworkClusterReserve(VectorManager vectorManager, out bool invalidParameters)
        {
            if (parseState.Count < 2)
            {
                invalidParameters = true;
                return true;
            }

            var kind = parseState.GetArgSliceByRef(0);
            if (!kind.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("VECTOR_SET_CONTEXTS"u8))
            {
                while (!RespWriteUtils.TryWriteError("Unrecognized reservation type"u8, ref dcurr, dend))
                    SendAndReset();

                invalidParameters = false;
                return true;
            }

            if (!parseState.TryGetInt(1, out var numVectorSetContexts) || numVectorSetContexts <= 0)
            {
                invalidParameters = true;
                return true;
            }

            invalidParameters = false;

            if (!vectorManager.TryReserveContextsForMigration(ref vectorContext, numVectorSetContexts, out var newContexts))
            {
                while (!RespWriteUtils.TryWriteError("Insufficients contexts available to reserve"u8, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            while (!RespWriteUtils.TryWriteArrayLength(newContexts.Count, ref dcurr, dend))
                SendAndReset();

            foreach (var ctx in newContexts)
            {
                while (!RespWriteUtils.TryWriteInt64AsSimpleString((long)ctx, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER aofsync command (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterAOFSync(out bool invalidParameters)
        {
            invalidParameters = false;

            if (parseState.Count != 2)
            {
                invalidParameters = true;
                return true;
            }

            var nodeId = parseState.GetString(0);

            if (!parseState.TryGetLong(1, out var nextAddress))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (clusterProvider.serverOptions.EnableAOF)
            {
                clusterProvider.replicationManager.TryAddReplicationTask(nodeId, nextAddress, out var aofSyncTaskInfo);
                if (!clusterProvider.replicationManager.TryConnectToReplica(nodeId, nextAddress, aofSyncTaskInfo, out var errorMessage))
                {
                    while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER appendlog command (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterAppendLog(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 5 arguments (5-th argument is AOF page parsed later)
            if (parseState.Count != 5)
            {
                invalidParameters = true;
                return true;
            }

            var nodeId = parseState.GetString(0);

            if (!parseState.TryGetLong(1, out var previousAddress) ||
                !parseState.TryGetLong(2, out var currentAddress) ||
                !parseState.TryGetLong(3, out var nextAddress))
            {
                logger?.LogError("{str}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER));
                return true;
            }

            var sbRecord = parseState.GetArgSliceByRef(4).SpanByte;

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

                clusterProvider.replicationManager.ProcessPrimaryStream(sbRecord.ToPointer(), sbRecord.Length,
                    previousAddress, currentAddress, nextAddress);
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER initiate_replica_sync command (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
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
            var checkpointEntryBytes = parseState.GetArgSliceByRef(2).SpanByte.ToByteArray();

            if (!parseState.TryGetLong(3, out var replicaAofBeginAddress) ||
                !parseState.TryGetLong(4, out var replicaAofTailAddress))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var replicaCheckpointEntry = CheckpointEntry.FromByteArray(checkpointEntryBytes);

            if (!clusterProvider.replicationManager.TryBeginPrimarySync(
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

            var checkpointMetadata = parseState.GetArgSliceByRef(2).SpanByte.ToByteArray();

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
        private bool NetworkClusterBeginReplicaRecover(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 7 arguments
            if (parseState.Count != 7)
            {
                invalidParameters = true;
                return true;
            }

            if (!parseState.TryGetBool(0, out var recoverMainStoreFromToken) ||
                !parseState.TryGetBool(1, out var recoverObjectStoreFromToken) ||
                !parseState.TryGetBool(2, out var replayAOF))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_BOOLEAN, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var primaryReplicaId = parseState.GetString(3);
            var checkpointEntryBytes = parseState.GetArgSliceByRef(4).SpanByte.ToByteArray();

            if (!parseState.TryGetLong(5, out var beginAddress) ||
                !parseState.TryGetLong(6, out var tailAddress))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var entry = CheckpointEntry.FromByteArray(checkpointEntryBytes);
            var replicationOffset = clusterProvider.replicationManager.BeginReplicaRecover(
                recoverMainStoreFromToken,
                recoverObjectStoreFromToken,
                replayAOF,
                primaryReplicaId,
                entry,
                beginAddress,
                tailAddress,
                out var errorMessage);

            if (errorMessage.IsEmpty)
            {
                while (!RespWriteUtils.TryWriteInt64(replicationOffset, ref dcurr, dend))
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
        private bool NetworkClusterAttachSync(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 arguments
            if (parseState.Count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var checkpointEntryBytes = parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();
            var syncMetadata = SyncMetadata.FromByteArray(checkpointEntryBytes);

            ReadOnlySpan<byte> errorMessage = default;
            long replicationOffset = -1;
            if (syncMetadata.originNodeRole == NodeRole.REPLICA)
                _ = clusterProvider.replicationManager.TryAttachSync(syncMetadata, out errorMessage);
            else
                replicationOffset = clusterProvider.replicationManager.ReplicaRecoverDiskless(syncMetadata, out errorMessage);

            if (!errorMessage.IsEmpty)
            {
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteInt64(replicationOffset, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER SYNC
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSync(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 3 arguments
            if (parseState.Count != 3)
            {
                invalidParameters = true;
                return true;
            }

            var primaryNodeId = parseState.GetString(0);
            var storeTypeSpan = parseState.GetArgSliceByRef(1).ReadOnlySpan;
            var payload = parseState.GetArgSliceByRef(2).SpanByte;
            var payloadPtr = payload.ToPointer();
            var lastParam = parseState.GetArgSliceByRef(parseState.Count - 1).SpanByte;
            var payloadEndPtr = lastParam.ToPointer() + lastParam.Length;

            var keyValuePairCount = *(int*)payloadPtr;
            var i = 0;
            payloadPtr += 4;
            if (storeTypeSpan.EqualsUpperCaseSpanIgnoringCase("SSTORE"u8))
            {
                TrackImportProgress(keyValuePairCount, isMainStore: true, keyValuePairCount == 0);
                while (i < keyValuePairCount)
                {
                    ref var key = ref SpanByte.Reinterpret(payloadPtr);
                    payloadPtr += key.TotalSize;
                    ref var value = ref SpanByte.Reinterpret(payloadPtr);
                    payloadPtr += value.TotalSize;

                    _ = basicGarnetApi.SET(ref key, ref value);
                    i++;
                }
            }
            else if (storeTypeSpan.EqualsUpperCaseSpanIgnoringCase("OSTORE"u8))
            {
                TrackImportProgress(keyValuePairCount, isMainStore: false, keyValuePairCount == 0);
                while (i < keyValuePairCount)
                {
                    if (!RespReadUtils.TryReadSerializedData(out var key, out var data, out var expiration, ref payloadPtr, payloadEndPtr))
                        return false;

                    var value = clusterProvider.storeWrapper.GarnetObjectSerializer.Deserialize(data);
                    value.Expiration = expiration;
                    _ = basicGarnetApi.SET(key, value);
                    i++;
                }
            }

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER FLUSHALL
        /// </summary>
        /// <returns></returns>
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
    }
}