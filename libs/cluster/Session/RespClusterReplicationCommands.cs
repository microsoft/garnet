// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
using Garnet.client;
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

                // Cannot avoid blocking here we're on the network thread
                var (success, errorMessage) =
                    AsyncUtils.BlockingWait(
                        clusterProvider.serverOptions.ReplicaDisklessSync ?
                            clusterProvider.replicationManager.TryReplicateDisklessSyncAsync(this, syncOpts) :
                            clusterProvider.replicationManager.TryReplicateDiskbasedSyncAsync(this, syncOpts)
                    );

                if (success)
                {
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteError(errorMessage.Span, ref dcurr, dend))
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

            if (!vectorManager.TryReserveContextsForMigration(ref vectorBasicContext, numVectorSetContexts, out var newContexts))
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
        /// Implements CLUSTER appendlog command (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="M:Garnet.client.GarnetClientSession.ExecuteClusterAppendLog"/>
        private bool NetworkClusterAppendLog(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 6 arguments (6-th argument is AOF page parsed later)
            if (parseState.Count > 6 || parseState.Count < 5)
            {
                invalidParameters = true;
                return true;
            }

            var nodeId = parseState.GetString(0);

            if (!parseState.TryGetInt(1, out var physicalSublogIdx) ||
                !parseState.TryGetLong(2, allowLeadingZeros: true, out var previousAddress) ||
                !parseState.TryGetLong(3, allowLeadingZeros: true, out var currentAddress) ||
                !parseState.TryGetLong(4, allowLeadingZeros: true, out var nextAddress))
            {
                logger?.LogError("{str}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER));
                return true;
            }

            LogPrimaryStream(physicalSublogIdx, previousAddress, currentAddress, nextAddress, logger);

            // This is an initialization message
            if (previousAddress == -1 && currentAddress == -1 && nextAddress == -1)
            {
                if (clusterProvider.replicationManager.InitializeReplicaReplayDriver(physicalSublogIdx, networkSender))
                    replicaReplayDriverStore = clusterProvider.replicationManager.ReplicaReplayDriverStore;
                else
                    throw new GarnetException($"Failed to process {nameof(NetworkClusterAppendLog)}: [physicalSublogIdx: {physicalSublogIdx}] Received initialization message but ReplicaReplayDriver is already initialized!", LogLevel.Error, clientResponse: false);

                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
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

            [Conditional("DEBUG")]
            static void LogPrimaryStream(int physicalSublogIdx, long previousAddress, long currentAddress, long nextAddress, ILogger logger)
            {
                var state = new GarnetTestLoggingEvent()
                {
                    Type = GarnetTestLoggingEventType.LogPrimaryStreamType,
                    Message = $"physicalSublogIdx: {physicalSublogIdx}, previousAddress: {previousAddress}, currentAddress: {currentAddress}, nextAddress: {nextAddress}",
                };

                logger?.LogTesting(state);
            }
        }

        /// <summary>
        /// Implements CLUSTER initiate_replica_sync command (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="M:Garnet.client.GarnetClientSession.ExecuteClusterInitiateReplicaSync"/>
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

            var replicaAofBeginAddress = AofAddress.FromSpan(parseState.GetArgSliceByRef(3).Span);
            var replicaAofTailAddress = AofAddress.FromSpan(parseState.GetArgSliceByRef(4).Span);

            var replicaCheckpointEntry = CheckpointEntry.FromByteArray(checkpointEntryBytes);

            var beginPrimarySyncTask = clusterProvider.replicationManager.TryBeginDiskbasedSyncAsync(replicaNodeId, replicaAssignedPrimaryId, replicaCheckpointEntry, replicaAofBeginAddress, replicaAofTailAddress);

            // No choice but to block here, we're on the network thread
            var (success, errorMessage) = AsyncUtils.BlockingWait(beginPrimarySyncTask);

            if (!success)
            {
                while (!RespWriteUtils.TryWriteError(errorMessage.Span, ref dcurr, dend))
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
        /// <seealso cref="M:Garnet.client.GarnetClientSession.ExecuteClusterSendCheckpointMetadata"/>
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

            var checkpointMetadata = parseState.GetArgSliceByRef(2).ReadOnlySpan;

            var fileToken = new Guid(fileTokenBytes);
            var fileType = (CheckpointFileType)fileTypeInt;
            clusterProvider.replicationManager.recvCheckpointHandler.ProcessMetadata(fileToken, fileType, checkpointMetadata);
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER send_ckpt_file_segment command (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="M:Garnet.client.GarnetClientSession.ExecuteClusterSendCheckpointFileSegment"/>
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

            // segmentId is validated for backward compatibility but not used;
            // disk-based replication now uses the SNAPSHOT_DATA command path instead.
            _ = segmentId;

            var fileToken = new Guid(fileTokenBytes);
            var ckptFileType = (CheckpointFileType)ckptFileTypeInt;

            // Commenting due to high verbosity
            // logger?.LogTrace("send_ckpt_file_segment {fileToken} {ckptFileType} {startAddress} {dataLength}", fileToken, ckptFileType, startAddress, data.Length);
            clusterProvider.replicationManager.recvCheckpointHandler.ProcessFileSegment(fileToken, ckptFileType, startAddress, data);
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER SNAPSHOT_DATA command (only for internode use).
        /// Unified command for receiving both file segments and metadata from a primary.
        /// API: CLUSTER SNAPSHOT_DATA token type startAddress data
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="M:Garnet.client.GarnetClientSession.ExecuteClusterSnapshotData"/>
        private bool NetworkClusterSnapshotData(out bool invalidParameters)
        {
            invalidParameters = false;

            if (parseState.Count != 4)
            {
                invalidParameters = true;
                return true;
            }

            var fileTokenBytes = parseState.GetArgSliceByRef(0).ReadOnlySpan;

            if (!parseState.TryGetInt(1, out var ckptFileTypeInt) ||
                !parseState.TryGetLong(2, out var startAddress))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var data = parseState.GetArgSliceByRef(3).ReadOnlySpan;

            var fileToken = new Guid(fileTokenBytes);
            var ckptFileType = (CheckpointFileType)ckptFileTypeInt;

            clusterProvider.replicationManager.recvCheckpointHandler.ProcessSnapshotData(fileToken, ckptFileType, startAddress, data);
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER begin_replica_recover (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="M:Garnet.client.GarnetClientSession.ExecuteClusterBeginReplicaRecover"/>
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

            var beginAddress = AofAddress.FromSpan(parseState.GetArgSliceByRef(4).Span);
            var tailAddress = AofAddress.FromSpan(parseState.GetArgSliceByRef(5).Span);

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
        /// <seealso cref="M:Garnet.client.GarnetClientSession.ExecuteClusterAttachSync"/>
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
            {
                var attachTask = clusterProvider.replicationManager.TryBeginDisklessSyncAsync(syncMetadata);

                // Must block here because we're on the network thread
                var (_, err) = AsyncUtils.BlockingWait(attachTask);
                errorMessage = err.Span;
            }
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
        /// <seealso cref="M:Garnet.cluster.SnapshotIteratorManager"/>
        /// <seealso cref="M:Garnet.cluster.ReplicaSyncSession.TryWriteRecordSpan"/>        
        /// <seealso cref="M:Garnet.client.GarnetClientSession.SetClusterSyncHeader"/>
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
                    var kind = (MigrationRecordSpanType)(*payloadPtr);
                    payloadPtr++;

                    if (kind == MigrationRecordSpanType.LogRecord)
                    {

                        if (!RespReadUtils.GetSerializedRecordSpan(out var recordSpan, ref payloadPtr, payloadEndPtr))
                            return false;

                        diskLogRecord = DiskLogRecord.Deserialize(recordSpan, storeWrapper.GarnetObjectSerializer, transientObjectIdMap, storeWrapper.storeFunctions);
                        _ = basicGarnetApi.SET(in diskLogRecord);
                        storeWrapper.storeFunctions.OnDisposeDiskRecord(ref diskLogRecord, DisposeReason.DeserializedFromDisk);
                        diskLogRecord.Dispose();
                        diskLogRecord = default; // prevent double-trigger in catch
                    }
                    else
                    {
                        throw new InvalidOperationException($"Unexpected {nameof(MigrationRecordSpanType)}: {kind}");
                    }

                    i++;
                }
            }
            catch
            {
                // Dispose the diskLogRecord if there was an exception in SET
                if (diskLogRecord.IsSet)
                {
                    storeWrapper.storeFunctions.OnDisposeDiskRecord(ref diskLogRecord, DisposeReason.DeserializedFromDisk);
                    diskLogRecord.Dispose();
                }
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
        /// <seealso cref="M:Garnet.cluster.ReplicaSyncSession.IssueFlushAllAsync"/>
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
        /// Implements CLUSTER_ADVANCE_TIME
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="M:Garnet.client.GarnetClientSession.ExecuteClusterAdvanceTime"/>
        private bool NetworkClusterAdvanceTime(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 2
            if (parseState.Count != 2)
            {
                invalidParameters = true;
                return true;
            }

            var sequenceNumber = parseState.GetLong(0);
            var tailAddressSpan = parseState.GetArgSliceByRef(1).Span;
            clusterProvider.replicationManager.SignalAdvanceTime(sequenceNumber, AofAddress.FromSpan(tailAddressSpan));
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// Implements CLUSTER MLOG_KEY_TIME command.
        /// If node is replica, it returns the sequence number associated with the provided key otherwise the latest sequence number as generated by the sequence number generator.
        /// For nodes configured with single log it returns an error.
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterMlogKeyTime(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting 1 or 2 arguments (key and optional FRONTIER)
            if (parseState.Count < 1 || parseState.Count > 2)
            {
                invalidParameters = true;
                return true;
            }

            // Check if multi-log is enabled
            if (!clusterProvider.serverOptions.MultiLogEnabled)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_MULTI_LOG_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var sbKey = parseState.GetArgSliceByRef(0).Span;

            long sequenceNumber;
            // Return sequence number for the specific key
            // Check if this node is a replica
            if (!clusterProvider.clusterManager.CurrentConfig.IsPrimary)
            {
                var getFrontier = parseState.Count == 2 ? parseState.GetBool(1) : false;
                // Get sequence number from the replay state
                var sn = clusterProvider.storeWrapper.appendOnlyFile.readConsistencyManager?.GetKeySequenceNumber(sbKey, getFrontier);
                sequenceNumber = sn.GetValueOrDefault(-1);
            }
            else
            {
                // Get sequence number from the primary's sequence number generator
                sequenceNumber = clusterProvider.storeWrapper.appendOnlyFile.seqNumGen.GetSequenceNumber();
            }
            while (!RespWriteUtils.TryWriteInt64(sequenceNumber, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}