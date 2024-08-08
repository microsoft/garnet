// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using Garnet.server;

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

            // Expecting exactly 0 arguments
            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var nodeId = parseState.GetString(0);
            var replicas = clusterProvider.clusterManager.ListReplicas(nodeId);

            while (!RespWriteUtils.WriteArrayLength(replicas.Count, ref dcurr, dend))
                SendAndReset();

            foreach (var replica in replicas)
            {
                while (!RespWriteUtils.WriteAsciiBulkString(replica, ref dcurr, dend))
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

            var background = false;
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
                    while (!RespWriteUtils.WriteError(
                               $"ERR Invalid CLUSTER REPLICATE FLAG ({Encoding.ASCII.GetString(backgroundFlagSpan)}) not valid",
                               ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            if (!clusterProvider.serverOptions.EnableAOF)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!clusterProvider.replicationManager.TryBeginReplicate(this, nodeId, background: background, force: false, out var errorMessage))
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
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
            var nextAddress = parseState.GetLong(1);

            if (clusterProvider.serverOptions.EnableAOF)
            {
                clusterProvider.replicationManager.TryAddReplicationTask(nodeId, nextAddress, out var aofSyncTaskInfo);
                if (!clusterProvider.replicationManager.TryConnectToReplica(nodeId, nextAddress, aofSyncTaskInfo, out var errorMessage))
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF, ref dcurr, dend))
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
            var previousAddress = parseState.GetLong(1);
            var currentAddress = parseState.GetLong(2);
            var nextAddress = parseState.GetLong(3);
            var sbRecord = parseState.GetArgSliceByRef(4).SpanByte;

            var currentConfig = clusterProvider.clusterManager.CurrentConfig;
            var localRole = currentConfig.LocalNodeRole;
            var primaryId = currentConfig.LocalNodePrimaryId;
            if (localRole != NodeRole.REPLICA)
            {
                // TODO: handle this
                //while (!RespWriteUtils.WriteError("ERR aofsync node not a replica"u8, ref dcurr, dend))
                //    SendAndReset();
            }
            else if (!primaryId.Equals(nodeId))
            {
                // TODO: handle this
                //while (!RespWriteUtils.WriteError($"ERR aofsync node replicating {primaryId}", ref dcurr, dend))
                //    SendAndReset();
            }
            else
            {
                clusterProvider.replicationManager.ProcessPrimaryStream(sbRecord.ToPointer(), sbRecord.Length,
                    previousAddress, currentAddress, nextAddress);
                //while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                //    SendAndReset();
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

            var nodeId = parseState.GetString(0);
            var primaryReplicaId = parseState.GetString(1);
            var checkpointEntryBytes = parseState.GetArgSliceByRef(2).SpanByte.ToByteArray();
            var replicaAofBeginAddress = parseState.GetLong(3);
            var replicaAofTailAddress = parseState.GetLong(4);
            
            var remoteEntry = CheckpointEntry.FromByteArray(checkpointEntryBytes);

            if (!clusterProvider.replicationManager.TryBeginReplicaSyncSession(
                nodeId, primaryReplicaId, remoteEntry, replicaAofBeginAddress, replicaAofTailAddress, out var errorMessage))
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
            var fileTypeInt = parseState.GetInt(1);
            var checkpointMetadata = parseState.GetArgSliceByRef(2).SpanByte.ToByteArray();

            var fileToken = new Guid(fileTokenBytes);
            var fileType = (CheckpointFileType)fileTypeInt;
            clusterProvider.replicationManager.ProcessCheckpointMetadata(fileToken, fileType, checkpointMetadata);
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
            var ckptFileTypeInt = parseState.GetInt(1);
            var startAddress = parseState.GetLong(2);
            var data = parseState.GetArgSliceByRef(3).ReadOnlySpan;
            var segmentId = parseState.GetInt(4);
            
            var fileToken = new Guid(fileTokenBytes);
            var ckptFileType = (CheckpointFileType)ckptFileTypeInt;

            // Commenting due to high verbosity
            // logger?.LogTrace("send_ckpt_file_segment {fileToken} {ckptFileType} {startAddress} {dataLength}", fileToken, ckptFileType, startAddress, data.Length);
            clusterProvider.replicationManager.recvCheckpointHandler.ProcessFileSegments(segmentId, fileToken, ckptFileType, startAddress, data);
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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

            var recoverMainStoreFromToken = parseState.GetBool(0);
            var recoverObjectStoreFromToken = parseState.GetBool(1);
            var replayAOF = parseState.GetBool(2);
            var primaryReplicaId = parseState.GetString(3);
            var checkpointEntryBytes = parseState.GetArgSliceByRef(4).SpanByte.ToByteArray();
            var beginAddress = parseState.GetLong(5);
            var tailAddress = parseState.GetLong(6);

            var entry = CheckpointEntry.FromByteArray(checkpointEntryBytes);
            var replicationOffset = clusterProvider.replicationManager.BeginReplicaRecover(
                recoverMainStoreFromToken,
                recoverObjectStoreFromToken,
                replayAOF,
                primaryReplicaId,
                entry,
                beginAddress,
                tailAddress);
            while (!RespWriteUtils.WriteInteger(replicationOffset, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}