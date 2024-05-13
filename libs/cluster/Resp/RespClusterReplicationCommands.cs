// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        /// <summary>
        /// Implements CLUSTER REPLICAS command
        /// </summary>
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterReplicas(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting exactly 0 arguments
            if (count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadStringWithLengthHeader(out var nodeid, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);
            var replicas = clusterProvider.clusterManager.ListReplicas(nodeid);

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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterReplicate(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting 1 or 2 arguments
            if (count is < 1 or > 2)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            var background = false;
            if (!RespReadUtils.ReadStringWithLengthHeader(out var nodeid, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (count > 1)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out var backgroundFlag, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (backgroundFlag.Equals("SYNC", StringComparison.OrdinalIgnoreCase))
                    background = false;
                else if (backgroundFlag.Equals("ASYNC", StringComparison.OrdinalIgnoreCase))
                    background = true;
                else
                {
                    while (!RespWriteUtils.WriteError($"ERR Invalid CLUSTER REPLICATE FLAG ({backgroundFlag}) not valid", ref dcurr, dend))
                        SendAndReset();
                    readHead = (int)(ptr - recvBufferPtr);
                    return true;
                }
            }
            readHead = (int)(ptr - recvBufferPtr);

            if (!clusterProvider.serverOptions.EnableAOF)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!clusterProvider.replicationManager.TryBeginReplicate(this, nodeid, background, false, out var errorMessage))
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
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterAOFSync(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (count != 2)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadStringWithLengthHeader(out var nodeid, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadLongWithLengthHeader(out long nextAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (clusterProvider.serverOptions.EnableAOF)
            {
                clusterProvider.replicationManager.TryAddReplicationTask(nodeid, nextAddress, out var aofSyncTaskInfo);
                if (!clusterProvider.replicationManager.TryConnectToReplica(nodeid, nextAddress, aofSyncTaskInfo, out var errorMessage))
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
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterAppendLog(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 5 arguments (5-th argument is AOF page parsed later)
            if (count != 5)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadStringWithLengthHeader(out string nodeId, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadLongWithLengthHeader(out long previousAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadLongWithLengthHeader(out long currentAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadLongWithLengthHeader(out long nextAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;

            byte* record = null;
            var recordLength = 0;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref record, ref recordLength, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

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
                clusterProvider.replicationManager.ProcessPrimaryStream(record, recordLength, previousAddress, currentAddress, nextAddress);
                //while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                //    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER initiate_replica_sync command (only for internode use)
        /// </summary>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterInitiateReplicaSync(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 5 arguments
            if (count != 5)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadStringWithLengthHeader(out var nodeId, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadStringWithLengthHeader(out var primary_replid, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var cEntryByteArray, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadLongWithLengthHeader(out var replicaAofBeginAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadLongWithLengthHeader(out var replicaAofTailAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            var remoteEntry = CheckpointEntry.FromByteArray(cEntryByteArray);

            if (!clusterProvider.replicationManager.TryBeginReplicaSyncSession(
                nodeId, primary_replid, remoteEntry, replicaAofBeginAddress, replicaAofTailAddress, out var errorMessage))
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
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSendCheckpointMetadata(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 3 arguments
            if (count != 3)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var fileTokenBytes, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadIntWithLengthHeader(out var fileTypeInt, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var checkpointMetadata, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

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
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSendCheckpointFileSegment(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (count != 5)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            Span<byte> data = default;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var fileTokenBytes, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadIntWithLengthHeader(out var ckptFileTypeInt, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadLongWithLengthHeader(out var startAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadSpanByteWithLengthHeader(ref data, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadIntWithLengthHeader(out var segmentId, ref ptr, recvBufferPtr + bytesRead))
                return false;

            readHead = (int)(ptr - recvBufferPtr);
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
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterBeginReplicaRecover(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 7 arguments
            if (count != 7)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadBoolWithLengthHeader(out var recoverMainStoreFromToken, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadBoolWithLengthHeader(out var recoverObjectStoreFromToken, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadBoolWithLengthHeader(out var replayAOF, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadStringWithLengthHeader(out var primary_replid, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var cEntryByteArray, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadLongWithLengthHeader(out var beginAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadLongWithLengthHeader(out var tailAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            var entry = CheckpointEntry.FromByteArray(cEntryByteArray);
            var replicationOffset = clusterProvider.replicationManager.BeginReplicaRecover(
                recoverMainStoreFromToken,
                recoverObjectStoreFromToken,
                replayAOF,
                primary_replid,
                entry,
                beginAddress,
                tailAddress);
            while (!RespWriteUtils.WriteInteger(replicationOffset, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}