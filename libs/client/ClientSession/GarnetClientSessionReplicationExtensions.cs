// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;

namespace Garnet.client
{
    /// <summary>
    /// Mono-threaded remote client session for Garnet (a session makes a single network connection, and 
    /// expects mono-threaded client access, i.e., no concurrent invocations of API by client)
    /// </summary>
    public sealed unsafe partial class GarnetClientSession : IServerHook, IMessageConsumer
    {
        private static ReadOnlySpan<byte> initiate_replica_sync => "INITIATE_REPLICA_SYNC"u8;
        private static ReadOnlySpan<byte> send_ckpt_metadata => "SEND_CKPT_METADATA"u8;
        private static ReadOnlySpan<byte> send_ckpt_file_segment => "SEND_CKPT_FILE_SEGMENT"u8;
        private static ReadOnlySpan<byte> begin_replica_recover => "BEGIN_REPLICA_RECOVER"u8;
        private static ReadOnlySpan<byte> attach_sync => "ATTACH_SYNC"u8;
        private static ReadOnlySpan<byte> sync => "SYNC"u8;
        private static ReadOnlySpan<byte> advance_time => "ADVANCE_TIME"u8;
        private static ReadOnlySpan<byte> snapshot_data => "SNAPSHOT_DATA"u8;

        /// <summary>
        /// Initiate checkpoint retrieval from replica by sending replica checkpoint information and AOF address range
        /// </summary>
        /// <param name="nodeId"></param>
        /// <param name="primary_replid"></param>
        /// <param name="checkpointEntryData"></param>
        /// <param name="aofBeginAddress"></param>
        /// <param name="aofTailAddress"></param>
        /// <returns></returns>
        /// <seealso cref="M:Garnet.cluster.ClusterSession.NetworkClusterInitiateReplicaSync"/>
        public Task<string> ExecuteClusterInitiateReplicaSync(string nodeId, string primary_replid, byte[] checkpointEntryData, Span<byte> aofBeginAddress, Span<byte> aofTailAddress)
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            byte* curr = offset;
            int arraySize = 7;

            while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //2
            while (!RespWriteUtils.TryWriteBulkString(initiate_replica_sync, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.TryWriteAsciiBulkString(nodeId, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //4
            while (!RespWriteUtils.TryWriteAsciiBulkString(primary_replid, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //5
            while (!RespWriteUtils.TryWriteBulkString(checkpointEntryData, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //6
            while (!RespWriteUtils.TryWriteBulkString(aofBeginAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //7
            while (!RespWriteUtils.TryWriteBulkString(aofTailAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            Flush();
            Interlocked.Increment(ref numCommands);
            return tcs.Task;
        }

        /// <summary>
        /// Send checkpoint metadata to replica
        /// </summary>
        /// <param name="fileTokenBytes"></param>
        /// <param name="fileType"></param>
        /// <param name="data"></param>
        /// <seealso cref="M:Garnet.cluster.ClusterSession.NetworkClusterSendCheckpointMetadata"/>
        public Task<string> ExecuteClusterSendCheckpointMetadata(Memory<byte> fileTokenBytes, int fileType, Memory<byte> data)
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            byte* curr = offset;
            int arraySize = 5;

            while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //2
            while (!RespWriteUtils.TryWriteBulkString(send_ckpt_metadata, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.TryWriteBulkString(fileTokenBytes.Span, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //4
            while (!RespWriteUtils.TryWriteArrayItem(fileType, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //5
            while (!RespWriteUtils.TryWriteBulkString(data.Span, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;
            Flush();
            Interlocked.Increment(ref numCommands);
            return tcs.Task;
        }

        /// <summary>
        /// Send checkpoint file segments
        /// </summary>
        /// <param name="fileTokenBytes"></param>
        /// <param name="fileType"></param>
        /// <param name="startAddress"></param>
        /// <param name="data"></param>
        /// <param name="segmentId"></param>
        /// <seealso cref="M:Garnet.cluster.ClusterSession.NetworkClusterSendCheckpointFileSegment"/>
        public Task<string> ExecuteClusterSendCheckpointFileSegment(Memory<byte> fileTokenBytes, int fileType, long startAddress, Span<byte> data, int segmentId = -1)
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            byte* curr = offset;
            int arraySize = 7;

            while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //2
            while (!RespWriteUtils.TryWriteBulkString(send_ckpt_file_segment, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.TryWriteBulkString(fileTokenBytes.Span, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //4
            while (!RespWriteUtils.TryWriteArrayItem(fileType, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //5
            while (!RespWriteUtils.TryWriteArrayItem(startAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //6
            while (!RespWriteUtils.TryWriteBulkString(data, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //7
            while (!RespWriteUtils.TryWriteArrayItem(segmentId, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            Flush();
            Interlocked.Increment(ref numCommands);
            return tcs.Task;
        }

        /// <summary>
        /// Send snapshot data (file segments or metadata) via unified CLUSTER SNAPSHOT_DATA command.
        /// </summary>
        /// <param name="fileTokenBytes">The checkpoint token bytes.</param>
        /// <param name="fileType">The checkpoint file type (including metadata variants).</param>
        /// <param name="startAddress">The start address for this chunk (-1 for metadata).</param>
        /// <param name="data">The data to send.</param>
        /// <seealso cref="M:Garnet.cluster.ClusterSession.NetworkClusterSnapshotData"/>
        public Task<string> ExecuteClusterSnapshotData(Memory<byte> fileTokenBytes, int fileType, long startAddress, Span<byte> data)
        {
            // The data payload must fit in the send buffer (as a RESP bulk string) after a flush,
            // otherwise the TryWriteBulkString/Flush loop below will spin forever.
            if (data.Length > networkBufferSettings.sendBufferSize)
                ExceptionUtils.ThrowException(new InvalidOperationException(
                    $"Snapshot data chunk ({data.Length} bytes) exceeds send buffer size ({networkBufferSettings.sendBufferSize} bytes)"));

            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            byte* curr = offset;
            int arraySize = 6;

            while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //2
            while (!RespWriteUtils.TryWriteBulkString(snapshot_data, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.TryWriteBulkString(fileTokenBytes.Span, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //4
            while (!RespWriteUtils.TryWriteArrayItem(fileType, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //5
            while (!RespWriteUtils.TryWriteArrayItem(startAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //6
            while (!RespWriteUtils.TryWriteBulkString(data, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            Flush();
            Interlocked.Increment(ref numCommands);
            return tcs.Task;
        }

        /// <summary>
        /// Signal replica to recover
        /// </summary>
        /// <param name="sendStoreCheckpoint"></param>
        /// <param name="replayAOF"></param>
        /// <param name="primary_replid"></param>
        /// <param name="checkpointEntryData"></param>
        /// <param name="beginAddress"></param>
        /// <param name="tailAddress"></param>
        /// <returns></returns>
        /// <seealso cref="M:Garnet.cluster.ClusterSession.NetworkClusterBeginReplicaRecover"/>
        public Task<string> ExecuteClusterBeginReplicaRecover(
            bool sendStoreCheckpoint,
            ulong replayAOF,
            string primary_replid,
            Span<byte> checkpointEntryData,
            Span<byte> beginAddress,
            Span<byte> tailAddress)
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            var curr = offset;
            var arraySize = 8;

            while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //2
            while (!RespWriteUtils.TryWriteBulkString(begin_replica_recover, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.TryWriteBulkString(sendStoreCheckpoint ? "1"u8 : "0"u8, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //4
            while (!RespWriteUtils.TryWriteArrayItem((long)replayAOF, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //5
            while (!RespWriteUtils.TryWriteAsciiBulkString(primary_replid, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //6
            while (!RespWriteUtils.TryWriteBulkString(checkpointEntryData, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //7
            while (!RespWriteUtils.TryWriteBulkString(beginAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //8
            while (!RespWriteUtils.TryWriteBulkString(tailAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            Flush();
            Interlocked.Increment(ref numCommands);
            return tcs.Task;
        }

        /// <summary>
        /// Initiate attach from replica
        /// </summary>
        /// <param name="syncMetadata"></param>
        /// <returns></returns>
        /// <seealso cref="M:Garnet.cluster.ClusterSession.NetworkClusterAttachSync"/>
        public Task<string> ExecuteClusterAttachSync(byte[] syncMetadata)
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            byte* curr = offset;
            int arraySize = 3;

            while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //2
            while (!RespWriteUtils.TryWriteBulkString(attach_sync, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.TryWriteBulkString(syncMetadata, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            Flush();
            Interlocked.Increment(ref numCommands);
            return tcs.Task;
        }

        /// <summary>
        /// Set CLUSTER ATTACH_SYNC header info
        /// </summary>
        /// <param name="sourceNodeId"></param>
        /// <seealso cref="T:Garnet.cluster.SnapshotIteratorManager"/>
        /// <seealso cref="M:Garnet.cluster.ClusterSession.NetworkClusterSync"/>
        public void SetClusterSyncHeader(string sourceNodeId)
        {
            // Unlike Migration, where we don't know at the time of header initialization if we have a record or not, in Replication 
            // we know we have a record at the time this is called, so we can initialize it directly.
            currTcsIterationTask = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(currTcsIterationTask);
            curr = offset;
            this.ist = IncrementalSendType.SYNC;

            var arraySize = 4;
            while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 1
            while (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 2
            while (!RespWriteUtils.TryWriteBulkString(sync, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 3
            while (!RespWriteUtils.TryWriteAsciiBulkString(sourceNodeId, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 4
            // Reserve space for the bulk string header + final newline
            while (ExtraSpace + 2 > (int)(end - curr))
            {
                Flush();
                curr = offset;
            }
            head = curr;
            curr += ExtraSpace;
        }

        /// <summary>
        /// Issue CLUSTER ADVANCE_TIME
        /// </summary>
        /// <param name="sequenceNumber"></param>
        /// <param name="aofAddress"></param>
        /// <returns></returns>
        /// <seealso cref="M:Garnet.cluster.ClusterSession.NetworkClusterAdvanceTime"/>
        public Task<string> ExecuteClusterAdvanceTime(long sequenceNumber, Span<byte> aofAddress)
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            var curr = offset;
            var argCount = 2;
            var arraySize = 2 + argCount;

            while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //2
            while (!RespWriteUtils.TryWriteBulkString(advance_time, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.TryWriteArrayItem(sequenceNumber, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //4
            while (!RespWriteUtils.TryWriteBulkString(aofAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            Flush();
            Interlocked.Increment(ref numCommands);
            return tcs.Task;
        }

        /// <summary>
        /// Issue CLUSTER APPEND_LOG with initialization parameters
        /// </summary>
        /// <seealso cref="T:Garnet.cluster.ClusterSession.NetworkClusterAppendLogInit"/>
        /// <param name="nodeId"></param>
        /// <param name="physicalSublogIdx"></param>
        /// <param name="previousAddress"></param>
        /// <param name="currentAddress"></param>
        /// <param name="nextAddress"></param>
        /// <exception cref="Exception"></exception>
        public Task<string> ExecuteClusterAppendLogInit(string nodeId, int physicalSublogIdx, long previousAddress, long currentAddress, long nextAddress)
        {
            Debug.Assert(nodeId != null);

            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            var curr = offset;
            var arraySize = 7;

            while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 1
            while (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 2
            while (!RespWriteUtils.TryWriteBulkString(appendLog, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 3
            while (!RespWriteUtils.TryWriteAsciiBulkString(nodeId, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 4
            while (!RespWriteUtils.TryWriteArrayItem(physicalSublogIdx, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 5
            while (!RespWriteUtils.TryWriteArrayItem(previousAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 6
            while (!RespWriteUtils.TryWriteArrayItem(currentAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 7
            while (!RespWriteUtils.TryWriteArrayItem(nextAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            Flush();
            Interlocked.Increment(ref numCommands);
            return tcs.Task;
        }
    }
}