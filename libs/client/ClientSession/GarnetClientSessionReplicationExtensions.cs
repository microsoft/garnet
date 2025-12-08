// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        static ReadOnlySpan<byte> initiate_replica_sync => "INITIATE_REPLICA_SYNC"u8;
        static ReadOnlySpan<byte> send_ckpt_metadata => "SEND_CKPT_METADATA"u8;
        static ReadOnlySpan<byte> send_ckpt_file_segment => "SEND_CKPT_FILE_SEGMENT"u8;
        static ReadOnlySpan<byte> begin_replica_recover => "BEGIN_REPLICA_RECOVER"u8;
        static ReadOnlySpan<byte> attach_sync => "ATTACH_SYNC"u8;
        static ReadOnlySpan<byte> sync => "SYNC"u8;
        static ReadOnlySpan<byte> sharded_log_key_sequence_vector => "SHARDED_LOG_KEY_SEQUENCE_VECTOR"u8;

        /// <summary>
        /// Initiate checkpoint retrieval from replica by sending replica checkpoint information and AOF address range
        /// </summary>
        /// <param name="nodeId"></param>
        /// <param name="primary_replid"></param>
        /// <param name="checkpointEntryData"></param>
        /// <param name="aofBeginAddress"></param>
        /// <param name="aofTailAddress"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.cluster.ClusterSession.NetworkClusterInitiateReplicaSync"/>
        public Task<string> ExecuteClusterInitiateReplicaSync(string nodeId, string primary_replid, byte[] checkpointEntryData, byte[] aofBeginAddress, byte[] aofTailAddress)
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
        /// <seealso cref="T:Garnet.cluster.ClusterSession.NetworkClusterSendCheckpointMetadata"/>
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
        /// <seealso cref="T:Garnet.cluster.ClusterSession.NetworkClusterSendCheckpointFileSegment"/>
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
        /// Signal replica to recover
        /// </summary>
        /// <param name="sendStoreCheckpoint"></param>
        /// <param name="replayAOF"></param>
        /// <param name="primary_replid"></param>
        /// <param name="checkpointEntryData"></param>
        /// <param name="beginAddress"></param>
        /// <param name="tailAddress"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.cluster.ClusterSession.NetworkClusterBeginReplicaRecover"/>
        public Task<string> ExecuteClusterBeginReplicaRecover(
            bool sendStoreCheckpoint,
            ulong replayAOF,
            string primary_replid,
            byte[] checkpointEntryData,
            byte[] beginAddress,
            byte[] tailAddress)
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
        /// <seealso cref="T:Garnet.cluster.ClusterSession.NetworkClusterAttachSync"/>
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
        /// <seealso cref="T:Garnet.cluster.ClusterSession.NetworkClusterSync"/>
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
        /// Issue CLUSTER SHARDED_LOG_KEY_SEQUENCE_VECTOR
        /// </summary>
        /// <returns></returns>
        public Task<string> ExecuteClusterShardedLogKeySequenceVector()
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            var curr = offset;
            var arraySize = 2;

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
            while (!RespWriteUtils.TryWriteBulkString(sharded_log_key_sequence_vector, ref curr, end))
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