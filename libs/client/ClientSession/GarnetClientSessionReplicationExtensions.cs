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

        /// <summary>
        /// Initiate checkpoint retrieval from replica by sending replica checkpoint information and AOF address range
        /// </summary>
        /// <param name="nodeId"></param>
        /// <param name="primary_replid"></param>
        /// <param name="checkpointEntryData"></param>
        /// <param name="aofBeginAddress"></param>
        /// <param name="aofTailAddress"></param>
        /// <returns></returns>
        public Task<string> ExecuteReplicaSync(string nodeId, string primary_replid, byte[] checkpointEntryData, long aofBeginAddress, long aofTailAddress)
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
            while (!RespWriteUtils.TryWriteArrayItem(aofBeginAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //7
            while (!RespWriteUtils.TryWriteArrayItem(aofTailAddress, ref curr, end))
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
        public Task<string> ExecuteSendCkptMetadata(Memory<byte> fileTokenBytes, int fileType, Memory<byte> data)
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
        public Task<string> ExecuteSendFileSegments(Memory<byte> fileTokenBytes, int fileType, long startAddress, Span<byte> data, int segmentId = -1)
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
        /// <param name="sendObjectStoreCheckpoint"></param>
        /// <param name="replayAOF"></param>
        /// <param name="primary_replid"></param>
        /// <param name="checkpointEntryData"></param>
        /// <param name="beginAddress"></param>
        /// <param name="tailAddress"></param>
        /// <returns></returns>
        public Task<string> ExecuteBeginReplicaRecover(bool sendStoreCheckpoint, bool sendObjectStoreCheckpoint, bool replayAOF, string primary_replid, byte[] checkpointEntryData, long beginAddress, long tailAddress)
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            byte* curr = offset;
            int arraySize = 9;

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
            while (!RespWriteUtils.TryWriteBulkString(sendObjectStoreCheckpoint ? "1"u8 : "0"u8, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //5
            while (!RespWriteUtils.TryWriteBulkString(replayAOF ? "1"u8 : "0"u8, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //6
            while (!RespWriteUtils.TryWriteAsciiBulkString(primary_replid, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //7
            while (!RespWriteUtils.TryWriteBulkString(checkpointEntryData, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //8
            while (!RespWriteUtils.TryWriteArrayItem(beginAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //9
            while (!RespWriteUtils.TryWriteArrayItem(tailAddress, ref curr, end))
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
        public Task<string> ExecuteAttachSync(byte[] syncMetadata)
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
        /// Set CLUSTER SYNC header info
        /// </summary>
        /// <param name="sourceNodeId"></param>
        /// <param name="isMainStore"></param>
        public void SetClusterSyncHeader(string sourceNodeId, bool isMainStore)
        {
            currTcsIterationTask = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(currTcsIterationTask);
            curr = offset;
            this.isMainStore = isMainStore;
            this.ist = IncrementalSendType.SYNC;
            var storeType = isMainStore ? MAIN_STORE : OBJECT_STORE;

            var arraySize = 5;
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
            while (!RespWriteUtils.TryWriteBulkString(storeType, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 5
            // Reserve space for the bulk string header + final newline
            while (ExtraSpace + 2 > (int)(end - curr))
            {
                Flush();
                curr = offset;
            }
            head = curr;
            curr += ExtraSpace;
        }
    }
}