// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
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
        static ReadOnlySpan<byte> initiate_replica_sync => "initiate_replica_sync"u8;
        static ReadOnlySpan<byte> send_ckpt_metadata => "send_ckpt_metadata"u8;
        static ReadOnlySpan<byte> send_ckpt_file_segment => "send_ckpt_file_segment"u8;
        static ReadOnlySpan<byte> begin_replica_recover => "begin_replica_recover"u8;

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

            while (!RespWriteUtils.WriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.WriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //2
            while (!RespWriteUtils.WriteBulkString(initiate_replica_sync, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.WriteBulkString(nodeId, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //4
            while (!RespWriteUtils.WriteBulkString(primary_replid, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //5
            while (!RespWriteUtils.WriteBulkString(checkpointEntryData, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //6
            while (!RespWriteUtils.WriteArrayItem(aofBeginAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //7
            while (!RespWriteUtils.WriteArrayItem(aofTailAddress, ref curr, end))
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

            while (!RespWriteUtils.WriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.WriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //2
            while (!RespWriteUtils.WriteBulkString(send_ckpt_metadata, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.WriteBulkString(fileTokenBytes.Span, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //4
            while (!RespWriteUtils.WriteArrayItem(fileType, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //5
            while (!RespWriteUtils.WriteBulkString(data.Span, ref curr, end))
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

            while (!RespWriteUtils.WriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.WriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //2
            while (!RespWriteUtils.WriteBulkString(send_ckpt_file_segment, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.WriteBulkString(fileTokenBytes.Span, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //4
            while (!RespWriteUtils.WriteArrayItem(fileType, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //5
            while (!RespWriteUtils.WriteArrayItem(startAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //6
            while (!RespWriteUtils.WriteBulkString(data, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //7
            while (!RespWriteUtils.WriteArrayItem(segmentId, ref curr, end))
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
            int arraySize = 8;

            while (!RespWriteUtils.WriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.WriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //2
            while (!RespWriteUtils.WriteBulkString(begin_replica_recover, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.WriteBulkString(sendStoreCheckpoint ? Encoding.ASCII.GetBytes("1") : Encoding.ASCII.GetBytes("0"), ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //4
            while (!RespWriteUtils.WriteBulkString(sendObjectStoreCheckpoint ? Encoding.ASCII.GetBytes("1") : Encoding.ASCII.GetBytes("0"), ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //5
            while (!RespWriteUtils.WriteBulkString(replayAOF ? Encoding.ASCII.GetBytes("1") : Encoding.ASCII.GetBytes("0"), ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //6
            while (!RespWriteUtils.WriteBulkString(primary_replid, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //7
            while (!RespWriteUtils.WriteBulkString(checkpointEntryData, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //8
            while (!RespWriteUtils.WriteArrayItem(beginAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //9
            while (!RespWriteUtils.WriteArrayItem(tailAddress, ref curr, end))
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