// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
        static ReadOnlySpan<byte> AUTH => "AUTH"u8;
        static ReadOnlySpan<byte> SETSLOTSRANGE => "SETSLOTSRANGE"u8;
        static ReadOnlySpan<byte> MIGRATE => "MIGRATE"u8;
        static ReadOnlySpan<byte> DELKEY => "DELKEY"u8;
        static ReadOnlySpan<byte> GETKVPAIRINSLOT => "GETKVPAIRINSLOT"u8;

        static ReadOnlySpan<byte> T => "T"u8;
        static ReadOnlySpan<byte> F => "F"u8;

        /// <summary>
        /// Send AUTH command to target node to authenticate connection.
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public Task<string> Authenticate(string username, string password)
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            byte* curr = offset;
            int arraySize = username == null ? 2 : 3;

            while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.TryWriteBulkString(AUTH, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            if (username != null)
            {
                //2
                while (!RespWriteUtils.TryWriteAsciiBulkString(username, ref curr, end))
                {
                    Flush();
                    curr = offset;
                }
                offset = curr;
            }

            //3
            while (!RespWriteUtils.TryWriteAsciiBulkString(password, ref curr, end))
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
        /// SETSLOTSRANGE command
        /// </summary>
        /// <param name="state"></param>
        /// <param name="nodeid"></param>
        /// <param name="slotRanges"></param>
        /// <returns></returns>
        public Task<string> SetSlotRange(Memory<byte> state, string nodeid, List<(int, int)> slotRanges)
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            byte* curr = offset;
            int arraySize = nodeid == null ? 3 + slotRanges.Count * 2 : 4 + slotRanges.Count * 2;

            //CLUSTER SETSLOTRANGE IMPORTING <source-node-id> <slot-start> <slot-end> [slot-start slot-end]
            //CLUSTER SETSLOTRANGE MIGRATING <destination-node-id> <slot-start> <slot-end> [slot-start slot-end]
            //CLUSTER SETSLOTRANGE NODE <node-id> <slot-start> <slot-end> [slot-start slot-end]
            //CLUSTER SETSLOTRANGE STABLE <slot-start> <slot-end> [slot-start slot-end]

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
            while (!RespWriteUtils.TryWriteBulkString(SETSLOTSRANGE, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.TryWriteBulkString(state.Span, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            if (nodeid != null)
            {
                //4
                while (!RespWriteUtils.TryWriteAsciiBulkString(nodeid, ref curr, end))
                {
                    Flush();
                    curr = offset;
                }
                offset = curr;
            }

            //5+
            foreach (var slotRange in slotRanges)
            {
                while (!RespWriteUtils.TryWriteInt32AsBulkString(slotRange.Item1, ref curr, end))
                {
                    Flush();
                    curr = offset;
                }
                offset = curr;

                while (!RespWriteUtils.TryWriteInt32AsBulkString(slotRange.Item2, ref curr, end))
                {
                    Flush();
                    curr = offset;
                }
                offset = curr;
            }

            Flush();
            Interlocked.Increment(ref numCommands);
            return tcs.Task;
        }

        /// <summary>
        /// Write parameters of CLUSTER MIGRATE directly to the client buffer
        /// </summary>
        /// <param name="sourceNodeId"></param>
        /// <param name="replace"></param>
        public void SetClusterMigrateHeader(string sourceNodeId, bool replace)
        {
            // For Migration we send the (curr - end) buffer as the SpanByteAndMemory.SpanByte output to Tsavorite. Thus we must
            // initialize the header first, so we have curr properly positioned, but we cannot yet enqueue currTcsIterationTask.
            // Therefore we defer this until the actual Flush(), when we know we have records to send. This is not a concern for
            // Replication, because it uses an iterator and thus knows it has a record before it initializes the header.
            curr = offset;
            this.ist = IncrementalSendType.MIGRATE;
            var replaceOption = replace ? T : F;

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
            while (!RespWriteUtils.TryWriteBulkString(MIGRATE, ref curr, end))
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
            while (!RespWriteUtils.TryWriteBulkString(replaceOption, ref curr, end))
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

        /// <summary>
        /// Signal completion of migration by sending an empty payload
        /// </summary>
        /// <param name="sourceNodeId"></param>
        /// <param name="replace"></param>
        /// <returns></returns>
        public Task<string> CompleteMigrate(string sourceNodeId, bool replace)
        {
            SetClusterMigrateHeader(sourceNodeId, replace);

            Debug.Assert(end - curr >= 2);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';

            // Payload format = [$length\r\n][number of keys (4 bytes)][raw key value pairs]\r\n
            var size = (int)(curr - 2 - head - (ExtraSpace - 4));
            TrackIterationProgress(recordCount, size, completed: true);
            var success = RespWriteUtils.TryWritePaddedBulkStringLength(size, ExtraSpace - 4, ref head, end);
            Debug.Assert(success);

            // Number of key value pairs in payload
            *(int*)head = recordCount;

            // Reset offset and flush buffer
            offset = curr;
            EnsureTcsIsEnqueued();
            Flush();
            Interlocked.Increment(ref numCommands);

            // Return outstanding task and reset current tcs
            var task = currTcsIterationTask.Task;
            currTcsIterationTask = null;
            curr = head = null;
            recordCount = 0;
            return task;
        }
    }
}