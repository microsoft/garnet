// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Tsavorite.core;

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

        static ReadOnlySpan<byte> MAIN_STORE => "SSTORE"u8;
        static ReadOnlySpan<byte> OBJECT_STORE => "OSTORE"u8;

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

            while (!RespWriteUtils.WriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //1
            while (!RespWriteUtils.WriteBulkString(AUTH, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            if (username != null)
            {
                //2
                while (!RespWriteUtils.WriteAsciiBulkString(username, ref curr, end))
                {
                    Flush();
                    curr = offset;
                }
                offset = curr;
            }

            //3
            while (!RespWriteUtils.WriteAsciiBulkString(password, ref curr, end))
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
            while (!RespWriteUtils.WriteBulkString(SETSLOTSRANGE, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            //3
            while (!RespWriteUtils.WriteBulkString(state.Span, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            if (nodeid != null)
            {
                //4
                while (!RespWriteUtils.WriteAsciiBulkString(nodeid, ref curr, end))
                {
                    Flush();
                    curr = offset;
                }
                offset = curr;
            }

            //5+
            foreach (var slotRange in slotRanges)
            {
                while (!RespWriteUtils.WriteIntegerAsBulkString(slotRange.Item1, ref curr, end))
                {
                    Flush();
                    curr = offset;
                }
                offset = curr;

                while (!RespWriteUtils.WriteIntegerAsBulkString(slotRange.Item2, ref curr, end))
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
        /// Check if migrate command parameters need to be initialized
        /// </summary>
        public bool InitMigrateCommand => curr == null;

        /// <summary>
        /// Getter to compute how much space to leave at the front of the buffer
        /// in order to write the maximum possible RESP length header (of length bufferSize)
        /// </summary>
        int ExtraSpace =>
            1                   // $
            + bufferSizeDigits  // Number of digits in maximum possible length (will be written with zero padding)
            + 2                 // \r\n
            + 4;                // We write a 4-byte int keyCount at the start of the payload

        bool isMainStore;
        byte* curr, head;
        int keyCount;
        TaskCompletionSource<string> currTcsMigrate = null;

        /// <summary>
        /// Flush and initialize buffers/parameters used for migrate command
        /// </summary>
        public void InitMigrateBuffer()
        {
            Flush();
            currTcsMigrate = null;
            curr = head = null;
            keyCount = 0;
        }

        /// <summary>
        /// Send key value pair and reset migrate buffers
        /// </summary>
        /// <returns></returns>
        public Task<string> SendAndResetMigrate()
        {
            if (keyCount == 0) return null;

            Debug.Assert(end - curr >= 2);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';

            // Payload format = [$length\r\n][number of keys (4 bytes)][raw key value pairs]\r\n
            var size = (int)(curr - 2 - head - (ExtraSpace - 4));
            //logger?.LogTrace("[MIGRATE]: storeType:{(storeType)} keyCount:({keyCount}) payLoadSize:({payloadSize})", isMainStore ? "MainStore" : "ObjectStore", keyCount, size);
            var success = RespWriteUtils.WritePaddedBulkStringLength(size, ExtraSpace - 4, ref head, end);
            Debug.Assert(success);

            // Number of key value pairs in payload
            *(int*)head = keyCount;

            // Reset offset and flush buffer
            offset = curr;
            Flush();
            Interlocked.Increment(ref numCommands);

            // Return outstanding task and reset current tcs
            var task = currTcsMigrate.Task;
            currTcsMigrate = null;
            curr = head = null;
            keyCount = 0;
            return task;
        }

        /// <summary>
        /// Try write key value pair for main store directly to the client buffer
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="migrateTask"></param>
        /// <returns></returns>
        public bool TryWriteKeyValueSpanByte(ref SpanByte key, ref SpanByte value, out Task<string> migrateTask)
        {
            migrateTask = null;
            // Try write key value pair directly to client buffer
            if (!WriteSerializedSpanByte(ref key, ref value))
            {
                // If failed to write because no space left send outstanding data and retrieve task
                // Caller is responsible for retrying
                migrateTask = SendAndResetMigrate();
                return false;
            }

            keyCount++;
            return true;
        }

        /// <summary>
        /// Try write key value pair for object store directly to the client buffer
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="expiration"></param>
        /// <param name="migrateTask"></param>
        /// <returns></returns>
        public bool TryWriteKeyValueByteArray(byte[] key, byte[] value, long expiration, out Task<string> migrateTask)
        {
            migrateTask = null;
            // Try write key value pair directly to client buffer
            if (!WriteSerializedKeyValueByteArray(key, value, expiration))
            {
                // If failed to write because no space left send outstanding data and retrieve task
                // Caller is responsible for retrying
                migrateTask = SendAndResetMigrate();
                return false;
            }

            keyCount++;
            return true;
        }

        /// <summary>
        /// Write parameters of CLUSTER MIGRATE directly to the client buffer
        /// </summary>
        /// <param name="sourceNodeId"></param>
        /// <param name="replaceOption"></param>
        /// <param name="isMainStore"></param>
        public void SetClusterMigrate(string sourceNodeId, Memory<byte> replaceOption, bool isMainStore)
        {
            currTcsMigrate = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(currTcsMigrate);
            curr = offset;
            this.isMainStore = isMainStore;
            var storeType = isMainStore ? MAIN_STORE : OBJECT_STORE;
            var arraySize = 6;

            while (!RespWriteUtils.WriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 1
            while (!RespWriteUtils.WriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 2
            while (!RespWriteUtils.WriteBulkString(MIGRATE, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 3
            while (!RespWriteUtils.WriteAsciiBulkString(sourceNodeId, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 4
            while (!RespWriteUtils.WriteBulkString(replaceOption.Span, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 5
            while (!RespWriteUtils.WriteBulkString(storeType, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 6
            // Reserve space for the bulk string header + final newline
            while (ExtraSpace + 2 > (int)(end - curr))
            {
                Flush();
                curr = offset;
            }
            head = curr;
            curr += ExtraSpace;
        }

        private bool WriteSerializedSpanByte(ref SpanByte key, ref SpanByte value)
        {
            // We include space for newline at the end, to be added before sending
            int totalLen = key.TotalSize + value.TotalSize + 2 + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *(int*)curr = key.Length;
            curr += sizeof(int);
            Buffer.MemoryCopy(key.ToPointerWithMetadata(), curr, key.Length, key.Length);
            curr += key.Length;
            *curr++ = (byte)key.MetadataSize;

            *(int*)curr = value.Length;
            curr += sizeof(int);
            Buffer.MemoryCopy(value.ToPointerWithMetadata(), curr, value.Length, value.Length);
            curr += value.Length;
            *curr++ = (byte)value.MetadataSize;

            return true;
        }

        private bool WriteSerializedKeyValueByteArray(byte[] key, byte[] value, long expiration)
        {
            // We include space for newline at the end, to be added before sending
            int totalLen = 4 + key.Length + 4 + value.Length + 8 + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *(int*)curr = key.Length;
            curr += 4;
            fixed (byte* keyPtr = key)
                Buffer.MemoryCopy(keyPtr, curr, key.Length, key.Length);
            curr += key.Length;

            *(int*)curr = value.Length;
            curr += 4;
            fixed (byte* valPtr = value)
                Buffer.MemoryCopy(valPtr, curr, value.Length, value.Length);
            curr += value.Length;

            *(long*)curr = expiration;
            curr += 8;

            return true;
        }
    }
}