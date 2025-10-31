// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.client
{
    enum IncrementalSendType : byte
    {
        MIGRATE,
        SYNC
    }

    public sealed unsafe partial class GarnetClientSession : IServerHook, IMessageConsumer
    {
        IncrementalSendType ist;
        bool isMainStore;
        byte* curr, head;
        int keyValuePairCount;
        TaskCompletionSource<string> currTcsIterationTask = null;

        /// <summary>
        /// Getter to compute how much space to leave at the front of the buffer
        /// in order to write the maximum possible RESP length header (of length bufferSize)
        /// </summary>
        int ExtraSpace =>
            1                   // $
            + bufferSizeDigits  // Number of digits in maximum possible length (will be written with zero padding)
            + 2                 // \r\n
            + 4;                // We write a 4-byte int keyCount at the start of the payload

        /// <summary>
        /// Check if header for batch is initialized
        /// </summary>
        public bool NeedsInitialization => curr == null;

        /// <summary>
        /// Flush and initialize buffers/parameters used for migrate command
        /// </summary>
        /// <param name="iterationProgressFreq"></param>
        public void InitializeIterationBuffer(TimeSpan iterationProgressFreq)
        {
            Flush();
            currTcsIterationTask = null;
            curr = head = null;
            keyValuePairCount = 0;
            this.iterationProgressFreq = default ? TimeSpan.FromSeconds(5) : iterationProgressFreq;
        }

        /// <summary>
        /// Send key value pair and reset migrate buffers
        /// </summary>
        public Task<string> SendAndResetIterationBuffer()
        {
            if (keyValuePairCount == 0) return null;

            Debug.Assert(end - curr >= 2);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';

            // Payload format = [$length\r\n][number of keys (4 bytes)][raw key value pairs]\r\n
            var size = (int)(curr - 2 - head - (ExtraSpace - 4));
            TrackIterationProgress(keyValuePairCount, size);
            var success = RespWriteUtils.TryWritePaddedBulkStringLength(size, ExtraSpace - 4, ref head, end);
            Debug.Assert(success);

            // Number of key value pairs in payload
            *(int*)head = keyValuePairCount;

            // Reset offset and flush buffer
            offset = curr;
            Flush();
            Interlocked.Increment(ref numCommands);

            // Return outstanding task and reset current tcs
            var task = currTcsIterationTask.Task;
            currTcsIterationTask = null;
            curr = head = null;
            keyValuePairCount = 0;
            return task;
        }

        /// <summary>
        /// Try write key value pair for main store directly to the client buffer
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="task"></param>
        /// <returns></returns>
        public bool TryWriteKeyValueSpanByte(ref SpanByte key, ref SpanByte value, out Task<string> task)
        {
            task = null;
            // Try write key value pair directly to client buffer
            if (!WriteSerializedSpanByte(ref key, ref value))
            {
                // If failed to write because no space left send outstanding data and retrieve task
                // Caller is responsible for retrying
                task = SendAndResetIterationBuffer();
                return false;
            }

            keyValuePairCount++;
            return true;

            bool WriteSerializedSpanByte(ref SpanByte key, ref SpanByte value)
            {
                var totalLen = key.TotalSize + value.TotalSize + 2 + 2;
                if (totalLen > (int)(end - curr))
                    return false;

                key.CopyTo(curr);
                curr += key.TotalSize;
                value.CopyTo(curr);
                curr += value.TotalSize;
                return true;
            }
        }

        /// <summary>
        /// Try write key value pair for object store directly to the client buffer
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="expiration"></param>
        /// <param name="task"></param>
        /// <returns></returns>
        public bool TryWriteKeyValueByteArray(byte[] key, byte[] value, long expiration, out Task<string> task)
        {
            task = null;
            // Try write key value pair directly to client buffer
            if (!WriteSerializedKeyValueByteArray(key, value, expiration))
            {
                // If failed to write because no space left send outstanding data and retrieve task
                // Caller is responsible for retrying
                task = SendAndResetIterationBuffer();
                return false;
            }

            keyValuePairCount++;
            return true;

            bool WriteSerializedKeyValueByteArray(byte[] key, byte[] value, long expiration)
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

        long lastLog = 0;
        long totalKeyCount = 0;
        long totalPayloadSize = 0;
        TimeSpan iterationProgressFreq;

        /// <summary>
        /// Logging of migrate session status
        /// </summary>
        /// <param name="keyCount"></param>
        /// <param name="size"></param>
        /// <param name="completed"></param>
        private void TrackIterationProgress(int keyCount, int size, bool completed = false)
        {
            totalKeyCount += keyCount;
            totalPayloadSize += size;
            var duration = TimeSpan.FromTicks(Stopwatch.GetTimestamp() - lastLog);
            if (completed || lastLog == 0 || duration >= iterationProgressFreq)
            {
                logger?.LogTrace("[{op}]: store:({storeType}) totalKeyCount:({totalKeyCount}), totalPayloadSize:({totalPayloadSize} KB)",
                    completed ? "COMPLETED" : ist,
                    isMainStore ? "MAIN STORE" : "OBJECT STORE",
                    totalKeyCount.ToString("N0"),
                    ((long)((double)totalPayloadSize / 1024)).ToString("N0"));
                lastLog = Stopwatch.GetTimestamp();
            }
        }
    }
}