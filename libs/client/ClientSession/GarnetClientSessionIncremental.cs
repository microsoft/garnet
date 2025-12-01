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
        byte* curr, head;
        int recordCount;
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
        /// Return a <see cref="Span{_byte_}"/> of all remaining available space in the network buffer.
        /// </summary>
        public PinnedSpanByte GetAvailableNetworkBufferSpan() => PinnedSpanByte.FromPinnedPointer(curr, (int)(end - curr));

        public void IncrementRecordDirect(int size)
        {
            ++recordCount;
            curr += size;
        }

        /// <summary>
        /// Flush and initialize buffers/parameters used for Migrate and Replica commands
        /// </summary>
        /// <param name="iterationProgressFreq"></param>
        public void InitializeIterationBuffer(TimeSpan iterationProgressFreq)
        {
            EnsureTcsIsEnqueued();
            Flush();
            currTcsIterationTask = null;
            curr = head = null;
            recordCount = 0;
            this.iterationProgressFreq = default ? TimeSpan.FromSeconds(5) : iterationProgressFreq;
        }

        /// <summary>
        /// Send key value pair and reset migrate buffers
        /// </summary>
        public Task<string> SendAndResetIterationBuffer()
        {
            Task<string> task = null;
            if (recordCount == 0)
            {
                // No records to Flush(), but we need to reset buffer offsets as we may have written a header due to the need to initialize the buffer
                // before passing it to Tsavorite as the output SpanByteAndMemory.SpanByte for Read().
                ResetOffset();
                goto done;
            }

            Debug.Assert(end - curr >= 2);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';

            // Payload format = [$length\r\n][number of keys (4 bytes)][raw key value pairs]\r\n
            var size = (int)(curr - 2 - head - (ExtraSpace - 4));
            TrackIterationProgress(recordCount, size);
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
            task = currTcsIterationTask.Task;
            currTcsIterationTask = null;
            recordCount = 0;

        done:
            curr = head = null;
            return task;
        }

        /// <summary>
        /// Try to write the span for the entire record directly to the client buffer
        /// </summary>
        public bool TryWriteRecordSpan(ReadOnlySpan<byte> recordSpan, out Task<string> task)
        {
            // We include space for newline at the end, to be added before sending
            var recordSpanSize = recordSpan.TotalSize();
            var totalLen = recordSpanSize + 2;
            if (totalLen > (int)(end - curr))
            {
                // If there is no space left, send outstanding data and return the send-completion task.
                // Caller is responsible for waiting for task completion and retrying.
                task = SendAndResetIterationBuffer();
                return false;
            }

            recordSpan.SerializeTo(curr);
            curr += recordSpanSize;
            ++recordCount;
            task = null;
            return true;
        }

        long lastLog;
        long totalKeyCount;
        long totalPayloadSize;
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
                logger?.LogTrace("[{op}]: totalKeyCount:({totalKeyCount}), totalPayloadSize:({totalPayloadSize} KB)",
                    completed ? "COMPLETED" : ist,
                    totalKeyCount.ToString("N0"),
                    ((long)((double)totalPayloadSize / 1024)).ToString("N0"));
                lastLog = Stopwatch.GetTimestamp();
            }
        }

        private void EnsureTcsIsEnqueued()
        {
            // See comments in SetClusterMigrateHeader() as to why this is decoupled from the header initialization.
            if (recordCount > 0 && currTcsIterationTask == null)
            {
                currTcsIterationTask = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
                tcsQueue.Enqueue(currTcsIterationTask);
            }
        }
    }
}