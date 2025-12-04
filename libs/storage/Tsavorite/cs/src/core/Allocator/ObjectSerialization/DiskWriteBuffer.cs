// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    internal sealed unsafe class DiskWriteBuffer : IDisposable
    {
        /// <summary>Signals when writes are complete. Allows multiple writes of buffer subsections to be enqueed, and uses a ManualResetEvent
        ///     so it remains signaled until Reset() is called.</summary>
        CountdownEvent countdownEvent;

        /// <summary>The buffer to build the page image for writing.</summary>
        internal SectorAlignedMemory memory;

        /// <summary>Current write position (we do not support read in this buffer). This class only supports Write and no Seek,
        /// so currentPosition equals the current length. Relevant for object serialization only; reset to 0 at start of DoSerialize().</summary>
        /// <remarks><see cref="currentPosition"/> is the current length, since it is *past* the last byte copied to the buffer.</remarks>
        internal int currentPosition;

        /// <summary>Last position flushed to in this buffer (i.e. 0->flushedUntilPosition have been flushed). This allows using the buffer for
        /// multiple small writes sandwiched around large internal direct writes from overflow.</summary>
        /// <remarks>If <see cref="flushedUntilPosition"/> is less than <see cref="currentPosition"/> then there is unflushed data in the buffer.</remarks>
        internal int flushedUntilPosition;

        /// <summary>The end position of the buffer. Usually the size of the buffer, but may be less if we're at the end of a segment.
        /// This should always be sector-aligned, because we start the partial flush sector-aligned and this is either a bufferSize past
        /// the previous buffer or a sector-aligned distance from the end of the segment; the latter may be less than the end of the buffer
        /// due to directly writing internal spans for <see cref="OverflowByteArray"/> Keys and Values that are less than a full buffer size.</summary>
        internal int endPosition;

        internal int RemainingCapacity => endPosition - currentPosition;  // DiskPageHeader.Size is included in currentPosition

        /// <summary>The remaining space in the buffer, from <see cref="currentPosition"/> to <see cref="endPosition"/>.</summary>
        internal Span<byte> RemainingSpan => new(memory.GetValidPointer(), RemainingCapacity);

        internal readonly IDevice device;
        internal readonly ILogger logger;

        internal DiskWriteBuffer(SectorAlignedMemory memory, IDevice device, ILogger logger)
        {
            this.memory = memory;
            this.device = device;
            this.logger = logger;
        }

        internal void WaitUntilFreeAndInitialize(int endPosition)
        {
            Debug.Assert(IsAligned(endPosition, (int)device.SectorSize), $"endPosition {endPosition} is not sector-aligned");

            // First wait for any pending write in this buffer to complete. If this is our first time in this buffer there won't be a CountdownEvent yet;
            // we defer that because we may not need all the buffers in the circular buffer.
            countdownEvent?.Wait();

            // Initialize fields.
            this.endPosition = endPosition;
            memory.valid_offset = 0;
            currentPosition = 0;
            flushedUntilPosition = 0;
        }

        internal static CountdownEvent IncrementOrResetCountdown(ref CountdownEvent countdownEvent)
        {
            if (countdownEvent is null)
                countdownEvent = new(1);
            else if (!countdownEvent.TryAddCount(1))
            {
                // This means we've enqueued one or more earlier writes which have completed.
                // First wait to be sure the callback has signaled the contained event, then Reset the event with a new count.
                countdownEvent.Wait(); // This should usually be immediate
                countdownEvent.Reset(1);
            }
            return countdownEvent;
        }

        internal void FlushToDevice(ref ObjectLogFilePositionInfo filePosition, DeviceIOCompletionCallback callback, DiskWriteCallbackContext pageWriteCallbackContext)
        {
            Debug.Assert(currentPosition <= endPosition, $"currentPosition ({currentPosition}) cannot exceed endPosition ({endPosition})");

            // We are flushing the buffer. currentPosition must already be sector-aligned; either it is at endPosition (which is always sector-aligned),
            // which is the normal "buffer is full so flush it" handling, or it is less than endPosition which means it is called from one of:
            //   a. OnPartialFlushComplete, in which case the caller has sector-aligned it before calling this
            //   b. OverflowByteArray sector-aligning writes at the beginning or end, which means we copied a sector-aligned number of bytes to the buffer.
            Debug.Assert(IsAligned(currentPosition, (int)device.SectorSize), $"currentPosition ({currentPosition}) is not sector-aligned");
            Debug.Assert(IsAligned(filePosition.Offset, (int)device.SectorSize), $"Starting file flush position ({filePosition}) is not sector-aligned");
            pageWriteCallbackContext.SetBufferCountdownEvent(IncrementOrResetCountdown(ref countdownEvent));

            var flushLength = (uint)(currentPosition - flushedUntilPosition);
            Debug.Assert(IsAligned(flushLength, (int)device.SectorSize), $"flushLength {flushLength} is not sector-aligned");
            Debug.Assert(flushLength <= filePosition.RemainingSizeInSegment, $"flushLength ({flushLength}) cannot be greater than filePosition.RemainingSize ({filePosition.RemainingSizeInSegment})");

            var spanPtr = memory.GetValidPointer() + flushedUntilPosition;
            device.WriteAsync((IntPtr)spanPtr, filePosition.SegmentId, filePosition.Offset, flushLength, callback, pageWriteCallbackContext);
            flushedUntilPosition = currentPosition;

            // This does not use .Advance() because we are already checking boundary conditions and calling .AdvanceToNextSegment() in ObjectLogWriter.
            filePosition.Offset += flushLength;
        }

        internal void Wait() => countdownEvent?.Wait();

        public void Dispose()
        {
            memory?.Return();
            memory = null;

            Debug.Assert(countdownEvent is null || countdownEvent.CurrentCount == 0, $"Unexpected count ({countdownEvent.CurrentCount}) remains");
            countdownEvent?.Dispose();
            countdownEvent = null;
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            var countdownString = countdownEvent?.CurrentCount.ToString() ?? "null";
            return $"currPos {currentPosition}; endPos {endPosition}; remCap {RemainingCapacity}; flushedUntilPos {flushedUntilPosition}; countDown {countdownString}; buf: {memory}";
        }
    }
}