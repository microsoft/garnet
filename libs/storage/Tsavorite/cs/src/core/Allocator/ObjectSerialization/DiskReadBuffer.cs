// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    internal sealed unsafe class DiskReadBuffer : IDisposable
    {
        internal readonly IDevice device;
        internal readonly ILogger logger;

        /// <summary>Signals completion of asynchronous device reads submitted into this buffer. The current implementation submits one
        /// contiguous request per initialization; the countdown form permits multiple requests if the buffer is later partitioned.</summary>
        internal CountdownEvent countdownEvent;

        /// <summary>The buffer to read (part of) the page image into.</summary>
        internal SectorAlignedMemory memory;

        /// <summary>
        /// This is the initialization value for <see cref="currentPosition"/>; it means no device read has been submitted for this buffer
        /// since its last <see cref="Initialize"/> call.
        /// </summary>
        const int NoPosition = -1;

        /// <summary>Current read position (we do not support write in this buffer). This class only supports Read and no Seek,
        /// so currentPosition is always where <see cref="ObjectLogReader{TStoreFunctions}"/> will read from next.</summary>
        /// <remarks>This will be either 0 or greater than or equal to <see cref="PageHeader.Size"/>.</remarks>
        internal int currentPosition;

        /// <summary>Non-inclusive last position in this buffer; the number of byte read. If <see cref="currentPosition"/> equals this, then we are out of space and
        /// must move to the next buffer.</summary>
        internal int endPosition;

        /// <summary>True after <see cref="ReadFromDevice"/> submits an <c>IDevice.ReadAsync</c> request into this buffer and until
        /// <see cref="Initialize"/> clears it. This includes both in-flight and completed requests. Set and cleared
        /// only on the reader thread (in <see cref="ReadFromDevice"/> and <see cref="Initialize"/>), so it is a race-free indicator of "this
        /// buffer already holds, or is loading, data" -- unlike <see cref="HasData"/>/<see cref="HasInFlightRead"/>, whose underlying
        /// <see cref="endPosition"/> and <see cref="countdownEvent"/> are updated from the IO completion callback and can be observed in a
        /// transiently inconsistent state (endPosition not yet visible after the countdown is signaled) by a thread that does not wait.</summary>
        internal bool readIssued;

        /// <summary>
        /// The starting position in the file that we read this buffer from.
        /// </summary>
        internal ObjectLogFilePositionInfo startFilePosition;

        internal int AvailableLength => endPosition - currentPosition;

        internal ReadOnlySpan<byte> AvailableSpan => new(memory.GetValidPointer() + currentPosition, endPosition - currentPosition);

        internal DiskReadBuffer(SectorAlignedMemory memory, IDevice device, ILogger logger)
        {
            this.memory = memory;
            countdownEvent = new CountdownEvent(0); // Start with 0; we'll increment at the time of read
            this.device = device;
            this.logger = logger;
            Initialize();
        }

        internal void Initialize()
        {
            currentPosition = endPosition = NoPosition;
            readIssued = false;
        }

        internal ReadOnlySpan<byte> GetTailSpan(int start) => new(memory.GetValidPointer() + start, currentPosition - start);

        /// <summary>
        /// Read the first chunk of an Object deserialization from the device.
        /// </summary>
        /// <param name="filePosition">Sector-aligned position in the device</param>
        /// <param name="startPosition">Start position in the buffer (relative to start of buffer)</param>
        /// <param name="alignedReadLength">Number of bytes to read</param>
        /// <param name="callback">The <see cref="CircularDiskReadBuffer"/> callback.</param>
        internal void ReadFromDevice(ObjectLogFilePositionInfo filePosition, int startPosition, uint alignedReadLength, DeviceIOCompletionCallback callback)
        {
            IncrementOrResetCountdown(ref countdownEvent);
            startFilePosition = filePosition;

            currentPosition = startPosition;
            endPosition = 0;
            readIssued = true;
            device.ReadAsync(filePosition.SegmentId, filePosition.Offset, (IntPtr)memory.aligned_pointer, (uint)alignedReadLength, callback, context: this);
        }

        internal static void IncrementOrResetCountdown(ref CountdownEvent countdownEvent) => DiskWriteBuffer.IncrementOrResetCountdown(ref countdownEvent);

        internal bool HasData => endPosition > 0;

        internal bool WaitForDataAvailable()
        {
            // NoPosition means the ring never submitted a read for this slot, so there is nothing to wait for or consume.
            if (currentPosition == NoPosition)
                return false;
            if (!HasData)
                countdownEvent.Wait();
            return HasData;
        }

        internal bool HasInFlightRead => countdownEvent is not null && !countdownEvent.IsSet;

        internal void WaitForReadCompletion()
        {
            if (HasInFlightRead)
                countdownEvent.Wait();
        }

        internal ObjectLogFilePositionInfo GetCurrentFilePosition()
        {
            var bufferFilePos = startFilePosition;
            bufferFilePos.Offset += (uint)currentPosition;

            // We only read from one segment into one buffer, so we should never exceed the segment size with this increment.
            Debug.Assert(bufferFilePos.Offset < bufferFilePos.SegmentSize, $"Incremented bufferFilePos.Offset {bufferFilePos.Offset} should be < bufferFilePos.SegmentSize {bufferFilePos.SegmentSize}");
            return bufferFilePos;
        }

        public void Dispose()
        {
            // Drain any in-flight read before returning the memory to the pool: read-ahead (fill-ahead or backfill) reads may not have
            // been consumed, and their completion callback writes to this memory. If it were returned to the shared pool first, a later
            // reader reusing that memory could be corrupted by the late-completing read. The callback always signals (even on error).
            if (countdownEvent is not null && !countdownEvent.IsSet)
                countdownEvent.Wait();
            memory?.Return();
            memory = null;
            countdownEvent?.Dispose();
            countdownEvent = null;
        }

        /// <inheritdoc/>
        public override string ToString()
            => $"currPos {currentPosition}; endPos {endPosition}; avLen {AvailableLength}; countDown {countdownEvent?.CurrentCount}; buf: {memory}";
    }
}