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

        /// <summary>Signals when reads are complete. Allows multiple reads of buffer subsections to be enqueed, and uses a ManualResetEvent
        ///     so it remains signaled until Reset() is called (currently only one read for a single span of the buffer is done).</summary>
        internal CountdownEvent countdownEvent;

        /// <summary>The buffer to read (part of) the page image into.</summary>
        internal SectorAlignedMemory memory;

        /// <summary>
        /// This is the initialization value for <see cref="currentPosition"/>; it means there is no data available for this buffer and no
        /// in-flight read (we issue reads ahead of the buffer-array traversal, so this means that by the time we got to this buffer all
        /// the data had already been read.
        /// </summary>
        const int NoPosition = -1;

        /// <summary>Current read position (we do not support write in this buffer). This class only supports Read and no Seek,
        /// so currentPosition is always where <see cref="ObjectLogReader{TStoreFunctions}"/> will read from next.</summary>
        /// <remarks>This will be either 0 or greater than or equal to <see cref="PageHeader.Size"/>.</remarks>
        internal int currentPosition;

        /// <summary>Non-inclusive last position in this buffer; the number of byte read. If <see cref="currentPosition"/> equals this, then we are out of space and
        /// must move to the next buffer.</summary>
        internal int endPosition;

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
            device.ReadAsync(filePosition.SegmentId, filePosition.Offset, (IntPtr)memory.aligned_pointer, (uint)alignedReadLength, callback, context: this);
        }

        internal static void IncrementOrResetCountdown(ref CountdownEvent countdownEvent) => DiskWriteBuffer.IncrementOrResetCountdown(ref countdownEvent);

        internal bool HasData => endPosition > 0;

        internal bool WaitForDataAvailable()
        {
            // Because we have issued reads ahead of the buffer wrap, if the currentPosition is NoPosition, we're done.
            if (currentPosition == NoPosition)
                return false;
            if (!HasData)
                countdownEvent.Wait();
            return true;
        }

        internal bool HasInFlightRead => countdownEvent is not null && !countdownEvent.IsSet;

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