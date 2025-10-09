// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>
    /// This class drives object-deserialization reading from the disk. It has multiple buffers and reads buffers ahead of the current one
    /// ahead while deserialization logic is running.</summary>
    public class CircularDiskReadBuffer : IDisposable
    {
        internal readonly SectorAlignedBufferPool bufferPool;
        internal readonly int bufferSize;
        internal readonly IDevice objectLogDevice;
        internal readonly ILogger logger;

        readonly DiskReadBuffer[] buffers;
        int currentIndex;

        /// <summary>Device address to do the next read from (segment and offset); set at the start of a record by <see cref="ObjectLogReader{TStoreFunctions}"/>
        /// and incremented with each buffer read; all of these should be aligned to sector size, so this address remains sector-aligned.</summary>
        internal ObjectLogFilePositionInfo nextReadFilePosition;

        /// <summary>Track the remaining length to be read for one or more records for Object values, and we can also read some or all of Overflow values into the buffer.</summary>
        ulong unreadLengthRemaining;

        internal CircularDiskReadBuffer(SectorAlignedBufferPool bufferPool, int bufferSize, int numBuffers, IDevice objectLogDevice, ILogger logger)
        {
            this.bufferPool = bufferPool;
            this.bufferSize = bufferSize;
            this.objectLogDevice = objectLogDevice;
            this.logger = logger;

            buffers = new DiskReadBuffer[numBuffers];
            currentIndex = 0;
        }

        internal DiskReadBuffer GetCurrentBuffer() => buffers[currentIndex];

        int GetNextBufferIndex(int curIndex)
        {
            var index = curIndex + 1;
            return index >= buffers.Length ? 0 : index;
        }

        /// <summary>
        /// Prepare the <see cref="DiskReadBuffer"/> and local variables to read the next buffer (or as much of it as we need). This is called 
        /// by OnBeginReadRecords and when we are leaving a buffer with more data, to fill that buffer so it is available when we wrap around
        /// to it again. For both of these, we do not have to worry that there is pending IO in the buffer.
        /// </summary>
        /// <param name="bufferIndex">The index into <see cref="buffers"/> of the <see cref="DiskReadBuffer"/> that will do the reading</param>
        /// <param name="unalignedRecordStartPosition">Start position on the page (relative to start of page)</param>
        private void ReadBuffer(int bufferIndex, int unalignedRecordStartPosition)
        {
            var buffer = buffers[bufferIndex];
            if (buffer is null)
            {
                buffer = new(bufferPool.Get(bufferSize), objectLogDevice, logger);
                buffers[bufferIndex] = buffer;
            }
            else
            {
                Debug.Assert(buffer.countdownEvent.CurrentCount == 0, $"Unexpected countdownEvent.CurrentCount ({buffer.countdownEvent.CurrentCount}) when preparing to read into buffer");
                buffer.Initialize();
            }

            var alignedReadStartPosition = RoundDown(unalignedRecordStartPosition, (int)objectLogDevice.SectorSize);
            var recordStartPosition = unalignedRecordStartPosition - alignedReadStartPosition;

            // See how much to read. We have two limits: the total size requested for this ReadAsync operation, and the segment size.
            var unalignedReadLength = bufferSize - alignedReadStartPosition;
            if ((ulong)unalignedReadLength > unreadLengthRemaining)
                unalignedReadLength = (int)unreadLengthRemaining;

            Debug.Assert(IsAligned(nextReadFilePosition.Offset, (int)objectLogDevice.SectorSize), $"filePosition.Offset ({nextReadFilePosition.Offset}) is not sector-aligned");
            if (nextReadFilePosition.Offset + (ulong)unalignedReadLength > nextReadFilePosition.SegmentSize)
                unalignedReadLength = (int)(nextReadFilePosition.SegmentSize - nextReadFilePosition.Offset);

            // We may not have had a sector-aligned amount of remaining unread data.
            var alignedReadLength = RoundUp(unalignedReadLength, (int)objectLogDevice.SectorSize);
            buffer.ReadFromDevice(nextReadFilePosition, recordStartPosition, (uint)alignedReadLength, ReadFromDeviceCallback);

            // Advance the filePosition. This used aligned read length so may advance it past end of record but that's OK because
            // filePosition is for the "read buffer-sized chunks" logic while data transfer via Read() uses buffer.currentPosition.
            nextReadFilePosition.Offset += (uint)alignedReadLength;

            Debug.Assert(nextReadFilePosition.Offset <= nextReadFilePosition.SegmentSize, $"filePosition.Offset ({nextReadFilePosition.Offset}) must be <= filePosition.SegmentSize ({nextReadFilePosition.SegmentSize})");
            if (nextReadFilePosition.Offset == nextReadFilePosition.SegmentSize)
                nextReadFilePosition.AdvanceToNextSegment();

            unreadLengthRemaining -= (uint)unalignedReadLength;
        }

        /// <summary>
        /// Called when one or more records are to be read via ReadAsync.
        /// </summary>
        /// <param name="startFilePosition">The initial file position to read</param>
        /// <param name="totalLength">The cumulative length of all object-log entries for the span of records to be read. We read ahead for all record
        ///     in the ReadAsync call.</param>
        internal void OnBeginReadRecords(ObjectLogFilePositionInfo startFilePosition, ulong totalLength)
        {
            Debug.Assert(totalLength > 0, "TotalLength cannot be 0");
            nextReadFilePosition = startFilePosition;
            unreadLengthRemaining = totalLength;

            // Initialize all buffers
            for (var ii = 0; ii < buffers.Length; ii++)
                buffers[ii]?.Initialize();
            currentIndex = 0;

            // Do an initial read to fill the buffers, at least as much as we have. Again, totalLength applies to all records in the ReadAsync range,
            // whether one or many. First align the initial read. recordStartPosition is the padding between rounded-down-to-align-readStart and recordStart.
            var alignedReadPosition = RoundDown(nextReadFilePosition.Offset, (int)objectLogDevice.SectorSize);
            var recordStartPosition = (int)(nextReadFilePosition.Offset - alignedReadPosition);
            unreadLengthRemaining += (uint)recordStartPosition;
            nextReadFilePosition.Offset -= (uint)recordStartPosition;

            // Load all the buffers as long as we have more unread data. Leave currentIndex at 0.
            for (var ii = 0; ii < buffers.Length; ii++)
            {
                if (unreadLengthRemaining == 0)
                    break;
                ReadBuffer(ii, recordStartPosition);
                recordStartPosition = 0;  // After the first read, subsequent reads start on an aligned address
            }
        }

        internal void OnBeginRecord(ObjectLogFilePositionInfo recordFilePosition)
        {
            var buffer = buffers[currentIndex] ?? throw new TsavoriteException($"Internal error in read buffer sequencing; empty buffer[{currentIndex}] encountered with unreadLengthRemaining {unreadLengthRemaining}");

            // Because each partial flush ends with a sector-aligning write, we may have a record start position greater than our ongoing buffer.currentPosition
            // incrementing. It should never be less. recordFilePosition is only guaranteed to be sector-aligned if it's the first record after a partial flush. 
            var bufferFilePosition = buffer.GetCurrentFilePosition();
            if (!buffer.HasData)
                _ = buffer.WaitForDataAvailable();
            Debug.Assert(recordFilePosition.word >= bufferFilePosition.word, $"Record file position ({recordFilePosition}) should be >= ongoing position {bufferFilePosition}");
            Debug.Assert(recordFilePosition.SegmentId == bufferFilePosition.SegmentId, $"Record file segment ({recordFilePosition.SegmentId}) should == ongoing position {bufferFilePosition.SegmentId}");
            var increment = recordFilePosition - bufferFilePosition;

            Debug.Assert(increment < objectLogDevice.SectorSize, $"Increment {increment} is more than SectorSize ({objectLogDevice.SectorSize})");
            Debug.Assert(buffer.currentPosition + (int)increment < buffer.endPosition, $"Increment {increment} overflows buffer (curPos {buffer.currentPosition}, endPos {buffer.endPosition})");
            buffer.currentPosition += (int)increment;
        }

        /// <summary>
        /// Begin the deserialization process for a single record.
        /// </summary>
        internal void OnBeginDeserialize()
        {
            // Currently nothing
        }

        /// <summary>
        /// Move to the next buffer and see if it has data.
        /// </summary>
        /// <param name="nextBuffer">The next buffer</param>
        /// <returns></returns>
        internal unsafe bool MoveToNextBuffer(out DiskReadBuffer nextBuffer)
        {
            // If we have more data to read, "backfill" this buffer with a read before departing it, else initialize it.
            if (unreadLengthRemaining > 0)
                ReadBuffer(currentIndex, unalignedRecordStartPosition: 0);
            else
                buffers[currentIndex].Initialize();

            // Move to the next buffer and wait for any in-flight read to complete. If there is no pending IO and the buffer is
            // empty, we are done with this read op.
            currentIndex = GetNextBufferIndex(currentIndex);
            nextBuffer = buffers[currentIndex];
            if (nextBuffer.WaitForDataAvailable())
                return true;

            Debug.Assert(unreadLengthRemaining == 0, $"unreadLengthRemaining ({unreadLengthRemaining}) was not 0 when WaitForDataAvailable returned false");
            return false;
        }

        internal unsafe void ReadFromDeviceCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(ReadFromDeviceCallback)} error: {{errorCode}}", errorCode);

            // Finish setting up the buffer, and extract optionals if this was the last buffer.
            var buffer = (DiskReadBuffer)context;
            buffer.endPosition += (int)numBytes;
            Debug.Assert(buffer.endPosition > buffer.currentPosition, $"buffer.endPosition ({buffer.endPosition}) must be >= buffer.currentPosition ({buffer.currentPosition})");

            // Signal the buffer's event to indicate the data is available.
            _ = buffer.countdownEvent.Signal();
        }

        public void Dispose()
        {
            for (var ii = 0; ii < buffers.Length; ii++)
                buffers[ii]?.Dispose();
        }

        /// <inheritdoc/>
        public override string ToString()
            => $"currIdx {currentIndex}; bufSize {bufferSize}; filePosition {nextReadFilePosition}; SecSize {(int)objectLogDevice.SectorSize}";
    }
}