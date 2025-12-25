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

    /// <summary>
    /// This class drives object-deserialization reading from the disk. It has multiple buffers and reads buffers ahead of the current one
    /// ahead while deserialization logic is running.</summary>
    public class CircularDiskReadBuffer : IDisposable
    {
        internal readonly SectorAlignedBufferPool bufferPool;
        internal readonly int bufferSize;
        internal readonly IDevice objectLogDevice;
        internal readonly ILogger logger;

        DiskReadBuffer[] buffers;
        int currentIndex;

        bool disposed;

        /// <summary>Device address to do the next read from (segment and offset); set at the start of a record by <see cref="ObjectLogReader{TStoreFunctions}"/>
        /// and incremented with each buffer read; all of these should be aligned to sector size, so this address remains sector-aligned.</summary>
        internal ObjectLogFilePositionInfo nextFileReadPosition;

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

        internal DiskReadBuffer GetCurrentBuffer()
        {
            if (disposed)
                throw new ObjectDisposedException(nameof(CircularDiskReadBuffer));
            return buffers[currentIndex];
        }

        int GetNextBufferIndex(int curIndex)
        {
            var index = curIndex + 1;
            return index >= buffers.Length ? 0 : index;
        }

        private DiskReadBuffer CreateBuffer(int bufferIndex)
        {
            DiskReadBuffer buffer = new(bufferPool.Get(bufferSize), objectLogDevice, logger);
            buffers[bufferIndex] = buffer;
            return buffer;
        }

        /// <summary>
        /// Prepare the <see cref="DiskReadBuffer"/> and local variables to read the next buffer (or as much of it as we need) and issue the read.
        /// This is called by OnBeginReadRecords and when we are leaving a buffer with more data, to fill that buffer so it is available when we
        /// wrap around to it again. For both of these, we do not have to worry that there is pending IO in the buffer.
        /// </summary>
        /// <param name="bufferIndex">The index into <see cref="buffers"/> of the <see cref="DiskReadBuffer"/> that will do the reading</param>
        /// <param name="unalignedReadStartPosition">The "actual" read start position in the buffer (relative to start of buffer), which will
        ///     become the "current position" of the buffer</param>
        private void DoReadBuffer(int bufferIndex, int unalignedReadStartPosition)
        {
            var buffer = buffers[bufferIndex];
            if (buffer is null)
                buffer = CreateBuffer(bufferIndex);
            else
            {
                Debug.Assert(buffer.countdownEvent.CurrentCount == 0, $"Unexpected countdownEvent.CurrentCount ({buffer.countdownEvent.CurrentCount}) when preparing to read into buffer");
                buffer.Initialize();
            }

            var alignedReadStartPosition = RoundDown(unalignedReadStartPosition, (int)objectLogDevice.SectorSize);
            var bufferStartPosition = unalignedReadStartPosition - alignedReadStartPosition;

            // See how much to read. We have two limits: the total size requested for this ReadAsync operation, and the segment size.
            var unalignedReadLength = bufferSize - alignedReadStartPosition;
            if ((ulong)unalignedReadLength > unreadLengthRemaining)
                unalignedReadLength = (int)unreadLengthRemaining;

            Debug.Assert(IsAligned(nextFileReadPosition.Offset, (int)objectLogDevice.SectorSize), $"filePosition.Offset ({nextFileReadPosition.Offset}) is not sector-aligned");
            var segmentIsComplete = false;
            if (nextFileReadPosition.Offset + (ulong)unalignedReadLength >= nextFileReadPosition.SegmentSize)
            {
                unalignedReadLength = (int)(nextFileReadPosition.SegmentSize - nextFileReadPosition.Offset);
                Debug.Assert(IsAligned(unalignedReadLength, (int)objectLogDevice.SectorSize), $"unalignedReadLength ({unalignedReadLength}) is not sector-aligned at segment end");
                segmentIsComplete = true;
            }

            // We may not have had a sector-aligned amount of remaining unread data.
            var alignedReadLength = RoundUp(unalignedReadLength, (int)objectLogDevice.SectorSize);
            buffer.ReadFromDevice(nextFileReadPosition, bufferStartPosition, (uint)alignedReadLength, ReadFromDeviceCallback);

            // Advance the filePosition. This used aligned read length so may advance it past end of record but that's OK because
            // filePosition is for the "read buffer-sized chunks" logic while data transfer via Read() uses buffer.currentPosition.
            // Note: If segmentIsComplete, this increment results in nextFileReadPosition.Offset == SegmentSize, which will mask off to a 0.
            nextFileReadPosition.Offset += (uint)alignedReadLength;

            Debug.Assert(nextFileReadPosition.Offset <= nextFileReadPosition.SegmentSize, $"filePosition.Offset ({nextFileReadPosition.Offset}) must be <= filePosition.SegmentSize ({nextFileReadPosition.SegmentSize})");
            if (segmentIsComplete)
                nextFileReadPosition.AdvanceToNextSegment();

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
            nextFileReadPosition = startFilePosition;
            unreadLengthRemaining = totalLength;

            // Initialize all buffers
            for (var ii = 0; ii < buffers.Length; ii++)
                buffers[ii]?.Initialize();
            currentIndex = 0;

            // Do an initial read to fill the buffers, at least as much as we have. Again, totalLength applies to all records in the ReadAsync range,
            // whether one or many. First align the initial read. recordStartPosition is the padding between rounded-down-to-align-readStart and recordStart.
            var alignedReadPosition = RoundDown(nextFileReadPosition.Offset, (int)objectLogDevice.SectorSize);
            var recordStartPosition = (int)(nextFileReadPosition.Offset - alignedReadPosition);
            unreadLengthRemaining += (uint)recordStartPosition;
            nextFileReadPosition.Offset -= (uint)recordStartPosition;

            // Load all the buffers as long as we have more unread data. Leave currentIndex at 0.
            for (var ii = 0; ii < buffers.Length; ii++)
            {
                if (unreadLengthRemaining == 0)
                    break;
                DoReadBuffer(ii, recordStartPosition);
                recordStartPosition = 0;  // After the first read, subsequent reads start on an aligned address
            }
        }

        /// <summary>
        /// Called when one or more records with Objects have been read and via ReadAsync, e.g. being processed by AsyncReadPageWithObjectsCallback,
        /// and we have completed reading and deserializing those objects.
        /// </summary>
        internal void OnEndReadRecords()
        {
            for (var ii = 0; ii < buffers.Length; ii++)
            {
                Debug.Assert(buffers[ii] is null || !buffers[ii].HasInFlightRead, $"All reads should have been completed by OnEndReadRecords()");
            }
        }

        internal bool OnBeginRecord(ObjectLogFilePositionInfo recordFilePosition)
        {
            var buffer = buffers[currentIndex] ?? throw new TsavoriteException($"Internal error in read buffer sequencing; empty buffer[{currentIndex}] encountered with unreadLengthRemaining {unreadLengthRemaining}");

            // Because each partial flush ends with a sector-aligning write, we may have a record start position greater than our ongoing buffer.currentPosition
            // incrementing. It should never be less. recordFilePosition is only guaranteed to be sector-aligned if it's the first record after a partial flush. 
            if (!buffer.HasData && !buffer.WaitForDataAvailable())
                return false;

            while (true)
            {
                var bufferFilePosition = buffer.GetCurrentFilePosition();
                Debug.Assert(recordFilePosition.word >= bufferFilePosition.word, $"Record file position ({recordFilePosition}) should be >= ongoing position {bufferFilePosition}");
                Debug.Assert(recordFilePosition.SegmentId == bufferFilePosition.SegmentId, $"Record file segment ({recordFilePosition.SegmentId}) should == ongoing position {bufferFilePosition.SegmentId}");
                var increment = recordFilePosition - bufferFilePosition;
                Debug.Assert(increment < objectLogDevice.SectorSize, $"Increment {increment} must be less than SectorSize ({objectLogDevice.SectorSize})");

                // We might cleanly align to the start of the next buffer, if there was a flush that ended on a buffer boundary.
                // Otherwise, we should always be within the current buffer. We should only do this "continue" once.
                if (buffer.currentPosition + (int)increment < buffer.endPosition)
                {
                    buffer.currentPosition += (int)increment;
                    break;
                }

                Debug.Assert(buffer.currentPosition + (int)increment == buffer.endPosition, $"Increment {increment} overflows buffer (curPos {buffer.currentPosition}, endPos {buffer.endPosition}) by more than alignment");
                if (!MoveToNextBuffer(out buffer))
                    break;
            }
            return true;
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
        internal bool MoveToNextBuffer(out DiskReadBuffer nextBuffer)
        {
            // If we have more data to read, "backfill" this buffer with a read before departing it, else initialize it.
            if (unreadLengthRemaining > 0)
                DoReadBuffer(currentIndex, unalignedReadStartPosition: 0);
            else
                buffers[currentIndex].Initialize();

            // Move to the next buffer and wait for any in-flight read to complete. If there is no pending IO and the buffer is
            // empty, we are done with this read op.
            currentIndex = GetNextBufferIndex(currentIndex);
            nextBuffer = buffers[currentIndex];
            if (nextBuffer is not null && nextBuffer.WaitForDataAvailable())
                return true;

            Debug.Assert(unreadLengthRemaining == 0, $"unreadLengthRemaining ({unreadLengthRemaining}) was not 0 when WaitForDataAvailable returned false");
            return false;
        }

        internal void ReadFromDeviceCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(ReadFromDeviceCallback)} error: {{errorCode}}", errorCode);

            // Finish setting up the buffer
            var buffer = (DiskReadBuffer)context;

            buffer.endPosition += (int)numBytes;
            if (buffer.endPosition == 0)
                Debug.Assert(buffer.currentPosition == 0, $"buffer.currentPosition ({buffer.currentPosition}) must be 0 if buffer.endPosition ({buffer.endPosition}) is 0");
            else
                Debug.Assert(buffer.endPosition > buffer.currentPosition, $"buffer.endPosition ({buffer.endPosition}) must be >= buffer.currentPosition ({buffer.currentPosition})");

            // Signal the buffer's event to indicate the data is available.
            _ = buffer.countdownEvent.Signal();
        }

        public void Dispose()
        {
            disposed = true;
            
            // Atomic swap to avoid clearing twice.
            var localBuffers = Interlocked.Exchange(ref buffers, null);
            if (localBuffers == null)
                return;

            for (var ii = 0; ii < localBuffers.Length; ii++)
                localBuffers[ii]?.Dispose();

            // Restore the now-cleared buffers array.
            //buffers = localBuffers;
        }

        /// <inheritdoc/>
        public override string ToString()
            => $"currIdx {currentIndex}; bufSize {bufferSize}; filePosition {nextFileReadPosition}; SecSize {(int)objectLogDevice.SectorSize}";
    }
}