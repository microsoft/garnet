// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;
    using static VarbyteLengthUtility;

    /// <summary>
    /// This class drives object-deserialization reading from the disk. It has multiple buffers and operates in two modes for
    /// optimal read-ahead while deserialization logic is running. The mode is determined by whether the object implementation
    /// supports <see cref="IHeapObject.SerializedSizeIsExact"/>:
    /// <list type="bullet">
    ///     <item>Supported: We know the full object size up front, so at the start of deserialization we launch parallel reads on as many
    ///         buffers as necessary to pages of the serialized object into the buffers. If the object size exceeds the total capacity
    ///         of all buffers, then as soon as we finish processing a buffer we enqueue a read of the next chunk into that buffer before
    ///         moving to the next buffer.</item>
    ///     <item>Not supported: We don't know the full object size up front; we didn't know it at write time, so we generated page
    ///         images that have a "continuation length" in the last sizeof(int) bytes of the page, to "chain" the chunks together.
    ///         This tells us how much data to read on the next page. It may be zero, in which case we are done. Unlike the case where
    ///         <see cref="IHeapObject.SerializedSizeIsExact"/> is supported, we cannot read multiple pages at once because we don't
    ///         know the total size, except for the second page, which is part of the first page's "valueLength" (because Flush must
    ///         serialize one page ahead before updating the length on the current page). After that we simply read ahead one buffer
    ///         (when we move to a buffer, we read its continuation length and issue a read for the next buffer, which executes while
    ///         we are doing deserialization of the newly moved-to buffer). We also limit the buffer count to two for chained chunks,
    ///         as we can only read one buffer ahead at a time (the initial read of two buffers is consistent with this).</item>
    /// </list>
    /// </summary>
    /// <remarks>This class implements <see cref="IDisposable"/> because reads are all Wait()ed on (in parallel) before the
    ///     <see cref="DiskStreamReader{TStoreFunctions}.ReadFromDevice(SectorAlignedMemory, long, int, int, System.Threading.CountdownEvent)"/> returns.</remarks>
    public class CircularDiskPageReadBuffer : IDisposable
    {
        internal readonly SectorAlignedBufferPool bufferPool;
        internal readonly int pageBufferSize;
        internal readonly IDevice device;
        internal readonly ILogger logger;

        readonly DiskPageReadBuffer[] buffers;
        int currentIndex;

        /// <summary>Device address for next read; incremented with each buffer read. All reads should be aligned to sector size, so this address remains sector-aligned.</summary>
        internal ulong alignedDeviceAddress;

        internal int SectorSize => (int)device.SectorSize;

        /// <summary>The amount of data space on the disk page between the header and end of page. Does not include any lengthSpaceReserve for continuation-chunk lengths;
        /// that's included in the read-request length and examined in the buffer processing.</summary>
        internal int UsablePageSize => pageBufferSize - DiskPageHeader.Size;

        /// <summary>If true, then there is another chunk to read after the currently in-flight chunk read.</summary>
        /// <remarks>This becomes false on the last chunk, which does not have the continuation bit set.</remarks>
        internal bool hasContinuationChunk;

        /// <summary>For object deserialization we need to track the remaining chunk length to be read.</summary>
        /// <remarks>For chained chunks with the continuation tag, this is the next chunk length to read.
        /// For <see cref="IHeapObject.SerializedSizeIsExact"/>, we wrote a single chunk that may be very large; this is the total amount remaining.
        /// In either case, if this is 0, we have no more data to read.</remarks>
        long unreadChunkLength;

        /// <summary>Optional fields (eTag, expiration) that we read as part of the last object-data buffer read.</summary>
        internal RecordOptionals recordOptionals;
        internal bool optionalsWereRead;

        /// <summary>Number of buffers to use. If this is a chained chunk object, we only need two, since we have to read one at a time to evaluate next-chunk length.</summary>
        int numBuffers;

        /// <summary>Kept to indicate which optional fields we have, if any.</summary>
        RecordInfo recordInfo;
        int optionalLength;

        /// <summary>The cumulative length read; this includes page-break metadata, optionals if read, etc. It allows us to position ourselves to the next record start after the
        /// <see cref="DiskStreamReader{TStoreFunctions}.ReadFromDevice(SectorAlignedMemory, long, int, int, System.Threading.CountdownEvent)"/>> is complete.</summary>
        internal long readCumulativeLength = 0;

        /// <summary>The cumulative length of object data read from the device.</summary>
        internal long valueCumulativeLength;

        internal CircularDiskPageReadBuffer(SectorAlignedBufferPool bufferPool, int pageBufferSize, int numPageBuffers, IDevice device, ILogger logger) 
        {
            this.bufferPool = bufferPool;
            this.pageBufferSize = pageBufferSize;
            this.device = device;
            this.logger = logger;

            buffers = new DiskPageReadBuffer[numPageBuffers];
            currentIndex = 0;
        }

        internal DiskPageReadBuffer GetCurrentBuffer() => buffers[currentIndex];

        int GetNextBufferIndex(int curIndex)
        {
            var index = curIndex + 1;
            return index > numBuffers ? 0 : index;
        }

        /// <summary>
        /// Begin the deserialization process.
        /// </summary>
        /// <param name="hasContinuationChunk">If true, then we are currently in a chunk that is followed by another ("continuation") chunk.</param>
        /// <param name="valueStartAddress">Start address of the value in the device space (e.g. file)</param>
        /// <param name="valueLength">The length of the value; if <paramref name="hasContinuationChunk"/>, it is the length of the first chunk (which may span two pages); otherwise it is the full serialized length.</param>
        /// <param name="recordInfo">RecordInfo header for the record; among other things, it indicates which optionals (eTag, expiration) are supported.</param>
        /// <param name="optionalLength">Length of optionals, if any</param>
        internal void OnBeginDeserialize(bool hasContinuationChunk, long valueStartAddress, long valueLength, RecordInfo recordInfo, int optionalLength)
        {
            // Initialize members
            this.hasContinuationChunk = hasContinuationChunk;
            currentIndex = 0;
            alignedDeviceAddress = (ulong)RoundDown(valueStartAddress, SectorSize);

            unreadChunkLength = valueLength;
            recordOptionals = default;
            this.recordInfo = recordInfo;
            this.optionalLength = optionalLength;
            optionalsWereRead = false;

            // First initialize all buffers
            for (var ii = 0; ii < buffers.Length; ii++)
                buffers[ii].Initialize();

            currentIndex = 0;
            numBuffers = hasContinuationChunk ? 2 : buffers.Length;

            // Now do the initial buffer reads. For both chainedChunk and Exact, we issue a read for the first buffer immediately.
            // For chained-chunk reads, valueLength is the length of the first chunk, which may span two pages; thus, we can handle
            // chained-chunk and Exact the same way for the first two pages, except that we must read the continuation length on the
            // second page to see if it has the continuation bit set.
            var startPosition = (int)(valueStartAddress & (pageBufferSize - 1));

            var buffer = PrepareToReadChunk(currentIndex, startPosition, unreadChunkLength, out var unalignedReadLength, out var alignedReadLength);
            buffer.ReadFromDevice(alignedDeviceAddress, startPosition, (int)unreadChunkLength + buffer.optionalLength, ReadFromDeviceCallback);

            if (!buffer.isLastChunk)
            {
                alignedDeviceAddress += alignedReadLength;
                unreadChunkLength -= unalignedReadLength;
                startPosition = DiskPageHeader.Size;

                // We have more than one page so move to the next buffer. A chained chunk will include the second page in the initial valueLength,
                // so we can handle this just like Exact by subtracting the length we read above and then reading the second page's data here, plus
                // we must read the continuation length on the second page's callback to see if it has the continuation bit set, indicating more pages.
                // So we must jump out here after the second page if we are a chained chunk.
                for (var ii = 1; ii < numBuffers; ii++)
                {
                    // Leave currentIndex at 0.
                    buffer = PrepareToReadChunk(ii, startPosition, unreadChunkLength, out unalignedReadLength, out alignedReadLength);
                    buffer.ReadFromDevice(alignedDeviceAddress, startPosition, (int)unreadChunkLength + buffer.optionalLength, ReadFromDeviceCallback);
                    if (buffer.isLastChunk || hasContinuationChunk)
                    {
                        Debug.Assert(unreadChunkLength == 0, $"Should have no leftover value length here; inChainedChunk {hasContinuationChunk}");
                        break;
                    }

                    alignedDeviceAddress += alignedReadLength;
                    unreadChunkLength -= unalignedReadLength;

                    // Jump out here after the second page if we are a chained chunk.
                    if (hasContinuationChunk)
                        break;
                }
            }

            // Now that we've issued all the reads we can here, wait for the first buffer.
            buffer = buffers[currentIndex];
            if (!buffer.WaitForDataAvailable())
                Debug.Fail("Unexpected failure to find data in first buffer");
        }

        /// <summary>
        /// Prepare the <see cref="DiskPageReadBuffer"/> and local variables to read a chunk and be ready to compute the next chunk's sizes
        /// as well as prepare for the callback to determine whether we have more to read (including optionals).
        /// </summary>
        /// <param name="bufferIndex">The index into <see cref="buffers"/> of the <see cref="DiskPageReadBuffer"/> that will do the reading</param>
        /// <param name="startPosition">Start position on the page (relative to start of page)</param>
        /// <param name="chunkLength">Remaining chunk length to read</param>
        /// <param name="unalignedReadLength">Returns the unaligned number of bytes to read (this will be subtracted from the ongoing <paramref name="chunkLength"/>).</param>
        /// <param name="alignedReadLength">Returns the aligned number of bytes to read (this will be added to the ongoing <see cref="alignedDeviceAddress"/>).</param>
        /// <returns>The buffer at <paramref name="bufferIndex"/>; created if it does not already exist</returns>
        private DiskPageReadBuffer PrepareToReadChunk(int bufferIndex, int startPosition, long chunkLength, out int unalignedReadLength, out uint alignedReadLength)
        {
            var buffer = buffers[bufferIndex];
            if (buffer is null)
            {
                buffer = new DiskPageReadBuffer(bufferPool.Get(pageBufferSize), device, logger);
                buffers[bufferIndex] = buffer;
            }

            var alignedStartPosition = RoundDown(startPosition, SectorSize);
            var startPadding = startPosition - alignedStartPosition;

            buffer.isLastChunk = startPosition + chunkLength < pageBufferSize;
            if (buffer.isLastChunk)
            {
                // It all fits on one page. See if there is room on the page to add the optionals to it, then issue the read and return.
                if (optionalLength > 0 && startPosition + (int)chunkLength < pageBufferSize - optionalLength)
                {
                    buffer.optionalLength = optionalLength;
                    optionalsWereRead = true;
                }
                unalignedReadLength = (int)chunkLength + startPadding + buffer.optionalLength;
                alignedReadLength = (uint)RoundUp(unalignedReadLength, SectorSize);
            }
            else
            {
                unalignedReadLength = pageBufferSize - startPosition;
                alignedReadLength = (uint)(pageBufferSize - alignedStartPosition);
            }
            readCumulativeLength += unalignedReadLength;
            return buffer;
        }

        /// <summary>
        /// Move to the next buffer and see if it has data.
        /// </summary>
        /// <param name="nextBuffer">The next buffer</param>
        /// <returns></returns>
        internal unsafe bool MoveToNextBuffer(out DiskPageReadBuffer nextBuffer)
        {
            var currentBuffer = buffers[currentIndex];
            currentBuffer.Initialize();

            // If we're in a SerializedSizeIsExact object and have more data to read, queue the read into the current buffer before departing it.
            if (!hasContinuationChunk && unreadChunkLength > 0)
            {
                var startPosition = DiskPageHeader.Size;
                _ = PrepareToReadChunk(currentIndex, startPosition, unreadChunkLength, out var unalignedReadLength, out var alignedReadLength);
                currentBuffer.ReadFromDevice(alignedDeviceAddress, startPosition, (int)unreadChunkLength + currentBuffer.optionalLength, ReadFromDeviceCallback);

                alignedDeviceAddress += alignedReadLength;
                unreadChunkLength -= unalignedReadLength;
            }

            // Move to the next buffer and wait for any in-flight read to complete.
            // - For SerializedSizeIsExact objects, we do this after having queued a read into the current buffer.
            // - For chained-chunk objects, we do this *before* enqueueing the read of the next buffer, because we need to obtain the next-chunk length from the next-buffer read.
            currentIndex = GetNextBufferIndex(currentIndex);
            nextBuffer = buffers[currentIndex];
            if (!nextBuffer.WaitForDataAvailable())
                return false;

            if (hasContinuationChunk)
            {
                // We must read one buffer at a time for chained chunks, to get the length of the next buffer. Issue that read here, which lets it load
                // in the background while we proceed with deserialization of the current buffer. (We start by loading the first two buffers, so the first
                // call to MoveToNextBuffer causes us to move to the second buffer, and thus this loads the third buffer (which, since we only use two 
                // buffers for chained chunks, wraps around to the first buffer again).

                // First get the data length in the current buffer; it is at buffer.endPosition, which was already adjusted by ReadFromDeviceCallback.
                // hasContinuationChunk applies whether there's a read *after* the next buffer; we'll still fetch the next buffer if the length is nonzero.
                unreadChunkLength = GetChainedChunkValueLength((byte*)nextBuffer.endPosition, out hasContinuationChunk);

                var nextNextIndex = GetNextBufferIndex(currentIndex);
                var nextNextBuffer = buffers[nextNextIndex];
                nextNextBuffer.Initialize();

                if (unreadChunkLength > 0)
                {
                    var startPosition = DiskPageHeader.Size;
                    _ = PrepareToReadChunk(currentIndex, startPosition, unreadChunkLength, out var unalignedReadLength, out var alignedReadLength);
                    nextNextBuffer.ReadFromDevice(alignedDeviceAddress, startPosition, (int)unreadChunkLength + nextNextBuffer.optionalLength, ReadFromDeviceCallback);

                    alignedDeviceAddress += alignedReadLength;
                    unreadChunkLength -= unalignedReadLength;
                }
            }
            return true;
        }

        internal unsafe void ReadFromDeviceCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(ReadFromDeviceCallback)} error: {{errorCode}}", errorCode);

            // Finish setting up the buffer, and extract optionals if this was the last buffer.
            var buffer = (DiskPageReadBuffer)context;
            buffer.endPosition = buffer.currentPosition + (int)numBytes;

            if (buffer.isLastChunk)
            {
                if (buffer.optionalLength > 0)
                {
                    // We no longer want the optionals to count in the size of the last page fragment
                    buffer.endPosition -= buffer.optionalLength;
                    ValueSingleAllocation.ExtractOptionals(recordInfo, buffer.memory.GetValidPointer() + buffer.endPosition, out recordOptionals);
                }
            }
            else if (hasContinuationChunk)
            {
                // Not the last buffer and we're in chained chunks with more to fetch, so the read size included the next-chunk length. Remove that here.
                buffer.endPosition -= sizeof(int);
            }

            // Signal the buffer's event to indicate the data is available.
            _ = buffer.countdownEvent.Signal();
        }

        public void Dispose()
        {
            for (var ii = 0; ii < buffers.Length; ii++)
                buffers[ii].Dispose();
        }

        /// <inheritdoc/>
        public override string ToString()
            => $"currIdx {currentIndex}; pageBufSize {pageBufferSize}; UsablePageSize {UsablePageSize}; alignDevAddr {alignedDeviceAddress}; SecSize {SectorSize}";
    }
}
