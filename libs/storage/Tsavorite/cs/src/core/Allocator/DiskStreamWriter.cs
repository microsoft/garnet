// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static LogAddress;
    using static VarbyteLengthUtility;

    /// <summary>
    /// The class that manages IO writing of ObjectAllocator records. It manages the write buffer at two levels:
    /// <list type="bullet">
    ///     <item>At the higher level, called by IO routines, it manages the overall record writing, including flushing to disk as the buffer is filled.</item>
    ///     <item>At the lower level, it provides the stream for the valueObjectSerializer, which is called via Serialize() by the higher level.</item>
    /// </list>
    /// </summary>
    internal unsafe partial class DiskStreamWriter : IStreamBuffer
    {
        readonly IDevice logDevice;
        readonly IObjectSerializer<IHeapObject> valueObjectSerializer;

        /// <summary>The circular buffer we cycle through for parallelization of writes.</summary>
        internal CircularDiskPageWriteBuffer circularPageFlushBuffers;

        /// <summary>The last usable position on the page.</summary>
        internal int PageEndPosition => circularPageFlushBuffers.pageBufferSize;

        /// <summary>The current buffer being written to in the circular buffer list.</summary>
        internal DiskPageWriteBuffer pageBuffer;

        /// <summary>In the most common case, SerializedSizeIsExact is true and this is the expected length of the serialized value object
        /// (used to verify the serialized size after serialization completes).</summary>
        long expectedSerializedLength;

        /// <summary>For object serialization via chained chunks, indicates that the position is not set (i.e. we're not doing chunk chaining)</summary>
        const int NoPosition = -1;
        /// <summary>For object serialization via chained chunks, we need to track the length prefix for the current chunk to update it before
        /// flushing the buffer to disk</summary>
        int valueLengthPosition;
        /// <summary>For object serialization, the length of the current chunk.</summary>
        int currentChunkLength;
        /// <summary>For object serialization, the cumulative length of the value bytes.</summary>
        int valueCumulativeLength;
        /// <summary>The number of bytes in the intermediate chained-chunk length markers (4-byte int each) and <see cref="DiskPageHeader"/>. We don't want to consider
        /// this part of key or value lengths but we need to include it in the SerializedSize calculation, which is necessary for patching addresses.</summary>
        int numRecordOverheadBytes;

        /// <summary>The maximum number of key or value bytes to copy into the buffer rather than enqueue a DirectWrite.</summary>
        internal const int MaxCopySpanLen = 128 * 1024;

        /// <summary>The number of data bytes to allocate for a temp buffer to use to insert <see cref="DiskPageHeader"/>
        ///     into long keys or values that we will be writing the page-interior portion of directly from the data span, rather than copying to the buffer.</summary>
        /// <remarks>This must be a multiple of OS page size, and not too large so we don't spend too much time copying, and at least a couple sectors (assuming max at 4k).</remarks>
        internal const int PageBorderLen = 16 * 1024;

        /// <summary>For object serialization via chained chunks, the start of the key-end cap used for sector alignment when writing the key directly to the device.
        /// May be zero if the key length happens to coincide with the end of a sector.</summary>
        PageBreakInfo keyInteriorPageBreakInfo;
        /// <summary>When writing the key directly to the device, this is the pinned interior span of the key (between the two sector-alignment caps) to be written after the first
        /// object chunk is serialized to the buffer (this two-step approach is needed so we know the chunk length).</summary>
        PinnedSpanByte keyInteriorPinnedSpan;
        /// <summary>When writing the key directly to the device, this is the unpinned interior span of the key (between the two sector-alignment caps) to be written after the first
        /// object chunk is serialized to the buffer (this two-step approach is needed so we know the chunk length).</summary>
        ArraySegment<byte> keyInteriorArraySegment;

        bool HasKeyInterior => !keyInteriorPinnedSpan.IsEmpty || keyInteriorArraySegment.Count > 0;

        /// <summary>If true, we are in the Serialize call. If not we ignore things like <see cref="currentChunkLength"/> etc.</summary>
        bool inSerialize;

        /// <summary>If we are doing Inexact serialization (e.g. JsonObject), we may need this to cross a page-buffer boundary; e.g. a key ends 2 bytes from the end
        /// of the page buffer so we don't even have space for the "next chunk length" int. In this case we hold onto the previous buffer without flushing it until we
        /// have serialized into the next buffer.</summary>
        DiskPageWriteBuffer prevPageBuffer;

        /// <summary>The total capacity of the buffer.</summary>
        public bool IsForWrite => true;

        int SectorSize => circularPageFlushBuffers.SectorSize;

        /// <summary>Constructor. Creates the circular buffer pool.</summary>
        /// <param name="logDevice">The device to write to</param>
        /// <param name="circularPageBuffers">The circular buffer for writing records</param>
        /// <param name="valueObjectSerializer">Serialized value objects to the underlying stream</param>
        /// <exception cref="ArgumentNullException"></exception>
#pragma warning disable IDE0290 // Use primary constructor
        public DiskStreamWriter(IDevice logDevice, CircularDiskPageWriteBuffer circularPageBuffers, IObjectSerializer<IHeapObject> valueObjectSerializer)
        {
            this.logDevice = logDevice ?? throw new ArgumentNullException(nameof(logDevice));
            this.valueObjectSerializer = valueObjectSerializer;

            circularPageFlushBuffers = circularPageBuffers;
            pageBuffer = circularPageBuffers.GetCurrentBuffer();

            valueLengthPosition = NoPosition;
        }

        /// <inheritdoc/>
        public void FlushAndReset(CancellationToken cancellationToken = default) => throw new InvalidOperationException("Flushing must only be done under control of the Write() methods, due to possible Value length adjustments.");

        /// <inheritdoc/>
        public void Write(in LogRecord logRecord, long diskPreviousAddress, out int recordStartAdjustment)
        {
            // Initialize to not track the value length position (not serializing an object, or we know its exact length up-front so are not doing chunk chaining)
            valueLengthPosition = NoPosition;
            expectedSerializedLength = NoPosition;

            // Everything writes the RecordInfo first. Update to on-disk address if it's not done already (the OnPagesClosed thread may have already done it).
            // Do not update the logRecord with the on-disk address; until OnPagesClosed modifies PreviousAddress due to eviction, we can still access it in-memory.
            var tempInfo = logRecord.Info;
            if (!IsOnDisk(tempInfo.PreviousAddress) && tempInfo.PreviousAddress > kTempInvalidAddress)
                tempInfo.PreviousAddress = diskPreviousAddress;
            Write(new ReadOnlySpan<byte>(&tempInfo, RecordInfo.GetLength()));

            // If the record is inline, we can just write it directly; the indicator bytes will be correct.
            if (logRecord.Info.RecordIsInline)
            {
                Write(logRecord.AsReadOnlySpan().Slice(RecordInfo.GetLength()));
                OnRecordComplete();
                recordStartAdjustment = 0;
                return;
            }

            // The in-memory record is not inline, so we must form the indicator bytes for the inline disk image (expanding Overflow and Object).
            var keySpan = logRecord.Key;

            if (!logRecord.Info.ValueIsObject)
            {
                // We know the exact values for the varbyte key and value lengths.
                var valueSpan = logRecord.ValueSpan;
                recordStartAdjustment = WriteLengthMetadata(in logRecord, keySpan.Length, valueSpan.Length);
                if (keySpan.Length > MaxCopySpanLen)
                    WriteKeyDirect(in logRecord);
                else
                    Write(keySpan);
                if (valueSpan.Length > MaxCopySpanLen)
                    WriteValueDirect(in logRecord);
                else
                    Write(valueSpan);
                WriteOptionals(in logRecord);
                OnRecordComplete();
                return;
            }

            // We have a value object to serialize.
            if (valueObjectSerializer is null)
                throw new InvalidOperationException("Cannot write object value without a value serializer.");

            // 1. If ValueObject is null, we write varbytes with a value length of 0, the key, an empty value, optionals, done. TODO  test 0-len value
            var valueObject = logRecord.ValueObject;
            if (valueObject is null)
            {
                recordStartAdjustment = WriteLengthMetadata(in logRecord, keySpan.Length, valueLength: 0);
                if (keySpan.Length > MaxCopySpanLen)
                    WriteKeyDirect(in logRecord);
                else
                    Write(keySpan);
                WriteOptionals(in logRecord);
                OnRecordComplete();
                return;
            }

            // 2. If ValueObject.SerializedSizeIsExact, then write varbytes with the known value length, the key, serialize the value (without tracking valueLengthPosition), write optionals, done.
            if (valueObject.SerializedSizeIsExact)
            {
                // We can write the exact value length, so we will write the indicator byte with the key length and value length, then write the key and value spans.
                recordStartAdjustment = WriteLengthMetadata(in logRecord, keySpan.Length, valueObject.SerializedSize);
                if (keySpan.Length > MaxCopySpanLen)
                    WriteKeyDirect(in logRecord);
                else
                    Write(keySpan);

                // Serialize the value object into possibly multiple buffers, but we only need the one length we've just filled in. valueStartPosition is already set to NoPosition.
                expectedSerializedLength = valueObject.SerializedSize;
                DoSerialize(valueObject);
                WriteOptionals(in logRecord);
                OnRecordComplete();
                return;
            }

            // 3. We can't trust valueObject.SerializedSize here because the object cannot accurately track it (e.g. JsonObject), so we must chain chunks.
            // Initiate chunking. For this case, ValueLength will always be an int. There are two possibilities regarding handling of the first chunk, depending
            // on the size of the key:
            //  a. The key is "small"; it does not contain internal complete pages. In this case we copy it to the buffer in its entirety and then start serializing
            //      the value after. Keys should be far less than MaxCopyKeyLen the vast majority of the time, and because we must delay writing the length metadata
            //      until the first chunk is written, it would introduce complexity (tracking buffer usage, start positions, etc) that will almost never be used.
            //  b. The key is large enough that there are one or more interior pages (pages that will be entirely filled with a portion of the key data). In this case,
            //      we copy fragments that are on the first and last pages, and defer the interior-page writes until the first page has had valueLength filled in for
            //      the first chunk and can be flushed. The interior page writes use WriteDirect from the byte[], the same as large values, or large keys when we are
            //      not also dealing with !SerializedSizeIsExact.

            // 3a. If the key is "small", we'll just copy it to the buffer in its entirety, then track valueLengthPosition while we do ValueObject serialization.
            if (keySpan.Length < MaxCopySpanLen)
            {
                // Max value chunk size is limited to a single buffer so fits in an int. Use int.MaxValue to get the int varbyte size, but we won't write a chunk that large.
                recordStartAdjustment = WriteLengthMetadata(in logRecord, keySpan.Length, int.MaxValue, isChunked: true);    // valueLengthPosition is set because isChunked is true.
                Write(keySpan);

                // Serialize the value object into possibly multiple buffers; we will manage chunk updating in Write(ReadOnlySpan<byte> data).
                DoSerialize(valueObject);
                WriteOptionals(in logRecord);
                OnRecordComplete();
                return;
            }

            // TODO test with large keys, e.g. 10MB key, with and without sector alignment.
            recordStartAdjustment = WriteLengthMetadata(in logRecord, keySpan.Length, int.MaxValue, isChunked: true);    // valueLengthPosition is set because isChunked is true.

            var usablePageSize = circularPageFlushBuffers.UsablePageSize;
            var hasPageBreaks = CalculatePageBreaks(keySpan.Length, initialPageSpaceRemaining: 0, usablePageSize, out keyInteriorPageBreakInfo);

            // Copy the part of the key that fits on the first page (should usually be all of it).
            if (keyInteriorPageBreakInfo.firstPageFragmentSize > 0)
                Write(keySpan.Slice(0, keyInteriorPageBreakInfo.firstPageFragmentSize));

            // If there is more key data to be copied, we'll need to go to a second buffer for the end fragment, and may have interior pages before that.
            if (keyInteriorPageBreakInfo.firstPageFragmentSize < keySpan.Length)
            {
                // We will go to another buffer; this is done by setting prevPageBuffer in OnBufferComplete. First see if we have any internal pages.
                // If we do, we'll save off the span, then write it in FlushKeyInterior.
                if (hasPageBreaks)
                {
                    // Keys are limited to 32 bit sizes so casting to int is safe
                    var interiorLength = usablePageSize * (int)keyInteriorPageBreakInfo.internalPageCount;
                    if (logRecord.IsPinnedKey)
                        keyInteriorPinnedSpan = PinnedSpanByte.FromPinnedSpan(keySpan.Slice(keyInteriorPageBreakInfo.firstPageFragmentSize, interiorLength));
                    else
                    {
                        var overflowArray = logRecord.KeyOverflow;
                        keyInteriorArraySegment = new(overflowArray.Data, overflowArray.StartOffset + keyInteriorPageBreakInfo.firstPageFragmentSize, interiorLength);
                    }
                }

                // Now copy the end fragment to the next buffer. We should be at the end of the current page buffer, so this Write will immediately call
                // OnBufferComplete which will see that valueLengthPosition is set and therefore set up prevPageBuffer.
                Debug.Assert(pageBuffer.currentPosition == PageEndPosition, $"currentPosition {pageBuffer.currentPosition} should be at PageEndPosition {PageEndPosition}");
                Write(keySpan.Slice(keySpan.Length - keyInteriorPageBreakInfo.lastPageFragmentSize));
            }

            // Serialize the value object into possibly multiple buffers. OnBufferComplete will call FlushKeyInterior once the first value chunk
            // is complete (even if it is only a couple bytes).
            DoSerialize(valueObject);
            WriteOptionals(in logRecord);
            OnRecordComplete();
        }

        /// <summary>Determine the number of pages spanned by this size and start position. Used for writing directly from byte[] (rather than copying to the page disk buffer(s)).</summary>
        /// <param name="dataSize">Size to be written</param>
        /// <param name="initialPageSpaceRemaining">Space remaining in the current disk page buffer</param> 
        /// <param name="usablePageSize">Size between disk page header and end of page</param>
        /// <param name="info">The page break info</param>
        /// <returns>True if there are any page breaks, else false.</returns>
        internal static bool CalculatePageBreaks(long dataSize, int initialPageSpaceRemaining, int usablePageSize, out PageBreakInfo info)
        {
            info = default;
            if (dataSize <= initialPageSpaceRemaining)
            {
                info.lastPageFragmentSize = (int)dataSize;
                return false;
            }

            // First fragment size (on current page)
            info.firstPageFragmentSize = initialPageSpaceRemaining;
            dataSize -= initialPageSpaceRemaining;
            if (dataSize == 0)
                return false;

            // Number of internal pages. For each, there is page header overhead.
            info.internalPageCount = dataSize / usablePageSize;

            // Last fragment size (on final page)
            dataSize -= info.internalPageCount * usablePageSize;
            info.lastPageFragmentSize = (int)dataSize;    // This includes the zero case
            return info.hasPageBreak = true;
        }

        /// <summary>
        /// Write the indicator byte and key/value lengths for string (byte stream) values.
        /// </summary>
        /// <returns>The adjustment to record start position, if we had to advance it.</returns>
        private int WriteLengthMetadata(in LogRecord logRecord, int keyLength, long valueLength, bool isChunked = false)
        {
            // Use a local buffer so we can call Write() just once.
            var indicatorBuffer = stackalloc byte[RoundUp(LogRecord.MaxLengthMetadataBytes, sizeof(long))];
            var indicatorByte = ConstructIndicatorByte(keyLength, valueLength, out var keyByteCount, out var valueByteCount);
            if (isChunked)
                SetChunkedValueIndicator(ref indicatorByte);
            var recordStartAdjustment = 0;

            // We always write two pages for the first chained chunk, with the second page's chunk length added into the first page's normal valueLength
            // position. Therefore we don't need the continuation int at the end of the page. So all we have to do here for both chained and Exact is make
            // sure there is enough room for the varbyte length metadata; if we have a long key and value, they may not all fit into a single long. If not,
            // it means we are sizeof(long) from the end of the page; we'll back up to set the RecordInfo to Invalid+IsSectorForceAligned and bump the start
            // of the record to the next page. See CircularDiskPageWriteBuffer.OnPartialFlushComplete for a detailed explanation of IsSectorForceAligned.
            var valLenPos = pageBuffer.currentPosition + LogRecord.IndicatorBytes + keyByteCount + valueByteCount;  // Use a local; only set this.valueLengthPosition if isChunked
            if (valueLengthPosition > PageEndPosition - sizeof(int))
            {
                Debug.Assert(PageEndPosition - pageBuffer.currentPosition == sizeof(long), $"currentPosition expected to be sizeof(long) bytes from end of buffer but was ({PageEndPosition - pageBuffer.currentPosition})");

                var recordInfoPosition = pageBuffer.currentPosition - RecordInfo.GetLength();
                ref var redirectRecordInfo = ref *(RecordInfo*)recordInfoPosition;
                redirectRecordInfo = default;
                redirectRecordInfo.SetInvalid();
                redirectRecordInfo.IsSectorForceAligned = true;
                recordStartAdjustment = PageEndPosition - recordInfoPosition;

                // Write a dummy long to the current location and then rewrite the original recordInfo; that should cause a page buffer flush
                // and start the recordInfo on the next page (after header).
                var logRecordInfo = logRecord.Info;
                Write(new ReadOnlySpan<byte>((long*)pageBuffer.currentPosition, sizeof(long)));
                Write(new ReadOnlySpan<byte>(&logRecordInfo, RecordInfo.GetLength()));
            }

            var ptr = indicatorBuffer;
            *ptr++ = indicatorByte;
            WriteVarbyteLength(keyLength, keyByteCount, ptr);
            ptr += keyByteCount;
            WriteVarbyteLength(valueLength, valueByteCount, ptr);
            ptr += valueByteCount;
            Write(new ReadOnlySpan<byte>(indicatorBuffer, (int)(ptr - indicatorBuffer)));
            return recordStartAdjustment;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteOptionals(in LogRecord logRecord)
        {
            if (logRecord.Info.HasOptionalFields)
                Write(logRecord.GetOptionalFieldsSpan());
        }

        /// <inheritdoc/>
        public void Write(ReadOnlySpan<byte> data, CancellationToken cancellationToken = default)
        {
            // This is called by valueObjectSerializer.Serialize() as well as internally. No other calls should write data to flushBuffer.memory in a way
            // that increments flushBuffer.currentPosition, since we manage chained-chunk continuation and DiskPageHeader offsetting here.

            // Copy to the buffer. If it does not fit in the remaining capacity, we will write as much as does, update the previous length int to include
            // ValueChunkContinuationIndicator if we are chunk chaining, do any deferred key processing, flush the buffer, then start the next chunk off
            // with a new length int unless we know we had a SerializedSizeIsExact value length. Note: Most Garnet objects have relatively small Write()
            // Spans, coming from e.g. BinaryWriter.WriteInt32() or list items which are not usually large. So this is not currently optimized for flushing
            // large spans directly, copying only the sector-aligning "caps" to the buffer. This can be added later if needed (would want to make
            // *valueLengthPosition 0 initially, then add to it as the successive (large) writes are done "out of band" to the main buffer).

            // We want to have the length of the *next* chunk as the last sizeof(int) bytes of the current chunk, so it will be picked up on sector-aligned
            // reads. This means we need to flush up to the last sector of space, then shift that sector to the start of the buffer, leaving the entire buffer
            // (less that sector) available for the next chunk to be written. (This is one reason it's not a circular buffer: We want to have the entire buffer
            // space available for each chunk, and a circular buffer could not be written in one Write() call).
            var lengthSpaceReserve = valueLengthPosition == NoPosition ? 0 : sizeof(int);
            var dataStart = 0;
            while (data.Length - dataStart > 0)
            {
                Debug.Assert(pageBuffer.RemainingCapacity - lengthSpaceReserve > 0, 
                        $"RemainingCapacity {pageBuffer.RemainingCapacity} - lengthSpaceReserve {lengthSpaceReserve} == 0 (data.Length {data.Length}, dataStart {dataStart}) should have already triggered an OnChunkComplete call, which would have reset the buffer");
                cancellationToken.ThrowIfCancellationRequested();   // IDevice does not support cancellation, so just check this here

                // If it won't all fit in the remaining buffer, write as much as will.
                var requestLength = data.Length - dataStart;
                if (requestLength > pageBuffer.RemainingCapacity - lengthSpaceReserve)
                    requestLength = pageBuffer.RemainingCapacity - lengthSpaceReserve;

                data.Slice(dataStart, requestLength).CopyTo(pageBuffer.memory.TotalValidSpan.Slice(pageBuffer.currentPosition));
                dataStart += requestLength;
                pageBuffer.currentPosition += requestLength;
                if (inSerialize)
                    currentChunkLength += requestLength;

                // See if we're at the end of the buffer.
                if (pageBuffer.RemainingCapacity - lengthSpaceReserve == 0)
                    OnBufferComplete(lengthSpaceReserve);
            }
        }

        /// <summary>
        /// At the end of a buffer, see if we should update value length of the chunk and do any deferred key processing that may still be needed for the first chunk.
        /// We may recruit the next buffer instead, to make reads more performant and handle edge cases such as not having a full int worth of space in the available
        /// value space in the buffer (i.e. key ends 2 bytes before end of buffer).
        /// <remarks>Called both during Serialize() and after Serialize() completes.</remarks>
        /// </summary>
        void OnBufferComplete(int lengthSpaceReserve)
        {
            // This should only be called when the object serialization hits the end of the buffer; for partial buffers we will call
            // OnSerializeComplete() after the Serialize() call has returned. "End of buffer" ends before lengthSpaceReserve if any.
            Debug.Assert(pageBuffer.currentPosition == PageEndPosition - lengthSpaceReserve, $"CurrentPosition {pageBuffer.currentPosition} must be at PageEndPosition {PageEndPosition} - lengthSpaceReserve ({lengthSpaceReserve}).");

            // Iff we have a valueLengthPosition then we are doing chained-chunks, which we do as a "paired buffer" approach: we hold the pre-Key
            // data (the record data up to valueLengthPosition, as well as the first sector-aligning "cap" in buffer 1 and then write the pre-Key.
            // If we don't have a valueLengthPosition then we are either serializing an object with SerializedSizeIsExact == true, or writing non-object
            // data (key, value span, or  nboptionals).
            if (valueLengthPosition == NoPosition)
            {
                Debug.Assert(prevPageBuffer is null, "Should not have prevPageBuffer without valueLengthPosition");
                FlushPartialBuffer(pageBuffer);
                return;
            }

            // We're doing chained chunks. 

            // Here is where we write the first chained-chunk's value length in prevPage. Using two pages has two advantages:
            //  - For the first page, it means the first valueLengthPosition remains in the length metadata. This avoids the edge case where the key ends less than 4 bytes from
            //    the end of the buffer, thereby preventing writing the int "next chunk length"
            //  - Lets us use the entire second buffer (as well as any remainder of the first buffer, for the first chunk) for value serialization, resulting in less IO calls.
            // If there is no prevPageBuffer then this means we have serialized to the end of the current buffer without arriving at the end of serialization; if we had,
            // we'd be calling OnSerializationComplete instead. So the current serialization must continue into the next buffer, and the current pageBuffer becomes prevPageBuffer.
            if (prevPageBuffer is null)
            {
                prevPageBuffer = pageBuffer;
                pageBuffer = circularPageFlushBuffers.MoveToAndInitializeNextBuffer();
                return;
            }
            UpdateValueLength(IStreamBuffer.ValueChunkContinuationBit);

            // If this is the first chunk we may need to flush the key interior, which also flushes the rest of the prev buffer. Otherwise just flush the prev buffer.
            if (!FlushKeyInteriorIfNeeded(prevPageBuffer))
                FlushPartialBuffer(prevPageBuffer);

            // We're doing chunk chaining so the next chunk's length is in the last 'int' of the current page. Note that if serialization ends right on the end of the page
            // before this continuation length, OnSerializeComplete will set this to zero with no continuation bit; that's fine, just a wasted int we can't avoid.
            valueLengthPosition = pageBuffer.currentPosition;
            pageBuffer.currentPosition += sizeof(int);
            numRecordOverheadBytes += sizeof(int);
            Debug.Assert(pageBuffer.RemainingCapacity == 0, $"Current buffer RemainingCapacity {pageBuffer.RemainingCapacity} should == 0 at end of OnChunkComplete when chaining chunks");

            // Now make the current buffer the previous buffer and get the next buffer as the current buffer.
            prevPageBuffer = pageBuffer;
            pageBuffer = circularPageFlushBuffers.MoveToAndInitializeNextBuffer();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void UpdateValueLength(int continuationBit)
        {
            Debug.Assert(inSerialize, "Must be inSerialize and tracking valueLength to UpdateValueLength");

            // If we are not chaining chunks, we already wrote the SerializedSizeIsExact length, so there is nothing to do here.
            if (valueLengthPosition != NoPosition)
            {
                // This may set the value length to 0 if we didn't have any value data in this chunk (e.g. the value ended on the prior chunk boundary).
                var buffer = prevPageBuffer ?? pageBuffer;
                var lengthSpaceReserve = continuationBit == 0 ? 0 : sizeof(int);
                *(int*)(buffer.memory.GetValidPointer() + valueLengthPosition) = (currentChunkLength + lengthSpaceReserve) | continuationBit;
            }
            valueCumulativeLength += currentChunkLength;
            currentChunkLength = 0;
        }

        /// <summary>
        /// If we had a large key we may have copied only the start and end sector-alignment "caps" to the buffer, and write the large interior key data
        /// directly to the device: write start of record up to key start-alignment cap, 
        /// </summary>
        /// <returns></returns>
        bool FlushKeyInteriorIfNeeded(DiskPageWriteBuffer buffer)
        {
            if (!HasKeyInterior)
                return false;

            // Caller has updated the value length, so flush everything up to and including the key start cap for sector alignment
            // (this ends on keyEndSectorCapPosition). Then write the key interior span, which is sector-aligned start and size.
            FlushPartialBuffer(buffer, buffer.flushedUntilPosition, PageEndPosition - buffer.flushedUntilPosition);
            buffer.flushedUntilPosition = PageEndPosition;

            // When writing to a device we must have a pinned buffer. We should only be here for extremely large keys which we hope are extremely unusual.
            // That includes Overflow, which is a byte[], so we must pin it with GCHandle and unpin it when the write is complete.
            if (!keyInteriorPinnedSpan.IsEmpty)
                WriteDirectInterior(keyInteriorPinnedSpan, ref keyInteriorPageBreakInfo, targetArray: default);
            else
                WriteDirectInterior(keyInteriorArraySegment.AsSpan(), ref keyInteriorPageBreakInfo, keyInteriorArraySegment.Array);

            // We already wrote the end fragment to the buffer in the initial Write(LogRecord) call, before the first value chunk was written,
            // so we are done here; the rest of the value chunks can be serialized into the buffer and the buffer flushed as usual.

            // We no longer want these; remaining chunks will just Flush as below.
            keyInteriorPageBreakInfo = default;
            keyInteriorPinnedSpan = default;
            keyInteriorArraySegment = default;
            return true;
        }

        void WriteDirect(OverflowByteArray overflowArray)
        {
            _ = CalculatePageBreaks(overflowArray.Length, PageEndPosition - pageBuffer.currentPosition, circularPageFlushBuffers.UsablePageSize, out var pageBreakInfo);
            WriteDirect(overflowArray.ReadOnlySpan, ref pageBreakInfo, overflowArray.Data);
        }

        internal void WriteDirect(ReadOnlySpan<byte> data, byte[] targetArray = null)
        {
            // We don't care about the return on this; we'll inspect the info struct directly.
            _ = CalculatePageBreaks(data.Length, PageEndPosition - pageBuffer.currentPosition, circularPageFlushBuffers.UsablePageSize, out var pageBreakInfo);
            WriteDirect(data, ref pageBreakInfo, targetArray);
        }
        internal void WriteDirect(ReadOnlySpan<byte> data, ref PageBreakInfo pageBreakInfo, byte[] targetArray)
        {
            // Copy the fragment of the data that fits on the first page. If it all fits with space left over, we're done.
            if (pageBreakInfo.firstPageFragmentSize > 0)
                Write(data.Slice(0, pageBreakInfo.firstPageFragmentSize));
            if (pageBuffer.RemainingCapacity > 0)
                return;

            // We filled the buffer so flush it, in preparation for the end-fragment write.
            FlushPartialBuffer(pageBuffer);
            pageBuffer = circularPageFlushBuffers.MoveToAndInitializeNextBuffer();

            // Write any interior full pages.
            if (pageBreakInfo.internalPageCount > 0)
                WriteDirectInterior(data, ref pageBreakInfo, targetArray);

            // Now copy the end fragment to the next buffer we just moved to.
            Debug.Assert(pageBuffer.currentPosition == PageEndPosition, $"currentPosition {pageBuffer.currentPosition} should be at PageEndPosition {PageEndPosition}");
            Write(data.Slice(data.Length - pageBreakInfo.lastPageFragmentSize));
        }

        internal void WriteDirectInterior(ReadOnlySpan<byte> data, ref PageBreakInfo pageBreakInfo, byte[] targetArray)
        {
            // If we are writing direct to the device from a targetArray, we will have to pin the array.
            GCHandle gcHandle = default;
            RefCountedPinnedGCHandle refCountedGcHandle = default;
            if (targetArray is not null)
            {
                // We have to refcount the handle if we are issuing multiple async writes from the array; in that case, don't set gcHandle.
                if (pageBreakInfo.internalPageCount > 1)
                    refCountedGcHandle = new(GCHandle.Alloc(targetArray, GCHandleType.Pinned), pageBreakInfo.internalPageCount);
                else
                    gcHandle = GCHandle.Alloc(targetArray, GCHandleType.Pinned);
            }

            // Cumulative offset into the input data span.
            var dataOffset = 0;

            // Write the fully interior pages. Interior pages between the first and last can write the PageBorderLen buffer over the page break. TODO: Should this throttle the # of simultaneous writes?
            var bufferPageStartOffset = PageBorderLen;
            var bufferPageEndOffset = circularPageFlushBuffers.pageBufferSize;
            for (var ii = 0; ii < pageBreakInfo.internalPageCount; ii++)
            {
                // Write the PageBorderLen bytes containing header + some bytes from the data (adjust this number for efficiency).
                var pageWriteCallback = circularPageFlushBuffers.CreateDiskWriteCallbackContext();
                pageWriteCallback.buffer = circularPageFlushBuffers.bufferPool.Get(PageBorderLen);
                var bufferPtr = pageWriteCallback.buffer.GetValidPointer();
                // Initialize the header at the start of the PageBorder
                _ = (*(DiskPageHeader*)bufferPtr).Initialize(SectorSize);

                // Copy the remaining data for this page boundary after the header.
                var copyLen = PageBorderLen - DiskPageHeader.Size;
                var bufferSpan = new Span<byte>(bufferPtr, PageBorderLen);
                data.Slice(dataOffset, copyLen).CopyTo(bufferSpan.Slice(SectorSize + DiskPageHeader.Size, copyLen));
                circularPageFlushBuffers.FlushToDevice(bufferSpan, pageWriteCallback);
                dataOffset += copyLen;

                var interiorLen = bufferPageEndOffset - bufferPageStartOffset;
                pageWriteCallback = circularPageFlushBuffers.CreateDiskWriteCallbackContext();
                pageWriteCallback.gcHandle = gcHandle;
                pageWriteCallback.refCountedGCHandle = refCountedGcHandle;
                circularPageFlushBuffers.FlushToDevice(data.Slice(dataOffset, interiorLen), pageWriteCallback);
                dataOffset += interiorLen;
            }
        }

        void WriteKeyDirect(in LogRecord logRecord)
        {
            var keySpan = logRecord.Key;
            if (logRecord.IsPinnedKey)
                WriteDirect(keySpan);
            else
                WriteDirect(logRecord.KeyOverflow);
        }

        void WriteValueDirect(in LogRecord logRecord)
        {
            var valueSpan = logRecord.ValueSpan;
            if (logRecord.IsPinnedValue)
                WriteDirect(valueSpan);
            else
                WriteDirect(logRecord.ValueOverflow);
        }

        /// <summary>Flush the buffer up to sector alignment below <see cref="DiskPageWriteBuffer.currentPosition"/>,
        /// then copy any remaining data to the start of the next buffer and set <see cref="pageBuffer"/> to that next buffer.</summary>
        /// <remarks>This is static because we may be using prevPageBuffer instead, so we pass it in.</remarks>
        private void FlushPartialBuffer(DiskPageWriteBuffer buffer)
        {
            if (buffer.currentPosition == 0 || buffer.flushedUntilPosition == buffer.currentPosition)
                return; // Nothing to flush

            Debug.Assert(RoundDown(buffer.currentPosition, SectorSize) == buffer.currentPosition,
                $"currentPosition ({buffer.currentPosition}) should be either end-of-buffer or at a previously sector-aligned boundary between key begin/end 'caps'.");

            var flushLength = RoundDown(buffer.currentPosition, SectorSize) - buffer.flushedUntilPosition;
            FlushToDevice(buffer, buffer.memory.TotalValidSpan.Slice(buffer.flushedUntilPosition, flushLength));
            buffer.flushedUntilPosition += flushLength;
        }

        /// <summary>Flush the buffer from <paramref name="startPosition"/> to <paramref name="endPosition"/>; both should be sector-aligned.
        /// Do not shift the buffer or update <see cref="DiskPageWriteBuffer.currentPosition"/>; subsequent operations will handle that</summary>
        private void FlushPartialBuffer(DiskPageWriteBuffer buffer, int startPosition, int endPosition)
        {
            if (endPosition - startPosition == 0)
                return; // Nothing to flush

            var flushLength = endPosition - startPosition;
            Debug.Assert(IsAligned(startPosition, SectorSize), $"startPosition {startPosition} should be sector-aligned and was not");
            Debug.Assert(IsAligned(endPosition, SectorSize), $"endPosition {endPosition} should be sector-aligned and was not");
            Debug.Assert(IsAligned(flushLength, SectorSize), $"flushLength {flushLength} should be sector-aligned and was not");

            FlushToDevice(buffer, buffer.memory.TotalValidSpan.Slice(startPosition, flushLength));
        }

        /// <summary>Write the span directly to the device without changing the buffer.</summary>
        private void FlushToDevice(DiskPageWriteBuffer buffer, ReadOnlySpan<byte> span, DiskWriteCallbackContext pageWriteCallbackContext = null)
        {
            Debug.Assert(IsAligned(span.Length, SectorSize), $"span.Length {span.Length} should be sector-aligned and was not");
            Debug.Assert(IsAligned((long)circularPageFlushBuffers.alignedNextDiskFlushAddress, SectorSize), $"alignedDeviceAddress {circularPageFlushBuffers.alignedNextDiskFlushAddress} should be sector-aligned and was not");
            circularPageFlushBuffers.FlushToDevice(buffer, span, pageWriteCallbackContext);

            // This does not alter currentPosition; that is the caller's responsibility, e.g. it may ShiftTailToNextBuffer if this is called for partial flushes.
            // Note: When we trigger a flush for a flushBuffer, it will either be a series of partial flushes followed by a ShiftTailToNextBuffer, or a
            // single flush. Either way, we will then leave the buffer we flushed and move to the next one. I.e., we will never start a flush in a buffer
            // and then start adding more into that buffer. This saves us having to track a lastFlushedPosition for the chunk; the only time we would enqueue
            // multiple flushes currently is for keyInterior which should be quite rare.
            circularPageFlushBuffers.priorCumulativeLength += span.Length;
            circularPageFlushBuffers.alignedNextDiskFlushAddress += (ulong)span.Length;
        }

        void DoSerialize(IHeapObject valueObject)
        {
            // valueCumulativeLength is only relevant for object serialization; we increment it on all device writes to avoid "if", so here we reset it to the appropriate
            // "start at 0" by making it the negative of currentPosition. Subsequently if we write e.g. an int, we'll have Length and Position = (-currentPosition + currentPosition + 4).
            valueCumulativeLength = 0;
            currentChunkLength = 0;
            inSerialize = true;
            valueObjectSerializer.Serialize(valueObject);
            OnSerializeComplete(valueObject);
        }

        void OnSerializeComplete(IHeapObject valueObject)
        {
            // Update value length with the continuation bit NOT set. This may set it to zero if we did not have any more data in the object after the last buffer flush.
            UpdateValueLength(IStreamBuffer.NoValueChunkContinuationBit);
            valueLengthPosition = NoPosition;
            inSerialize = false;

            if (expectedSerializedLength != NoPosition)
            {
                if (valueCumulativeLength != expectedSerializedLength)
                    throw new TsavoriteException($"Expected value length {expectedSerializedLength} does not match actual value length {valueCumulativeLength}.");
            }
            else
            {
                valueObject.SerializedSize = valueCumulativeLength + numRecordOverheadBytes;
                numRecordOverheadBytes = 0;
            }

            // Check to write the key interior in case we had only the first chunk (did not need a continuation chunk).
            // We don't care if this has a key interior or not so ignore the return value; we'll leave currentPosition where it is anyway.
            _ = FlushKeyInteriorIfNeeded(prevPageBuffer ?? pageBuffer);
        }

        /// <summary>Called when a <see cref="LogRecord"/> Write is completed. Ensures end-of-record alignment.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnRecordComplete()
        {
            var newCurrentPosition = RoundUp(pageBuffer.currentPosition, Constants.kRecordAlignment);
            Debug.Assert(newCurrentPosition <= pageBuffer.memory.AlignedTotalCapacity, $"newCurrentPosition {newCurrentPosition} exceeds memory.AlignedTotalCapacity {pageBuffer.memory.AlignedTotalCapacity}");

            // CurrentPosition should be sector-aligned at the start of each buffer; either the initial page0 FirstValidAddress offset, or the
            // result of having rounded down to sector alignment when writing the prior buffer. So aligning currentPosition the kRecordAlignment
            // also aligns TotalBytesWritten (since kRecordAlignment is smaller than sector size).
            var alignmentIncrease = newCurrentPosition - pageBuffer.currentPosition;
            if (alignmentIncrease > 0)
            {
                // Write a zero'd span to align to end of record (this is automatically zero'd because we don't specify SkipLocalsInit).
                Span<byte> padSpan = stackalloc byte[alignmentIncrease];
                Write(padSpan);
            }
            Debug.Assert(circularPageFlushBuffers.TotalWrittenLength % Constants.kRecordAlignment == 0, $"TotalWrittenLength {circularPageFlushBuffers.TotalWrittenLength} is not record-aligned");
        }

        /// <inheritdoc/>
        public int Read(Span<byte> destinationSpan, CancellationToken cancellationToken = default) => throw new InvalidOperationException("Read is not supported for DiskStreamWriteBuffer");

        /// <inheritdoc/>
        public void Dispose()
        {
            // Currently nothing to do. In particular, do not Dispose() the FlushWriteBuffers; those must remain alive until flush completes.
        }
    }
}