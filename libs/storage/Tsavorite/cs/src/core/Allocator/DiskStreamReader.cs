// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;
using static Tsavorite.core.VarbyteLengthUtility;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>
    /// The class that manages IO read of ObjectAllocator records. It manages the read buffer at two levels:
    /// <list type="bullet">
    ///     <item>At the higher level, called by IO routines, it manages the overall record reading, including issuing additional reads as the buffer is drained.</item>
    ///     <item>At the lower level, it provides the stream for the valueObjectSerializer, which is called via Deserialize() by the higher level.</item>
    /// </list>
    /// </summary>
    internal unsafe partial class DiskStreamReader<TStoreFunctions> : IStreamBuffer
        where TStoreFunctions : IStoreFunctions
    {
        readonly DiskReadParameters readParams;
        readonly IDevice logDevice;
        IObjectSerializer<IHeapObject> valueObjectSerializer;
        PinnedMemoryStream<DiskStreamReader<TStoreFunctions>> pinnedMemoryStream;
        readonly ILogger logger;

        /// <summary>The <see cref="SectorAlignedMemory"/> of the non-overflow key buffer.</summary>
        /// <remarks>Held as a field instead of a local so it can be Dispose()d in case of an exception.</remarks>
        SectorAlignedMemory keyBuffer;
        /// <summary>The <see cref="SectorAlignedMemory"/> of the non-overflow value buffer or the buffer used to read value-object chunks.</summary>
        /// <remarks>Held as a field instead of a local so it can be Dispose()d in case of an exception.</remarks>
        SectorAlignedMemory valueBuffer;

        /// <summary>For object deserialization, the position (we do not support write in this buffer) in the current (sub-)chunk. This is less than or equal to <see cref="currentLength"/>;
        /// if equal to, then there is no more to data process in the buffer and another buffer must be read if the record is incomplete.</summary>
        int currentPosition;
        /// <summary>For object deserialization, the amount of data that has been read into the buffer for the current (sub-)chunk. This is greater than or equal to <see cref="currentPosition"/>;
        /// if equal to, then there is no more to data process in the buffer and another buffer must be read if the record is incomplete.</summary>
        int currentLength;
        /// <summary>The amount of data we have remaining for the current (sub-)chunk (<see cref="currentPosition"/> is 0-based).</summary>
        int currentRemainingLength => currentLength - currentPosition;

        /// <summary>Information about page breaks within non-object key data space. The page border metadata size is not part of the key length metadata, plus we have to
        /// omit the page metadata information from the user-visible span.</summary>
        PageBreakInfo keyPageBreakInfo;
        /// <summary>Information about page breaks within non-object value data space. The page border metadata size is not part of the value length metadata, plus we have to
        /// omit the page metadata information from the user-visible span.</summary>
        PageBreakInfo valuePageBreakInfo;

        /// <summary>For value object deserialization, the cumulative length of data copied into the caller's buffer by Read(). Does not apply to other record fields or value types</summary>
        long valueCumulativeLength;
        /// <summary>The number of bytes in the intermediate chained-chunk length markers (4-byte int each). We don't want to consider this part of
        /// <see cref="valueCumulativeLength"/> but we need to include it in offset calculations for the start of the next chunk.</summary>
        int numBytesInChainedChunkLengths;
        /// <summary>The offset (past <see cref="DiskReadParameters.recordAddress"/>) of the start of the value bytes.</summary>
        int offsetToValueStart;
        /// <summary>The amount of space remaining on the page after <see cref="offsetToValueStart"/>.</summary>
        int valueStartDistanceFromEndOfPage;

        /// <summary>If true, we are currently in a chunk in chained-chunk (continuation indicator) format that has another chunk following.</summary>
        bool isInChainedChunk;
        /// <summary>For object deserialization we need to track the remaining chunk length to be read.</summary>
        /// <remarks>For chained chunks with the continuation tag, we should usually have a buffer large enough for a full chunk, as it fit in the write buffer when we wrote it.
        /// But if <see cref="IHeapObject.SerializedSizeIsExact"/>, we write a single chunk that may be very large.</remarks>
        long unreadChunkLength;

        /// <summary>The current record header; used for chunks to identify when they need to extract the optionals after the final chunk.</summary>
        internal RecordInfo recordInfo;
        /// <summary>The number of bytes of optional fields (ETag and Expiration), if any.</summary>
        int optionalLength;

        /// <summary>Size of the disk page.</summary>
        const int MaxBufferSize = IStreamBuffer.PageBufferSize;

        /// <summary>The Span of current chunk data read into the buffer.</summary>
        public Span<byte> ChunkBufferSpan => valueBuffer.TotalValidSpan;

        /// <summary>The total capacity of the buffer.</summary>
        public bool IsForWrite => false;

        int SectorSize => (int)logDevice.SectorSize;

        public DiskStreamReader(in DiskReadParameters readParams, IDevice logDevice, ILogger logger)
        {
            this.readParams = readParams;
            this.logDevice = logDevice ?? throw new ArgumentNullException(nameof(logDevice));
            this.logger = logger;

            // Sectors and MaxInternalOffset are powers of 2; we want to be sure we have room for 2 sectors in OverflowByteArray offsets.
            Debug.Assert(SectorSize < OverflowByteArray.MaxInternalOffset, $"Sector size {SectorSize} must be less than OverflowByteArray.MaxInternalOffset {OverflowByteArray.MaxInternalOffset}");
        }

        /// <inheritdoc/>
        public void FlushAndReset(CancellationToken cancellationToken = default) => throw new InvalidOperationException("FlushAndReset is not supported for DiskStreamReadBuffer");

        /// <inheritdoc/>
        public void Write(in LogRecord logRecord, long diskTailOffset) => throw new InvalidOperationException("Write is not supported for DiskStreamReadBuffer");

        /// <inheritdoc/>
        public void Write(ReadOnlySpan<byte> data, CancellationToken cancellationToken = default) => throw new InvalidOperationException("Write is not supported for DiskStreamReadBuffer");

        /// <summary>
        /// Process the record in recordBuffer. which came from the initial IO operation or from an interator record:
        /// <list type="bullet">
        /// <item>If we have the key, compare it to the passed key and return false if it does not match. If we don't have the key, it is either
        ///       ReadAtAddress (which is an implicit match) or Scan.</item>
        /// <item>If we have the full record, create the output <paramref name="diskLogRecord"/> from it and return true.</item>
        /// <item>Otherwise, do additional reads as needed (possibly multiple, for object chunks), compare the key if needed as in the first bullet,
        ///       then create the output <paramref name="diskLogRecord"/> from it and return true.</item>
        /// </list>
        /// </summary>
        /// <param name="recordBuffer">The initial record read from disk--either from Pending IO, in which case it is of size <see cref="IStreamBuffer.InitialIOSize"/>,
        ///     or from iterator <see cref="ObjectFrame"/> initial read.</param>
        /// <param name="requestedKey">The requested key, if not ReadAtAddress; we will compare to see if it matches the record.</param>
        /// <param name="diskLogRecord">The output <see cref="DiskLogRecord"/>, which may be entirely defined by spans within <paramref name="recordBuffer"/>, or
        ///     may contain Key or Value overflows or Value Objects.</param>
        /// <remarks>
        /// The "fast path" for SpanByteAllocator goes through here with <see cref="DiskReadParameters.fixedPageSize"/> set to nonzero, checking for having
        /// a complete key for comparison and for having a complete record, allocating a full-size record if another read is necessary, in the same way as
        /// was previously done in <see cref="AllocatorBase{TStoreFunctions, TAllocator}.AsyncGetFromDiskCallback(uint, uint, object)"/>
        /// </remarks>
        public bool Read(ref SectorAlignedMemory recordBuffer, ReadOnlySpan<byte> requestedKey, out DiskLogRecord diskLogRecord)
        {
            var ptr = recordBuffer.GetValidPointer();
            diskLogRecord = default;

            // First see if we have a page footer/header combo in this (possibly partial) record, and pack over it if so.
            var keyPageBreakBytes = CopyOverPageBreakIfNeeded(recordBuffer, offsetToStartOfField: 0);
            currentLength = recordBuffer.available_bytes;

            // Check for RecordInfo and indicator byte. If we have that, check for key length; for disk IO we are called for a specific key
            // and can verify it here (unless NoKey, in which case key is empty and we assume we have the record we want; and for Scan(), the same).

            // In the vast majority of cases we will already have read at least one sector, which has all we need for length bytes, and maybe the full record.
            if (currentLength >= RecordInfo.GetLength() + LogRecord.MinLengthMetadataBytes)
            {
                (var keyLengthBytes, var valueLengthBytes, isInChainedChunk) = DeconstructIndicatorByte(*(ptr + RecordInfo.GetLength()));
                recordInfo = *(RecordInfo*)ptr;
                if (recordInfo.Invalid) // includes IsNull
                    return false;
                optionalLength = LogRecord.GetOptionalLength(recordInfo);

                var offsetToKeyStart = RecordInfo.GetLength() + LogRecord.IndicatorBytes + keyLengthBytes + valueLengthBytes;
                if (currentLength >= offsetToKeyStart)
                {
                    // We've checked whether the indicator byte says it is a chained chunk, but we need to recheck on the actual length retrieval
                    // as the last chunk (which may be the first one) will not have the continuation bit checked.
                    var keyLengthPtr = ptr + RecordInfo.GetLength() + LogRecord.IndicatorBytes;
                    var keyLength = GetKeyLength(keyLengthBytes, keyLengthPtr);
                    var valueLength = isInChainedChunk
                        ? GetChainedChunkValueLength(keyLengthPtr + keyLengthBytes, out isInChainedChunk)
                        : GetValueLength(valueLengthBytes, keyLengthPtr + keyLengthBytes);

                    // We have the key and value lengths here so we can calculate page break info. If the key was short, as is almost always the case,
                    // then we already have the full key, and it can only have at most one page break and we've already calculated it if so.
                    // If it's longer, we do the full page-break calculation.
                    if (keyLength > readParams.UsablePageSize && readParams.fixedPageSize <= 0)
                    {
                        if (readParams.CalculatePageBreaks(keyLength, readParams.CalculateOffsetDistanceFromEndOfPage(offsetToKeyStart), out keyPageBreakInfo))
                            keyPageBreakBytes = keyPageBreakInfo.TotalPageBreakBytes;
                    }

                    // Get the offset past recordAddress of the start of the value data as well as the distance of that offset from end of usable page data.
                    offsetToValueStart = offsetToKeyStart + keyLength + keyPageBreakBytes;
                    valueStartDistanceFromEndOfPage = readParams.CalculateOffsetDistanceFromEndOfPage(offsetToValueStart);

                    // If the value is not an object determine if there are page breaks in it (non-object length is 512MB max, so always an int).
                    if (!recordInfo.ValueIsObject && valueLength > valueStartDistanceFromEndOfPage && readParams.fixedPageSize <= 0)
                        _ = readParams.CalculatePageBreaks((int)valueLength, valueStartDistanceFromEndOfPage, out valuePageBreakInfo);

                    if (currentLength >= offsetToKeyStart + keyLength)
                    {
                        // We have the full key in recordBuffer
                        var ptrToKeyData = ptr + offsetToKeyStart;

                        // We have the full key, so check for a match if we had a requested key, and return if not.
                        if (!requestedKey.IsEmpty && !readParams.storeFunctions.KeysEqual(requestedKey, new ReadOnlySpan<byte>(ptrToKeyData, keyLength)))
                            return false;

                        // If we have the full record, we're done.
                        var totalLength = offsetToValueStart + valueLength + optionalLength;
                        if (currentLength >= totalLength)
                        {
                            currentPosition = offsetToKeyStart + keyLength;

                            // We need to set valueBuffer for Deserialize(), so transfer the recordBuffer to us, then to the output diskLogRecord.
                            valueBuffer = recordBuffer;
                            recordBuffer = default;
                            var valueObject = recordInfo.ValueIsObject ? DoDeserialize(out _) : null;       // optionals are in the recordBuffer if present, so ignore the outparams for them
                            diskLogRecord = DiskLogRecord.Transfer(ref valueBuffer, offsetToKeyStart, keyLength, (int)valueLength, valueObject);
                            return true;
                        }

                        // We have the full key in recordBuffer (and maybe in requestedKey) but don't have the value or optionals. If we have a fixed-size page,
                        // just read it and return; we won't have overflow or objects.
                        if (readParams.fixedPageSize > 0)
                        {
                            Debug.Assert(totalLength <= readParams.fixedPageSize, $"totalLength {totalLength} cannot be greater than fixedPageSize {readParams.fixedPageSize}");
                            
                            // Since we're fixed-size pages we did not populate either key or value pageBreakInfos, so just use either.
                            var fixedSizeRecordBuffer = AllocateBufferAndReadFromDevice(offsetToStartOfField: 0, (int)totalLength, ref keyPageBreakInfo);
                            diskLogRecord = DiskLogRecord.Transfer(ref fixedSizeRecordBuffer, offsetToKeyStart, keyLength, (int)valueLength);
                            return true;
                        }

                        // Transfer ownership of recordBuffer to be our keyBuffer with appropriate ranges.
                        keyBuffer = recordBuffer;
                        recordBuffer = default;
                        keyBuffer.valid_offset += (int)(ptrToKeyData - ptr);    // So DiskLogRecord can retrieve GetValidPointer() as the start of the key...
                        keyBuffer.required_bytes = keyLength;                   // ... and this is the length of the key

                        // Read the rest of the record, possibly in pieces. We are on the "fast path" for keys here; we have the full key in the buffer, and we've calculated
                        // valueStartPosition to include any page breaks in the key. So we are set to just read the value now.
                        ReadValue(keyOverflow: default, valueLength, out diskLogRecord);
                        return true;
                    }

                    // We don't have the full key in the buffer, so read the full record. Then it will have the record key available to compare to the requested key.
                    return ReadKeyAndValue(requestedKey, offsetToKeyStart, keyLength, valueLength, out diskLogRecord);
                }
            }

            // We do not have enough data to read the lengths so read a single sector into a new record buffer, then recursively call Read again (we are guaranteed
            // to have enough data on the next pass that we won't arrive here again, and we will have recordInfo and optionalLength).
            SectorAlignedMemory initialRecordBuffer = default;
            try
            {
                // Read() will clear initialRecordBuffer if it transfers it to the output diskLogRecord.
                // Since we did not have enough data we did not populate either key or value pageBreakInfos, so just use either.
                initialRecordBuffer = AllocateBufferAndReadFromDevice(offsetToStartOfField: 0, IStreamBuffer.InitialIOSize, ref keyPageBreakInfo);
                return Read(ref initialRecordBuffer, requestedKey, out diskLogRecord);
            }
            finally
            {
                initialRecordBuffer?.Return();
            }
        }

        /// <summary>
        /// If this record buffer spans a footer/header combo, shift the contents to be contiguous and update the size.
        /// </summary>
        /// <param name="recordBuffer">The record buffer to pack, if needed</param>
        /// <param name="offsetToStartOfField">The offset to the start of the field (or zero for start of record), which defines its offset in the page buffer</param>
        /// <returns>The number of bytes removed (due to the footer+header); may be 0</returns>
        internal int CopyOverPageBreakIfNeeded(SectorAlignedMemory recordBuffer, long offsetToStartOfField)
        {
            Debug.Assert(recordBuffer.required_bytes <= readParams.UsablePageSize, $"recordBuffer.required_bytes {recordBuffer.required_bytes} must be less than one usable page here");
            var recordOffset = (readParams.recordAddress + offsetToStartOfField) & (MaxBufferSize - 1);
            var spaceOnCurrentPage = (int)(MaxBufferSize - DiskPageFooter.Size - recordOffset);
            if (spaceOnCurrentPage < recordBuffer.available_bytes)
            {
                // Pack by doing the shorter copy
                var spaceOnNextPage = recordBuffer.available_bytes - spaceOnCurrentPage - DiskPageFooter.Size - DiskPageHeader.Size;
                if (spaceOnCurrentPage < spaceOnNextPage)
                {
                    new ReadOnlySpan<byte>(recordBuffer.GetValidPointer(), spaceOnCurrentPage).CopyTo(new Span<byte>(recordBuffer.GetValidPointer() + DiskPageFooter.Size + DiskPageHeader.Size, spaceOnCurrentPage));
                    recordBuffer.valid_offset += DiskPageFooter.Size + DiskPageHeader.Size;
                }
                else
                    new ReadOnlySpan<byte>(recordBuffer.GetValidPointer() + (recordBuffer.available_bytes - spaceOnNextPage), spaceOnNextPage).CopyTo(new Span<byte>(recordBuffer.GetValidPointer() + spaceOnCurrentPage, spaceOnNextPage));
                recordBuffer.available_bytes -= DiskPageFooter.Size + DiskPageHeader.Size;
                return DiskPageFooter.Size + DiskPageHeader.Size;
            }
            return 0;
        }

        /// <summary>Read the value (and optionals if present)</summary>
        private void ReadValue(OverflowByteArray keyOverflow, long valueLength, out DiskLogRecord diskLogRecord)
        {
            // The most common case is that we have the recordInfo and the key in either keyBuffer or keyOverflow.
            // This broken out into two functions to allow ReadKeyAndValue to wait on an event and then compare the key.
            BeginReadValue(valueLength, out var valueOverflow, out var optionals);
            diskLogRecord = EndReadValue(keyOverflow, valueOverflow, ref optionals);
            return;
        }

        private void BeginReadValue(long valueLength, out OverflowByteArray valueOverflow, out RecordOptionals optionals, CountdownEvent multiCountdownEvent = null)
        {
            // This is the most common initial case: we have the recordInfo and the key in either keyBuffer or keyOverflow.
            // If multiCountdownEvent is not null, then we're called from ReadKeyAndValue where the key has an IO pending in the countdownEvent; in that case
            // we must wait on it before initializing deserialization (it will not be signaled until both key and value have been read).
            // Note: We separate overflow even though we are reading into either a SectorAlignedMemory or a byte[], not an inline record;
            // we prefer the SectorAlignedMemory where possible as we have a pool for it, which saves GC overhead. However, we choose overflow
            // at a lower limit if there is a page break, to minimize copying to cover up the page boundary metadata.
            var valueIsOverflow = !recordInfo.ValueIsObject
                                    && valueLength > (valuePageBreakInfo.hasPageBreak ? DiskStreamWriter.MaxCopySpanLen : readParams.maxInlineValueLength);
            valueBuffer = default;
            valueOverflow = default;
            optionals = default;

            if (valueIsOverflow)
            {
                valueOverflow = AllocateOverflowAndReadFromDevice(offsetToValueStart, (int)(valueLength + optionalLength), ref valuePageBreakInfo, multiCountdownEvent);
                if (optionalLength > 0)
                    valueOverflow.ExtractOptionals(recordInfo, (int)valueLength, out optionals);
                return;
            }
            
            if (!recordInfo.ValueIsObject)
            {
                valueBuffer = AllocateBufferAndReadFromDevice(offsetToValueStart, (int)(valueLength + optionalLength), ref valuePageBreakInfo, multiCountdownEvent);
                if (optionalLength > 0)
                    ExtractOptionals((int)valueLength, out optionals);
                return;
            }

            // This is an object so we will read it into our separate deserialization buffer and then deserialize it from there (we retain ownership of this buffer).
            // Allocate the same size we used to write, plus an additional sector to ensure we have room for the optionals after the final chunk.
            currentPosition = offsetToValueStart;
            valueCumulativeLength = 0;
            unreadChunkLength = valueLength;

            if (isInChainedChunk)
            {
                // Start the chained-chunk Read sequence. Don't read optionals here; they are read after the last (sub-)chunk, but we have to allocate enough buffer
                // to read them when we get there, so do the adjustment here instead of calling AllocateBufferAndReadFromDevice(). valueLength includes the next-chunk length.
                var (alignedReadStart, startPadding) = readParams.GetAlignedReadStart(offsetToValueStart);
                var (alignedBytesToRead, _ /*endPadding*/) = readParams.GetAlignedBytesToRead((int)valueLength + startPadding);

                valueBuffer = readParams.bufferPool.Get(maxBufferSize);
                valueBuffer.valid_offset = startPadding;
                valueBuffer.required_bytes = (int)valueLength;
                valueBuffer.available_bytes = (int)valueLength;

                ReadFromDevice(valueBuffer, alignedReadStart, alignedBytesToRead, multiCountdownEvent);
                currentLength = (int)valueLength;
            }
            else
            {
                // We have a single chunk object, either from SerializedSizeIsExact or just a small enough inexact to fit in a single buffer (if Exact, it may be a large chunk).
                // If we can get it and the optionals into a single buffer, do so, else read in (with isChunkedValue/continuation) or sub-chunks (pieces of a full chunk; but no
                // isChunkedValue/continuation) and get the optionals on the final sub-chunk.
                if (valueLength + optionalLength <= maxBufferSize)
                {
                    valueBuffer = AllocateBufferAndReadFromDevice(offsetToValueStart, (int)(valueLength + optionalLength), ref valuePageBreakInfo, multiCountdownEvent);
                    Debug.Assert(valueBuffer.required_bytes == (int)valueLength, $"Expected required_bytes {(int)valueLength}, actual {valueBuffer.required_bytes}");
                    currentLength = (int)valueLength;
                }
                else
                {
                    // Start the multi-sub-chunk Read sequence. Don't read optionals here, they are read after the last sub-chunk.
                    valueBuffer = AllocateBuffer(offsetToValueStart, maxBufferSize, out var alignedReadStart, out var alignedBytesToRead);
                    ReadFromDevice(valueBuffer, alignedReadStart, alignedBytesToRead, multiCountdownEvent);
                    currentLength = valueBuffer.required_bytes = maxBufferSize;
                }
            }
            unreadChunkLength = unreadChunkLength > currentLength ? unreadChunkLength - currentLength : 0;

            // We have allocated a new buffer for just the value, so reset currentPosition. valueDataStartPosition must remain, as it is used as part of the file offset.
            currentPosition = 0;
        }

        private DiskLogRecord EndReadValue(OverflowByteArray keyOverflow, OverflowByteArray valueOverflow, ref RecordOptionals optionals)
        {
            // Deserialize the object if there is one. This will also read any optionals if they are there (they are after the value object data, so won't have been read yet).
            IHeapObject valueObject = null;
            if (recordInfo.ValueIsObject)
            {
                valueObject = DoDeserialize(out optionals);
                valueBuffer = default;
            }

            // We're done reading. Create the new DiskLogRecord, transferring any non-null keyBuffer or valueBuffer to it.
            // If valueObject is not null, Return() our valueBuffer first for immediate reuse.
            valueBuffer?.Return();
            valueBuffer = null;
            return new DiskLogRecord(recordInfo, ref keyBuffer, keyOverflow, ref valueBuffer, valueOverflow, optionals, valueObject);
        }

        /// <summary>Read the key and value (and optionals if present)</summary>
        private bool ReadKeyAndValue(ReadOnlySpan<byte> requestedKey, int offsetToKeyStart, int keyLength, long valueLength, out DiskLogRecord diskLogRecord)
        {
            Debug.Assert(offsetToValueStart == offsetToKeyStart + keyLength, $"valueDataStartPosition {offsetToValueStart} should == offsetToKeyStart ({offsetToKeyStart}) + keyLength ({keyLength})");

            // If the total length is small enough, just read the entire thing into a SectorAlignedMemory unless we have a value object that has a succeeding chunk.
            var totalLength = offsetToKeyStart + keyLength + valueLength + optionalLength;
            RecordOptionals optionals = default;
            if (totalLength < maxBufferSize && !isInChainedChunk)
            {
                valueBuffer?.Return();
                valueBuffer = default;
                valueBuffer = AllocateBufferAndReadFromDevice(offsetToStartOfField: 0, (int)totalLength, ref valuePageBreakInfo);
                currentLength = valueBuffer.required_bytes;

                currentPosition = offsetToKeyStart + keyLength;
                var valueObject = recordInfo.ValueIsObject ? DoDeserialize(out _) : null;       // optionals are in the recordBuffer if present, so ignore the outparams for them

                // We have the full key, so check for a match if we had a requested key.
                if (!requestedKey.IsEmpty && !readParams.storeFunctions.KeysEqual(requestedKey, new ReadOnlySpan<byte>(valueBuffer.GetValidPointer() + offsetToKeyStart, keyLength)))
                {
                    diskLogRecord = default;
                    return false;
                }

                diskLogRecord = DiskLogRecord.Transfer(ref valueBuffer, offsetToKeyStart, keyLength, (int)valueLength, valueObject);
                return true;
            }

            var keyIsOverflow = keyLength > readParams.maxInlineKeyLength;
            OverflowByteArray valueOverflow;

            // The record is large. If key is not overflow then the value is large, either an overflow or a chunked object.
            // If the value is large and not an object then it is overflow and we may be able to optimize the two reads to reduce copying of the large value,
            // while reading into an OverflowByteArray that can be directly transferred to a LogRecord to be inserted to log tail or readcache.
            if (!keyIsOverflow && !recordInfo.ValueIsObject)
            {
                // Since keys are usually small and optionals are at most two longs (which will at worst add an additional sector, which we've asserted are < MaxInternalOffset),
                // we can usually get all the data with one IO via ReadOverflowFromDevice, then copy out the key and optionals, set the Value offsets, and we're done.
                var alignedKeyStart = RoundDown(offsetToKeyStart, SectorSize);
                if (offsetToValueStart - alignedKeyStart <= OverflowByteArray.MaxInternalOffset)
                {
                    // Yes it will fit. We'll start reading at alignedKeyStart, which will use offsetToKeyStart as the OverflowByteArray's start, and include optionals.
                    // From that we'll extract the key, then update the offset to be at the beginning of the actual value.
                    valueOverflow = AllocateOverflowAndReadFromDevice(offsetToKeyStart, (int)(keyLength + valueLength + optionalLength), ref keyPageBreakInfo);

                    keyBuffer = readParams.bufferPool.Get(keyLength);
                    valueOverflow.ReadOnlySpan.Slice(0, keyLength).CopyTo(keyBuffer.TotalValidSpan);
                    valueOverflow.AdjustOffsetFromStart(keyLength);

                    if (optionalLength > 0)
                        valueOverflow.ExtractOptionals(recordInfo, (int)(keyLength + valueLength), out optionals);

                    Debug.Assert(valueBuffer is null, "Should not have allocated a valueBuffer when Value is overflow");
                    diskLogRecord = new(recordInfo, ref keyBuffer, keyOverflow: default, ref valueBuffer, valueOverflow, optionals, valueObject: null);
                    return true;
                }
            }

            // At this point the key is either:
            //   a. Not overflow, but we couldn't fit it in to the value read; we'll issue a separate AllocateBufferAndReadFromDevice into our SectorAlignedMemory keyBuffer field,
            //      because we know the value is overflow or a chunked object.
            //   b. Overflow; we'll issue a separate ReadOverflowFromDevice into a non-field OverflowByteArray.
            // Then we will wait for the Key IO to complete in parallel with a Read for the value, which may be:
            //   a. Not overflow; we'll read it into our SectorAlignedMemory valueBuffer field along with optionals. In this case we know the key is overflow.
            //   b. Overflow; we'll read it into a non-field OverflowByteArray valueOverflow, along with optionals as above
            //   c. A chunked value, in which case we'll read the first chunk into our SectorAlignedMemory valueBuffer field. This buffer must be large enough that the final chunk can include optionals.
            OverflowByteArray keyOverflow = default;

            // Set up the CountdownEvent outside Key and Value reading, so we can wait for those in parallel; the individual ReadFromDevice calls will not Wait().
            // Initiate reading the key.
            using var countdownEvent = new CountdownEvent(2);
            if (keyIsOverflow)
                keyOverflow = AllocateOverflowAndReadFromDevice(offsetToKeyStart, (int)(keyLength + valueLength + optionalLength), ref keyPageBreakInfo, countdownEvent);
            else
                keyBuffer = AllocateBufferAndReadFromDevice(offsetToKeyStart, keyLength, ref keyPageBreakInfo, countdownEvent);

            // Initiate reading the value, which may be the initial buffer for a chunked object.
            BeginReadValue(valueLength, out valueOverflow, out optionals, countdownEvent);

            // Wait until both ReadFromDevice()s complete.
            countdownEvent?.Wait();

            // We now have the full key, so check for a match if we had a requested key, and return false if not. Note: doing this here may save us from
            // deserializing a huge object unnecessarily.
            if (!requestedKey.IsEmpty)
            {
                var keySpan = keyIsOverflow
                    ? keyOverflow.ReadOnlySpan
                    : new ReadOnlySpan<byte>(keyBuffer.GetValidPointer(), keyLength);
                if (!readParams.storeFunctions.KeysEqual(requestedKey, keySpan))
                {
                    diskLogRecord = default;
                    return false;
                }
            }

            // Finish the value read, which at this point is done unless we are deserializing objects, and create and return the DiskLogRecord.
            diskLogRecord = EndReadValue(keyOverflow, valueOverflow, ref optionals);
            return true;
        }

        /// <summary>
        /// Read data from the device as an overflow allocation. This is because we may copy the record we've Read() to Tail or ReadCache, or the field
        /// may be larger than a single buffer (or even log page); in that case it was that size when it was created, so we're just restoring that.
        /// </summary>
        private OverflowByteArray AllocateOverflowAndReadFromDevice(int offsetToFieldStart, int unalignedBytesToRead, ref PageBreakInfo pageBreakInfo, CountdownEvent multiCountdownEvent = null)
        {
            var (alignedReadOffset, startPadding) = readParams.GetAlignedReadStart(offsetToFieldStart);
            OverflowByteArray overflowArray;
            var alignedBytesToRead = 0;

            // The common case is a single-page value read with no page breaks, so handle that with a 'fixed' so we don't have GCHandle.Alloc overhead.
            if (!pageBreakInfo.hasPageBreak)
            {
                (alignedBytesToRead, var endPadding) = readParams.GetAlignedBytesToRead(unalignedBytesToRead + startPadding);
                overflowArray = new OverflowByteArray(alignedBytesToRead, startPadding, endPadding, zeroInit: false);

                // The common case is that we already got the key on the first read, so here we do not have a multiCountdownEvent and thus will create
                // our own local countdownEvent and Wait() for it here. This also means we can use 'fixed' instead of incurring GCHandle.Alloc overhead.
                // TODO: Include optionals in this read
                DiskReadCallbackContext context = new(multiCountdownEvent ?? new CountdownEvent(1), refCountedGCHandle: default);
                if (multiCountdownEvent is null)
                {
                    fixed (byte* ptr = overflowArray.AlignedReadSpan)
                    {
                        logDevice.ReadAsync((ulong)alignedReadOffset, (IntPtr)ptr, (uint)alignedBytesToRead, ReadFromDeviceCallback, context);
                        context.countdownEvent.Wait();
                        if (context.numBytesRead != (uint)alignedBytesToRead)
                            throw new TsavoriteException($"Expected number of alignedBytesToRead {alignedBytesToRead}, actual {context.numBytesRead}");
                    }
                }
                else
                {
                    // The multiCountdownEvent case must use a GCHandle because we will leave the 'fixed' scope before doing the Wait().
                    context.gcHandle = GCHandle.Alloc(overflowArray.Data, GCHandleType.Pinned);
                    logDevice.ReadAsync((ulong)alignedReadOffset, (IntPtr)Unsafe.AsPointer(ref overflowArray.Data[0]), (uint)alignedBytesToRead, ReadFromDeviceCallback, context);
                }
                return overflowArray;
            }

            // We have one or more page breaks so will issue multiple reads so we can strip out page metadata. If we don't have a multiCountdownEvent, create one (also with an initial count of 1).
            var countdownEvent = multiCountdownEvent ?? new CountdownEvent(1);

            // We will copy the actual data spans to their locations in the buffer (or read them directly to the buffer), so we don't need any sector alignment in this OverflowByteArray.
            var overflowLen = pageBreakInfo.firstPageFragmentSize + (readParams.UsablePageSize * pageBreakInfo.internalPageCount) + pageBreakInfo.lastPageFragmentSize;
            overflowArray = new OverflowByteArray(overflowLen, startOffset: 0, endOffset:0, zeroInit: false);
            
            // Create the refcounted pinned GCHandle with a refcount of 1, so that if a read completes while we're still setting up, we won't get an early unpin.
            RefCountedPinnedGCHandle refCountedGCHandle = new(GCHandle.Alloc(overflowArray.Data, GCHandleType.Pinned), initialCount: 1);
            var destinationOffset = 0;

            // Leave an extra sector out of the page border allowance; PageBorderLen is probably a multiple of OS page size, so we don't want to force it to read
            // another OS page of data just to get the first sector on the following page.
            var bufferSizeAtEndOfPage = DiskStreamWriter.PageBorderLen - SectorSize; 

            // Handle the first-page fragment
            if (pageBreakInfo.firstPageFragmentSize > 0)
            {
                var alignedFirstFragmentSize = readParams.CalculateOffsetDistanceFromEndOfPage(alignedReadOffset);
                alignedBytesToRead = readParams.pageBufferSize - alignedFirstFragmentSize;

                DiskReadCallbackContext context = new(countdownEvent, refCountedGCHandle)
                {
                    copyTargetFirstSourceOffset = startPadding,
                    copyTargetFirstSourceLength = pageBreakInfo.firstPageFragmentSize,
                    copyTargetDestinationOffset = destinationOffset
                };

                // If the first fragment is large, issue a direct read for the longest sector-aligned portion so we don't have to allocate and copy from a large buffer.
                if (pageBreakInfo.firstPageFragmentSize > DiskStreamWriter.MaxCopySpanLen)
                {
                    // Read up to the last sector of the page before, and adjust the copy length to be from bufferSizeAtEndOfPage to one sector before end of page.
                    var firstReadLen = alignedFirstFragmentSize - bufferSizeAtEndOfPage;
                    context.copyTargetFirstSourceLength = firstReadLen - startPadding;
                    context.buffer = readParams.bufferPool.Get(firstReadLen);
                    logDevice.ReadAsync((ulong)alignedReadOffset, (IntPtr)context.buffer.GetValidPointer(), (uint)firstReadLen, ReadFromDeviceCallback, context);

                    alignedBytesToRead -= firstReadLen;
                    alignedReadOffset += firstReadLen;
                    destinationOffset += context.TotalCopyLength;

                    // Now replace the context with a new one with updated start and length of copy.
                    context = new(countdownEvent, refCountedGCHandle)
                    {
                        copyTargetFirstSourceOffset = 0,    // Already sector-aligned
                        copyTargetFirstSourceLength = alignedBytesToRead - DiskPageFooter.Size,
                        copyTargetDestinationOffset = destinationOffset
                    };
                }

                // We know there's a following page so we'll read up to the first sector on that following page; there'll be two pieces separated by the page break metadata.
                alignedBytesToRead += SectorSize;
                context.copyTargetSecondSourceOffset = context.copyTargetFirstSourceOffset + context.copyTargetFirstSourceLength + DiskPageFooter.Size + DiskPageHeader.Size;
                context.copyTargetSecondSourceLength = SectorSize - DiskPageHeader.Size;

                // If the next page is not an internal page, make sure we only copy the lastPageFragmentSize (this handles the zero case as well).
                if (pageBreakInfo.internalPageCount == 0 && pageBreakInfo.lastPageFragmentSize < context.copyTargetSecondSourceLength)
                    context.copyTargetSecondSourceLength = pageBreakInfo.lastPageFragmentSize;

                // Get the buffer we'll read this fragment into.
                context = new (countdownEvent, refCountedGCHandle) { buffer = readParams.bufferPool.Get(alignedBytesToRead) };
                logDevice.ReadAsync((ulong)alignedReadOffset, (IntPtr)context.buffer.GetValidPointer(), (uint)alignedBytesToRead, ReadFromDeviceCallback, context);
                alignedReadOffset += alignedBytesToRead;
                destinationOffset += context.TotalCopyLength;
            }

            // Handle internal pages. The alignedBytesToRead components are all already sector-aligned.
            alignedBytesToRead = readParams.pageBufferSize - SectorSize - bufferSizeAtEndOfPage;
            for (var ii = 0; ii < pageBreakInfo.internalPageCount; ii++)
            {
                // The boundaries of this read are already sector-aligned.
                DiskReadCallbackContext context = new(countdownEvent, refCountedGCHandle)
                {
                    buffer = readParams.bufferPool.Get(alignedBytesToRead),

                    copyTargetFirstSourceOffset = 0,    // Already sector-aligned
                    copyTargetFirstSourceLength = alignedBytesToRead,
                    copyTargetDestinationOffset = destinationOffset
                };

                logDevice.ReadAsync((ulong)alignedReadOffset, (IntPtr)context.buffer.GetValidPointer(), (uint)alignedBytesToRead, ReadFromDeviceCallback, context);
                alignedReadOffset += alignedBytesToRead;
                destinationOffset += context.TotalCopyLength;
            }

            // Handle the last-page fragment
            if (pageBreakInfo.lastPageFragmentSize > SectorSize - DiskPageHeader.Size)
            {
                // TODO: Include the full lastPageFragmentSize in the last "interior page to next page" transition if it is < MaxCopyLen, including optionals if space available
                // TODO: include optionals on this read. Need to flag which context has them so the callback can copy them into member variables.
                // The start boundary of this read is already sector-aligned. We'll need to sector-align the length.
                var remainingLastFragmentSize = pageBreakInfo.lastPageFragmentSize - (SectorSize - DiskPageHeader.Size);
                alignedBytesToRead = RoundUp(remainingLastFragmentSize, SectorSize);
                DiskReadCallbackContext context = new(countdownEvent, refCountedGCHandle)
                {
                    buffer = readParams.bufferPool.Get(alignedBytesToRead),

                    copyTargetFirstSourceOffset = 0,    // Already sector-aligned
                    copyTargetFirstSourceLength = remainingLastFragmentSize,
                    copyTargetDestinationOffset = destinationOffset
                };

                logDevice.ReadAsync((ulong)alignedReadOffset, (IntPtr)context.buffer.GetValidPointer(), (uint)alignedBytesToRead, ReadFromDeviceCallback, context);
            }

            // We had the initial count in either multiCountdownEvent before it was passed in, or in the countdownEvent we created here. We incremented for each
            // fragment, so now for the latter case we decrement to remove the original one, then Wait (but *not* if multi; that's waited on after we return).
            if (multiCountdownEvent is null)
            {
                _ = countdownEvent.Signal();
                countdownEvent.Wait();
            }

            // Remove the refcount we initialized refCountedGCHandle with, so the final read completion (which may have been the non-multiCountdownEvent Wait()
            // we just did) will final-release it.
            refCountedGCHandle.Release();

            return overflowArray;
        }

        private SectorAlignedMemory AllocateBufferAndReadFromDevice(int offsetToStartOfField, int unalignedBytesToRead, ref PageBreakInfo pageBreakInfo, CountdownEvent multiCountdownEvent = null)
        {
            var recordBuffer = AllocateBuffer(offsetToStartOfField, unalignedBytesToRead, out var alignedReadStart, out var alignedBytesToRead);
            ReadFromDevice(recordBuffer, alignedReadStart, alignedBytesToRead, multiCountdownEvent);

            // If we are calling this routine, we have already determined there is no page break or the length is small enough to just copy over it.
            if (pageBreakInfo.hasPageBreak)
                _ = CopyOverPageBreakIfNeeded(recordBuffer, offsetToStartOfField);

            return recordBuffer;
        }

        private SectorAlignedMemory AllocateBuffer(int offsetToFieldStart, int unalignedBytesToRead, out long alignedReadStart, out int alignedBytesToRead)
        {
            (alignedReadStart, var startPadding) = readParams.GetAlignedReadStart(offsetToFieldStart);
            (alignedBytesToRead, var _ /*endPadding*/) = readParams.GetAlignedBytesToRead(unalignedBytesToRead + startPadding);

            var recordBuffer = readParams.bufferPool.Get(alignedBytesToRead);
            recordBuffer.valid_offset = startPadding;
            recordBuffer.required_bytes = unalignedBytesToRead;
            recordBuffer.available_bytes = alignedBytesToRead - startPadding;
            return recordBuffer;
        }

        private void ReadFromDevice(SectorAlignedMemory recordBuffer, long alignedReadStart, int alignedBytesToRead, CountdownEvent multiCountdownEvent = null)
        {
            Debug.Assert(alignedBytesToRead <= recordBuffer.AlignedTotalCapacity, $"alignedBytesToRead {alignedBytesToRead} is greater than AlignedTotalCapacity {recordBuffer.AlignedTotalCapacity}");

            // If a CountdownEvent was passed in, we're part of a multi-IO operation; otherwise, just create one for a single IO and wait for it here.
            DiskReadCallbackContext context = new(multiCountdownEvent ?? new CountdownEvent(1), refCountedGCHandle: default);
            logDevice.ReadAsync((ulong)alignedReadStart, (IntPtr)recordBuffer.aligned_pointer, (uint)alignedBytesToRead, ReadFromDeviceCallback, context);
            if (multiCountdownEvent is null)
            {
                context.countdownEvent.Wait();
                if (context.numBytesRead != (uint)alignedBytesToRead)
                    throw new TsavoriteException($"Expected number of alignedBytesToRead {alignedBytesToRead}, actual {context.numBytesRead}");
            }
        }

        private protected void ReadFromDeviceCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(ReadFromDeviceCallback)} error: {{errorCode}}", errorCode);

            var result = (DiskReadCallbackContext)context;
            result.numBytesRead = numBytes;

            if (result.CopyTarget is not null)
            {
                if (result.copyTargetFirstSourceOffset >= 0)
                    result.buffer.RequiredValidSpan
                        .Slice(result.copyTargetFirstSourceOffset, result.copyTargetFirstSourceLength)
                        .CopyTo(new Span<byte>(result.CopyTarget).Slice(result.copyTargetDestinationOffset, result.copyTargetFirstSourceLength));
                if (result.copyTargetSecondSourceOffset >= 0)
                    result.buffer.RequiredValidSpan
                        .Slice(result.copyTargetSecondSourceOffset, result.copyTargetSecondSourceLength)
                        .CopyTo(new Span<byte>(result.CopyTarget).Slice(result.copyTargetDestinationOffset + result.copyTargetFirstSourceLength, result.copyTargetSecondSourceLength));
            }

            // Signal the event so the waiter can continue
            _ = result.countdownEvent?.Signal();
        }

        /// <inheritdoc/>
        public int Read(Span<byte> destinationSpan, CancellationToken cancellationToken = default)
        {
            // This is called by valueObjectSerializer.Deserialize() to read up to destinationSpan.Length bytes. First see if we have enough data for the request.

            // When we get to the last chunk we will add in the optionals at the end of the read (if we have any optionals), and this will become nonzero.
            var getOptionalLength = 0;

            // Amount of data copied to destination on previous iterations of the loop below.
            var prevCopyLength = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();   // IDevice does not support cancellation, so just check this here

                var isLastChunk = !isInChainedChunk && unreadChunkLength == 0;
                var availableLength = currentRemainingLength - (isInChainedChunk ? sizeof(int) : 0) - (isLastChunk ? optionalLength : 0);
                Debug.Assert(availableLength >= 0, $"Available data length cannot be negative");

                // If we can fill the entire request, do so and we're done. Otherwise, just copy what data we have, and we'll read additional chunks below.
                var destinationSpanAppend = destinationSpan.Slice(prevCopyLength);
                var copyLength = (availableLength >= destinationSpanAppend.Length) ? destinationSpanAppend.Length : availableLength;
                if (copyLength > 0)
                {
                    ChunkBufferSpan.Slice(currentPosition, copyLength).CopyTo(destinationSpanAppend);
                    currentPosition += copyLength;
                    valueCumulativeLength += copyLength;
                    if (copyLength == destinationSpanAppend.Length)
                        return destinationSpan.Length;
                    prevCopyLength += copyLength;
                }

                // If there is no more to read, including no optionals to get, we're done. If there are optionals, OnDeserializeComplete will extract them from currentPosition.
                if (isLastChunk && optionalLength == 0)
                    return prevCopyLength;

                // Read the next chunk. Initialize to the amount remaining in this chunk, then modify that based on whether we have a continuation chunk after this
                // or need to request less to fit in the buffer.
                var valueLength = unreadChunkLength;
                if (valueLength > maxBufferSize - optionalLength)
                {
                    // We have more than a buffer's worth of unread chunk data, so go get the next buffer.
                    valueLength = maxBufferSize - optionalLength;

                    // If isInChainedChunk and we still have unread chunk bytes, we need to read those to know how many bytes are in the next chunk.
                    // Otherwise, this will be our last read and we have enough room to grab any optionals also.
                    if (!isInChainedChunk)
                        getOptionalLength = optionalLength;
                }
                else
                { 
                    // The amount of unread data is less than a buffer, and may be 0 if we have a continuation chunk.
                    // Get the next (sub-)chunk. If it's the last chunk and small enough to fit in the buffer, get the optionals too.
                    // chunks (with isChunkedValue/continuation) or sub-chunks (pieces of a full chunk; but no isChunkedValue/continuation) and get the optionals on the final sub-chunk.
                    if (isInChainedChunk)
                    {
                        // If isInChainedChunk becomes false then there is no chunk after this, so read the optionals (if any).
                        // There may be 0 chunk bytes remaining to be read even if isInChainedChunk, if the object serialization ended on a buffer boundary.
                        if (valueLength == 0)
                        {
                            valueLength = GetChainedChunkValueLength(valueBuffer.GetValidPointer() + currentPosition, out isInChainedChunk);
                            numBytesInChainedChunkLengths += sizeof(int);

                            // We'll subtract this below; setting this here makes us consistent with the SerializedSizeIsExact case.
                            unreadChunkLength = valueLength;
                        }
                        if (valueLength < maxBufferSize - optionalLength)
                            getOptionalLength = optionalLength;
                    }
                    else
                    {
                        // If there is still unread chunk data, we have enough room for it and any optionals.
                        getOptionalLength = optionalLength;
                    }
                }

                if (valueLength + getOptionalLength == 0)
                    return prevCopyLength;

                // Read from the device.
                var offsetFromRecordStart = offsetToValueStart + valueCumulativeLength + numBytesInChainedChunkLengths;

                (var alignedReadStart, var startPadding) = readParams.GetAlignedReadStart(offsetFromRecordStart);
                var (alignedBytesToRead, _ /*endPadding*/) = readParams.GetAlignedBytesToRead((int)(valueLength + startPadding + getOptionalLength));
                valueBuffer.valid_offset = startPadding;
                valueBuffer.required_bytes = (int)valueLength + getOptionalLength;
                valueBuffer.available_bytes = valueBuffer.required_bytes;

                if (alignedBytesToRead > 0)
                    ReadFromDevice(valueBuffer, alignedReadStart, alignedBytesToRead);
                currentPosition = 0;
                currentLength = valueBuffer.required_bytes;
                unreadChunkLength -= valueLength;
                Debug.Assert(unreadChunkLength >= 0, $"unreadChunkLength {unreadChunkLength} cannot be less than zero");
            }
        }

        IHeapObject DoDeserialize(out RecordOptionals optionals)
        {
            // TODO: For multi-buffer, consider using two buffers, with the next one being read from disk on a background thread while the foreground thread processes the current one in parallel.
            // This could simply hold the CountdownEvent for the "disk read in progress" buffer and Wait() on it when DoDeserialize() is ready for the next buffer.
            if (valueObjectSerializer is null)
            {
                pinnedMemoryStream = new(this);
                valueObjectSerializer = readParams.storeFunctions.CreateValueObjectSerializer();
                valueObjectSerializer.BeginDeserialize(pinnedMemoryStream);
            }

            // valueCumulativeLength is only relevant for object serialization; we increment it on all device reads to avoid "if", so here we reset it to the appropriate
            // "start at 0" by making it the negative of currentPosition. Subsequently if we read e.g. an int, we'll have Position = (-currentPosition + currentPosition + 4)
            // and similar for Length.
            valueCumulativeLength = 0;
            valueObjectSerializer.Deserialize(out var valueObject);
            OnDeserializeComplete(valueObject, out optionals);
            return valueObject;
        }

        void OnDeserializeComplete(IHeapObject valueObject, out RecordOptionals optionals)
        {
            if (valueObject.SerializedSizeIsExact)
                Debug.Assert(valueObject.SerializedSize == valueCumulativeLength, $"valueObject.SerializedSize(Exact) {valueObject.SerializedSize} != valueCumulativeLength {valueCumulativeLength}");
            else
                valueObject.SerializedSize = valueCumulativeLength;

            // Extract optionals if they are present. This assumes currentPosition has been correctly set to the first byte after value data.
            ExtractOptionals(currentPosition, out optionals);
        }

        private void ExtractOptionals(int offsetToOptionals, out RecordOptionals optionals)
        {
            var ptrToOptionals = valueBuffer.GetValidPointer() + offsetToOptionals;
            optionals = default;
            if (recordInfo.HasETag)
            {
                optionals.eTag = *(long*)ptrToOptionals;
                offsetToOptionals += LogRecord.ETagSize;
            }
            if (recordInfo.HasExpiration)
                optionals.expiration = *(long*)(ptrToOptionals + offsetToOptionals);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            pinnedMemoryStream?.Dispose();
            valueObjectSerializer?.EndDeserialize();

            keyBuffer?.Return();
            keyBuffer = default;
            valueBuffer?.Return();
            valueBuffer = default;

        }
    }
}
