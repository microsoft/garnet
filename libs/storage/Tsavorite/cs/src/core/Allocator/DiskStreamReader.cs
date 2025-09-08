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

        /// <summary>Information about page breaks within non-object key data space. The page border metadata size is not part of the key length metadata, plus we have to
        /// omit the page metadata information from the user-visible span.</summary>
        PageBreakInfo keyPageBreakInfo;
        /// <summary>Information about page breaks within non-object value data space, and following optionals if present (so we can optimize them along with the value).
        /// The page border metadata size is not part of the value length metadata, plus we have to omit the page metadata information from the user-visible span.</summary>
        PageBreakInfo valueAndOptionalsPageBreakInfo;

        /// <summary>The offset (past <see cref="DiskReadParameters.recordAddress"/>) of the start of the value bytes.</summary>
        int offsetToValueStart;

        /// <summary>The actual on-disk size of the value (including page breaks).</summary>
        long actualValueLength;

        /// <summary>If true, the object uses chained "continuation chunks"; it does not support <see cref="IHeapObject.SerializedSizeIsExact"/>.</summary>
        bool usesChainedChunks;

        /// <summary>If true, then <see cref="usesChainedChunks"/> is also true, and we are currently in a chunk that is followed by another ("continuation") chunk.</summary>
        bool hasContinuationChunk;

        /// <summary>The current record header; used for chunks to identify when they need to extract the optionals after the final chunk.</summary>
        internal RecordInfo recordInfo;
        /// <summary>The number of bytes of optional fields (ETag and Expiration), if any.</summary>
        int optionalLength;

        /// <summary>Size of the disk page.</summary>
        const int MaxBufferSize = IStreamBuffer.PageBufferSize;

        /// <summary>The single buffer or overflowArray if we could retrieve the value (either object or overflow) in a single allocation.</summary>
        ValueSingleAllocation valueSingleAllocation;

        /// <summary>The circular buffer we cycle through for large-object deserialization.</summary>
        readonly CircularDiskPageReadBuffer circularDeserializationBuffers;

        /// <summary>Record optionals extracted from the buffer or read from disk.</summary>
        RecordOptionals recordOptionals;

        /// <summary>The total capacity of the buffer.</summary>
        public bool IsForWrite => false;

        int SectorSize => (int)logDevice.SectorSize;

#pragma warning disable IDE0290 // Use primary constructor
        public DiskStreamReader(in DiskReadParameters readParams, IDevice logDevice, CircularDiskPageReadBuffer deserializationBuffers, ILogger logger)
        {
            this.readParams = readParams;
            this.logDevice = logDevice ?? throw new ArgumentNullException(nameof(logDevice));
            circularDeserializationBuffers = deserializationBuffers;
            this.logger = logger;
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
        /// <param name="recordLength">Receives the length of the record, so the caller can align to get the next record from disk</param>
        public bool Read(ref SectorAlignedMemory recordBuffer, ReadOnlySpan<byte> requestedKey, out DiskLogRecord diskLogRecord, out long recordLength)
        {
            var ptr = recordBuffer.GetValidPointer();
            diskLogRecord = default;
            recordOptionals.Initialize();

            // First see if we have a page footer/header combo in this initial (possibly partial) record, and compact over it if so.
            var initialPageBreakBytes = CompactOverPageBreakIfNeeded(recordBuffer, offsetToStartOfField: 0, out var offsetToInitialPageBreak);
            var currentLength = recordBuffer.required_bytes;   // This is adjusted if we compacted over the page break

            // Check for RecordInfo and indicator byte. If we have that, check for key length; for disk IO we are called for a specific key
            // and can verify it here (unless NoKey, in which case key is empty and we assume we have the record we want; and for Scan(), the same).

            // In the vast majority of cases we will already have read at least one sector, which has all we need for length bytes, and maybe the full record.
            if (currentLength >= RecordInfo.GetLength() + LogRecord.MinLengthMetadataBytes)
            {
                (var keyLengthBytes, var valueLengthBytes, usesChainedChunks) = DeconstructIndicatorByte(*(ptr + RecordInfo.GetLength()));
                recordInfo = *(RecordInfo*)ptr;
                if (recordInfo.Invalid) // includes IsNull
                {
                    recordLength = 0;
                    return false;
                }
                optionalLength = LogRecord.GetOptionalLength(recordInfo);

                // We are using the compacted-over-pagebreak record, so here it does not matter if there was a page break. Later we'll have to add it,
                // when we start doing calculations based on the actual layout.
                var offsetToKeyStart = RecordInfo.GetLength() + LogRecord.IndicatorBytes + keyLengthBytes + valueLengthBytes;

                // If the length is up to offsetToKeyStart, we can read the full lengths.
                if (currentLength >= offsetToKeyStart)
                {
                    // We've checked whether the indicator byte says it is a chained chunk, but we need to recheck on the actual length retrieval
                    // as the last chunk (which may be the first one) will not have the continuation bit checked. These variables are based on the
                    // compacted-over-page-break buffer.
                    var keyLengthPtr = ptr + RecordInfo.GetLength() + LogRecord.IndicatorBytes;
                    var keyLength = GetKeyLength(keyLengthBytes, keyLengthPtr);
                    var valueLength = usesChainedChunks
                        ? GetChainedChunkValueLength(keyLengthPtr + keyLengthBytes, out hasContinuationChunk)
                        : GetValueLength(valueLengthBytes, keyLengthPtr + keyLengthBytes);

                    // We have the key and value lengths here so we can calculate page break info. If the key was short, as is almost always the case,
                    // then we already have the full key, and it can only have at most one page break and we've already calculated it if so. If it's longer
                    // than the key data we already have, do the full page-break calculation, which requires the actual key offset in the full on-disk page(s).
                    var actualOffsetToKeyStart = (offsetToKeyStart > offsetToInitialPageBreak) ? offsetToKeyStart + initialPageBreakBytes : offsetToKeyStart;
                    if (keyLength >= currentLength - offsetToKeyStart)
                    {
                        if (readParams.CalculatePageBreaks(keyLength, readParams.CalculateOffsetDistanceFromEndOfPage(actualOffsetToKeyStart), out keyPageBreakInfo))
                        {
                            // If we detected the initial page break after the start of the key data, it will be included in keyPageBreakInfo.TotalPageBreakBytes
                            // so zero initialPageBreakBytes to avoid double-counting.
                            if (actualOffsetToKeyStart != offsetToKeyStart)
                                initialPageBreakBytes = 0;
                        }
                    }

                    // Get the offset past recordAddress of the start of the value data as well as the distance of that offset from end of usable page data.
                    // This requires the actual value offset in the full on-disk page(s). Keys are limited to 32 bit sizes so casting to int is safe
                    offsetToValueStart = offsetToKeyStart + keyLength;
                    var actualOffsetToValueStart = offsetToValueStart + initialPageBreakBytes + (int)keyPageBreakInfo.TotalPageBreakBytes;
                    var valueStartDistanceFromEndOfPage = readParams.CalculateOffsetDistanceFromEndOfPage(actualOffsetToValueStart);

                    // If the value is not a chained-chunk object we can determine how many page breaks are in it.
                    if (!recordInfo.ValueIsObject && valueLength > valueStartDistanceFromEndOfPage)
                        _ = readParams.CalculatePageBreaks(valueLength + optionalLength, valueStartDistanceFromEndOfPage, out valueAndOptionalsPageBreakInfo);
                    
                    // Initialize this to what will be the valid value for everything except chained chunks. It will be overridden for large chunked value objects
                    // (more than two buffers).
                    actualValueLength = valueLength + valueAndOptionalsPageBreakInfo.TotalPageBreakBytes;

                    if (currentLength >= offsetToKeyStart + keyLength)
                    {
                        // We have the full key in recordBuffer, so use key variables relative to the compacted-over-page-break buffer.
                         var ptrToKeyData = ptr + offsetToKeyStart;

                        // We have the full key, so check for a match if we had a requested key, and return if not.
                        if (!requestedKey.IsEmpty && !readParams.storeFunctions.KeysEqual(requestedKey, new ReadOnlySpan<byte>(ptrToKeyData, keyLength)))
                        {
                            recordLength = 0;
                            return false;
                        }

                        // If we have the full record, we're done.
                        var totalLength = offsetToValueStart + valueLength + optionalLength;
                        if (currentLength >= totalLength)
                        {
                            // We need to set valueSingleAllocation for Deserialize(), so transfer the recordBuffer to us, then to the output diskLogRecord.
                            valueSingleAllocation.Set(recordBuffer, currentPosition: offsetToKeyStart + keyLength);
                            recordBuffer = default;

                            // This is using the compacted-over-pagebreak buffer, so does not need to use pagebreak info. And Transfer() will extract optionals.
                            var valueObject = recordInfo.ValueIsObject ? DoDeserialize() : null;
                            diskLogRecord = DiskLogRecord.Transfer(ref valueSingleAllocation.memoryBuffer, offsetToKeyStart, keyLength, (int)valueLength, valueObject);
                            recordLength = totalLength;
                            return true;
                        }

                        // Transfer ownership of recordBuffer to be our keyBuffer with appropriate ranges. Set the value offset to the on-disk one.
                        offsetToValueStart = actualOffsetToValueStart;
                        keyBuffer = recordBuffer;
                        recordBuffer = default;
                        keyBuffer.valid_offset += (int)(ptrToKeyData - ptr);    // So DiskLogRecord can retrieve GetValidPointer() as the start of the key...
                        keyBuffer.required_bytes = keyLength;                   // ... and this is the length of the key

                        // Read the rest of the record, possibly in pieces. We are still on the "fast path" for keys here; we have the full key in the buffer, and we've calculated
                        // valueStartPosition to include any page breaks in the key. So we are set to just read the value now. We already know 
                        ReadValue(keyOverflow: default, valueLength, out diskLogRecord);
                        recordLength = AddOptionalLength(offsetToValueStart + actualValueLength, optionalLength);
                        return true;
                    }

                    // We don't have the full key in the buffer, so read the full record. Then we will have the record key available to compare to the requested key.
                    // Set the key and value offsets to the on-disk ones
                    offsetToKeyStart = actualOffsetToKeyStart;
                    offsetToValueStart = actualOffsetToValueStart;
                    recordBuffer?.Return();
                    recordBuffer = default;
                    if (ReadKeyAndValue(requestedKey, actualOffsetToKeyStart, keyLength, valueLength, out diskLogRecord))
                    {
                        recordLength = AddOptionalLength(offsetToValueStart + actualValueLength, optionalLength);
                        return true;
                    }
                    recordLength = 0;
                    return false;
                }
            }

            // We do not have enough data to read the lengths so read a single sector into a new record buffer, then recursively call Read again (we are guaranteed
            // to have enough data on the next pass that we won't arrive here again, and we will have recordInfo and optionalLength).
            SectorAlignedMemory initialRecordBuffer = default;
            try
            {
                // Read() will clear initialRecordBuffer if it transfers it to the output diskLogRecord.
                // Since we did not have enough data we did not populate either key or value pageBreakInfos, so just use either.
                initialRecordBuffer = AllocateBufferAndReadFromDevice(offsetToStartOfField: 0, IStreamBuffer.InitialIOSize, ref keyPageBreakInfo, extractOptionals: false);
                return Read(ref initialRecordBuffer, requestedKey, out diskLogRecord, out recordLength);
            }
            finally
            {
                initialRecordBuffer?.Return();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetPage(long address) => address / readParams.pageBufferSize;

        private long AddOptionalLength(long actualOffsetToOptionalStart, long optionalLength)
        {
            var address = readParams.recordAddress + actualOffsetToOptionalStart;
            var result = address + optionalLength;
            if (optionalLength == 0)
                return result;
            return GetPage(address) == GetPage(result) ? result : result + DiskPageFooter.Size + DiskPageHeader.Size;
        }

        /// <summary>
        /// If this record buffer spans a single footer/header combo, shift the contents to be contiguous and update the size. Caller must ensure the size is less than a full page.
        /// </summary>
        /// <param name="recordBuffer">The record buffer to pack, if needed</param>
        /// <param name="offsetToStartOfField">The offset (from <see cref="DiskReadParameters.recordAddress"/>) to the start of the field (or zero for start of record), which defines its offset in the page buffer</param>
        /// <param name="offsetToPageBreak">The offset to the page break, if one was detected</param>
        /// <returns>The number of bytes removed (due to the footer+header); may be 0</returns>
        internal int CompactOverPageBreakIfNeeded(SectorAlignedMemory recordBuffer, long offsetToStartOfField, out int offsetToPageBreak)
        {
            Debug.Assert(recordBuffer.required_bytes <= readParams.UsablePageSize, $"recordBuffer.required_bytes {recordBuffer.required_bytes} must be less than one usable page here");
            var fieldOffset = (readParams.recordAddress + offsetToStartOfField) & (readParams.pageBufferSize - 1);
            offsetToPageBreak = (int)(readParams.pageBufferSize - DiskPageFooter.Size - fieldOffset);
            if (offsetToPageBreak < recordBuffer.required_bytes)
            {
                // Pack by doing the shorter copy
                var spaceOnNextPage = recordBuffer.required_bytes - offsetToPageBreak - DiskPageFooter.Size - DiskPageHeader.Size;
                if (offsetToPageBreak < spaceOnNextPage)
                {
                    new ReadOnlySpan<byte>(recordBuffer.GetValidPointer(), offsetToPageBreak).CopyTo(new Span<byte>(recordBuffer.GetValidPointer() + DiskPageFooter.Size + DiskPageHeader.Size, offsetToPageBreak));
                    recordBuffer.valid_offset += DiskPageFooter.Size + DiskPageHeader.Size;
                }
                else
                    new ReadOnlySpan<byte>(recordBuffer.GetValidPointer() + (recordBuffer.required_bytes - spaceOnNextPage), spaceOnNextPage).CopyTo(new Span<byte>(recordBuffer.GetValidPointer() + offsetToPageBreak, spaceOnNextPage));

                // Now we no longer want the page-break metadata to count in the "requested size"
                recordBuffer.required_bytes -= DiskPageFooter.Size + DiskPageHeader.Size;
                return DiskPageFooter.Size + DiskPageHeader.Size;
            }

            // There is no page break. Set offsetToPageBreak to the max value so callers don't have to do two "if"s
            offsetToPageBreak = int.MaxValue;
            return 0;
        }

        /// <summary>Read the value (and optionals if present)</summary>
        private void ReadValue(OverflowByteArray keyOverflow, long valueLength, out DiskLogRecord diskLogRecord)
        {
            // The most common case is that we have the recordInfo and the key in either keyBuffer or keyOverflow.
            // This broken out into two functions to allow ReadKeyAndValue to wait on an event and then compare the key.
            BeginReadValue(valueLength);
            diskLogRecord = EndReadValue(keyOverflow);
            return;
        }

        private void BeginReadValue(long valueLength, CountdownEvent multiCountdownEvent = null)
        {
            // This is the most common initial case: we have the recordInfo and the key in either keyBuffer or keyOverflow.
            // If multiCountdownEvent is not null, then we're called from ReadKeyAndValue where the key has an IO pending in the countdownEvent; in that case
            // we must wait on it before initializing deserialization (it will not be signaled until both key and value have been read).
            // Note: We separate overflow even though we are reading into either a SectorAlignedMemory or a byte[], not an inline record;
            // we prefer the SectorAlignedMemory where possible as we have a pool for it, which saves GC overhead. However, we choose overflow
            // at a lower limit if there is a page break, to minimize copying to cover up the page boundary metadata.
            var valueIsOverflow = !recordInfo.ValueIsObject
                                    && valueLength > (valueAndOptionalsPageBreakInfo.hasPageBreak ? DiskStreamWriter.MaxCopySpanLen : readParams.maxInlineValueLength);
            valueSingleAllocation = default;

            if (valueIsOverflow)
            {
                valueSingleAllocation.Set(AllocateOverflowAndReadFromDevice(offsetToValueStart, (int)(valueLength + optionalLength), ref valueAndOptionalsPageBreakInfo, extractOptionals: true, multiCountdownEvent));
                return;
            }
            
            if (!recordInfo.ValueIsObject)
            {
                valueSingleAllocation.Set(AllocateBufferAndReadFromDevice(offsetToValueStart, (int)(valueLength + optionalLength), ref valueAndOptionalsPageBreakInfo, extractOptionals: true, multiCountdownEvent), currentPosition:0);
                return;
            }

            // It's a value object. If it is "small", use our overflow logic to populate an overflow buffer then read from that. Otherwise, use the CircularDiskPageReadBuffer.
            // If this was a non-Exact object that spanned only one or two pages, then we stored the full length without needing to chain-chunk, so isChainedChunk is false;
            // otherwise we will have to go to the circular buffer to handle the chain.
            if (!hasContinuationChunk && (valueLength + optionalLength < readParams.UsablePageSize * 2))
                valueSingleAllocation.Set(AllocateOverflowAndReadFromDevice(offsetToValueStart, (int)(valueLength + optionalLength), ref valueAndOptionalsPageBreakInfo, extractOptionals: true, multiCountdownEvent));
            else
            {
                // This is a large object so we will read it into our separate deserialization buffer and then deserialize it from there (we retain ownership of this buffer).
                // If multiCountdownEvent is set, we are reading both the key and value. In that case, we actually do the deserialization in EndReadValue, so we signal
                // multiCountdownEvent here to let the caller continue to the key comparison (we don't want to load a large object for a key mismatch).
                _ = multiCountdownEvent?.Signal();

                circularDeserializationBuffers.OnBeginDeserialize(hasContinuationChunk, readParams.recordAddress + offsetToValueStart, valueLength, recordInfo, optionalLength);
            }
        }

        private DiskLogRecord EndReadValue(OverflowByteArray keyOverflow)
        {
            // Deserialize the object if there is one. This will also read any optionals if they are there (they are after the value object data, so won't have been read yet).
            var valueObject = recordInfo.ValueIsObject ? DoDeserialize() : null;

            // We're done reading. Create the new DiskLogRecord, transferring any non-null keyBuffer or valueBuffer to it.
            // If valueObject is not null, Return() our valueBuffer first for immediate reuse.
            if (valueObject is not null)
                valueSingleAllocation.Dispose();
            var diskLogRecord = new DiskLogRecord(recordInfo, ref keyBuffer, keyOverflow, ref valueSingleAllocation.memoryBuffer, valueSingleAllocation.overflowArray, recordOptionals, valueObject);

            // Dispose() unconditionally this time.
            valueSingleAllocation.Dispose();
            return diskLogRecord;
        }

        /// <summary>Read the key and value (and optionals if present)</summary>
        private bool ReadKeyAndValue(ReadOnlySpan<byte> requestedKey, int offsetToKeyStart, int keyLength, long valueLength, out DiskLogRecord diskLogRecord)
        {
            Debug.Assert(offsetToValueStart == offsetToKeyStart + keyLength, $"valueDataStartPosition {offsetToValueStart} should == offsetToKeyStart ({offsetToKeyStart}) + keyLength ({keyLength})");

            // If the total length is small enough, just read the entire thing into a SectorAlignedMemory unless we have a value object that has a continuation chunk.
            var totalLength = offsetToKeyStart + keyLength + valueLength + optionalLength;

            var recordHasPageBreaks = readParams.CalculatePageBreaks((int)totalLength, distanceFromEndOfPage: readParams.CalculateOffsetDistanceFromEndOfPage(0), out var recordPageBreakInfo);

            if (totalLength < MaxBufferSize && !hasContinuationChunk)
            {
                // See if this record crosses a page boundary. If it does and is less than MaxCopySpanLen, just read and compact it in-place; otherwise fall through to do more complex reading.
                if (!recordHasPageBreaks || totalLength <= DiskStreamWriter.MaxCopySpanLen)
                {
                    // If the optionals are present in the recordBuffer DiskLogRecord.Transfer extracts them, so we don't need to process them here.
                    valueSingleAllocation.Set(AllocateBufferAndReadFromDevice(offsetToStartOfField: 0, (int)totalLength, ref recordPageBreakInfo, extractOptionals: false), currentPosition: offsetToValueStart);
                    var valueObject = recordInfo.ValueIsObject ? DoDeserialize() : null;

                    // We have the full key, so check for a match if we had a requested key.
                    if (!requestedKey.IsEmpty && !readParams.storeFunctions.KeysEqual(requestedKey, valueSingleAllocation.Span.Slice(offsetToKeyStart, keyLength)))
                    {
                        diskLogRecord = default;
                        return false;
                    }

                    diskLogRecord = DiskLogRecord.Transfer(ref valueSingleAllocation.memoryBuffer, offsetToKeyStart, keyLength, (int)valueLength, valueObject);
                    return true;
                }
            }

            // At this point we are reading "large" keys, values, or both. 
            var keyIsOverflow = keyLength > readParams.maxInlineKeyLength;

            // The record is large or has page breaks. If there is no page break and the key is not overflow then the value is large, either an overflow or a chunked object.
            // If the value is large and not an object then it is overflow and we may be able to combine the Key and Value (and optional) reads to reduce copying of the large
            // value, while reading into an OverflowByteArray that can be directly transferred to a LogRecord to be inserted to log tail or readcache.
            if (!recordHasPageBreaks && !keyIsOverflow && !recordInfo.ValueIsObject)
            {
                // Since keys are usually small and optionals are at most two longs (which will at worst add an additional sector), we can usually get all the data with one
                // IO via AllocateOverflowAndReadFromDevice, then copy out the key and optionals, set the Value offsets, and we're done. See if the key is small enough to 
                // read and copy (if it is too large, we'll prefer multiple IOs).
                if (keyLength <= DiskStreamWriter.MaxCopySpanLen)
                {
                    // We'll start reading at alignedKeyStart, which will use offsetToKeyStart as the OverflowByteArray's start, and include optionals.
                    // From that we'll extract the key and update the offset to be at the beginning of the actual value, and extract optionals and adjust the end offset.
                    valueSingleAllocation.Set(AllocateOverflowAndReadFromDevice(offsetToKeyStart, unalignedBytesToRead: (int)(keyLength + valueLength + optionalLength), ref recordPageBreakInfo, extractOptionals: true));
                    var keySpan = valueSingleAllocation.ReadOnlySpan.Slice(0, keyLength);

                    // We have the key now so check for a match if we had a requested key.
                    if (!requestedKey.IsEmpty && !readParams.storeFunctions.KeysEqual(requestedKey, keySpan))
                    {
                        diskLogRecord = default;
                        return false;
                    }

                    keyBuffer = readParams.bufferPool.Get(keyLength);
                    valueSingleAllocation.ReadOnlySpan.Slice(0, keyLength).CopyTo(keyBuffer.TotalValidSpan);
                    valueSingleAllocation.overflowArray.AdjustOffsetFromStart(keyLength);

                    // We have not allocated anything into valueSingleAllocation, so use a ref to its memoryBuffer which is not set, since we have the overflow.
                    diskLogRecord = new(recordInfo, ref keyBuffer, keyOverflow: default, ref valueSingleAllocation.memoryBuffer, valueSingleAllocation.overflowArray, recordOptionals, valueObject: null);
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
            //   c. A value object, in which case we will read it into valueSingleAllocation if it is not too large, or will return here for key verification before
            //      we load the full large object into CircularDiskPageReadBuffer.
            OverflowByteArray keyOverflow = default;

            // Set up the CountdownEvent outside Key and Value reading, so we can wait for those in parallel; the individual ReadFromDevice calls will not Wait().
            // Initiate reading the key.
            using var countdownEvent = new CountdownEvent(2);
            if (keyIsOverflow)
                keyOverflow = AllocateOverflowAndReadFromDevice(offsetToKeyStart, (int)(keyLength + valueLength + optionalLength), ref keyPageBreakInfo, extractOptionals: false, countdownEvent);
            else
                keyBuffer = AllocateBufferAndReadFromDevice(offsetToKeyStart, keyLength, ref keyPageBreakInfo, extractOptionals: false, countdownEvent);

            // Initiate reading the value, which may be the initial buffer for a chunked object.
            BeginReadValue(valueLength, countdownEvent);

            // Wait until both ReadFromDevice()s complete.
            countdownEvent?.Wait();

            // We now have the full key, so check for a match if we had a requested key, and return false if not. Note: doing this here may save us from
            // deserializing a huge object unnecessarily.
            if (!requestedKey.IsEmpty)
            {
                var keySpan = keyIsOverflow ? keyOverflow.ReadOnlySpan : new ReadOnlySpan<byte>(keyBuffer.GetValidPointer(), keyLength);
                if (!readParams.storeFunctions.KeysEqual(requestedKey, keySpan))
                {
                    diskLogRecord = default;
                    return false;
                }
            }

            // Finish the value read, which at this point is done unless we are deserializing objects, and create and return the DiskLogRecord.
            diskLogRecord = EndReadValue(keyOverflow);
            return true;
        }

        /// <summary>
        /// Read data from the device as an overflow allocation. This is because we may copy the record we've Read() to Tail or ReadCache, or the field
        /// may be larger than a single buffer (or even log page); in that case it was that size when it was created, so we're just restoring that.
        /// </summary>
        private OverflowByteArray AllocateOverflowAndReadFromDevice(int offsetToFieldStart, int unalignedBytesToRead, ref PageBreakInfo pageBreakInfo,
            bool extractOptionals, CountdownEvent multiCountdownEvent = null)
        {
            var (alignedReadOffset, startPadding) = readParams.GetAlignedReadStart(offsetToFieldStart);
            OverflowByteArray overflowArray;
            var alignedBytesToRead = 0;

            // The common case is a reasonably small key and a single-page value read with no page breaks.
            if (!pageBreakInfo.hasPageBreak)
            {
                (alignedBytesToRead, var endPadding) = readParams.GetAlignedBytesToRead(unalignedBytesToRead + startPadding);
                overflowArray = new OverflowByteArray(alignedBytesToRead, startPadding, endPadding, zeroInit: false);

                // The common case is that we already got the key on the first read, so here we do not have a multiCountdownEvent and thus will create
                // our own local countdownEvent and Wait() for it here. This also means we can use 'fixed' instead of incurring GCHandle.Alloc overhead.
                DiskReadCallbackContext context = new(multiCountdownEvent ?? new CountdownEvent(1), refCountedGCHandle: default);
                if (multiCountdownEvent is null)
                {
                    try
                    {
                        fixed (byte* ptr = overflowArray.AlignedReadSpan)
                        {
                            logDevice.ReadAsync((ulong)alignedReadOffset, (IntPtr)ptr, (uint)alignedBytesToRead, ReadFromDeviceCallback, context);
                            context.countdownEvent.Wait();
                            if (context.buffer.available_bytes != (uint)alignedBytesToRead)
                                throw new TsavoriteException($"Expected number of alignedBytesToRead {alignedBytesToRead}, actual available_bytes {context.buffer.available_bytes}");
                        }
                    }
                    finally
                    {
                        context.countdownEvent.Dispose();
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

            // We have one or more page breaks so will issue multiple reads so we can strip out page metadata. If we have a multiCountdownEvent it was created with
            // a count for this operation; if we don't, create one (also with an initial count of 1).
            var countdownEvent = multiCountdownEvent ?? new CountdownEvent(1);

            // We will copy the actual data spans to their locations in the buffer (or read them directly to the buffer), so we don't need any sector alignment in this OverflowByteArray.
            // As this is a string which is limited to 512MB we are safe to cast to int.
            var overflowLen = pageBreakInfo.firstPageFragmentSize + (readParams.UsablePageSize * pageBreakInfo.internalPageCount) + pageBreakInfo.lastPageFragmentSize;

            // We will adjust endOffset below.
            overflowArray = new OverflowByteArray((int)overflowLen, startOffset: startPadding, endOffset:0, zeroInit: false);
            
            // Create the refcounted pinned GCHandle with a refcount of 1, so that if a read completes while we're still setting up, we won't get an early unpin.
            RefCountedPinnedGCHandle refCountedGCHandle = new(GCHandle.Alloc(overflowArray.Data, GCHandleType.Pinned), initialCount: 1);
            var destinationOffset = 0;

            // Leave an extra sector out of the page border allowance; PageBorderLen is probably a multiple of OS page size, so we don't want to force it to read
            // another OS page of data just to get the first sector on the following page.
            var bufferSizeAtEndOfPage = DiskStreamWriter.PageBorderLen - SectorSize;

            // Do this inside try/finally to ensure we release resources.
            try
            {
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
                    context = new(countdownEvent, refCountedGCHandle) { buffer = readParams.bufferPool.Get(alignedBytesToRead) };
                    logDevice.ReadAsync((ulong)alignedReadOffset, (IntPtr)context.buffer.GetValidPointer(), (uint)alignedBytesToRead, ReadFromDeviceCallback, context);
                    alignedReadOffset += alignedBytesToRead;
                    destinationOffset += context.TotalCopyLength;
                }

                // Handle internal pages. The alignedBytesToRead components are all already sector-aligned. TODO: Should this throttle the # of simultaneous reads?
                for (var ii = 0; ii < pageBreakInfo.internalPageCount; ii++)
                {
                    // Minus sectorSize for header (already read on previous iteration); minus bufferSizeAtEndOfPage for footer
                    alignedBytesToRead = readParams.pageBufferSize - SectorSize - bufferSizeAtEndOfPage;

                    // Direct-read the page interior. The boundaries of this read are already sector-aligned.
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

                    // Direct-write the page interior. The start of this read us already sector-aligned. As above, read over into the following page, which may be an
                    // interior page (taking the whole page) or may be the end page (with a limited fragment size, which may be smaller than a sector).
                    alignedBytesToRead = bufferSizeAtEndOfPage + SectorSize;
                    context = new(countdownEvent, refCountedGCHandle)
                    {
                        buffer = readParams.bufferPool.Get(alignedBytesToRead),

                        copyTargetFirstSourceOffset = 0,    // Already sector-aligned
                        copyTargetFirstSourceLength = alignedBytesToRead,
                        copyTargetDestinationOffset = destinationOffset,

                        // Calculate based on previous context, before we assign the newly-constructed replacement context to the local variable.
                        copyTargetSecondSourceOffset = context.copyTargetFirstSourceOffset + context.copyTargetFirstSourceLength + DiskPageFooter.Size + DiskPageHeader.Size,
                        copyTargetSecondSourceLength = SectorSize - DiskPageHeader.Size
                    };

                    // If the next page is not an internal page, make sure we only copy the lastPageFragmentSize (this handles the zero case as well).
                    if (pageBreakInfo.internalPageCount == 0 && pageBreakInfo.lastPageFragmentSize < context.copyTargetSecondSourceLength)
                        context.copyTargetSecondSourceLength = pageBreakInfo.lastPageFragmentSize;

                    logDevice.ReadAsync((ulong)alignedReadOffset, (IntPtr)context.buffer.GetValidPointer(), (uint)alignedBytesToRead, ReadFromDeviceCallback, context);
                    alignedReadOffset += alignedBytesToRead;
                    destinationOffset += context.TotalCopyLength;
                }

                // Handle the last-page fragment
                if (pageBreakInfo.lastPageFragmentSize > SectorSize - DiskPageHeader.Size)
                {
                    // TODO optimization: Include the full lastPageFragmentSize in the [last interior page to next page] transition if it is < MaxCopyLen.
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
                    // This is the last read; no need to update offset variables
                }
            }
            finally
            {
                // We had the initial count in either multiCountdownEvent before it was passed in, or in the countdownEvent we created here. We incremented for each
                // fragment, so now we decrement to remove the original one, then Wait if *not* multi; multi is waited on after we return.
                _ = countdownEvent.Signal();

                // MultiCountdown is awaited above us; a single countdown should be awaited (then disposed) here.
                if (multiCountdownEvent is null)
                {
                    countdownEvent.Wait();
                    countdownEvent.Dispose();
                }

                // Remove the refcount we initialized refCountedGCHandle with, so the final read completion (which may have been the non-multiCountdownEvent Wait()
                // we just did) will final-release it.
                refCountedGCHandle.Release();
            }

            if (extractOptionals && optionalLength > 0)
                overflowArray.ExtractOptionalsAtEnd(recordInfo, optionalLength, out recordOptionals);

            return overflowArray;
        }

        private SectorAlignedMemory AllocateBufferAndReadFromDevice(int offsetToStartOfField, int unalignedBytesToRead, ref PageBreakInfo pageBreakInfo, bool extractOptionals, CountdownEvent multiCountdownEvent = null)
        {
            var recordBuffer = AllocateBuffer(offsetToStartOfField, unalignedBytesToRead + (int)pageBreakInfo.TotalPageBreakBytes, out var alignedReadStart, out var alignedBytesToRead);
            ReadFromDevice(recordBuffer, alignedReadStart, alignedBytesToRead, startOffsetInBuffer:0, multiCountdownEvent);

            // If we are calling this routine, we have already determined there is no page break or the length is small enough to just copy over it.
            if (pageBreakInfo.hasPageBreak)
                _ = CompactOverPageBreakIfNeeded(recordBuffer, offsetToStartOfField, out _ /*offsetToPageBreak*/);

            if (extractOptionals && optionalLength > 0)
                valueSingleAllocation.ExtractOptionalsAtEnd(recordInfo, optionalLength, out recordOptionals);

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

        private void ReadFromDevice(SectorAlignedMemory recordBuffer, long alignedReadStart, int alignedBytesToRead, int startOffsetInBuffer, CountdownEvent multiCountdownEvent = null)
        {
            Debug.Assert(alignedBytesToRead <= recordBuffer.AlignedTotalCapacity, $"alignedBytesToRead {alignedBytesToRead} is greater than AlignedTotalCapacity {recordBuffer.AlignedTotalCapacity}");

            // If a CountdownEvent was passed in, we're part of a multi-IO operation; otherwise, just create one for a single IO and wait for it here.
            DiskReadCallbackContext context = new(multiCountdownEvent ?? new CountdownEvent(1), refCountedGCHandle: default);
            try
            {
                logDevice.ReadAsync((ulong)alignedReadStart, (IntPtr)recordBuffer.aligned_pointer + startOffsetInBuffer, (uint)alignedBytesToRead, ReadFromDeviceCallback, context);
                if (multiCountdownEvent is null)
                {
                    context.countdownEvent.Wait();
                    if (context.buffer.available_bytes != (uint)alignedBytesToRead)
                        throw new TsavoriteException($"Expected number of alignedBytesToRead {alignedBytesToRead}, actual available_bytes {context.buffer.available_bytes}");
                }
            }
            finally
            {
                if (multiCountdownEvent is null)
                    context.countdownEvent.Dispose();
            }
        }

        private protected void ReadFromDeviceCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(ReadFromDeviceCallback)} error: {{errorCode}}", errorCode);

            var result = (DiskReadCallbackContext)context;
            result.buffer.available_bytes = (int)numBytes;

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
            result.Dispose();
        }

        /// <inheritdoc/>
        public int Read(Span<byte> destinationSpan, CancellationToken cancellationToken = default)
        {
            // This is called by valueObjectSerializer.Deserialize() to read up to destinationSpan.Length bytes.

            var prevCopyLength = 0;
            var destinationSpanAppend = destinationSpan.Slice(prevCopyLength);

            // Read from the single allocation if we have it.
            if (!valueSingleAllocation.IsEmpty)
            {
                var copyLength = valueSingleAllocation.AvailableLength;
                if (copyLength > destinationSpanAppend.Length)
                    copyLength = destinationSpanAppend.Length;

                if (copyLength > 0)
                {
                    valueSingleAllocation.ReadOnlySpan.Slice(valueSingleAllocation.currentPosition, copyLength).CopyTo(destinationSpanAppend);
                    valueSingleAllocation.currentPosition += copyLength;
                    valueSingleAllocation.valueCumulativeLength += copyLength;
                }
                return copyLength;
            }

            // Read from the circular buffer.
            var buffer = circularDeserializationBuffers.GetCurrentBuffer();
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();   // IDevice does not support cancellation, so just check this here

                var copyLength = buffer.AvailableLength;
                if (copyLength > destinationSpanAppend.Length)
                    copyLength = destinationSpanAppend.Length;

                if (copyLength > 0)
                {
                    buffer.AvailableSpan.Slice(0, copyLength).CopyTo(destinationSpanAppend);
                    valueSingleAllocation.currentPosition += copyLength;
                    if (copyLength == destinationSpanAppend.Length)
                        return destinationSpan.Length;
                }

                prevCopyLength += copyLength;
                if (buffer.AvailableLength == 0)
                {
                    if (!circularDeserializationBuffers.MoveToNextBuffer(out buffer))
                        return prevCopyLength;
                }
                destinationSpanAppend = destinationSpan.Slice(prevCopyLength);
            }
        }

        IHeapObject DoDeserialize()
        {
            // If we haven't yet instantiated the serializer do so now.
            if (valueObjectSerializer is null)
            {
                pinnedMemoryStream = new(this);
                valueObjectSerializer = readParams.storeFunctions.CreateValueObjectSerializer();
                valueObjectSerializer.BeginDeserialize(pinnedMemoryStream);
            }

            valueObjectSerializer.Deserialize(out var valueObject);
            OnDeserializeComplete(valueObject);
            return valueObject;
        }

        void OnDeserializeComplete(IHeapObject valueObject)
        {
            Debug.Assert(usesChainedChunks == !valueObject.SerializedSizeIsExact, $"usesChainedChunks ({usesChainedChunks}) == !valueObject.SerializedSizeIsExact ({valueObject.SerializedSizeIsExact})");
            if (!valueSingleAllocation.IsEmpty)
            {
                if (valueObject.SerializedSizeIsExact)
                    Debug.Assert(valueObject.SerializedSize == valueSingleAllocation.valueCumulativeLength, $"valueObject.SerializedSize(Exact) {valueObject.SerializedSize} != valueCumulativeLength {valueSingleAllocation.valueCumulativeLength}, pt 1");
                else
                    valueObject.SerializedSize = valueSingleAllocation.valueCumulativeLength;

                // Optionals were already extracted on valueSingleAllocation.Set()
            }
            else
            {
                if (valueObject.SerializedSizeIsExact)
                    Debug.Assert(valueObject.SerializedSize == circularDeserializationBuffers.valueCumulativeLength, $"valueObject.SerializedSize(Exact) {valueObject.SerializedSize} != valueCumulativeLength {circularDeserializationBuffers.valueCumulativeLength}, pt 2");
                else
                    valueObject.SerializedSize = circularDeserializationBuffers.valueCumulativeLength;

                // Do this before getting optionals; we may have to issue a read for them, which will distort the *CumulativeLength fields.
                actualValueLength = circularDeserializationBuffers.readCumulativeLength;

                // Extract optionals if they have not yet been obtained. This assumes currentPosition has been correctly set to the first byte after value data.
                if (circularDeserializationBuffers.optionalsWereRead)
                {
                    recordOptionals = circularDeserializationBuffers.recordOptionals;
                    return;
                }

                // If there are no optionals, there's nothing to do.
                if (optionalLength == 0)
                    return;

                // Reuse the current page buffer's memory and countdownEvent to read this. We should be here only because the optionals spanned the page boundary after the last serialized
                // object data, so very rarely, and only need a single sector.
                var buffer = circularDeserializationBuffers.GetCurrentBuffer();
                buffer.isLastChunk = false;
                Debug.Assert(!circularDeserializationBuffers.hasContinuationChunk, "circularDeserializationBuffers.hasContinuationChunk should have been set false to complete deserialization");

                var actualOptionalPosition = offsetToValueStart + actualValueLength;
                var optionalAddress = readParams.recordAddress + actualOptionalPosition;
                var readLength = DiskPageFooter.Size + DiskPageHeader.Size + optionalLength;
                var (alignedDeviceAddress, padding) = readParams.GetAlignedReadStart(optionalAddress);
                var optionalPositionInBuffer = optionalAddress & (SectorSize - 1);  // Set this to Sector alignment, not Page-alignment
                buffer.ReadFromDevice((ulong)alignedDeviceAddress, (int)optionalPositionInBuffer, readLength, circularDeserializationBuffers.ReadFromDeviceCallback);
                _ = buffer.WaitForDataAvailable();

                var offsetToOptionalPosition = (int)(actualOptionalPosition & (readParams.pageBufferSize - 1));
                _ = CompactOverPageBreakIfNeeded(buffer.memory, offsetToOptionalPosition, out var offsetToInitialPageBreak);
                if (optionalLength > 0)
                    ValueSingleAllocation.ExtractOptionalsAtEnd(recordInfo, buffer.memory, optionalLength, out recordOptionals);
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            pinnedMemoryStream?.Dispose();
            valueObjectSerializer?.EndDeserialize();

            keyBuffer?.Return();
            keyBuffer = default;
            valueSingleAllocation.Dispose();
        }
    }
}
