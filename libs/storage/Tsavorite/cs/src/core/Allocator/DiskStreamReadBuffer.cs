// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
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
    internal unsafe class DiskStreamReadBuffer<TStoreFunctions> : IStreamBuffer
        where TStoreFunctions : IStoreFunctions
    {
        /// <summary>
        /// Information for the IO read operation.
        /// </summary>
        internal readonly struct ReadParameters
        {
            /// <summary>The <see cref="SectorAlignedBufferPool"/> for <see cref="SectorAlignedMemory"/> buffer allocations</summary>
            internal readonly SectorAlignedBufferPool bufferPool;

            /// <summary>For <see cref="SpanByteAllocator{TStoreFunctions}"/>, the fixed page size for buffer allocation</summary>
            internal readonly int fixedPageSize;

            /// <summary>The threshold at which a key becomes overflow</summary>
            internal readonly int maxInlineKeyLength;

            /// <summary>The threshold at which a value becomes overflow</summary>
            internal readonly int maxInlineValueLength;

            /// <summary>The sector size of the device; will be either 512b or 4kb</summary>
            internal readonly uint sectorSize;

            /// <summary>The unaligned start of the record on the device</summary>
            internal readonly long unalignedRecordStartOffset;

            /// <summary>The <see cref="IStoreFunctions"/> implementation to use</summary>
            internal readonly TStoreFunctions storeFunctions;

            /// <summary>Constructor for SpanByteAllocator, which has a fixed page size and no overflow</summary>
            internal ReadParameters(SectorAlignedBufferPool bufferPool, int fixedPageSize, uint sectorSize, long unalignedRecordStartOffset, TStoreFunctions storeFunctions)
            {
                this.bufferPool = bufferPool ?? throw new ArgumentNullException(nameof(bufferPool));
                this.fixedPageSize = fixedPageSize;
                maxInlineKeyLength = LogSettings.kMaxInlineKeySize;     // Note: We throw an exception if it exceeds this but include it here for consistency
                maxInlineValueLength = -1;
                this.sectorSize = sectorSize;
                this.unalignedRecordStartOffset = unalignedRecordStartOffset;
                this.storeFunctions = storeFunctions ?? throw new ArgumentNullException(nameof(storeFunctions));
            }

            /// <summary>Constructor for ObjectAllocator, which has no fixed pages size and may have overflow and objects</summary>
            internal ReadParameters(SectorAlignedBufferPool bufferPool, int maxInlineKeyLength, int maxInlineValueLength, uint sectorSize, long unalignedRecordStartOffset, TStoreFunctions storeFunctions)
            {
                this.bufferPool = bufferPool ?? throw new ArgumentNullException(nameof(bufferPool));
                fixedPageSize = -1;
                this.maxInlineKeyLength = maxInlineKeyLength > IStreamBuffer.DiskReadForceOverflowSize ? IStreamBuffer.DiskReadForceOverflowSize : maxInlineKeyLength;
                this.maxInlineValueLength = maxInlineValueLength > IStreamBuffer.DiskReadForceOverflowSize ? IStreamBuffer.DiskReadForceOverflowSize : maxInlineValueLength;
                this.sectorSize = sectorSize;
                this.unalignedRecordStartOffset = unalignedRecordStartOffset;
                this.storeFunctions = storeFunctions ?? throw new ArgumentNullException(nameof(storeFunctions));
            }

            internal readonly (long alignedFieldOffset, int padding) GetAlignedReadStart(long unalignedFieldOffset)
            {
                var alignedFieldOffset = RoundDown(unalignedRecordStartOffset + unalignedFieldOffset, (int)sectorSize);
                return (alignedFieldOffset, (int)(unalignedRecordStartOffset + unalignedFieldOffset - alignedFieldOffset));
            }

            internal readonly (int alignedBytesToRead, int padding) GetAlignedBytesToRead(int unalignedBytesToRead)
            {
                var alignedBytesToRead = RoundUp(unalignedBytesToRead, (int)sectorSize);
                return (alignedBytesToRead, alignedBytesToRead - unalignedBytesToRead);
            }
        }

        readonly ReadParameters readParams;
        readonly IDevice logDevice;
        IObjectSerializer<IHeapObject> valueObjectSerializer;
        PinnedMemoryStream<DiskStreamReadBuffer<TStoreFunctions>> pinnedMemoryStream;

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

        /// <summary>For value object deserialization, the cumulative length of data copied into the caller's buffer by Read(). Does not apply to other record fields or value types</summary>
        long valueCumulativeLength;
        /// <summary>The number of bytes in the intermediate chained-chunk length markers (4-byte int each). We don't want to consider this part of
        /// <see cref="valueCumulativeLength"/> but we need to include it in offset calculations for the start of the next chunk.</summary>
        int numBytesInChainedChunkLengths;
        /// <summary>For object deserialization, the position of the start of the value bytes.</summary>
        int valueDataStartPosition;

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

        /// <summary>
        /// Maximum buffer size to read for a (sub-)chunk; includes an extra sector to get the optionalLength on the final chunk.
        /// </summary>
        int maxBufferSize => IStreamBuffer.DiskWriteBufferSize + (int)readParams.sectorSize;

        /// <summary>
        /// When we read from the buffer this callback will be used and must set its <see cref="PageAsyncReadResult{T}.numBytesRead"/> numBytesRead
        ///     and signal the <see cref="PageAsyncReadResult{T}.handle"/> event so we can continue.
        /// </summary>
        readonly DeviceIOCompletionCallback ioCompletionCallback;

        /// <summary>The Span of current chunk data read into the buffer.</summary>
        public Span<byte> ChunkBufferSpan => valueBuffer.TotalValidSpan;

        /// <summary>The total capacity of the buffer.</summary>
        public bool IsForWrite => false;

        int SectorSize => (int)logDevice.SectorSize;

        public DiskStreamReadBuffer(in ReadParameters readParams, IDevice logDevice, DeviceIOCompletionCallback ioCompletionCallback)
        {
            this.readParams = readParams;
            this.logDevice = logDevice ?? throw new ArgumentNullException(nameof(logDevice));
            this.ioCompletionCallback = ioCompletionCallback ?? throw new ArgumentNullException(nameof(ioCompletionCallback));

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
        /// <remarks>
        /// The "fast path" for SpanByteAllocator goes through here with <see cref="ReadParameters.fixedPageSize"/> set to nonzero, checking for having
        /// a complete key for comparison and for having a complete record, allocating a full-size record if another read is necessary, in the same way as
        /// was previously done in <see cref="AllocatorBase{TStoreFunctions, TAllocator}.AsyncGetFromDiskCallback(uint, uint, object)"/>
        /// </remarks>
        public bool Read(ref SectorAlignedMemory recordBuffer, ReadOnlySpan<byte> requestedKey, out DiskLogRecord diskLogRecord)
        {
            currentLength = recordBuffer.available_bytes;
            var ptr = recordBuffer.GetValidPointer();
            diskLogRecord = default;

            // Check for RecordInfo and indicator byte. If we have that, check for key length; for disk IO we are called for a specific key
            // and can verify it here (unless NoKey, in which case key is empty and we assume we have the record we want; and for Scan(), the same).

            // In the vast majority of cases we will already have read at least one sector, which has all we need for length bytes, and maybe the full record.
            if (currentLength >= RecordInfo.GetLength() + 1 + 2) // + 1 for indicator byte + the minimum of 2 1-byte lengths for key and value
            {
                (var keyLengthBytes, var valueLengthBytes, isInChainedChunk) = DeconstructIndicatorByte(*(ptr + RecordInfo.GetLength()));
                recordInfo = *(RecordInfo*)ptr;
                if (recordInfo.Invalid) // includes IsNull
                    return false;
                optionalLength = LogRecord.GetOptionalLength(recordInfo);

                var offsetToKeyStart = RecordInfo.GetLength() + 1 + keyLengthBytes + valueLengthBytes;
                if (currentLength >= offsetToKeyStart)
                {
                    // We've checked whether the indicator byte says it is a chained chunk, but we need to recheck on the actual length retrieval
                    // as the last chunk (which may be the first one) will not have the continuation bit checked.
                    var keyLength = GetKeyLength(keyLengthBytes, ptr + RecordInfo.GetLength() + 1);
                    var valueLength = isInChainedChunk
                        ? GetChainedChunkValueLength(ptr + RecordInfo.GetLength() + 1 + keyLengthBytes, out isInChainedChunk)
                        : GetValueLength(valueLengthBytes, ptr + RecordInfo.GetLength() + 1 + keyLengthBytes);
                    valueDataStartPosition = offsetToKeyStart + keyLength;

                    if (currentLength >= offsetToKeyStart + keyLength)
                    {
                        // We have the full key in recordBuffer
                        var ptrToKeyData = ptr + offsetToKeyStart;

                        // We have the full key, so check for a match if we had a requested key, and return if not.
                        if (!requestedKey.IsEmpty && !readParams.storeFunctions.KeysEqual(requestedKey, new ReadOnlySpan<byte>(ptrToKeyData, keyLength)))
                            return false;

                        // If we have the full record, we're done.
                        var totalLength = valueDataStartPosition + valueLength + optionalLength;
                        if (currentLength >= totalLength)
                        {
                            currentPosition = offsetToKeyStart + keyLength;

                            // We need to set valueBuffer for Deserialize(), so transfer the recordBuffer to us, then to the output diskLogRecord.
                            valueBuffer = recordBuffer;
                            recordBuffer = default;
                            var valueObject = recordInfo.ValueIsObject ? DoDeserialize(out _, out _) : null;    // optionals are in the recordBuffer if present, so ignore the outparams for them
                            diskLogRecord = DiskLogRecord.Transfer(ref valueBuffer, offsetToKeyStart, keyLength, (int)valueLength, valueObject);
                            return true;
                        }

                        // We have the full key in recordBuffer (and maybe in requestedKey) but don't have the value or optionals. If we have a fixed-size page,
                        // just read it and return; we won't have overflow or objects.
                        if (readParams.fixedPageSize > 0)
                        {
                            Debug.Assert(totalLength <= readParams.fixedPageSize, $"totalLength {totalLength} cannot be greater than fixedPageSize {readParams.fixedPageSize}");
                            var fixedSizeRecordBuffer = AllocateBufferAndReadFromDevice(offsetToFieldStart: 0, (int)totalLength);
                            diskLogRecord = DiskLogRecord.Transfer(ref fixedSizeRecordBuffer, offsetToKeyStart, keyLength, (int)valueLength);
                            return true;
                        }

                        // Transfer ownership of recordBuffer to be our keyBuffer with appropriate ranges.
                        keyBuffer = recordBuffer;
                        recordBuffer = default;
                        keyBuffer.valid_offset += (int)(ptrToKeyData - ptr);    // So DiskLogRecord can retrieve GetValidPointer() as the start of the key...
                        keyBuffer.required_bytes = keyLength;                   // ... and this is the length of the key

                        // Read the rest of the record, possibly in pieces.
                        ReadValue(keyOverflow: default, valueLength, out diskLogRecord);
                        return true;
                    }

                    // We don't have the full key in the buffer so we can't compare here, but we do know the lengths, so read the full record and compare the key if we have it.
                    if (!ReadKeyAndValue(requestedKey, offsetToKeyStart, keyLength, valueLength, out diskLogRecord))
                        return false;

                    // We now have the full key, so check for a match if we had a requested key.
                    return requestedKey.IsEmpty || readParams.storeFunctions.KeysEqual(requestedKey, diskLogRecord.Key);
                }
            }

            // We do not have enough data to read the lengths so read a single sector into a new record buffer, then recursively call Read again (we are guaranteed
            // to have enough data on the next pass that we won't arrive here again, and we will have recordInfo and optionalLength).
            SectorAlignedMemory initialRecordBuffer = default;
            try
            {
                // Read() will clear initialRecordBuffer if it transfers it to the output diskLogRecord.
                initialRecordBuffer = AllocateBufferAndReadFromDevice(offsetToFieldStart: 0, IStreamBuffer.InitialIOSize);
                return Read(ref initialRecordBuffer, requestedKey, out diskLogRecord);
            }
            finally
            {
                initialRecordBuffer?.Return();
            }
        }

        /// <summary>Read the value (and optionals if present)</summary>
        private void ReadValue(OverflowByteArray keyOverflow, long valueLength, out DiskLogRecord diskLogRecord)
        {
            // The most common case is that we have the recordInfo and the key in either keyBuffer or keyOverflow.
            // This broken out into two functions to allow ReadKeyAndValue to wait on an event and then compare the key.
            BeginReadValue(valueLength, out var valueOverflow, out var eTag, out var expiration);
            diskLogRecord = EndReadValue(keyOverflow, valueOverflow, eTag, expiration);
            return;
        }

        private void BeginReadValue(long valueLength, out OverflowByteArray valueOverflow,
                out long eTag, out long expiration, CountdownEvent multiCountdownEvent = null)
        {
            // This is the most common initial case: we have the recordInfo and the key in either keyBuffer or keyOverflow.
            // If multiCountdownEvent is not null, then we're called from ReadKeyAndValue where the key has an IO pending in the countdownEvent; in that case
            // we must wait on it before initializing deserialization (it will not be signaled until both key and value have been read).
            var valueIsOverflow = !recordInfo.ValueIsObject && valueLength > readParams.maxInlineValueLength;
            valueBuffer = default;
            valueOverflow = default;
            eTag = expiration = 0;

            if (valueIsOverflow)
            {
                valueOverflow = AllocateOverflowAndReadFromDevice(valueDataStartPosition, (int)(valueLength + optionalLength), multiCountdownEvent);
                if (optionalLength > 0)
                    valueOverflow.ExtractOptionals(recordInfo, (int)valueLength, out eTag, out expiration);
                return;
            }
            
            if (!recordInfo.ValueIsObject)
            {
                valueBuffer = AllocateBufferAndReadFromDevice(valueDataStartPosition, (int)(valueLength + optionalLength), multiCountdownEvent);
                if (optionalLength > 0)
                    ExtractOptionals((int)valueLength, out eTag, out expiration);
                return;
            }

            // This is an object so we will read it into our separate deserialization buffer and then deserialize it from there (we retain ownership of this buffer).
            // Allocate the same size we used to write, plus an additional sector to ensure we have room for the optionals after the final chunk.
            currentPosition = valueDataStartPosition;
            valueCumulativeLength = 0;
            unreadChunkLength = valueLength;

            if (isInChainedChunk)
            {
                // Start the chained-chunk Read sequence. Don't read optionals here; they are read after the last (sub-)chunk, but we have to allocate enough buffer
                // to read them when we get there, so do the adjustment here instead of calling AllocateBufferAndReadFromDevice(). valueLength includes the next-chunk length.
                var (alignedReadStart, startPadding) = readParams.GetAlignedReadStart(valueDataStartPosition);
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
                    valueBuffer = AllocateBufferAndReadFromDevice(valueDataStartPosition, (int)(valueLength + optionalLength), multiCountdownEvent);
                    Debug.Assert(valueBuffer.required_bytes == (int)valueLength, $"Expected required_bytes {(int)valueLength}, actual {valueBuffer.required_bytes}");
                    currentLength = (int)valueLength;
                }
                else
                {
                    // Start the multi-sub-chunk Read sequence. Don't read optionals here, they are read after the last sub-chunk.
                    valueBuffer = AllocateBuffer(valueDataStartPosition, maxBufferSize, out var alignedReadStart, out var alignedBytesToRead);
                    ReadFromDevice(valueBuffer, alignedReadStart, alignedBytesToRead, multiCountdownEvent);
                    currentLength = valueBuffer.required_bytes = maxBufferSize;
                }
            }
            unreadChunkLength = unreadChunkLength > currentLength ? unreadChunkLength - currentLength : 0;

            // We have allocated a new buffer for just the value, so reset currentPosition. valueDataStartPosition must remain, as it is used as part of the file offset.
            currentPosition = 0;
        }

        private DiskLogRecord EndReadValue(OverflowByteArray keyOverflow, OverflowByteArray valueOverflow,
            long eTag, long expiration)
        {
            // Deserialize the object if there is one. This will also read any optionals if they are there.
            IHeapObject valueObject = null;
            if (recordInfo.ValueIsObject)
            {
                valueObject = DoDeserialize(out eTag, out expiration);
                valueBuffer = default;
            }

            // Transfer any non-null keyBuffer or valueBuffer to the DiskLogRecord. If valueObject is not null, Return() our valueBuffer first for immediate reuse.
            valueBuffer?.Return();
            valueBuffer = null;
            return new DiskLogRecord(recordInfo, ref keyBuffer, keyOverflow, ref valueBuffer, valueOverflow, eTag, expiration, valueObject);
        }

        /// <summary>Read the key and value (and optionals if present)</summary>
        private bool ReadKeyAndValue(ReadOnlySpan<byte> requestedKey, int offsetToKeyStart, int keyLength, long valueLength, out DiskLogRecord diskLogRecord)
        {
            Debug.Assert(valueDataStartPosition == offsetToKeyStart + keyLength, $"valueDataStartPosition {valueDataStartPosition} should == offsetToKeyStart ({offsetToKeyStart}) + keyLength ({keyLength})");

            // If the total length is small enough, just read the entire thing into a SectorAlignedMemory unless we have a value object that has a succeeding chunk.
            var totalLength = offsetToKeyStart + keyLength + valueLength + optionalLength;
            long eTag = 0, expiration = 0;
            if (totalLength < maxBufferSize && !isInChainedChunk)
            {
                valueBuffer?.Return();
                valueBuffer = default;
                valueBuffer = AllocateBufferAndReadFromDevice(offsetToFieldStart: 0, (int)totalLength);
                currentLength = valueBuffer.required_bytes;

                currentPosition = offsetToKeyStart + keyLength;
                var valueObject = recordInfo.ValueIsObject ? DoDeserialize(out _, out _) : null;    // optionals are in the recordBuffer if present, so ignore the outparams for them

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
                if (valueDataStartPosition - alignedKeyStart <= OverflowByteArray.MaxInternalOffset)
                {
                    // Yes it will fit. We'll start reading at alignedKeyStart, which will use offsetToKeyStart as the OverflowByteArray's start, and include optionals.
                    // From that we'll extract the key, then update the offset to be at the beginning of the actual value.
                    valueOverflow = AllocateOverflowAndReadFromDevice(offsetToKeyStart, (int)(keyLength + valueLength + optionalLength));

                    keyBuffer = readParams.bufferPool.Get(keyLength);
                    valueOverflow.ReadOnlySpan.Slice(0, keyLength).CopyTo(keyBuffer.TotalValidSpan);
                    valueOverflow.AdjustOffsetFromStart(keyLength);

                    if (optionalLength > 0)
                        valueOverflow.ExtractOptionals(recordInfo, (int)(keyLength + valueLength), out eTag, out expiration);

                    Debug.Assert(valueBuffer is null, "Should not have allocated a valueBuffer when Value is overflow");
                    diskLogRecord = new(recordInfo, ref keyBuffer, keyOverflow: default, ref valueBuffer, valueOverflow, eTag, expiration, valueObject: null);
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
                keyOverflow = AllocateOverflowAndReadFromDevice(offsetToKeyStart, (int)(keyLength + valueLength + optionalLength), countdownEvent);
            else
                keyBuffer = AllocateBufferAndReadFromDevice(offsetToKeyStart, keyLength, countdownEvent);

            // Initiate reading the value, which may be the initial buffer for a chunked object.
            BeginReadValue(valueLength, out valueOverflow, out eTag, out expiration, countdownEvent);

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
            diskLogRecord = EndReadValue(keyOverflow, valueOverflow, eTag, expiration);
            return true;
        }

        /// <summary>
        /// Read data from the device as an overflow allocation. This is because we may copy the record we've Read() to Tail or ReadCache, or the field
        /// may be larger than a single buffer (or even log page); in that case it was that size when it was created, so we're just restoring that.
        /// </summary>
        private OverflowByteArray AllocateOverflowAndReadFromDevice(int offsetToFieldStart, int unalignedBytesToRead, CountdownEvent multiCountdownEvent = null)
        {
            var (alignedOffset, startPadding) = readParams.GetAlignedReadStart(offsetToFieldStart);
            var (alignedBytesToRead, endPadding) = readParams.GetAlignedBytesToRead(unalignedBytesToRead + startPadding);
            var keyOverflow = new OverflowByteArray(alignedBytesToRead, startPadding, endPadding, zeroInit: false);
            fixed (byte* ptr = keyOverflow.AlignedReadSpan)
            {
                // If a CountdownEvent was passed in, we're part of a multi-IO operation; otherwise, just create one for a single IO and wait for it here.
                PageAsyncReadResult<Empty> result = new() { handle = multiCountdownEvent ?? new CountdownEvent(1) };
                logDevice.ReadAsync((ulong)alignedOffset, (IntPtr)ptr, (uint)alignedBytesToRead, ioCompletionCallback, result);
                if (multiCountdownEvent is null)
                {
                    result.handle.Wait();
                    if (result.numBytesRead != (uint)alignedBytesToRead)
                        throw new TsavoriteException($"Expected number of alignedBytesToRead {alignedBytesToRead}, actual {result.numBytesRead}");
                }
            }
            return keyOverflow;
        }

        private SectorAlignedMemory AllocateBufferAndReadFromDevice(int offsetToFieldStart, int unalignedBytesToRead, CountdownEvent multiCountdownEvent = null)
        {
            var recordBuffer = AllocateBuffer(offsetToFieldStart, unalignedBytesToRead, out var alignedReadStart, out var alignedBytesToRead);
            ReadFromDevice(recordBuffer, alignedReadStart, alignedBytesToRead, multiCountdownEvent);
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
            using var localCountdownEvent = multiCountdownEvent is null ? new CountdownEvent(1) : default;
            PageAsyncReadResult<Empty> result = new() { handle = multiCountdownEvent ?? localCountdownEvent };
            logDevice.ReadAsync((ulong)alignedReadStart, (IntPtr)recordBuffer.aligned_pointer, (uint)alignedBytesToRead, ioCompletionCallback, result);
            if (multiCountdownEvent is null)
            {
                result.handle.Wait();
                if (result.numBytesRead != (uint)alignedBytesToRead)
                    throw new TsavoriteException($"Expected number of alignedBytesToRead {alignedBytesToRead}, actual {result.numBytesRead}");
            }
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
                var offsetFromRecordStart = valueDataStartPosition + valueCumulativeLength + numBytesInChainedChunkLengths;

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

        IHeapObject DoDeserialize(out long eTag, out long expiration)
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
            OnDeserializeComplete(valueObject, out eTag, out expiration);
            return valueObject;
        }

        void OnDeserializeComplete(IHeapObject valueObject, out long eTag, out long expiration)
        {
            if (valueObject.SerializedSizeIsExact)
                Debug.Assert(valueObject.SerializedSize == valueCumulativeLength, $"valueObject.SerializedSize(Exact) {valueObject.SerializedSize} != valueCumulativeLength {valueCumulativeLength}");
            else
                valueObject.SerializedSize = valueCumulativeLength;

            // Extract optionals if they are present. This assumes currentPosition has been correctly set to the first byte after value data.
            ExtractOptionals(currentPosition, out eTag, out expiration);
        }

        private void ExtractOptionals(int offsetToOptionals, out long eTag, out long expiration)
        {
            var ptrToOptionals = valueBuffer.GetValidPointer() + offsetToOptionals;
            eTag = expiration = 0;
            if (recordInfo.HasETag)
            {
                eTag = *(long*)ptrToOptionals;
                offsetToOptionals += LogRecord.ETagSize;
            }
            if (recordInfo.HasExpiration)
                expiration = *(long*)(ptrToOptionals + offsetToOptionals);
        }

        /// <inheritdoc/>
        public void OnFlushComplete(DeviceIOCompletionCallback originalCallback, object originalContext) => throw new InvalidOperationException("OnFlushComplete is not supported for DiskStreamReadBuffer");

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
