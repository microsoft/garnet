// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;
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
    internal unsafe class DiskStreamWriteBuffer : IStreamBuffer
    {
        readonly IDevice logDevice;
        readonly IObjectSerializer<IHeapObject> valueObjectSerializer;

        /// <summary>The circular buffer we cycle through for parallelization of writes.</summary>
        internal CircularFlushWriteBuffer flushBuffers;

        /// <summary>The current buffer being written to in the circular buffer list.</summary>
        internal FlushWriteBuffer flushBuffer;

        /// <summary>Cumulative length of data written to the buffer before the current buffer contents</summary>
        long priorCumulativeLength;

        /// <summary>Device address to write to; incremented with each Write.</summary>
        ulong alignedDeviceAddress;

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
        /// <summary>The number of bytes in the intermediate chained-chunk length markers (4-byte int each). We don't want to consider this part of
        /// <see cref="valueCumulativeLength"/> but we need to include it in the SerializedSize calculation, which is necessary for patching addresses.</summary>
        int numBytesInChainedChunkLengths;

        /// <summary>For object serialization via chained chunks, the start of the key-end cap used for sector alignment when writing the key directly to the device.
        /// May be zero if the key length happens to coincide with the end of a sector.</summary>
        int keyEndSectorCapPosition;
        /// <summary>When writing the key directly to the device, this is the interior span of the key (between the two sector-alignment caps) to be written after the first
        /// object chunk is serialized to the buffer (this two-step approach is needed so we know the chunk length).</summary>
        PinnedSpanByte keyInteriorSpan;

        /// <summary>The amount of bytes we wrote. Used by the caller (e.g. <see cref="ObjectAllocatorImpl{TStoreFunctions}"/> to track actual file output length.</summary>
        /// <remarks><see cref="FlushWriteBuffer.currentPosition"/> is the current length, since it is *past* the last byte copied to the buffer. Subtract the intermediate chain-chunk length markers.</remarks>
        public long TotalWrittenLength => priorCumulativeLength + flushBuffer.currentPosition;

        /// <summary>The total capacity of the buffer.</summary>
        public bool IsForWrite => true;

        int SectorSize => flushBuffers.SectorSize;

        /// <summary>Constructor. Creates the circular buffer pool.</summary>
        /// <param name="logDevice">The device to write to</param>
        /// <param name="logger">The logger to write to</param>
        /// <param name="bufferPool">The pool for creating memory buffers</param>
        /// <param name="bufferSize">The size to use for memory buffers</param>
        /// <param name="valueObjectSerializer">Serialized value objects to the underlying stream</param>
        /// <param name="externalCallback">Callback sent to the initial Flush() command. Called when we are done with this flush operation. 
        ///     It usually signals the <see cref="PageAsyncFlushResult{T}.done"/> event so the caller knows the flush is complete and it can continue.</param>
        /// <param name="externalContext">Context sent to <paramref name="externalCallback"/>.</param>
        /// <exception cref="ArgumentNullException"></exception>
        public DiskStreamWriteBuffer(IDevice logDevice, ILogger logger, SectorAlignedBufferPool bufferPool, int bufferSize, IObjectSerializer<IHeapObject> valueObjectSerializer, DeviceIOCompletionCallback externalCallback, object externalContext)
        {
            this.logDevice = logDevice ?? throw new ArgumentNullException(nameof(logDevice));
            this.valueObjectSerializer = valueObjectSerializer;

            flushBuffers = new CircularFlushWriteBuffer(bufferPool, bufferSize, logDevice, logger, externalCallback, externalContext);
            flushBuffer = flushBuffers.GetAndInitializeCurrentBuffer();

            valueLengthPosition = NoPosition;
        }

        internal void SetAlignedDeviceAddress(ulong alignedDeviceAddress) => this.alignedDeviceAddress = alignedDeviceAddress;

        internal void OnInitialSectorReadComplete(int position) => flushBuffer.currentPosition = position;

        /// <inheritdoc/>
        public void FlushAndReset(CancellationToken cancellationToken = default) => throw new InvalidOperationException("Flushing must only be done under control of the Write() methods, due to possible Value length adjustments.");

        /// <inheritdoc/>
        public void Write(in LogRecord logRecord, long diskTailOffset)
        {
            // Initialize to not track the value length position (not serializing an object, or we know its exact length up-front so are not doing chunk chaining)
            valueLengthPosition = NoPosition;
            keyEndSectorCapPosition = NoPosition;
            expectedSerializedLength = 0;

            // Everything writes the RecordInfo first. Update to on-disk address if it's not done already (the OnPagesClosed thread may have already done it).
            var tempInfo = logRecord.Info;
            if (!IsOnDisk(tempInfo.PreviousAddress) && tempInfo.PreviousAddress > kTempInvalidAddress)
                tempInfo.PreviousAddress = SetIsOnDisk(tempInfo.PreviousAddress + diskTailOffset);
            Write(new ReadOnlySpan<byte>(&tempInfo, RecordInfo.GetLength()));

            // If the record is inline, we can just write it directly; the indicator bytes will be correct.
            if (logRecord.Info.RecordIsInline)
            {
                Write(logRecord.AsReadOnlySpan().Slice(RecordInfo.GetLength()));
                OnRecordComplete();
                return;
            }

            // The in-memory record is not inline, so we must form the indicator bytes for the inline disk image (expanding Overflow and Object).
            var keySpan = logRecord.Key;

            if (!logRecord.Info.ValueIsObject)
            {
                // We know the exact values for the varbyte key and value lengths.
                var valueSpan = logRecord.ValueSpan;
                WriteIndicatorByteAndLengths(keySpan.Length, valueSpan.Length);
                Write(logRecord.Key);   // TODO: optimize write of large keys directly from the Span, as is done when we have chained-chunk values below
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
                WriteIndicatorByteAndLengths(keySpan.Length, 0);
                Write(logRecord.Key);   // TODO: optimize write of large keys directly from the Span, as is done when we have chained-chunk values below
                WriteOptionals(in logRecord);
                OnRecordComplete();
                return;
            }

            // 2. If ValueObject.SerializedSizeIsExact, then write varbytes with the known value length, the key, serialize the value (without tracking valueLengthPosition), write optionals, done.
            if (valueObject.SerializedSizeIsExact)
            {
                // We can write the exact value length, so we will write the indicator byte with the key length and value length, then write the key and value spans.
                WriteIndicatorByteAndLengths(keySpan.Length, valueObject.SerializedSize);
                Write(logRecord.Key);   // TODO: optimize write of large keys directly from the Span, as is done when we have chained-chunk values below

                // Serialize the value object into possibly multiple buffers, but we only need the one length we've just filled in. valueStartPosition is already set to NoPosition.
                expectedSerializedLength = valueObject.SerializedSize;
                DoSerialize(valueObject);
                WriteOptionals(in logRecord);
                OnRecordComplete();
                return;
            }

            // 3. We can't trust valueObject.SerializedSize here because the object cannot accurately track it (e.g. JsonObject), so we must chain chunks.
            // Ensure there is space in the to fit a key (usually not too large) and a reasonably-sized first ValueObject serialized chunk and still be able to update Value length.
            if (flushBuffer.RemainingCapacity < flushBuffer.memory.AlignedTotalCapacity / 4)
                FlushPartialBufferAndShift();

            // Initiate chunking. ValueLength will always be an int. There are two possibilities regarding handling of the first chunk:
            //  a. The key can be copied to the buffer in its entirety, so we only need to track the amount of key data between the value length and start of value bytes
            //     so we can calculate the actual value length for the chunk.
            //  b. The key is too large to fit in the buffer (or at least as much space therein as we we want to copy), so we copy the sector-aligning "caps" from the
            //     start and end of the key to the buffer and track these so we know how much to write for:
            //     - the initial flush (including the "key start cap")
            //     - the slice of the key that is to be directly written (saved as a PinnedSpanByte)
            //     - the length of the "key end cap":
            //       - this length combines with the "start cap" length to determine the offset from valueLengthPosition to the start of the value bytes
            //       - the start of the "key end cap" is the starting position of the flush of the end of the buffer
            //  After the first chunk, all we need to track is the valueLengthPosition, which we update as we write each chunk (or the final partial buffer).

            // 3a. If the key is "small", we'll just copy it to the buffer in its entirety, then track valueLengthPosition while we do ValueObject serialization.
            if (keySpan.Length < flushBuffer.memory.AlignedTotalCapacity / 8)
            {
                // Max value chunk size is limited to a single buffer so fits in an int. Use int.MaxValue to get the int varbyte size, but we won't write a chunk that large.
                WriteIndicatorByteAndLengths(keySpan.Length, int.MaxValue, isChunked: true);    // valueLengthPosition is set because isChunked is true.
                Write(keySpan);

                // Serialize the value object into possibly multiple buffers; we will manage chunk updating in Write(ReadOnlySpan<byte> data).
                DoSerialize(valueObject);
                WriteOptionals(in logRecord);
                OnRecordComplete();
                return;
            }

            // 3b. The key is large (which should be quite rare), so we can't just copy it to the buffer. Because we are chaining chunks here we can't write the start of the
            // key until we have written the first value chunk, because we have to be able to update the value length. We'll just copy the necessary sector-aligning byte "caps"
            // at its start and end to the buffer and write the sector-aligned interior portion of the key directly. Because this is chunked, use int.MaxValue as above.
            // TODO test with large keys, e.g. 10MB key, with and without sector alignment.
            WriteIndicatorByteAndLengths(keySpan.Length, int.MaxValue, isChunked: true);    // valueLengthPosition is set because isChunked is true.

            // Copy the beginning sector-aligning "cap" bytes to the buffer.
            var beginCapLength = RoundUp(flushBuffer.currentPosition, SectorSize) - flushBuffer.currentPosition;
            var capSpan = keySpan.Slice(0, beginCapLength);
            if (capSpan.Length > 0)
                Write(capSpan);

            // Hold onto the span of the interior bytes; we must serialize the first object chunk before we can write the value length and key bytes.
            var interiorLength = RoundDown(keySpan.Length - beginCapLength, SectorSize);
            keyInteriorSpan = PinnedSpanByte.FromPinnedSpan(keySpan.Slice(beginCapLength, interiorLength));

            // Copy the ending sector-aligning "cap" bytes to the buffer
            keyEndSectorCapPosition = flushBuffer.currentPosition;
            capSpan = keySpan.Slice(beginCapLength + interiorLength);
            if (capSpan.Length > 0)
                Write(capSpan);

            // Serialize the value object into possibly multiple buffers.
            DoSerialize(valueObject);
            WriteOptionals(in logRecord);
            OnRecordComplete();
        }

        private void WriteIndicatorByteAndLengths(int keyLength, long valueLength, bool isChunked = false)
        {
            // Use a local buffer so we can call Write() just once. We are maxed at one byte indicator, 3 bytes keylen, and 8 bytes valuelen.
            var indicatorBuffer = stackalloc byte[sizeof(long) * 2];
            var indicatorByte = ConstructIndicatorByte(keyLength, valueLength, out var keyByteCount, out var valueByteCount);
            if (isChunked)
            {
                SetChunkedValueIndicator(ref indicatorByte);
                valueLengthPosition = flushBuffer.currentPosition + 1 + keyByteCount;   // skip indicator byte and key length varbytes
            }

            var ptr = indicatorBuffer;
            *ptr++ = indicatorByte;
            WriteVarbyteLength(keyLength, keyByteCount, ptr);
            ptr += keyByteCount;
            WriteVarbyteLength(valueLength, valueByteCount, ptr);
            ptr += valueByteCount;
            Write(new ReadOnlySpan<byte>(indicatorBuffer, (int)(ptr - indicatorBuffer)));
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
            // This is called by valueObjectSerializer.Serialize().

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
                Debug.Assert(flushBuffer.RemainingCapacity - lengthSpaceReserve > 0, 
                        $"RemainingCapacity {flushBuffer.RemainingCapacity} - lengthSpaceReserve {lengthSpaceReserve} == 0 (data.Length {data.Length}, dataStart {dataStart}) should have already triggered an OnChunkComplete call, which would have reset the buffer");
                cancellationToken.ThrowIfCancellationRequested();   // IDevice does not support cancellation, so just check this here

                // If it won't all fit in the remaining buffer, write as much as will.
                var requestLength = data.Length - dataStart;
                if (requestLength > flushBuffer.RemainingCapacity - lengthSpaceReserve)
                    requestLength = flushBuffer.RemainingCapacity - lengthSpaceReserve;

                data.Slice(dataStart, requestLength).CopyTo(flushBuffer.memory.TotalValidSpan.Slice(flushBuffer.currentPosition));
                dataStart += requestLength;
                flushBuffer.currentPosition += requestLength;
                currentChunkLength += requestLength;    // This is ignored if we are not in the middle of Serialize() (DoSerialize() resets it to 0)

                // See if we're at the end of the buffer.
                if (flushBuffer.RemainingCapacity - lengthSpaceReserve == 0)
                    OnChunkComplete(lengthSpaceReserve);
            }
        }

        /// <summary>
        /// At the end of a chunk, update value length of the chunk and do any deferred key processing that may still be needed for the first chunk.
        /// Called both during Serialize() and after Serialize() completes.
        /// </summary>
        void OnChunkComplete(int lengthSpaceReserve)
        {
            // This should only be called when the object serialization hits the end of the buffer; for partial buffers we will call
            // OnSerializeComplete() after the Serialize() call has returned.
            Debug.Assert(flushBuffer.currentPosition == flushBuffer.memory.AlignedTotalCapacity - lengthSpaceReserve, "Current position must be at end of buffer.");
            UpdateValueLength(IStreamBuffer.ValueChunkContinuationBit);

            // If this is the first chunk we may need to flush the key interior, which also flushes the rest of the buffer. Otherwise just flush the buffer.
            if (!FlushKeyInteriorIfNeeded())
                FlushPartialBufferAndShift();

            // If we're doing chunk chaining, the next chunk's length starts at the end of the sector we moved to the start of the buffer.
            // Note that if serialization ends right on the end of the sector, we'll call OnSerializeComplete which will set this to zero with
            // no continuation bit; that's fine, just a wasted int we can't avoid.
            if (valueLengthPosition != NoPosition)
            {
                valueLengthPosition = flushBuffer.currentPosition;
                flushBuffer.currentPosition += sizeof(int);
                numBytesInChainedChunkLengths += sizeof(int);
            }
            Debug.Assert(flushBuffer.RemainingCapacity - lengthSpaceReserve > 0,
                    $"RemainingCapacity {flushBuffer.RemainingCapacity} - lengthSpaceReserve {lengthSpaceReserve} == 0 at end of OnChunkComplete");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void UpdateValueLength(int continuationBit)
        {
            // If we are not chaining chunks, we already wrote the SerializedSizeIsExact length, so there is nothing to do here.
            if (valueLengthPosition != NoPosition)
            {
                // This may set the value length to 0 if we didn't have any value data in this chunk (e.g. the value ended on the prior chunk boundary).
                // Note that currentChunkLength is at the end of the current chunk's data and the next-chunk length is *after* that, so add sizeof(int)
                // if there is a continuation bit.
                var lengthSpaceReserve = continuationBit == 0 ? 0 : sizeof(int);
                *(int*)(flushBuffer.memory.GetValidPointer() + valueLengthPosition) = (currentChunkLength + lengthSpaceReserve) | continuationBit;
            }
            valueCumulativeLength += currentChunkLength;
            currentChunkLength = 0;
        }

        /// <summary>
        /// If we had a large key we may have copied only the start and end sector-alignment "caps" to the buffer, and write the large interior key data
        /// directly to the device: write start of record up to key start-alignment cap, 
        /// </summary>
        /// <returns></returns>
        bool FlushKeyInteriorIfNeeded()
        {
            if (keyEndSectorCapPosition == NoPosition)
                return false;

            // Caller has updated the value length, so write everything including the key start cap for sector alignment
            // (this ends on keyEndSectorCapPosition). Then write the key span, which is sector-aligned size.
            FlushPartialBuffer(0, keyEndSectorCapPosition);

            // When writing to a device we must have a pinned buffer. This means we have to wait until the write completes. We should only be
            // here for extremely large keys which we hope are extremely unusual.
            FlushToDevice(keyInteriorSpan, wait: true);

            // Write the end cap of the key and the value; keyEndSectorCapPosition is sector-aligned so flushLength will be also.
            // This is basically FlushPartialBufferAndShift with a start position > 0.
            var flushLength = RoundDown(flushBuffer.currentPosition, SectorSize) - keyEndSectorCapPosition;
            Debug.Assert(RoundUp(flushLength, SectorSize) == flushLength, $"flushLength {flushLength} should be sector-aligned and was not");
            FlushPartialBuffer(keyEndSectorCapPosition, keyEndSectorCapPosition + flushLength);
            flushBuffer = flushBuffers.ShiftTailToNextBuffer(keyEndSectorCapPosition + flushLength);

            // We no longer want these; remaining chunks will just Flush as below.
            keyEndSectorCapPosition = NoPosition;
            keyInteriorSpan = default;
            return true;
        }

        /// <summary>Flush the buffer up to sector alignment below <see cref="FlushWriteBuffer.currentPosition"/>,
        /// then copy any remaining data to the start of the next buffer and set <see cref="flushBuffer"/> to that next buffer.</summary>
        private void FlushPartialBufferAndShift()
        {
            if (flushBuffer.currentPosition == 0)
                return; // Nothing to flush and a full buffer available

            var flushLength = RoundDown(flushBuffer.currentPosition, SectorSize);
            FlushToDevice(flushBuffer.memory.TotalValidSpan.Slice(0, flushLength));
            flushBuffer = flushBuffers.ShiftTailToNextBuffer(flushLength);
        }

        /// <summary>Flush the buffer from <paramref name="startPosition"/> to <paramref name="endPosition"/>; both should be sector-aligned.
        /// Do not shift the buffer or update <see cref="FlushWriteBuffer.currentPosition"/>; subsequent operations will handle that</summary>
        private void FlushPartialBuffer(int startPosition, int endPosition)
        {
            if (endPosition - startPosition == 0)
                return; // Nothing to flush

            var flushLength = endPosition - startPosition;
            Debug.Assert(RoundUp(startPosition, SectorSize) == startPosition, $"startPosition {startPosition} should be sector-aligned and was not");
            Debug.Assert(RoundUp(endPosition, SectorSize) == endPosition, $"endPosition {endPosition} should be sector-aligned and was not");
            Debug.Assert(RoundUp(flushLength, SectorSize) == flushLength, $"flushLength {flushLength} should be sector-aligned and was not");

            FlushToDevice(flushBuffer.memory.TotalValidSpan.Slice(startPosition, flushLength));
        }

        /// <summary>Write the span directly to the device without changing the buffer.</summary>
        private void FlushToDevice(ReadOnlySpan<byte> span, bool wait = false)
        {
            Debug.Assert(RoundUp(span.Length, SectorSize) == span.Length, $"span.Length {span.Length} should be sector-aligned and was not");
            flushBuffer.FlushToDevice(span, alignedDeviceAddress, wait);

            // This does not alter currentPosition; that is the caller's responsibility, e.g. it may ShiftTailToNextBuffer if this is called for partial flushes.
            // Note: When we trigger a flush for a flushBuffer, it will either be a series of partial flushes followed by a ShiftTailToNextBuffer, or a
            // single flush. Either way, we will then leave the buffer we flushed and move to the next one. I.e., we will never start a flush in a buffer
            // and then start adding more into that buffer. This saves us having to track a lastFlushedPosition for the chunk; the only time we would enqueue
            // multiple flushes currently is for keyInterior which should be quite rare.
            priorCumulativeLength += span.Length;
            alignedDeviceAddress += (ulong)span.Length;
        }

        /// <inheritdoc/>
        public void OnFlushComplete(DeviceIOCompletionCallback originalCallback, object originalContext)
        {
            // TODO: TotalWrittenLength may exceed uint.MaxValue so the callback's numBytes will be incorrect.
            flushBuffers.BeginFlushComplete(alignedDeviceAddress, (uint)TotalWrittenLength);

            priorCumulativeLength = 0;
            valueCumulativeLength = 0;
            alignedDeviceAddress = 0;
        }

        void DoSerialize(IHeapObject valueObject)
        {
            // valueCumulativeLength is only relevant for object serialization; we increment it on all device writes to avoid "if", so here we reset it to the appropriate
            // "start at 0" by making it the negative of currentPosition. Subsequently if we write e.g. an int, we'll have Length and Position = (-currentPosition + currentPosition + 4).
            valueCumulativeLength = 0;
            currentChunkLength = 0;
            valueObjectSerializer.Serialize(valueObject);
            OnSerializeComplete(valueObject);
        }

        void OnSerializeComplete(IHeapObject valueObject)
        {
            // Update value length with the continuation bit NOT set. This may set it to zero if we did not have any more data in the object after the last buffer flush.
            UpdateValueLength(IStreamBuffer.NoValueChunkContinuationBit);

            if (valueLengthPosition == NoPosition)
            {
                if (valueCumulativeLength != expectedSerializedLength)
                    throw new TsavoriteException($"Expected value length {expectedSerializedLength} does not match actual value length {valueCumulativeLength}.");
            }
            else
            {
                valueObject.SerializedSize = valueCumulativeLength + numBytesInChainedChunkLengths;
                numBytesInChainedChunkLengths = 0;
            }

            // Check to write the key interior in case we had no chunks.
            // We don't care if this has a key interior or not so ignore the return value; we'll leave currentPosition where it is anyway.
            _ = FlushKeyInteriorIfNeeded();
        }

        /// <summary>Called when a <see cref="LogRecord"/> Write is completed. Ensures end-of-record alignment.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnRecordComplete()
        {
            flushBuffer = flushBuffers.OnRecordComplete(alignedDeviceAddress, out var numBytesFlushed);
            priorCumulativeLength += numBytesFlushed;
            alignedDeviceAddress += (ulong)numBytesFlushed;
            Debug.Assert(TotalWrittenLength % Constants.kRecordAlignment == 0, $"TotalWrittenLength {TotalWrittenLength} is not record-aligned");
        }

        /// <inheritdoc/>
        public int Read(Span<byte> destinationSpan, CancellationToken cancellationToken = default) => throw new InvalidOperationException("Read is not supported for DiskStreamWriteBuffer");

        /// <inheritdoc/>
        public void Dispose()
        {
            // Currently nothing to do
        }
    }
}