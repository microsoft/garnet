// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static VarbyteLengthUtility;
    using static LogAddress;

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
        readonly SectorAlignedMemory buffer;
        readonly IObjectSerializer<IHeapObject> valueObjectSerializer;

        /// <summary>Current write position (we do not support read in this buffer). This class only supports Write and no Seek,
        /// so currentPosition equals the current length. Relevant for object serialization only; reset to 0 at start of DoSerialize().</summary>
        /// <remarks><see cref="currentPosition"/> is the current length, since it is *past* the last byte copied to the buffer.</remarks>
        int currentPosition;
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
        /// <summary>For object serialization via chained chunks, the start of the key-end cap used for sector alignment when writing the key directly to the device.
        /// May be zero if the key length happens to coincide with the end of a sector.</summary>
        int keyEndSectorCapPosition;
        /// <summary>For object serialization, the position of the start of the value bytes.</summary>
        int valueDataStartPosition;
        /// <summary>For object serialization, the cumulative length of the value bytes.</summary>
        int valueCumulativeLength;
        /// <summary>The number of bytes in the intermediate chained-chunk length markers (4-byte int each). We don't want to consider this part of
        /// <see cref="valueCumulativeLength"/> but we need to include it in the SerializedSize calculation, which is necessary for patching addresses.</summary>
        int numBytesInChainedChunkLengths;
        /// <summary>When writing the key directly to the device, this is the interior span of the key (between the two sector-alignment caps) to be written after the first
        /// object chunk is serialized to the buffer (this two-step approach is needed so we know the chunk length).</summary>
        PinnedSpanByte keyInteriorSpan;

        /// <summary>
        /// When we write the buffer this callback will be used and must signal the <see cref="PageAsyncFlushResult{T}.done"/> event so we can continue.
        /// </summary>
        readonly DeviceIOCompletionCallback ioCompletionCallback;

        /// <summary>The Span of data written to the buffer so far (from Read() or Write()).</summary>
        public Span<byte> BufferSpan => buffer.Span;

        internal byte* BufferPointer => buffer.GetValidPointer();

        private int RemainingCapacity => buffer.AlignedTotalCapacity - currentPosition;

        /// <summary>The Span of data in the buffer past the <see cref="currentPosition"/> position.</summary>
        public Span<byte> RemainingSpan => buffer.Span.Slice(currentPosition);

        /// <summary>The amount of data written to the buffer so far (from Read() or Write()). Used for object serialization only.</summary>
        /// <remarks><see cref="currentPosition"/> is the current length, since it is *past* the last byte copied to the buffer. Subtract the intermediate chain-chunk length markers.</remarks>
        public long Length => valueCumulativeLength + currentPosition;

        /// <summary>The current position the buffer so far, including cumulative lengths of previous buffers (before FlushAndReset). Used for object serialization only.</summary>
        /// <remarks><see cref="currentPosition"/> is the current length, since it is *past* the last byte copied to the buffer. Subtract the intermediate chain-chunk length markers.</remarks>
        public long Position => valueCumulativeLength + currentPosition;

        /// <summary>The amount of bytes we wrote. Used by the caller (e.g. <see cref="ObjectAllocatorImpl{TStoreFunctions}"/> to track actual file output length.</summary>
        /// <remarks><see cref="currentPosition"/> is the current length, since it is *past* the last byte copied to the buffer. Subtract the intermediate chain-chunk length markers.</remarks>
        public long TotalWrittenLength => priorCumulativeLength + currentPosition;

        /// <summary>The total capacity of the buffer.</summary>
        public bool IsForWrite => true;

        int SectorSize => (int)logDevice.SectorSize;

        public DiskStreamWriteBuffer(IDevice logDevice, SectorAlignedMemory buffer, IObjectSerializer<IHeapObject> valueObjectSerializer, DeviceIOCompletionCallback ioCompletionCallback)
        {
            this.logDevice = logDevice ?? throw new ArgumentNullException(nameof(logDevice));
            this.buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
            this.ioCompletionCallback = ioCompletionCallback ?? throw new ArgumentNullException(nameof(ioCompletionCallback));
            this.valueObjectSerializer = valueObjectSerializer;
            valueLengthPosition = NoPosition;
        }

        internal void SetAlignedDeviceAddress(ulong alignedDeviceAddress) => this.alignedDeviceAddress = alignedDeviceAddress;

        internal void OnInitialSectorReadComplete(int position) => currentPosition = position;

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
            if (RemainingCapacity < buffer.AlignedTotalCapacity / 4)
                FlushPartialBuffer();

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
            if (keySpan.Length < buffer.AlignedTotalCapacity / 8)
            {
                // Max value chunk size is limited to a single buffer so fits in an int. Use int.MaxValue to get the int varbyte size, but we won't write a chunk that large.
                WriteIndicatorByteAndLengths(keySpan.Length, int.MaxValue, isChunked: true);    // valueLengthPosition is set because isChunked is true.
                keySpan.CopyTo(RemainingSpan);
                currentPosition += keySpan.Length;

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
            var beginCapLength = RoundUp(currentPosition, SectorSize) - currentPosition;
            var capSpan = keySpan.Slice(0, beginCapLength);
            if (capSpan.Length > 0)
                capSpan.CopyTo(RemainingSpan);
            currentPosition += beginCapLength;

            // Hold onto the span of the interior bytes; we must serialize the first object chunk before we can write the value length and key bytes.
            var interiorLength = RoundDown(keySpan.Length - beginCapLength, SectorSize);
            keyInteriorSpan = PinnedSpanByte.FromPinnedSpan(keySpan.Slice(beginCapLength, interiorLength));

            // Copy the ending sector-aligning "cap" bytes to the buffer
            keyEndSectorCapPosition = currentPosition;
            capSpan = keySpan.Slice(beginCapLength + interiorLength);
            if (capSpan.Length > 0)
                capSpan.CopyTo(buffer.Span);
            currentPosition += capSpan.Length;
            
            // Serialize the value object into possibly multiple buffers.
            DoSerialize(valueObject);
            WriteOptionals(in logRecord);
            OnRecordComplete();
        }

        private void WriteIndicatorByteAndLengths(int keyLength, long valueLength, bool isChunked = false)
        {
            var indicatorByte = ConstructIndicatorByte(keyLength, valueLength, out var keyByteCount, out var valueByteCount);
            if (isChunked)
            {
                SetChunkedValueIndicator(ref indicatorByte);
                valueLengthPosition = currentPosition + 1 + keyByteCount;   // skip indicator byte and key length varbytes
            }

            var ptr = BufferPointer + currentPosition;
            *ptr++ = indicatorByte;
            WriteVarbyteLength(keyLength, keyByteCount, ptr);
            ptr += keyByteCount;
            WriteVarbyteLength(valueLength, valueByteCount, ptr);
            ptr += valueByteCount;
            currentPosition = (int)(ptr - BufferPointer);
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
            // This is called by valueObjectSerializer.Serialize(). TODO: handle cancellationToken; can IDevice support cancellation?

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
                Debug.Assert(RemainingCapacity - lengthSpaceReserve > 0, "RemainingCapacity  - lengthSpaceReserve == 0 should have already triggered an OnChunkComplete call, which would have reset the buffer");

                // If it won't all fit in the remaining buffer, write as much as will.
                var chunkLength = data.Length - dataStart;
                if (chunkLength > RemainingCapacity - lengthSpaceReserve)
                    chunkLength = RemainingCapacity - lengthSpaceReserve;

                var chunk = data.Slice(dataStart, chunkLength);
                chunk.CopyTo(RemainingSpan);
                dataStart += chunk.Length;
                currentPosition += chunk.Length;

                // See if we're at the end of the buffer.
                if (RemainingCapacity - lengthSpaceReserve == 0)
                    OnChunkComplete(lengthSpaceReserve);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void UpdateValueLength(long continuationBit)
        {
            // If we are not chunking, we already wrote the SerializedSizeIsExact length, so there is nothing to do here.
            if (valueLengthPosition == NoPosition)
                return;

            // This may set the value length to 0 if we didn't have any value data in this chunk (e.g. the value ended on the prior chunk boundary).
            var chunkLength = currentPosition - valueDataStartPosition;
            *(long*)(BufferPointer + valueLengthPosition) = (long)chunkLength | continuationBit;
        }

        /// <summary>
        /// At the end of a chunk, update value length of the chunk and do any deferred key processing that may still be needed.
        /// Called both during Serialize() and after Serialize() completes.
        /// </summary>
        void OnChunkComplete(int lengthSpaceReserve)
        {
            // This should only be called when the object serialization hits the end of the buffer; for partial buffers we will call
            // OnSerializeComplete() after the Serialize() call has returned.
            Debug.Assert(currentPosition == buffer.AlignedTotalCapacity - lengthSpaceReserve, "Current position must be at end of buffer.");

            // If we are chaining chunks, update value length with the continuation bit set.
            UpdateValueLength(IStreamBuffer.ValueChunkContinuationBit);

            // Either flush the key interior, which also flushes the rest of the buffer, or just flush the buffer if we don't have a key interior span.
            if (!FlushKeyInteriorIfNeeded())
                FlushPartialBuffer();

            // If we're doing chunk chaining, the next chunk's length starts at the end of the sector we moved to the start of the buffer.
            // Note that if serialization ends right on the end of the sector, we'll call OnSerializeComplete which will set this to zero with
            // no continuation bit; that's fine, just a wasted int we can't avoid.
            if (valueLengthPosition != NoPosition)
            {
                valueLengthPosition = currentPosition - sizeof(int);
                valueDataStartPosition = currentPosition;
                
                // We don't want Position or Length to include the bytes used by the intermediate chained-chunk length markers but we need them
                // for SerializedSize tracking so track them separately.
                valueCumulativeLength -= sizeof(int);
                numBytesInChainedChunkLengths += sizeof(int);
            }
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
            FlushToDevice(keyInteriorSpan);

            // Write the end cap of the key and the value; keyEndSectorCapPosition is sector-aligned so flushLength will be also.
            var flushLength = RoundDown(currentPosition, SectorSize);
            Debug.Assert(RoundUp(flushLength, SectorSize) == flushLength, $"flushLength {flushLength} should be sector-aligned and was not");
            FlushPartialBuffer(keyEndSectorCapPosition, currentPosition);
            CopyRemainingDataToStartOfBuffer(keyEndSectorCapPosition + flushLength);

            // We no longer want these; remaining chunks will just Flush as below.
            keyEndSectorCapPosition = NoPosition;
            keyInteriorSpan = default;
            return true;
        }

        private void CopyRemainingDataToStartOfBuffer(int startPosition)
        {
            var copyLength = currentPosition - startPosition;
            if (copyLength > 0)
                BufferSpan.Slice(startPosition, copyLength).CopyTo(buffer.Span);
            currentPosition = copyLength;
        }

        /// <summary>Flush the buffer up to sector alignment below <see cref="currentPosition"/>,
        /// then copy any remaining data to the start of the buffer and update <see cref="currentPosition"/></summary>
        private void FlushPartialBuffer()
        {
            if (currentPosition == 0)
                return; // Nothing to flush

            var flushLength = RoundDown(currentPosition, SectorSize);
            FlushToDevice(BufferSpan.Slice(0, flushLength));
            CopyRemainingDataToStartOfBuffer(flushLength);
        }

        /// <summary>Flush the buffer from <paramref name="startPosition"/> to <paramref name="endPosition"/>; both should be sector-aligned.
        /// Do not shift the buffer or update <see cref="currentPosition"/>; subsequent operations will handle that</summary>
        private void FlushPartialBuffer(int startPosition, int endPosition)
        {
            if (endPosition - startPosition == 0)
                return; // Nothing to flush

            var flushLength = endPosition - startPosition;
            Debug.Assert(RoundUp(startPosition, SectorSize) == startPosition, $"startPosition {startPosition} should be sector-aligned and was not");
            Debug.Assert(RoundUp(endPosition, SectorSize) == endPosition, $"endPosition {endPosition} should be sector-aligned and was not");
            Debug.Assert(RoundUp(flushLength, SectorSize) == flushLength, $"flushLength {flushLength} should be sector-aligned and was not");

            FlushToDevice(BufferSpan.Slice(startPosition, flushLength));
        }

        /// <summary>Write the span directly to the device without changing the buffer.</summary>
        private void FlushToDevice(ReadOnlySpan<byte> span)
        {
            Debug.Assert(RoundUp(span.Length, SectorSize) == span.Length, $"span.Length {span.Length} should be sector-aligned and was not");
            PageAsyncFlushResult<Empty> asyncResult = new() { done = new AutoResetEvent(false) };

            // We may be writing a ReadOnlySpan that's around an unpinned byte[], so pin the buffer to do the write.
            fixed (byte* spanPtr = span)
                logDevice.WriteAsync((IntPtr)spanPtr, alignedDeviceAddress, (uint)span.Length, ioCompletionCallback, asyncResult);
            _ = asyncResult.done.WaitOne();
            asyncResult.done = default;
            priorCumulativeLength += span.Length;
            valueCumulativeLength += span.Length;
            alignedDeviceAddress += (ulong)span.Length;
            // This does not alter currentPosition; that is the caller's responsibility, as this is called for partial flushes.
        }

        /// <inheritdoc/>
        public void OnFlushComplete(DeviceIOCompletionCallback originalCallback, object originalContext)
        {
            // Flush the last of the buffer, sector-aligning and zeroing the end. (All chunk-length updates have been done if we were chunk-chaining).
            if (currentPosition == 0)
            {
                // Nothing to flush. TODO: TotalWrittenLength may exceed uint.MaxValue so the callback's numBytes will be incorrect.
                originalCallback(errorCode: 0, (uint)TotalWrittenLength, originalContext);
                return;
            }

            // Flush the final piece to disk, using the caller's original callback and context.
            var flushLength = RoundUp(currentPosition, SectorSize);
            if (flushLength > currentPosition)
                new Span<byte>(BufferPointer + currentPosition, flushLength - currentPosition).Clear();

            // TODO: This will send flushLength, not TotalWrittenLength, to the callback as numBytes.
            logDevice.WriteAsync((IntPtr)BufferPointer, alignedDeviceAddress, (uint)flushLength, originalCallback, originalContext);

            priorCumulativeLength = 0;
            valueCumulativeLength = 0;
            alignedDeviceAddress = 0;
        }

        void DoSerialize(IHeapObject valueObject)
        {
            // TODO: For multi-buffer, consider using a circular buffer array (possibly of size 2), with the full buffers being flushed to disk in sequence
            // on a background thread while the foreground thread populates the "current" one in parallel, sets it to flush to disk, and Wait()s for a free
            // one to continue the population.

            // valueCumulativeLength is only relevant for object serialization; we increment it on all device writes to avoid "if", so here we reset it to the appropriate
            // "start at 0" by making it the negative of currentPosition. Subsequently if we write e.g. an int, we'll have Length and Position = (-currentPosition + currentPosition + 4).
            valueCumulativeLength = -currentPosition;
            valueDataStartPosition = currentPosition;
            valueObjectSerializer.Serialize(valueObject);
            OnSerializeComplete(valueObject);
        }

        void OnSerializeComplete(IHeapObject valueObject)
        {
            // Update value length with the continuation bit NOT set. This may set it to zero if we did not have any more data in the object after the last buffer flush.
            UpdateValueLength(IStreamBuffer.NoValueChunkContinuationBit);
            valueObject.SerializedSize = Length + numBytesInChainedChunkLengths;
            valueDataStartPosition = 0;
            numBytesInChainedChunkLengths = 0;

            if (valueLengthPosition == NoPosition)
            {
                // We are not chunking. Verify the expected length and we're done.
                if (Length != expectedSerializedLength)
                    throw new TsavoriteException($"Expected value length {expectedSerializedLength} does not match actual value length {Length}.");
                return;
            }

            // Check to write the key interior in case we had no chunks.
            // We don't care if this has a key interior or not so ignore the return value; we'll leave currentPosition where it is anyway.
            _ = FlushKeyInteriorIfNeeded();
        }

        /// <summary>Called when a <see cref="LogRecord"/> Write is completed. Ensures end-of-record alignment.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnRecordComplete()
        {
            // The buffer is aligned to sector size, which will be a multiple of kRecordAlignment, so this should always be true.
            var newCurrentPosition = RoundUp(currentPosition, Constants.kRecordAlignment);
            Debug.Assert(newCurrentPosition < buffer.AlignedTotalCapacity);

            var alignmentIncrease = newCurrentPosition - currentPosition;
            if (alignmentIncrease > 0)
            {
                // Zeroinit the extra and update current position.
                new Span<byte>(BufferPointer + currentPosition, alignmentIncrease).Clear();
                currentPosition = newCurrentPosition;
            }

            Debug.Assert(TotalWrittenLength % Constants.kRecordAlignment == 0, $"TotalWrittenLength {TotalWrittenLength} is not record-aligned");
        }

        /// <inheritdoc/>
        public int Read(Span<byte> destinationSpan, CancellationToken cancellationToken = default) => throw new InvalidOperationException("Read is not supported for DiskStreamWriteBuffer");

        /// <inheritdoc/>
        public void Dispose()
        {
            buffer.Return();
        }
    }
}