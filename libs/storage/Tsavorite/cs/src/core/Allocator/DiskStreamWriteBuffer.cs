// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    using static VarbyteLengthUtility;

    internal unsafe class DiskStreamWriteBuffer : IStreamBuffer
    {
        readonly IDevice logDevice;
        readonly SectorAlignedMemory buffer;
        readonly IObjectSerializer<IHeapObject> valueObjectSerializer;

        /// <summary>Current write position (we do not support read in this buffer). This class only supports Write and no Seek,
        /// so currentPosition equals the current length</summary>
        /// <remarks><see cref="currentPosition"/> is the current length, since it is *past* the last byte copied to the buffer.</remarks>
        int currentPosition;
        /// <summary>Cumulative length of data written to the buffer before the current buffer contents</summary>
        long priorCumulativeLength;

        /// <summary>Destination address to write to; incremented with each write.</summary>
        ulong alignedDeviceAddress;

        /// <summary>In the most common case, SerializedSizeIsExact is true and this is the expected length of the serialized value object
        /// (used to verify the serialized size after serialization completes).</summary>
        long expectedSerializedLength;
        /// <summary>The actual full length of the value; verified on completion against <see cref="expectedSerializedLength"/> if SerializedSizeIsExact.</summary>
        long actualSerializedLength;

        /// <summary>For object serialization via chained chunks, indicates that the position is not set (i.e. we're not doing chunk chaining)</summary>
        const int NoPosition = -1;
        /// <summary>For object serialization via chained chunks, we need to track the length prefix for the current chunk to update it before
        /// flushing the buffer to disk</summary>
        int valueLengthPosition;
        /// <summary>For object serialization via chained chunks, the start of the key-end cap used for sector alignment when writing the key directly to the device.
        /// May be zero if the key length happens to coincide with the end of a sector.</summary>
        int keyEndSectorCapPosition;
        /// <summary>For object serialization via chained chunks, the offset by which the start of the value bytes us past valueLengthPosition + sizeof(int).
        /// Needed only for the first chunk when we are writing the key directly to the device</summary>
        int valueStartOffsetFromEndOfLength;
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

        /// <summary>The amount of data written to the buffer so far (from Read() or Write()). TODO: Make sure Length and priorCumulativeLength are for object serialization only.</summary>
        /// <remarks><see cref="currentPosition"/> is the current length, since it is *past* the last byte copied to the buffer.</remarks>
        public long Length => priorCumulativeLength + currentPosition;

        /// <summary>The current position the buffer so far, including cumulative lengths of previous buffers (before FlushAndReset).</summary>
        /// <remarks><see cref="currentPosition"/> is the current length, since it is *past* the last byte copied to the buffer.</remarks>
        public long Position => priorCumulativeLength + currentPosition;

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
            Debug.Assert((buffer.AlignedTotalCapacity & SectorSize) == 0, "Buffer size must be aligned to device sector size.");
        }

        internal void SetAlignedDeviceAddress(ulong alignedDeviceAddress) => this.alignedDeviceAddress = alignedDeviceAddress;

        internal void OnInitialSectorReadComplete(int position) => currentPosition = position;

        /// <inheritdoc/>
        public void FlushAndReset(CancellationToken cancellationToken = default) => throw new InvalidOperationException("Flushing must only be done under control of the Write() methods, due to possible Value length adjustments.");

        /// <inheritdoc/>
        public void Write(in LogRecord logRecord)
        {
            // Initialize to not track the value length position (not serializing an object, or we know its exact length up-front so are not doing chunk chaining)
            valueLengthPosition = NoPosition;
            keyEndSectorCapPosition = NoPosition;
            expectedSerializedLength = 0;

            // If the record is inline, we can just write it directly; the indicator bytes will be correct.
            if (logRecord.Info.RecordIsInline)
            {
                Write(logRecord.AsReadOnlySpan());
                return;
            }

            // Everything writes the RecordInfo first. Because the record is not inline, we cannot just write the indicator bytes directly.
            Write(logRecord.RecordInfoSpan);

            ReadOnlySpan<byte> keySpan = logRecord.Key, valueSpan = logRecord.ValueSpan;

            if (!logRecord.Info.ValueIsObject)
            {
                // We know the exact values for the varbyte key and value lengths.
                WriteIndicatorByteAndLengths(keySpan.Length, valueSpan.Length);
                Write(logRecord.Key);
                Write(logRecord.ValueSpan);
                WriteOptionals(in logRecord);
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
                Write(logRecord.Key);
                WriteOptionals(in logRecord);
                return;
            }

            // 2. If ValueObject.SerializedSizeIsExact, then write varbytes with the known value length, the key, serialize the value (without tracking valueLengthPosition), write optionals, done.
            if (valueObject.SerializedSizeIsExact)
            {
                // We can write the exact value length, so we will write the indicator byte with the key length and value length, then write the key and value spans.
                WriteIndicatorByteAndLengths(keySpan.Length, valueObject.SerializedSize);
                Write(logRecord.Key);

                // Serialize the value object into possibly multiple buffers, but we only need the one length we've just filled in. valueStartPosition is already set to NoPosition.
                expectedSerializedLength = valueObject.SerializedSize;
                DoSerialize(valueObject);
                WriteOptionals(in logRecord);
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
                valueStartOffsetFromEndOfLength = currentPosition - valueLengthPosition + sizeof(int);

                // Serialize the value object into possibly multiple buffers; we will manage chunk updating in Write(ReadOnlySpan<byte> data).
                expectedSerializedLength = keySpan.Length;
                DoSerialize(valueObject);
                WriteOptionals(in logRecord);
                return;
            }

            // 3b. The key is large (which should be quite rare), so we can't just copy it to the buffer. We'll just copy the necessary sector-aligning byte "caps"
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
            valueStartOffsetFromEndOfLength = currentPosition - valueLengthPosition + sizeof(int);

            // Serialize the value object into possibly multiple buffers.
            DoSerialize(valueObject);
            WriteOptionals(in logRecord);
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
            currentPosition += (int)(ptr - (BufferPointer + currentPosition));
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
                // If it won't all fit in the remaining buffer, write the first part that will (that may be a full buffer, if it's a multi-buffer span).
                if (data.Length - dataStart > RemainingCapacity - lengthSpaceReserve)
                {
                    Debug.Assert(RemainingCapacity > 0, "RemainingCapacity == 0 should have already triggered an OnChunkComplete call, which would have reset the buffer");

                    // Write the part that fits.
                    var chunk = data.Slice(dataStart, RemainingCapacity);
                    chunk.CopyTo(RemainingSpan);
                    dataStart += RemainingCapacity;
                    currentPosition += RemainingCapacity;
                    OnChunkComplete(lengthSpaceReserve);
                }

                // If the remaining data fits in the remaining buffer, copy it and return.
                if (data.Length - dataStart <= RemainingCapacity)
                {
                    // Copy the partial chunk.
                    var chunk = data.Slice(dataStart);
                    Debug.Assert(chunk.Length <= RemainingCapacity, $"chunk.Length {chunk.Length} > RemainingCapacity {RemainingCapacity}");
                    chunk.CopyTo(RemainingSpan);
                    currentPosition += chunk.Length;
                    return;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void UpdateValueLength(long continuationBit)
        {
            var valueStartPosition = valueLengthPosition + sizeof(int) + valueStartOffsetFromEndOfLength;
            var chunkLength = currentPosition - valueStartPosition;
            actualSerializedLength += chunkLength;

            if (valueLengthPosition == NoPosition)
                return;

            // This may set the value length to 0 if we didn't have any value data in this chunk (e.g. the value ended on the prior chunk boundary).
            *(long*)(BufferPointer + valueLengthPosition) = (long)chunkLength | continuationBit;
            valueStartOffsetFromEndOfLength = 0; // only needed the first time
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
                Debug.Assert(currentPosition == SectorSize, $"currentPosition {currentPosition} must be at the end of the first sector afte OnChunkComplete for chunk-chaining.");
                valueLengthPosition = currentPosition - sizeof(int);
            }
        }

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
            alignedDeviceAddress += (ulong)span.Length;
        }

        /// <inheritdoc/>
        public void OnFlushComplete()
        {
            // Flush the last of the buffer, sector-aligning and zeroing the end. (All chunk-length updates have been done if we were chunk-chaining).
            if (currentPosition == 0)
                return; // Nothing to flush

            var flushLength = RoundUp(currentPosition, SectorSize);
            if (flushLength > currentPosition)
                new Span<byte>(BufferPointer + currentPosition, flushLength - currentPosition).Clear();
            FlushToDevice(new ReadOnlySpan<byte>(BufferPointer, flushLength));
        }

        void DoSerialize(IHeapObject valueObject)
        {
            // TODO: For multi-buffer, consider using two buffers, a full one being flushed to disk on a background thread while the foreground thread populates the next one in parallel.
            // This could simply hold the CountdownEvent for the "disk write in progress" buffer and Wait() on it when DoSerialize() has filled the next buffer.
            valueObjectSerializer.Serialize(valueObject);
            OnSerializeComplete();
        }

        void OnSerializeComplete()
        {
            // Update value length with the continuation bit NOT set. This may set it to zero if we did not have any more data in the object after the last buffer flush.
            UpdateValueLength(IStreamBuffer.NoValueChunkContinuationBit);

            if (valueLengthPosition == NoPosition)
            {
                // We are not chunking, so only need to verify the expected length and we're done.
                if (actualSerializedLength != expectedSerializedLength)
                    throw new TsavoriteException($"Expected value length {expectedSerializedLength} does not match actual value length {actualSerializedLength}.");
                return;
            }

            // We don't care if this has a key interior or not so ignore the return value; we'll leave currentPosition where it is anyway.
            _ = FlushKeyInteriorIfNeeded();
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