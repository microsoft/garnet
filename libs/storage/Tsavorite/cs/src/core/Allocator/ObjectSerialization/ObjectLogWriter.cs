// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    /// <summary>
    /// The class that manages IO writing of Overflow and Object Keys and Values for <see cref="ObjectAllocator{TStoreFunctions}"/> records. It manages the write buffer at two levels:
    /// <list type="bullet">
    ///     <item>At the higher level, called by <see cref="ObjectAllocator{TStoreFunctions}"/> routines, it manages the overall Key and Value writing, including flushing to disk as the buffer is filled.</item>
    ///     <item>At the lower level, it provides the stream for the valueObjectSerializer, which is called via Serialize() by the higher level.</item>
    /// </list>
    /// </summary>
    /// <remarks>This handles only Overflow Keys and Values, and Object Values; inline Keys and Values (of any length) are written to the main log device as part of the main log record.</remarks>
    internal unsafe partial class ObjectLogWriter<TStoreFunctions> : IStreamBuffer
        where TStoreFunctions : IStoreFunctions
    {
        readonly IDevice device;
        IObjectSerializer<IHeapObject> valueObjectSerializer;
        PinnedMemoryStream<ObjectLogWriter<TStoreFunctions>> pinnedMemoryStream;

        /// <summary>The circular buffer we cycle through for parallelization of writes.</summary>
        internal CircularDiskWriteBuffer flushBuffers;

        /// <summary>The <see cref="IStoreFunctions"/> implementation to use</summary>
        internal readonly TStoreFunctions storeFunctions;

        /// <summary>The current buffer being written to in the circular buffer list.</summary>
        internal DiskWriteBuffer writeBuffer;

        /// <summary>For object serialization, the cumulative length of the value bytes.</summary>
        ulong valueObjectBytesWritten;

        /// <summary>The maximum number of key or value bytes to copy into the buffer rather than enqueue a DirectWrite.</summary>
        internal const int MaxCopySpanLen = 128 * 1024;

        /// <summary>If true, we are in the Serialize call. If not we ignore things like <see cref="valueObjectBytesWritten"/> etc.</summary>
        bool inSerialize;

        /// <summary>The total capacity of the buffer.</summary>
        public bool IsForWrite => true;

        /// <summary>Constructor. Creates the circular buffer pool.</summary>
#pragma warning disable IDE0290 // Use primary constructor
        public ObjectLogWriter(IDevice device, CircularDiskWriteBuffer flushBuffers, TStoreFunctions storeFunctions)
        {
            this.device = device ?? throw new ArgumentNullException(nameof(device));
            this.flushBuffers = flushBuffers ?? throw new ArgumentNullException(nameof(flushBuffers));
            this.storeFunctions = storeFunctions;
        }

        /// <inheritdoc/>
        /// <remarks>This is a no-op because we have already flushed under control of the Write() and OnPartialFlushComplete() methods.</remarks>
        public void FlushAndReset(CancellationToken cancellationToken = default) { }

        internal ObjectLogFilePositionInfo GetNextRecordStartPosition() => flushBuffers.GetNextRecordStartPosition();

        /// <summary>Resets start positions for the next partial flush.</summary>
        internal DiskWriteBuffer OnBeginPartialFlush(ObjectLogFilePositionInfo filePosition)
        {
            valueObjectBytesWritten = 0;
            inSerialize = false;
            writeBuffer = flushBuffers.OnBeginPartialFlush(filePosition);
            return writeBuffer;
        }

        /// <summary>
        /// Finish all the current partial flushes, then write the main log page (or page fragment).
        /// </summary>
        /// <param name="mainLogPageSpanPtr">Starting pointer of the main log page span to write</param>
        /// <param name="mainLogPageSpanLength">Length of the main log page span to write</param>
        /// <param name="mainLogDevice">The main log device to write to</param>
        /// <param name="alignedMainLogFlushAddress">The offset in the main log to write at; aligned to sector</param>
        /// <param name="externalCallback">Callback sent to the initial Flush() command. Called when we are done with this partial flush operation.</param>
        /// <param name="externalContext">Context sent to <paramref name="externalCallback"/>.</param>
        /// <param name="endFilePosition">The ending file position after the partial flush is complete</param>
        internal unsafe void OnPartialFlushComplete(byte* mainLogPageSpanPtr, int mainLogPageSpanLength, IDevice mainLogDevice, ulong alignedMainLogFlushAddress,
                DeviceIOCompletionCallback externalCallback, object externalContext, ref ObjectLogFilePositionInfo endFilePosition)
            => flushBuffers.OnPartialFlushComplete(mainLogPageSpanPtr, mainLogPageSpanLength, mainLogDevice, alignedMainLogFlushAddress,
                externalCallback, externalContext, ref endFilePosition);

        /// <summary>
        /// Write Overflow and Object Keys and values in a <see cref="LogRecord"/> to the device.
        /// </summary>
        /// <param name="logRecord">The <see cref="LogRecord"/> whose Keys and Values are to be written to the device.</param>
        /// <remarks>This only writes Overflow and Object Keys and Values; inline portions of the record are written separately.</remarks>
        /// <returns>The number of bytes written for the value object, if any.</returns>
        public ulong WriteRecordObjects(in LogRecord logRecord)
        {
            Debug.Assert(logRecord.Info.RecordHasObjects, "Cannot call ObjectLogWriter with an inline record");

            // If the key is overflow, start with that. (Inline keys are written as part of the main-log record.)
            if (logRecord.Info.KeyIsOverflow)
                WriteDirect(logRecord.KeyOverflow);

            if (logRecord.Info.ValueIsOverflow)
                WriteDirect(logRecord.ValueOverflow);
            else if (logRecord.Info.ValueIsObject)
            {
                var obj = logRecord.ValueObject;
                DoSerialize(obj);
            }
            flushBuffers.OnRecordComplete();
            return valueObjectBytesWritten;
        }

        /// <summary>Start off the write using the full span of the <see cref="OverflowByteArray"/>.</summary>
        /// <param name="overflow">The <see cref="OverflowByteArray"/> to write.</param>
        void WriteDirect(OverflowByteArray overflow) => WriteDirect(overflow, overflow.ReadOnlySpan, refCountedGCHandle: default);

        /// <summary>Write the <paramref name="fullDataSpan"/> of the <paramref name="overflow"/>.</summary>
        /// <param name="overflow">The <see cref="OverflowByteArray"/> to write.</param>
        /// <param name="fullDataSpan">The span of <paramref name="overflow"/> to write. Initially it is the full <paramref name="overflow"/>; if the write
        ///   spans segments, then it is a recursive call for the last segment's fraction.</param>
        /// <param name="refCountedGCHandle">The refcounted GC handle if this is a recursive call</param>
        void WriteDirect(OverflowByteArray overflow, ReadOnlySpan<byte> fullDataSpan, RefCountedPinnedGCHandle refCountedGCHandle)
        {
            if (overflow.Length <= MaxCopySpanLen)
                Write(fullDataSpan);
            else
            {
                // 1. Write the sector-aligning start fragment into the buffers and flush the current buffer (if we cross a buffer boundary,
                //    previous buffers will already have been flushed).
                var dataStart = 0;
                var copyLength = RoundUp(writeBuffer.currentPosition, (int)device.SectorSize) - writeBuffer.currentPosition;
                if (copyLength != 0)
                {
                    Debug.Assert(refCountedGCHandle is null, $"If refCountedGCHandle is not null then buffer.currentPosition ({writeBuffer.currentPosition}) should already be sector-aligned");
                    Write(fullDataSpan.Slice(dataStart, copyLength));
                    dataStart += copyLength;
                    flushBuffers.FlushCurrentBuffer();
                }

                // 2. Flush the sector-aligned span interior. We are writing direct to the device from a byte[], so we have to pin the array.
                //    We may have to split across multiple segments.
                var interiorLen = RoundDown(overflow.Array.Length - dataStart, (int)device.SectorSize);
                var segmentRemainingLen = flushBuffers.filePosition.RemainingSizeInSegment;
                var gcHandle = (refCountedGCHandle is null) ? GCHandle.Alloc(overflow.Array, GCHandleType.Pinned) : default;
                var localGcHandle = refCountedGCHandle?.gcHandle ?? gcHandle;
                var overflowStartPtr = (byte*)localGcHandle.AddrOfPinnedObject() + overflow.StartOffset;
                if ((uint)interiorLen <= segmentRemainingLen)
                {
                    // We have enough room in the segment to write the full interior span in one chunk.
                    var writeCallback = refCountedGCHandle is null
                        ? flushBuffers.CreateDiskWriteCallbackContext(gcHandle)
                        : flushBuffers.CreateDiskWriteCallbackContext(refCountedGCHandle);
                    flushBuffers.FlushToDevice(overflowStartPtr + dataStart, interiorLen, writeCallback);
                    dataStart += interiorLen;
                }
                else
                {
                    // Multi-segment write so we will need to refcount the GCHandle. SegmentRemainingLength is <= int.MaxValue so we can cast it to int.
                    // TODO: This and other segment-limiting logic could be pushed down into StorageDeviceBase, which could iterate on the segments.
                    // However this could have complications with e.g. callback and countdown counts (there would be more than one callback invocation
                    // on that; this could be handled by defining some way for the StorageDeviceBase to know the calback uses a CountdownEvent and
                    // incrementing that count, or by having a local callback, similarly to how CircularDiskWriteBuffer handles multiple possibly-concurrent
                    // writes before calling the main callback, that handles doing the "final" callback). In this case we could defer the "segment id" logic
                    // to StorageDeviceBase, and just have a ulong position, from which we could compute the segment id (e.g. for truncation), and
                    // ObjectLogFilePositionInfo would be simplified.
                    Debug.Assert(segmentRemainingLen <= int.MaxValue, $"segmentRemainingLen ({segmentRemainingLen}) should be <= int.MaxValue");

                    // Create the refcounted pinned GCHandle with a refcount of 1, so that if a read completes while we're still setting up, we won't get an early unpin.
                    refCountedGCHandle ??= new RefCountedPinnedGCHandle(gcHandle, initialCount: 1);

                    // Copy chunks to segments and advance the segment.
                    while (interiorLen > (int)segmentRemainingLen)
                    {
                        var writeCallback = flushBuffers.CreateDiskWriteCallbackContext(refCountedGCHandle);
                        flushBuffers.FlushToDevice(overflowStartPtr + dataStart, (int)segmentRemainingLen, writeCallback);
                        dataStart += (int)segmentRemainingLen;

                        Debug.Assert(flushBuffers.filePosition.RemainingSizeInSegment == 0, $"Expected to be at end of segment but there were {flushBuffers.filePosition.RemainingSizeInSegment} bytes remaining");
                        flushBuffers.filePosition.AdvanceToNextSegment();
                        segmentRemainingLen = flushBuffers.filePosition.RemainingSizeInSegment;
                    }

                    // Now we know we will fit in the last segment, so call recursively to optimize the "copy vs. direct" final fragment.
                    // First adjust the endPosition in case we don't have a full buffer of space remaining in the segment.
                    if ((ulong)writeBuffer.RemainingCapacity > flushBuffers.filePosition.RemainingSizeInSegment)
                        writeBuffer.endPosition = (int)flushBuffers.filePosition.RemainingSizeInSegment - writeBuffer.currentPosition;
                    WriteDirect(overflow, fullDataSpan.Slice(dataStart), refCountedGCHandle);
                }

                // 3. Copy the end sector-aligning fragment to the buffers.
                if (dataStart < overflow.Length)
                    Write(fullDataSpan.Slice(dataStart));
            }

            // Release the initial refcount on this, if we created it. This will let it final-release when all writes are complete.
            refCountedGCHandle?.Release();
        }

        /// <inheritdoc/>
        public void Write(ReadOnlySpan<byte> data, CancellationToken cancellationToken = default)
        {
            // This is called by valueObjectSerializer.Serialize() as well as internally. No other calls should write data to flushBuffer.memory in a way
            // that increments flushBuffer.currentPosition, since we manage chained-chunk continuation and DiskPageHeader offsetting here.

            // Copy to the buffer. If it does not fit in the remaining capacity, we will write as much as does, flush the buffer, and move to next buffer.
            var dataStart = 0;
            var segmentRemainingLen = flushBuffers.filePosition.SegmentSize - flushBuffers.GetNextRecordStartPosition().Offset;
            while (data.Length - dataStart > 0)
            {
                Debug.Assert(writeBuffer.RemainingCapacity > 0,
                        $"RemainingCapacity {writeBuffer.RemainingCapacity} should not be 0 (data.Length {data.Length}, dataStart {dataStart}); this should have already triggered an OnChunkComplete call, which would have reset the buffer");
                cancellationToken.ThrowIfCancellationRequested();   // IDevice does not support cancellation, so just check this here

                // If it won't all fit in the remaining buffer, write as much as will.
                var requestLength = (uint)(data.Length - dataStart);
                if (requestLength > writeBuffer.RemainingCapacity)
                    requestLength = (uint)writeBuffer.RemainingCapacity;

                // If it won't all fit in the remaining segment, write as much as will.
                if ((ulong)requestLength > segmentRemainingLen)
                    requestLength = (uint)segmentRemainingLen;
                segmentRemainingLen -= requestLength;

                data.Slice(dataStart, (int)requestLength).CopyTo(writeBuffer.memory.TotalValidSpan.Slice(writeBuffer.currentPosition));
                dataStart += (int)requestLength;
                writeBuffer.currentPosition += (int)requestLength;
                if (inSerialize)
                {
                    valueObjectBytesWritten += requestLength;
                    if (valueObjectBytesWritten >= IHeapObject.MaxSerializedObjectSize)
                        throw new TsavoriteException($"Object serialized size currently at {valueObjectBytesWritten} which exceeds max serialization limit of {IHeapObject.MaxSerializedObjectSize}");
                }

                // See if we're at the end of the buffer or segment.
                if (writeBuffer.RemainingCapacity == 0 || segmentRemainingLen == 0)
                    OnBufferComplete();

                if (segmentRemainingLen == 0)
                {
                    flushBuffers.filePosition.AdvanceToNextSegment();
                    segmentRemainingLen = flushBuffers.filePosition.RemainingSizeInSegment;
                }
            }
        }

        /// <summary>At the end of a buffer, do any processing, flush the current buffer, and move to the next buffer. </summary>
        /// <remarks>Called during Serialize().</remarks>
        void OnBufferComplete()
        {
            // This should only be called when the object serialization hits the end of the buffer; for partial buffers we will call
            // OnSerializeComplete() after the Serialize() call has returned. "End of buffer" ends before lengthSpaceReserve if any.
            Debug.Assert(writeBuffer.currentPosition == writeBuffer.endPosition, $"CurrentPosition {writeBuffer.currentPosition} must be at writeBuffer.endPosition {writeBuffer.endPosition}).");

            flushBuffers.FlushCurrentBuffer();
            writeBuffer = flushBuffers.MoveToAndInitializeNextBuffer();
        }

        void DoSerialize(IHeapObject valueObject)
        {
            // valueCumulativeLength is only relevant for object serialization; we increment it on all device writes to avoid "if", so here we reset it to the appropriate
            // "start at 0" by making it the negative of currentPosition. Subsequently if we write e.g. an int, we'll have Length and Position = (-currentPosition + currentPosition + 4).
            inSerialize = true;
            valueObjectBytesWritten = 0;

            // If we haven't yet instantiated the serializer do so now.
            if (valueObjectSerializer is null)
            {
                pinnedMemoryStream = new(this);
                valueObjectSerializer = storeFunctions.CreateValueObjectSerializer();
                valueObjectSerializer.BeginSerialize(pinnedMemoryStream);
            }

            valueObjectSerializer.Serialize(valueObject);
            OnSerializeComplete(valueObject);
        }

        void OnSerializeComplete(IHeapObject valueObject)
        {
            inSerialize = false;
        }

        /// <inheritdoc/>
        public int Read(Span<byte> destinationSpan, CancellationToken cancellationToken = default) => throw new InvalidOperationException("Read is not supported for DiskStreamWriteBuffer");

        /// <inheritdoc/>
        public void Dispose()
        {
            var localMemoryStream = Interlocked.Exchange(ref pinnedMemoryStream, null);
            if (localMemoryStream is not null)
            {
                // End serialization before disposing the pinned memory stream as it may try to flush final data which would use the pinnedMemoryStream.
                valueObjectSerializer?.EndSerialize();
                localMemoryStream.Dispose();
            }
        }
    }
}