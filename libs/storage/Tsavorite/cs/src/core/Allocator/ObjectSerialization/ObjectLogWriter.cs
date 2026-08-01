// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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

        /// <summary>
        /// Enables the zero-copy direct-DMA write of large overflow key/value spans (> <see cref="MaxCopySpanLen"/>) in
        /// <see cref="WriteOverflowDma(in OverflowByteArray)"/>: the ChunkHeader + alignment padding + a small source-alignment initial
        /// fragment are copied through the buffer so the DMA disk offset lands on a sector boundary while the DMA source (the pinned byte[]
        /// data) is also sector-aligned; the sector-aligned interior is DMA'd straight from the byte[], and a small end fragment (plus any
        /// remainder past a 1 GB segment boundary) is copied through the buffer. Set to <c>false</c> to route all overflow spans through the
        /// sector-aligned buffered <see cref="Write(ReadOnlySpan{byte}, System.Threading.CancellationToken)"/> path (identical on-disk output).
        /// Deliberately <c>static readonly</c> (not <c>const</c>) so both branches of the gate stay reachable under <c>TreatWarningsAsErrors</c>.
        /// </summary>
        static readonly bool EnableDirectObjectLogWrite = true;

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
        internal void OnPartialFlushComplete(byte* mainLogPageSpanPtr, int mainLogPageSpanLength, IDevice mainLogDevice, ulong alignedMainLogFlushAddress,
                DeviceIOCompletionCallback externalCallback, object externalContext, ref ObjectLogFilePositionInfo endFilePosition)
            => flushBuffers.OnPartialFlushComplete(mainLogPageSpanPtr, mainLogPageSpanLength, mainLogDevice, alignedMainLogFlushAddress,
                externalCallback, externalContext, ref endFilePosition);

        /// <summary>
        /// Write Overflow and Object Keys and values in a <see cref="LogRecord"/> to the device.
        /// </summary>
        /// <remarks>This only writes Overflow and Object Keys and Values; inline portions of the record are written separately by the caller.
        /// <para>No length prefix is written to the object stream. The on-disk length is a read-size hint in the disk-image record's RDH
        /// KeyLength/ValueLength field (see <see cref="LogRecord.SetObjectLogPositionAndLengthHints"/>). Databases written before this format
        /// use the legacy split RDH + objectId-slot encoding, read via <see cref="LogRecord.GetObjectLogRecordStartPositionAndLengths_v21"/>.</para></remarks>
        /// <returns>The number of bytes written for the value object, if any.</returns>
        public ulong WriteRecordObjects(in OverflowByteArray keyOverflow, in OverflowByteArray valueOverflow, in IHeapObject valueObject)
        {
            lastValueAlignmentPadding = 0;

            // If the key is overflow, start with that. A key at/above the RDH KeyLength sentinel (1023) carries its full length in a leading
            // ChunkHeader; below the sentinel the RDH holds the exact length (no header). The key's DMA alignment padding is recovered by the
            // reader from the ChunkHeader (it is not encoded in the RDH sentinel), so it is not threaded back here.
            if (!keyOverflow.IsEmpty)
                _ = WriteOverflowComponent(keyOverflow, hasHeader: keyOverflow.Length >= (int)RecordDataHeader.kKeyLengthLowBitsMask);

            // Now do value overflow or object, if either is present.
            if (!valueOverflow.IsEmpty)
            {
                // Overflow value uses the v2.2 encoding: a value > kOutOfLineExactSizeCutoff (1023) carries its full length (and any DMA
                // alignment padding) in a leading ChunkHeader, and the RDH ValueLength encodes a 4 KB-page/sentinel read hint (which must
                // include the padding) plus the has-header bit; a value <= 1023 is headerless (exact length in the RDH). The value's
                // alignment padding is threaded back so the RDH page-count read hint spans the header + padding + data.
                lastValueAlignmentPadding = WriteOverflowComponent(valueOverflow, hasHeader: valueOverflow.Length > RecordDataHeader.kOutOfLineExactSizeCutoff);
            }
            else if (valueObject is not null)
                DoSerialize(valueObject);

            // Signal completion.
            flushBuffers.OnRecordComplete();
            return valueObjectBytesWritten;
        }

        /// <summary>The O_DIRECT alignment padding (bytes between the ChunkHeader and the sector-aligned data start) applied to the most
        /// recently written overflow VALUE; 0 for a buffered or headerless value. Read by <see cref="LogRecord.SetObjectLogPositionAndLengthHints"/>
        /// so the RDH page-count read hint spans header + padding + data. Reset at the start of each <see cref="WriteRecordObjects"/>.</summary>
        internal int lastValueAlignmentPadding;

        /// <summary>Write one overflow component (key or value): its leading <see cref="ChunkHeader"/> (when <paramref name="hasHeader"/>) and
        /// its bytes. Large components (> <see cref="MaxCopySpanLen"/>) are written mostly by direct O_DIRECT DMA from the pinned byte[] (see
        /// <see cref="WriteOverflowDma"/>); smaller ones are copied through the sector-aligned write buffer. Returns the DMA alignment padding
        /// applied (0 for the buffered path).</summary>
        int WriteOverflowComponent(in OverflowByteArray overflow, bool hasHeader)
        {
            if (EnableDirectObjectLogWrite && overflow.Length > MaxCopySpanLen)
            {
                Debug.Assert(hasHeader, $"A DMA-eligible overflow (length {overflow.Length} > {MaxCopySpanLen}) must always have a header");
                return WriteOverflowDma(overflow);
            }

            // Buffered path: header (no DMA padding) then the data copied through the buffer.
            if (hasHeader)
                WriteOverflowChunkHeader(overflow.Length, alignmentPadding: 0);
            Write(overflow.ReadOnlySpan);
            return 0;
        }

        /// <summary>Write the 8-byte <see cref="ChunkHeader"/> that precedes an overflow key/value with a leading header:
        /// <see cref="ChunkHeader.currentLength"/> carries the full length (single header, no continuation) and
        /// <see cref="ChunkHeader.alignmentPadding"/> the O_DIRECT alignment padding between the header and the sector-aligned data start
        /// (0 on the buffered path). See website/docs/dev/objectlog-serialization.md.</summary>
        void WriteOverflowChunkHeader(int overflowLength, uint alignmentPadding)
        {
            ChunkHeader header = default;
            header.currentLength = (uint)overflowLength;
            header.alignmentPadding = alignmentPadding;
            Write(new ReadOnlySpan<byte>(&header, ChunkHeader.TotalSize));
        }

        /// <summary>
        /// Copies <paramref name="totalLength"/> bytes of a record's serialized object data verbatim from the snapshot object-log (via
        /// <paramref name="reader"/>) into this (main) object-log, then signals record completion. Used by the snapshot-region recovery
        /// flush, which copies a record's object bytes without deserialize/reserialize. The <paramref name="reader"/> must already be
        /// positioned at the record (via <see cref="CircularDiskReadBuffer.OnBeginRecord"/>).
        /// </summary>
        /// <param name="reader">The reader over the snapshot object-log, positioned at the record to copy.</param>
        /// <param name="totalLength">The total number of object-log bytes for the record (key plus value).</param>
        public void CopyRecoveredObjectBytes(ObjectLogReader<TStoreFunctions> reader, ulong totalLength)
        {
            if (totalLength > 0)
            {
                var buffer = flushBuffers.bufferPool.Get(IStreamBuffer.BufferSize);
                try
                {
                    var chunkSpan = buffer.TotalValidSpan;
                    var remaining = totalLength;
                    while (remaining > 0)
                    {
                        var requestLength = (int)Math.Min(remaining, (ulong)chunkSpan.Length);
                        var bytesRead = reader.Read(chunkSpan.Slice(0, requestLength));
                        if (bytesRead == 0)
                            throw new TsavoriteException("Unexpected end of snapshot object-log data while copying objects during recovery");
                        Write(chunkSpan.Slice(0, bytesRead));
                        remaining -= (ulong)bytesRead;
                    }
                }
                finally
                {
                    flushBuffers.bufferPool.Return(buffer);
                }
            }

            // Signal completion, as WriteRecordObjects does.
            flushBuffers.OnRecordComplete();
        }

        /// <summary>Write a large overflow (key or value) mostly by direct O_DIRECT DMA from its pinned byte[], avoiding a copy through the
        /// write buffer. Layout on disk: [ChunkHeader][alignmentPadding][data]. The ChunkHeader + alignment padding + a small source-alignment
        /// initial fragment are copied through the buffer so the DMA disk offset lands on a sector boundary while the DMA source (the pinned
        /// byte[] data) is also sector-aligned; the sector-aligned interior is DMA'd straight from the byte[]; a small end fragment (and any
        /// remainder past a 1 GB segment boundary) is copied through the buffer. Returns the alignment padding (bytes after the header before
        /// the data). See website/docs/dev/objectlog-serialization.md.</summary>
        int WriteOverflowDma(in OverflowByteArray overflow)
        {
            var sectorSize = (int)device.SectorSize;
            var length = overflow.Length;
            var dataSpan = overflow.ReadOnlySpan;

            var gcHandle = overflow.Pin();
            var gcHandleIssued = false;
            try
            {
                var dataPtr = (byte*)gcHandle.AddrOfPinnedObject() + overflow.StartOffset;
                ObjectLogDmaAlignment.Compute((ulong)dataPtr, writeBuffer.currentPosition, sectorSize, out var sourceFragment, out var headerPadding);

                // ChunkHeader + zero alignment padding + the source-alignment initial fragment, copied through the buffer. After these, the
                // buffer write position (and thus the DMA disk offset) is sector-aligned, and dataPtr + sourceFragment is sector-aligned.
                WriteOverflowChunkHeader(length, (uint)headerPadding);
                WritePadding(headerPadding);
                if (sourceFragment > 0)
                    Write(dataSpan.Slice(0, sourceFragment));
                Debug.Assert(IsAligned(writeBuffer.currentPosition, sectorSize), $"currentPosition ({writeBuffer.currentPosition}) must be sector-aligned before the DMA");

                // Flush the buffer so filePosition.Offset is at the sector-aligned data start (unless a buffer boundary already flushed it).
                if (writeBuffer.currentPosition > writeBuffer.flushedUntilPosition)
                    flushBuffers.FlushCurrentBuffer();
                Debug.Assert(IsAligned(flushBuffers.filePosition.Offset, sectorSize), $"DMA filePosition.Offset ({flushBuffers.filePosition.Offset}) must be sector-aligned");

                // DMA the sector-aligned interior that fits in the current 1 GB segment. Any remainder past the segment boundary (rare) plus
                // the end fragment is copied through the buffer, which handles the segment crossing.
                var interior = length - sourceFragment;
                var segmentFit = (int)Math.Min((long)interior, (long)flushBuffers.filePosition.RemainingSizeInSegment);
                var dmaLength = RoundDown(segmentFit, sectorSize);
                if (dmaLength > 0)
                {
                    var writeCallback = flushBuffers.CreateDiskWriteCallbackContext(gcHandle);
                    gcHandleIssued = true;   // ownership transferred: the callback frees the handle on completion
                    flushBuffers.FlushToDevice(dataPtr + sourceFragment, dmaLength, writeCallback);
                }

                var written = sourceFragment + dmaLength;
                if (written < length)
                    Write(dataSpan.Slice(written));   // end fragment + any cross-segment remainder (managed copy; safe regardless of the pin)
                return headerPadding;
            }
            finally
            {
                // If no DMA was issued, we still own the handle; otherwise the write callback owns and frees it.
                if (!gcHandleIssued)
                    gcHandle.Free();
            }
        }

        /// <summary>Write <paramref name="count"/> zero bytes of O_DIRECT alignment padding through the buffer (the reader skips them).
        /// <paramref name="count"/> is less than the device sector size.</summary>
        void WritePadding(int count)
        {
            if (count == 0)
                return;
            zeroPadding ??= new byte[(int)device.SectorSize];
            Write(new ReadOnlySpan<byte>(zeroPadding, 0, count));
        }

        /// <summary>Lazily-allocated zeroed buffer (device sector size) used to write O_DIRECT alignment padding; never mutated.</summary>
        byte[] zeroPadding;

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