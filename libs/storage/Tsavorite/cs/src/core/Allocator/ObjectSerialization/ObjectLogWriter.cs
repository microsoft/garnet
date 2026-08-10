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

        // ── Object chunk framing (headered value objects, data length > RecordDataHeader.kOutOfLineExactSizeCutoff) ──────────
        // On-disk layout: [prefix headerless bytes][hdr_1][chunk_1]…[hdr_N][chunk_N]. A value whose data length is <= the cutoff
        // is fully headerless. Beyond the cutoff, a chunk == one write-buffer's (or segment's) worth of object data; each 8-byte
        // ChunkHeader is written on an 8-byte-aligned object-log position (so it never straddles a buffer/segment boundary) and its
        // currentLength (| ContinuationFlag) is BACK-FILLED when the buffer fills or the object ends -- so the header is finalized
        // while still in the unflushed buffer. Headers are APPENDED (position advances monotonically), never inserted-with-slide.
        // WriteObjectData runs its OWN explicit buffer loop (it does NOT reuse WriteRawBuffered, whose local segmentRemainingLen
        // would be corrupted by header pokes -- the prior multi-segment bug); header pokes/back-fills poke the buffer memory directly.
        internal const int ObjectHeaderlessPrefixLen = RecordDataHeader.kOutOfLineExactSizeCutoff;   // 511

        /// <summary>True once the current value object has crossed the headerless prefix and is emitting ChunkHeaders.</summary>
        bool objectHeadered;

        /// <summary>Buffer offset of the current object chunk's placeholder <see cref="ChunkHeader"/> to back-fill; -1 if none is pending.</summary>
        int currentChunkHeaderBufferPos;

        /// <summary>Object-log position captured at the start of the current value-object serialization.</summary>
        ObjectLogFilePositionInfo objectStartPosition;

        /// <summary>The on-disk extent through the end of the first framed chunk of the most recently serialized value object:
        /// headerless prefix + 8-align padding + first ChunkHeader + first chunk data. For a headerless object this is its exact data
        /// length. Used to stamp the objectId initial-read hint. Zero when there was no object value.</summary>
        internal ulong lastObjectFirstChunkExtent;

        /// <summary>The total on-disk extent (prefix + 8-align padding + ChunkHeaders + data) of the most recently serialized value object;
        /// zero when there was no object value.</summary>
        internal ulong lastObjectExtent;

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
        /// Initial-read hints are stamped into the objectId slots; RDH lengths remain exact inline/physical-slot lengths.</remarks>
        /// <returns>The number of bytes written for the value object, if any.</returns>
        public ulong WriteRecordObjects(in OverflowByteArray keyOverflow, in OverflowByteArray valueOverflow, in IHeapObject valueObject)
        {
            lastKeyAlignmentPadding = 0;
            lastValueAlignmentPadding = 0;
            lastObjectFirstChunkExtent = 0;
            lastObjectExtent = 0;

            // If the key is overflow, start with that. A key above the objectId exact-size limit carries its full length in a leading
            // ChunkHeader; otherwise it is headerless and the objectId hint carries its exact length.
            if (!keyOverflow.IsEmpty)
                lastKeyAlignmentPadding = WriteOverflowComponent(keyOverflow, hasHeader: keyOverflow.Length > ObjectIdMap.MaxObjectIdSizeHint);

            // Now do value overflow or object, if either is present.
            if (!valueOverflow.IsEmpty)
            {
                // A value above the objectId exact-size limit carries its full length and DMA alignment padding in a leading ChunkHeader;
                // otherwise it is headerless and the objectId hint carries its exact length.
                lastValueAlignmentPadding = WriteOverflowComponent(valueOverflow, hasHeader: valueOverflow.Length > RecordDataHeader.kOutOfLineExactSizeCutoff);
            }
            else if (valueObject is not null)
            {
                DoSerialize(valueObject);

                lastObjectExtent = flushBuffers.GetNextRecordStartPosition() - objectStartPosition;
                if (lastObjectFirstChunkExtent == 0)
                    lastObjectFirstChunkExtent = lastObjectExtent;
            }

            // Signal completion.
            flushBuffers.OnRecordComplete();
            return valueObjectBytesWritten;
        }

        /// <summary>The O_DIRECT alignment padding applied to the most recently written overflow key.</summary>
        internal int lastKeyAlignmentPadding;

        /// <summary>The O_DIRECT alignment padding applied to the most recently written overflow value.</summary>
        internal int lastValueAlignmentPadding;

        /// <summary>Pad between records so the next record starts at the same modulo-8 offset as <paramref name="sourcePosition"/>.
        /// Snapshot recovery uses this before a verbatim copy because the first object ChunkHeader is located by absolute 8-alignment.</summary>
        internal void AlignNextRecordStartLike(in ObjectLogFilePositionInfo sourcePosition)
        {
            var destination = flushBuffers.GetNextRecordStartPosition();
            var padding = (int)((sourcePosition.Offset - destination.Offset) & 7);
            WritePadding(padding);
        }

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
        /// (0 on the buffered path). See website/docs/dev/tsavorite/objectlog-serialization.md.</summary>
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
        /// flush for a record whose exact on-disk extent is known (a successor object record bounded it, or its size hints equal it). The
        /// <paramref name="reader"/> must already be positioned at the record (via <see cref="CircularDiskReadBuffer.OnBeginRecord"/>). A record
        /// that is the last on its page with a sentinel-sized value is copied instead by <see cref="CopyRecoveredObjectBytesFollowingFraming"/>.
        /// </summary>
        /// <param name="reader">The reader over the snapshot object-log, positioned at the record to copy.</param>
        /// <param name="totalLength">The exact total number of object-log bytes for the record (key plus value).</param>
        /// <returns>The number of object-log bytes copied (equal to <paramref name="totalLength"/>).</returns>
        public ulong CopyRecoveredObjectBytes(ObjectLogReader<TStoreFunctions> reader, ulong totalLength)
        {
            var copied = 0UL;
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
                        copied += (ulong)bytesRead;
                    }
                }
                finally
                {
                    flushBuffers.bufferPool.Return(buffer);
                }
            }

            // Signal completion, as WriteRecordObjects does.
            flushBuffers.OnRecordComplete();
            return copied;
        }

        /// <summary>
        /// Snapshot-recovery verbatim copy for a record that is the last object record on its page (no successor bounds its extent) and whose
        /// size hint under-counts a sentinel-sized value: drive <paramref name="reader"/>'s framing walk -- following the ChunkHeader chain to
        /// the object's exact on-disk extent and self-extending the snapshot read-ahead -- which tees every consumed byte into this (main)
        /// object-log, then signal record completion. Unlike <see cref="CopyRecoveredObjectBytes"/> (bounded by a caller-supplied length), this
        /// copies exactly the record's on-disk extent, so it neither truncates a multi-buffer value nor over-copies into the next record.
        /// </summary>
        /// <param name="reader">The reader over the snapshot object-log; this method positions it at <paramref name="snapshotPositionWord"/>.</param>
        /// <param name="logRecord">The record whose object bytes are copied (read for its framing flags and length hints).</param>
        /// <param name="snapshotPositionWord">The record's snapshot object-log start position word (segment+offset; flag bits ignored).</param>
        /// <param name="keyLength">The record's key length hint (exact for a below-sentinel overflow key; the sentinel-capped hint otherwise).</param>
        /// <param name="valueLength">The record's value initial-read-extent hint.</param>
        /// <param name="segmentSizeBits">The object-log segment size in bits, for decoding the position word.</param>
        /// <returns>The exact number of object-log bytes copied (key plus value).</returns>
        public ulong CopyRecoveredObjectBytesFollowingFraming(ObjectLogReader<TStoreFunctions> reader, in LogRecord logRecord, ulong snapshotPositionWord,
            int keyLength, ulong valueLength, int segmentSizeBits)
        {
            var copied = reader.CopyRecordObjectsFollowingFraming(in logRecord, snapshotPositionWord, keyLength, valueLength, segmentSizeBits, sink: this);

            // Signal completion, as WriteRecordObjects and CopyRecoveredObjectBytes do.
            flushBuffers.OnRecordComplete();
            return copied;
        }

        /// <summary>Write a large overflow (key or value) mostly by direct O_DIRECT DMA from its pinned byte[], avoiding a copy through the
        /// write buffer. Layout on disk: [ChunkHeader][alignmentPadding][data]. The ChunkHeader + alignment padding + a small source-alignment
        /// initial fragment are copied through the buffer so the DMA disk offset lands on a sector boundary while the DMA source (the pinned
        /// byte[] data) is also sector-aligned; the sector-aligned interior is DMA'd straight from the byte[], iterating across object-log
        /// segment boundaries (one <see cref="CircularDiskWriteBuffer.FlushToDevice"/> per segment, since a single device write cannot cross a
        /// segment); only a final sub-sector end fragment is copied through the buffer (and it never crosses a segment). Returns the alignment
        /// padding (bytes after the header before the data). See website/docs/dev/tsavorite/objectlog-serialization.md.</summary>
        int WriteOverflowDma(in OverflowByteArray overflow)
        {
            var sectorSize = (int)device.SectorSize;
            var length = overflow.Length;
            var dataSpan = overflow.ReadOnlySpan;

            var gcHandle = overflow.Pin();
            RefCountedPinnedGCHandle refCountedGCHandle = null;   // used when the interior spans >1 segment (multiple writes from the same byte[])
            var singleWriteOwnsHandle = false;                    // set when a single DMA write owns the plain handle
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

                // DMA the whole sector-aligned interior straight from the pinned byte[]. filePosition.Offset and RemainingSizeInSegment are both
                // sector-aligned, so each per-segment chunk is sector-aligned; only the sub-sector end fragment is left for the buffered path.
                var interior = length - sourceFragment;
                var dmaTotal = RoundDown(interior, sectorSize);
                if (dmaTotal > 0)
                {
                    // If the interior does not fit in the current segment we issue multiple writes from the same byte[], so refcount the pin so
                    // it is freed only after the last write completes. A single write uses the plain handle (no heap allocation).
                    var spansSegment = (ulong)dmaTotal > flushBuffers.filePosition.RemainingSizeInSegment;
                    if (spansSegment)
                        refCountedGCHandle = new RefCountedPinnedGCHandle(gcHandle, initialCount: 1);

                    var dmaOffset = sourceFragment;
                    var dmaRemaining = dmaTotal;
                    while (dmaRemaining > 0)
                    {
                        // Capture the segment remainder BEFORE the write: FlushToDevice does filePosition.Offset += chunk, and the Offset
                        // setter masks to the segment size, so a chunk that exactly fills the segment wraps Offset to 0 (leaving SegmentId
                        // stale). We must detect the fill from this pre-write remainder, not from RemainingSizeInSegment afterward.
                        var remainingInSegment = flushBuffers.filePosition.RemainingSizeInSegment;
                        var chunk = (int)Math.Min((long)dmaRemaining, (long)remainingInSegment);
                        var writeCallback = spansSegment
                            ? flushBuffers.CreateDiskWriteCallbackContext(refCountedGCHandle)
                            : flushBuffers.CreateDiskWriteCallbackContext(gcHandle);
                        if (!spansSegment)
                            singleWriteOwnsHandle = true;   // ownership transferred: the callback frees the handle on completion
                        flushBuffers.FlushToDevice(dataPtr + dmaOffset, chunk, writeCallback);
                        dmaOffset += chunk;
                        dmaRemaining -= chunk;

                        // If that chunk exactly filled the segment, advance to the next one (SegmentId++, Offset=0), matching the buffered
                        // path, which also advances on the boundary even when it was the final chunk, so any end fragment lands next segment.
                        if ((ulong)chunk == remainingInSegment)
                            flushBuffers.filePosition.AdvanceToNextSegment();
                    }
                }

                var written = sourceFragment + dmaTotal;
                if (written < length)
                    Write(dataSpan.Slice(written));   // sub-sector end fragment (never crosses a segment; managed copy, safe regardless of the pin)
                return headerPadding;
            }
            finally
            {
                // Drop the initial refcount (freed when the last DMA write completes); or free the plain handle if no DMA write took ownership.
                if (refCountedGCHandle is not null)
                    refCountedGCHandle.Release();
                else if (!singleWriteOwnsHandle)
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
            // The value-object serializer's writes are chunk-framed (headerless prefix + back-filled per-buffer ChunkHeaders); all other
            // writes (overflow bytes/headers/padding, the recovery verbatim copy) go straight to the raw buffered path.
            if (inSerialize)
            {
                WriteObjectData(data, cancellationToken);
                return;
            }
            WriteRawBuffered(data, cancellationToken);
        }

        /// <summary>Raw buffered write: copy <paramref name="data"/> into the sector-aligned write buffer, splitting across buffer and object-log
        /// segment boundaries (flushing full buffers via <see cref="OnBufferComplete"/>). Used for overflow bytes/headers/padding and the
        /// recovery verbatim copy. Never called while serializing an object (that path is <see cref="WriteObjectData"/>).</summary>
        void WriteRawBuffered(ReadOnlySpan<byte> data, CancellationToken cancellationToken = default)
        {

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
            inSerialize = true;
            valueObjectBytesWritten = 0;
            objectHeadered = false;
            currentChunkHeaderBufferPos = -1;
            objectStartPosition = flushBuffers.GetNextRecordStartPosition();

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
            // Finalize the last chunk's ChunkHeader (no continuation) before it flushes.
            if (objectHeadered && currentChunkHeaderBufferPos >= 0)
                BackfillObjectChunkHeader(hasContinuation: false);
            inSerialize = false;

            if (valueObjectBytesWritten >= IHeapObject.MaxSerializedObjectSize)
                throw new TsavoriteException($"Object serialized size {valueObjectBytesWritten} exceeds max serialization limit of {IHeapObject.MaxSerializedObjectSize}");
        }

        // ── Object chunk-framing write path (see the ObjectHeaderlessPrefixLen field comment and website/docs/dev/tsavorite/objectlog-serialization.md) ──

        /// <summary>Serialize object data through the chunk-framing path: the first <see cref="ObjectHeaderlessPrefixLen"/> data bytes are
        /// written headerless; beyond that, a fresh 8-aligned <see cref="ChunkHeader"/> is inserted and each buffer's chunk is back-filled at
        /// the buffer boundary. Runs its own explicit buffer loop (not <see cref="WriteRawBuffered"/>).</summary>
        void WriteObjectData(ReadOnlySpan<byte> data, CancellationToken cancellationToken)
        {
            var dataStart = 0;
            while (dataStart < data.Length)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!objectHeadered)
                {
                    var prefixRoom = ObjectHeaderlessPrefixLen - (int)valueObjectBytesWritten;
                    if (prefixRoom > 0)
                    {
                        dataStart += CopyObjectDataBytes(data.Slice(dataStart), prefixRoom);
                        continue;
                    }

                    // Prefix complete -> 8-align and insert the first chunk header, transitioning to the headered phase.
                    StartObjectHeaderedPhase();
                    continue;
                }
                dataStart += CopyObjectDataBytes(data.Slice(dataStart), int.MaxValue);
            }
        }

        /// <summary>Copy up to min(<paramref name="maxLen"/>, remaining buffer capacity) DATA bytes into the write buffer, counting them as
        /// object data; if the buffer fills, advance to the next buffer (back-filling/poking the chunk header when headered). Returns bytes copied.</summary>
        int CopyObjectDataBytes(ReadOnlySpan<byte> data, int maxLen)
        {
            var n = Math.Min(Math.Min(data.Length, maxLen), writeBuffer.RemainingCapacity);
            if (n > 0)
            {
                data.Slice(0, n).CopyTo(writeBuffer.memory.TotalValidSpan.Slice(writeBuffer.currentPosition));
                writeBuffer.currentPosition += n;
                valueObjectBytesWritten += (uint)n;
            }
            if (writeBuffer.RemainingCapacity == 0)
                AdvanceObjectBuffer();
            return n;
        }

        /// <summary>Called when the write buffer fills during object serialization: back-fill the current chunk header (if headered), flush the
        /// buffer, advance the segment if this flush filled it, move to the next buffer, and poke a fresh placeholder header (if headered).</summary>
        void AdvanceObjectBuffer()
        {
            if (objectHeadered && currentChunkHeaderBufferPos >= 0)
                BackfillObjectChunkHeader(hasContinuation: true);
            flushBuffers.FlushCurrentBuffer();

            // The Offset setter masks a value of SegmentSize to 0, so Offset == 0 after a (non-empty) flush means the flush filled the segment.
            if (flushBuffers.filePosition.Offset == 0)
                flushBuffers.filePosition.AdvanceToNextSegment();
            writeBuffer = flushBuffers.MoveToAndInitializeNextBuffer();

            if (objectHeadered)
                PokeObjectChunkPlaceholder();
        }

        /// <summary>At the end of the headerless prefix: 8-align the object-log position (padding through the buffer) and write the first chunk's
        /// placeholder <see cref="ChunkHeader"/>, entering the headered phase.</summary>
        void StartObjectHeaderedPhase()
        {
            var padLen = (int)((8 - (flushBuffers.GetNextRecordStartPosition().Offset & 7)) & 7);
            while (padLen > 0)
            {
                var n = Math.Min(padLen, writeBuffer.RemainingCapacity);
                writeBuffer.memory.TotalValidSpan.Slice(writeBuffer.currentPosition, n).Clear();
                writeBuffer.currentPosition += n;
                padLen -= n;
                if (writeBuffer.RemainingCapacity == 0)
                    AdvanceObjectBuffer();   // objectHeadered still false here, so this is a plain flush+next (no header)
            }

            objectHeadered = true;
            ObjectLogWriterDiagnostics.LastFirstObjectHeaderRoom = writeBuffer.RemainingCapacity;   // test instrumentation
            PokeObjectChunkPlaceholder();
        }

        /// <summary>Poke an empty placeholder <see cref="ChunkHeader"/> (to be back-filled) at the current 8-aligned buffer position. When exactly
        /// <see cref="ChunkHeader.TotalSize"/> bytes remain to the buffer end (a header landing at buffer_end-8, which only the first post-prefix
        /// header can hit), the header fills the buffer with no data room: the next <see cref="CopyObjectDataBytes"/> sees a full buffer and
        /// <see cref="AdvanceObjectBuffer"/> back-fills this header as a zero-length continuation chunk, resuming the data in the next buffer.</summary>
        void PokeObjectChunkPlaceholder()
        {
            Debug.Assert((flushBuffers.GetNextRecordStartPosition().Offset & 7) == 0, "ChunkHeader must be written on an 8-byte-aligned object-log position");
            var room = writeBuffer.RemainingCapacity;
            Debug.Assert(room >= ChunkHeader.TotalSize, $"no room for a ChunkHeader (RemainingCapacity {room})");

            ChunkHeader placeholder = default;
            currentChunkHeaderBufferPos = writeBuffer.currentPosition;
            new ReadOnlySpan<byte>(&placeholder, ChunkHeader.TotalSize).CopyTo(writeBuffer.memory.TotalValidSpan.Slice(writeBuffer.currentPosition));
            writeBuffer.currentPosition += ChunkHeader.TotalSize;
        }

        /// <summary>Back-fill the current chunk's placeholder <see cref="ChunkHeader.currentLength"/> (| <see cref="ChunkedRecordConstants.ContinuationFlag"/>
        /// when <paramref name="hasContinuation"/>) with the chunk's data length (the bytes written since the header), while it is still in the unflushed buffer.</summary>
        void BackfillObjectChunkHeader(bool hasContinuation)
        {
            var chunkDataLen = writeBuffer.currentPosition - (currentChunkHeaderBufferPos + ChunkHeader.TotalSize);
            Debug.Assert(chunkDataLen >= 0, $"chunk data length {chunkDataLen} must be non-negative");
            if (chunkDataLen == 0)
                ++ObjectLogWriterDiagnostics.ZeroLengthChunkCount;   // test instrumentation (boundary-filler zero-length chunk)
            var currentLength = (uint)chunkDataLen;
            if (hasContinuation)
                currentLength |= unchecked((uint)ChunkedRecordConstants.ContinuationFlag);
            *(uint*)(writeBuffer.memory.GetValidPointer() + currentChunkHeaderBufferPos) = currentLength;   // currentLength is at ChunkHeader FieldOffset(0)
            if (lastObjectFirstChunkExtent == 0)
                lastObjectFirstChunkExtent = flushBuffers.GetNextRecordStartPosition() - objectStartPosition;
            currentChunkHeaderBufferPos = -1;
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

    /// <summary>Test-only instrumentation for the object chunk-framing writer, exercised by the zero-length-chunk boundary test. Non-generic so a
    /// test can observe it independent of the store-functions type. Not used by production logic. NUnit runs these fixtures sequentially, so the
    /// static fields are set/read around a single flush without contention.</summary>
    internal static class ObjectLogWriterDiagnostics
    {
        /// <summary>The write-buffer bytes remaining when the first post-prefix object <see cref="ChunkHeader"/> was placed for the most recent
        /// serialized object; <see cref="ChunkHeader.TotalSize"/> indicates the zero-length-first-chunk boundary. -1 if no object was headered.</summary>
        internal static int LastFirstObjectHeaderRoom = -1;

        /// <summary>Count of zero-length object chunks written (a boundary filler emitted when a <see cref="ChunkHeader"/> lands at buffer_end-8).</summary>
        internal static long ZeroLengthChunkCount;

        internal static void Reset()
        {
            LastFirstObjectHeaderRoom = -1;
            ZeroLengthChunkCount = 0;
        }
    }
}