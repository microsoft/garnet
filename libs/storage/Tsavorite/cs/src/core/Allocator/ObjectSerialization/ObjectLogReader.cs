// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// The class that manages IO read of ObjectAllocator records. It manages the read buffer at two levels:
    /// <list type="bullet">
    ///     <item>At the higher level, called by IO routines, it manages the overall record reading, including issuing additional reads as the buffer is drained.</item>
    ///     <item>At the lower level, it provides the stream for the valueObjectSerializer, which is called via Deserialize() by the higher level.</item>
    /// </list>
    /// </summary>
    internal unsafe partial class ObjectLogReader<TStoreFunctions> : IStreamBuffer
        where TStoreFunctions : IStoreFunctions
    {
        IObjectSerializer<IHeapObject> valueObjectSerializer;
        PinnedMemoryStream<ObjectLogReader<TStoreFunctions>> pinnedMemoryStream;

        /// <summary>The current record header; used for chunks to identify when they need to extract the optionals after the final chunk.</summary>
        internal RecordInfo recordInfo;

        /// <summary>The circular buffer we cycle through for object-log deserialization.</summary>
        readonly CircularDiskReadBuffer readBuffers;

        /// <summary>The <see cref="IStoreFunctions"/> implementation to use</summary>
        internal readonly TStoreFunctions storeFunctions;

        /// <summary>If true, we are in the Deserialize call. If not we ignore things like <see cref="deserializedLength"/> etc.</summary>
        bool inDeserialize;

        /// <summary>The cumulative length of object data read from the device during deserialization.</summary>
        internal ulong deserializedLength;

        // ── Object chunk-framing read state (headered value objects; see ObjectLogWriter.ObjectHeaderlessPrefixLen) ──
        /// <summary>Cumulative object-log bytes consumed since the record began (key + value; data + ChunkHeaders + padding); used to
        /// 8-align the first ChunkHeader after the prefix and to compute the object value's on-disk extent for recovery.</summary>
        ulong recordStreamConsumed;
        /// <summary>True while reading a headered object: <see cref="Read"/> strips ChunkHeaders/padding and follows continuation.</summary>
        bool objectChunked;
        /// <summary>The object's RDH page count is the sentinel: extend the read-ahead in 4 MB blocks as chunks are consumed.</summary>
        bool objectSentinel;
        /// <summary>Headerless-prefix DATA bytes still to serve before the first ChunkHeader.</summary>
        long objectPrefixRemaining;
        /// <summary>DATA bytes still to serve in the current chunk.</summary>
        long objectChunkRemaining;
        /// <summary>Whether the 8-align skip before the first ChunkHeader has been performed.</summary>
        bool objectFirstHeaderRead;
        /// <summary>Low 3 bits of the record's object-log start offset, for the first-header 8-align.</summary>
        int objectRecordStartOffsetLow3;

        /// <summary>The total capacity of the buffer.</summary>
        public bool IsForWrite => false;

#pragma warning disable IDE0290 // Use primary constructor
        public ObjectLogReader(CircularDiskReadBuffer readBuffers, TStoreFunctions storeFunctions)
        {
            this.readBuffers = readBuffers;
            this.storeFunctions = storeFunctions ?? throw new ArgumentNullException(nameof(storeFunctions));
        }

        /// <summary>
        /// Called when one or more records with Objects have been read via ReadAsync, e.g. being processed by AsyncReadPageWithObjectsCallback.
        /// </summary>
        /// <param name="filePosition">The initial file position to read</param>
        /// <param name="totalLength">The cumulative length of all object-log entries for the span of records to be read. We read ahead for all record
        ///     in the ReadAsync call.</param>
        internal void OnBeginReadRecords(ObjectLogFilePositionInfo filePosition, ulong totalLength)
        {
            inDeserialize = false;
            deserializedLength = 0UL;
            readBuffers.OnBeginReadRecords(filePosition, totalLength);
        }

        /// <summary>
        /// Called when one or more records with Objects have been read and via ReadAsync, e.g. being processed by AsyncReadPageWithObjectsCallback,
        /// and we have completed reading and deserializing those objects.
        /// </summary>
        internal void OnEndReadRecords() => readBuffers.OnEndReadRecords();

        /// <inheritdoc/>
        public void FlushAndReset(CancellationToken cancellationToken = default) => throw new InvalidOperationException("FlushAndReset is not supported for DiskStreamReadBuffer");

        /// <inheritdoc/>
        public void Write(ReadOnlySpan<byte> data, CancellationToken cancellationToken = default) => throw new InvalidOperationException("Write is not supported for DiskStreamReadBuffer");

        /// <summary>
        /// Get the object log entries for Overflow Keys and Values and Object Values for the input <paramref name="logRecord"/>. We do not create the log record here;
        /// that was already done by the caller from a single-record disk IO or from Recovery.
        /// <list type="bullet">
        /// <item>If there is an Overflow key, read it and if we have a <paramref name="requestedKey"/> compare it and return false if it does not match.
        ///     Otherwise, store the Key Overflow in the transient <see cref="ObjectIdMap"/> in <paramref name="logRecord"/>.
        ///     If we don't have <paramref name="requestedKey"/>, this is either ReadAtAddress (which is an implicit match) or Scan or Restore.</item>
        /// <item>If we have an Overflow or Object value, read and store it in the transient <see cref="ObjectIdMap"/> in <paramref name="logRecord"/>.</item>
        /// </list>
        /// </summary>
        /// <param name="logRecord">The initial record read from disk from Pending IO, so it is of size <see cref="IStreamBuffer.DefaultInitialIORecordSize"/> or less.</param>
        /// <param name="requestedKey">The requested key, if not ReadAtAddress; we will compare to see if it matches the record.</param>
        /// <param name="segmentSizeBits">Number of bits in segment size</param>
        /// <returns>False if requestedKey is set and we read an Overflow key and it did not match; otherwise true</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public bool ReadRecordObjects<TKey>(ref LogRecord logRecord, TKey requestedKey, int segmentSizeBits)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            Debug.Assert(logRecord.DataHeader.RecordHasObjects, "Inline records should have been checked by the caller");
            if (readBuffers is null)
                throw new TsavoriteException("ReadBuffers are required to ReadRecordObjects");

            // GetObjectLogRecordStartPositionAndLengths returns the exact overflow/object lengths: from the RDH hints for a hint-format
            // record, or from the split RDH+objectId-slot encoding for a legacy record. The object stream carries no separate length prefix.
            var positionWord = logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength);
            var recordStartPosition = new ObjectLogFilePositionInfo(positionWord, segmentSizeBits);
            if (!readBuffers.OnBeginRecord(recordStartPosition))
                throw new TsavoriteException("ReadRecordObjects found no data available in ReadBuffers");

            // Reset per-record object-stream tracking; capture the low 3 bits of the record's object-log start offset for the first-header 8-align.
            recordStreamConsumed = 0;
            objectRecordStartOffsetLow3 = (int)(recordStartPosition.Offset & 7);

            // TODO: Optimize the reading of large internal sector-aligned parts of Overflow Keys and Values to read directly into the overflow, similar to how ObjectLogWriter writes
            //       directly from overflow. This requires changing the read-ahead in CircularDiskReadBuffer.OnBeginReadRecords and the "backfill" in CircularDiskReadBuffer.MoveToNextBuffer.

            // Note: Similar logic to this is in DiskLogRecord.Deserialize.
            var keyWasSet = false;
            try
            {
                if (logRecord.DataHeader.KeyIsOverflow)
                {
                    // For a key at/above the RDH KeyLength sentinel (1023), its full length precedes the key bytes in a leading ChunkHeader;
                    // otherwise the sentinel-capped hint is the exact length.
                    var actualKeyLength = logRecord.DataHeader.KeyLengthIsSentinel ? ReadOverflowHeaderAndExtend(keyLength) : keyLength;
                    // This assignment also allocates the slot in ObjectIdMap, overwriting whatever the objectId slot at keyAddress held
                    // on disk (a stale objectId for a hint-format record, or the key length high bits for a legacy record).
                    logRecord.KeyOverflow = new OverflowByteArray(actualKeyLength, startOffset: 0, endOffset: 0, zeroInit: false);
                    _ = Read(logRecord.KeyOverflow.Span);
                    if (!requestedKey.IsEmpty && !storeFunctions.KeysEqual(requestedKey, logRecord))
                        return false;
                    keyWasSet = true;
                }

                if (logRecord.DataHeader.ValueIsOverflow)
                {
                    // Overflow value v2.2 encoding: a headered value (ValueIsExactSize clear) carries its exact length (and any DMA alignment
                    // padding) in a leading ChunkHeader; a headerless (ValueIsExactSize set) value has its exact length in its objectId size hint.
                    var actualValueLength = logRecord.ValueIsExactSize
                        ? logRecord.ValueObjectIdSizeHint
                        : ReadOverflowHeaderAndExtend((long)valueLength);
                    logRecord.ValueOverflow = new OverflowByteArray(actualValueLength, startOffset: 0, endOffset: 0, zeroInit: false);
                    _ = Read(logRecord.ValueOverflow.Span);
                }
                else if (logRecord.DataHeader.ValueIsObject)
                {
                    // A headered object (data length > cutoff, ValueIsExactSize clear) is [prefix][hdr][chunk]…; the reader strips headers/padding
                    // and follows the continuation chain in ReadObjectData. A headerless object (ValueIsExactSize set) is a plain dense stream. The
                    // deserializer self-terminates in both cases.
                    if (!logRecord.ValueIsExactSize)
                    {
                        objectChunked = true;
                        // The proactive per-chunk read-ahead extension (AdvanceToNextObjectChunk) is only correct when the object's on-disk
                        // extent exceeds the initial read-ahead block, so key it to the RDH page-count sentinel (~4 MB) rather than the coarser
                        // objectId size-hint sentinel (~2 MB). The RDH ValueLength hint is still written for this; retiring it needs an
                        // extent-vs-read-ahead signal (see objectSizeBoundary 2/3 MB window).
                        objectSentinel = RecordDataHeader.FlushValuePageCountIsSentinel((uint)logRecord.DataHeader.GetValueLengthRaw());
                        objectPrefixRemaining = ObjectLogWriter<TStoreFunctions>.ObjectHeaderlessPrefixLen;
                        objectChunkRemaining = 0;
                        objectFirstHeaderRead = false;
                    }
                    DoDeserialize(ref logRecord);
                    objectChunked = false;
                    objectSentinel = false;
                }

                // Restore non-inline length fields to ObjectIdSize for in-memory record length correctness.
                logRecord.OnObjectReadComplete();
                return true;
            }
            catch
            {
                logRecord.OnDeserializationError(keyWasSet);
                throw;
            }
        }

        /// <summary>For an overflow key/value with a leading 8-byte <see cref="ChunkHeader"/> (key at/above its 1023 sentinel; value with the
        /// v2.2 has-header bit set), read that header, extend the read-ahead to cover the full on-disk extent (header + any DMA alignment
        /// padding + data) beyond what the initial read already accounted for, skip the alignment padding, and return the exact data length.</summary>
        /// <param name="alreadyAccounted">Bytes this component already contributed to the initial read total: the key's sentinel-capped RDH
        /// hint, or the value's v2.2 initial read-ahead extent (page count * 4 KB, one 4 MB block for the sentinel, or the exact size).</param>
        int ReadOverflowHeaderAndExtend(long alreadyAccounted)
        {
            var currentLength = ReadOverflowChunkHeader(out var alignmentPadding);

            // Extend the read-ahead only by the shortfall of the full on-disk extent over what the initial read already covered. The initial
            // read for a below-sentinel value already includes the header (page count rounds the whole extent up to 4 KB), so the shortfall is
            // <= 0 and no extend is issued; a sentinel value (initial read one 4 MB block) or a sentinel-capped key extends by the remainder.
            var extra = (ChunkHeader.TotalSize + (long)alignmentPadding + currentLength) - alreadyAccounted;
            if (extra > 0)
                readBuffers.ExtendUnreadLengthRemaining(extra);

            // Skip any O_DIRECT alignment padding between the header and the sector-aligned data start (0 on the buffered write path).
            if (alignmentPadding > 0)
                SkipReadBytes(alignmentPadding);
            return currentLength;
        }

        /// <summary>Read the 8-byte <see cref="ChunkHeader"/> that precedes an overflow (key or value) with a leading header, returning the
        /// full overflow length from <see cref="ChunkHeader.currentLength"/> and its O_DIRECT alignment padding via <paramref name="alignmentPadding"/>.
        /// <para>This issues no extra IO: the header is included in the value's v2.2 initial read extent (or the key's sentinel-capped hint)
        /// passed to <see cref="CircularDiskReadBuffer.OnBeginReadRecords"/> — the single-record read path sums those per-component hints (see
        /// <c>ObjectAllocatorImpl.VerifyRecordFromDiskCallback</c>) and multi-record scans size from absolute position differences — so the
        /// header bytes are already present in the read-ahead ring. The header may still span read buffers or a segment boundary, so it is read
        /// through the buffered <see cref="Read(Span{byte}, CancellationToken)"/>.</para></summary>
        int ReadOverflowChunkHeader(out int alignmentPadding)
        {
            ChunkHeader header = default;
            var n = Read(new Span<byte>(&header, ChunkHeader.TotalSize));
            if (n != ChunkHeader.TotalSize)
                throw new TsavoriteException($"Expected {ChunkHeader.TotalSize} ChunkHeader bytes but read {n}");
            alignmentPadding = (int)header.alignmentPadding;
            return (int)header.currentLength;
        }

        /// <summary>Consume and discard <paramref name="count"/> bytes from the read stream (e.g. O_DIRECT alignment padding between an overflow
        /// ChunkHeader and its sector-aligned data). <paramref name="count"/> is less than the device sector size.</summary>
        void SkipReadBytes(int count)
        {
            Span<byte> discard = stackalloc byte[512];
            while (count > 0)
            {
                var chunk = count < discard.Length ? count : discard.Length;
                var n = Read(discard.Slice(0, chunk));
                if (n != chunk)
                    throw new TsavoriteException($"Expected to skip {chunk} alignment-padding bytes but read {n}");
                count -= chunk;
            }
        }

        /// <inheritdoc/>
        public int Read(Span<byte> destinationSpan, CancellationToken cancellationToken = default)
        {
            // A headered value object is read through the chunk-stripping path; everything else (overflow, headerless object, ChunkHeader
            // reads, padding skips) reads raw stream bytes.
            if (objectChunked)
                return ReadObjectData(destinationSpan, cancellationToken);
            return ReadRawStream(destinationSpan, cancellationToken);
        }

        /// <summary>Read up to <paramref name="destinationSpan"/>.Length raw bytes from the object-log read-ahead ring, extending the ring in
        /// 4 MB blocks for a sentinel object still being deserialized. Tracks <see cref="recordStreamConsumed"/> (and, during deserialize,
        /// <see cref="deserializedLength"/>).</summary>
        int ReadRawStream(Span<byte> destinationSpan, CancellationToken cancellationToken = default)
        {
            var prevCopyLength = 0;
            var destinationSpanAppend = destinationSpan.Slice(prevCopyLength);

            // Read from the circular buffer.
            var buffer = readBuffers.GetCurrentBuffer();
            if (buffer is null || !buffer.HasData)
                return 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();   // IDevice does not support cancellation, so just check this here

                var copyLength = buffer.AvailableLength;
                if (copyLength > destinationSpanAppend.Length)
                    copyLength = destinationSpanAppend.Length;

                if (copyLength > 0)
                {
                    buffer.AvailableSpan.Slice(0, copyLength).CopyTo(destinationSpanAppend);
                    buffer.currentPosition += copyLength;
                    recordStreamConsumed += (uint)copyLength;
                    if (inDeserialize)
                        deserializedLength += (uint)copyLength;
                    if (copyLength == destinationSpanAppend.Length)
                        return destinationSpan.Length;
                }

                prevCopyLength += copyLength;
                if (buffer.AvailableLength == 0)
                {
                    if (!readBuffers.MoveToNextBuffer(out buffer))
                        return prevCopyLength;
                }
                destinationSpanAppend = destinationSpan.Slice(prevCopyLength);
            }
        }

        /// <summary>Serve object DATA bytes to the deserializer, transparently stripping the headerless-prefix boundary, the 8-align padding,
        /// per-chunk <see cref="ChunkHeader"/>s, and zero-length continuation chunks. The deserializer self-terminates, so this is driven by
        /// its requested lengths.</summary>
        int ReadObjectData(Span<byte> destinationSpan, CancellationToken cancellationToken)
        {
            var filled = 0;
            while (filled < destinationSpan.Length)
            {
                if (objectPrefixRemaining > 0)
                {
                    var want = (int)Math.Min(objectPrefixRemaining, destinationSpan.Length - filled);
                    var n = ReadRawStream(destinationSpan.Slice(filled, want), cancellationToken);
                    if (n == 0)
                        break;
                    objectPrefixRemaining -= n;
                    filled += n;
                    continue;
                }
                if (objectChunkRemaining == 0)
                {
                    if (!AdvanceToNextObjectChunk())
                        break;
                    continue;
                }
                var m = (int)Math.Min(objectChunkRemaining, destinationSpan.Length - filled);
                var got = ReadRawStream(destinationSpan.Slice(filled, m), cancellationToken);
                if (got == 0)
                    break;
                objectChunkRemaining -= got;
                filled += got;
            }
            return filled;
        }

        /// <summary>Advance to the next object chunk: on the first call, skip the 8-align padding after the prefix; then read the next
        /// <see cref="ChunkHeader"/>, skipping zero-length continuation chunks. Returns false if a terminal (non-continuation) empty chunk ends the object.</summary>
        bool AdvanceToNextObjectChunk()
        {
            if (!objectFirstHeaderRead)
            {
                // The first ChunkHeader is 8-aligned in the object-log; skip the padding written after the 1023-byte prefix.
                var padLen = (8 - ((objectRecordStartOffsetLow3 + (int)(recordStreamConsumed & 7)) & 7)) & 7;
                if (padLen > 0)
                {
                    Span<byte> discard = stackalloc byte[8];
                    var n = ReadRawStream(discard.Slice(0, padLen));
                    if (n != padLen)
                        throw new TsavoriteException($"Expected {padLen} object 8-align pad bytes but read {n}");
                }
                objectFirstHeaderRead = true;
            }

            while (true)
            {
                ChunkHeader header = default;
                var n = ReadRawStream(new Span<byte>(&header, ChunkHeader.TotalSize));
                if (n != ChunkHeader.TotalSize)
                    throw new TsavoriteException($"Expected {ChunkHeader.TotalSize} object ChunkHeader bytes but read {n}");
                var raw = header.currentLength;
                objectChunkRemaining = raw & ~unchecked((uint)ChunkedRecordConstants.ContinuationFlag);
                var continues = (raw & unchecked((uint)ChunkedRecordConstants.ContinuationFlag)) != 0;
                if (objectChunkRemaining > 0)
                {
                    // Keep the read-ahead ring ahead of consumption for a sentinel object (its initial read-ahead is only one 4 MB block):
                    // proactively request this chunk's data plus the next chunk's header, mirroring the overflow ReadOverflowHeaderAndExtend pattern.
                    if (objectSentinel)
                        readBuffers.ExtendUnreadLengthRemaining(objectChunkRemaining + ChunkHeader.TotalSize);
                    return true;                // a real (possibly final) chunk
                }
                if (!continues)
                    return false;               // terminal zero-length chunk: object done
                // zero-length continuation chunk (boundary filler): read the next header.
            }
        }

        void DoDeserialize(ref LogRecord logRecord)
        {
            deserializedLength = 0;
            inDeserialize = true;
            var startConsumed = recordStreamConsumed;   // the object value's on-disk bytes begin here (after the key)

            // If we haven't yet instantiated the serializer do so now.
            if (valueObjectSerializer is null)
            {
                pinnedMemoryStream = new(this);
                valueObjectSerializer = storeFunctions.CreateValueObjectSerializer();
                valueObjectSerializer.BeginDeserialize(pinnedMemoryStream);
            }

            valueObjectSerializer.Deserialize(out var valueObject);

            // Store the object value's on-disk EXTENT (data + 8-align padding + ChunkHeaders for a headered object; == data length for a
            // headerless one), not the deserialized data length, so recovery reconstructs the correct on-disk footprint and RDH page-count hint.
            var objectExtent = recordStreamConsumed - startConsumed;
            logRecord.SetDeserializedValueObject(valueObject, objectExtent);
            OnDeserializeComplete(valueObject);
        }

        void OnDeserializeComplete(IHeapObject valueObject)
        {
            // TODO add size tracking; do not track deserialization size changes if we are deserializing to a frame

            inDeserialize = false;
            deserializedLength = 0UL;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            pinnedMemoryStream?.Dispose();
            valueObjectSerializer?.EndDeserialize();
        }
    }
}