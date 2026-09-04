// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Reads out-of-line record components from the object log. It manages the read buffer at two levels:
    /// <list type="bullet">
    ///     <item>At the record level, it decodes exact-size flags and framing, updates the ring's absolute demand endpoint, and chooses
    ///     buffered or direct overflow reads.</item>
    ///     <item>At the stream level, it supplies dense object data to the serializer while consuming object-log headers and padding internally.</item>
    /// </list>
    /// </summary>
    /// <remarks><see cref="CircularDiskReadBuffer"/> owns device-read submission and sector alignment. This class owns the logical stream
    /// position within a record. Framing converts record-relative lengths into absolute endpoints so the ring can submit enough IO without
    /// treating an initial size hint as an authoritative total length.</remarks>
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
        /// <summary>Current record's object-log start position, used to convert framing-relative offsets to absolute read endpoints.</summary>
        ObjectLogFilePositionInfo recordStartPosition;
        /// <summary>Headerless-prefix DATA bytes still to serve before the first ChunkHeader.</summary>
        long objectPrefixRemaining;
        /// <summary>DATA bytes still to serve in the current chunk.</summary>
        long objectChunkRemaining;
        /// <summary>Whether the 8-align skip before the first ChunkHeader has been performed.</summary>
        bool objectFirstHeaderRead;
        /// <summary>Low 3 bits of the record's object-log start offset, for the first-header 8-align.</summary>
        int objectRecordStartOffsetLow3;
        /// <summary>Continuation flag of the most recently entered data-bearing chunk. Only the writer's serialize-completion back-fills a
        /// non-continuing header, so a data chunk with this clear is provably the object's final chunk. Consulted only in copy-to-end mode.</summary>
        bool objectCurrentChunkContinues;
        /// <summary>When set (a recovery verbatim copy via <see cref="CopyRecordObjectsFollowingFraming"/>, which has no deserializer to
        /// self-terminate on), <see cref="ReadObjectData"/> ends the object after consuming a final (non-continuing) data chunk rather than
        /// reading another header. Clear on the normal deserialize path, which is driven to completion by the serializer's requested lengths.</summary>
        bool objectFollowToEnd;

        /// <summary>When set (a recovery verbatim copy via <see cref="CopyRecordObjectsFollowingFraming"/>), every raw byte consumed from the
        /// read-ahead ring is also written to this sink, so a framing walk that discards the deserialized data still produces a byte-exact
        /// copy of the record's object-log extent into the main object-log. Null during normal reads.</summary>
        ObjectLogWriter<TStoreFunctions> verbatimCopySink;

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
        /// <param name="totalLength">Initial object-log extent for the first record(s). Framing may revise the dynamic endpoint while
        /// records are consumed.</param>
        /// <param name="hardReadEndPosition">Exclusive durable tail in the same object-log address space as
        /// <paramref name="filePosition"/>. For a snapshot object log with no supplied logical tail, pass a position whose word is
        /// <see cref="ObjectLogFilePositionInfo.NotSet"/> and whose segment-size bits match the reader; the reader must not use the main
        /// object-log tail as a cross-device bound. An unset main tail likewise disables the bound during early recovery.</param>
        internal void OnBeginReadRecords(ObjectLogFilePositionInfo filePosition, ulong totalLength, ObjectLogFilePositionInfo hardReadEndPosition)
        {
            inDeserialize = false;
            deserializedLength = 0UL;
            readBuffers.OnBeginReadRecords(filePosition, totalLength, hardReadEndPosition);
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

            // GetObjectLogRecordStartPositionAndLengths returns initial read extents from objectId hints for a current record, or exact
            // lengths from the split RDH+objectId-slot encoding for a legacy record.
            var positionWord = logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength);
            var isLegacy = logRecord.HasReuseObjectIdForSize;
            recordStartPosition = new ObjectLogFilePositionInfo(positionWord, segmentSizeBits);
            var initialLength = logRecord.DataHeader.KeyIsOverflow ? (ulong)keyLength : valueLength;
            var initialEnd = recordStartPosition;
            initialEnd.Advance(initialLength);
            readBuffers.SetDynamicReadThrough(initialEnd,
                isDiscoveryWindow: !isLegacy && (logRecord.DataHeader.KeyIsOverflow ? !logRecord.KeyIsExactSize : !logRecord.ValueIsExactSize));
            if (!readBuffers.OnBeginRecord(recordStartPosition))
                throw new TsavoriteException("ReadRecordObjects found no data available in ReadBuffers");

            // Reset per-record object-stream tracking; capture the low 3 bits of the record's object-log start offset for the first-header 8-align.
            recordStreamConsumed = 0;
            objectRecordStartOffsetLow3 = (int)(recordStartPosition.Offset & 7);

            // Note: Similar logic to this is in DiskLogRecord.Deserialize.
            var keyWasSet = false;
            try
            {
                if (logRecord.DataHeader.KeyIsOverflow)
                {
                    // This assignment also allocates the slot in ObjectIdMap, overwriting whatever the objectId slot at keyAddress held
                    // on disk (a stale objectId for a hint-format record, or the key length high bits for a legacy record).
                    var keyIsExactSize = isLegacy || logRecord.KeyIsExactSize;
                    var exactKeyLength = isLegacy ? keyLength : logRecord.KeyObjectIdSizeHint;
                    logRecord.KeyOverflow = ReadOverflow(keyIsExactSize, exactKeyLength);
                    if (!requestedKey.IsEmpty && !storeFunctions.KeysEqual(requestedKey, logRecord))
                        return false;
                    keyWasSet = true;
                }

                // The record-level base extent was formed from the key's initial hint. Once a headered key reveals its exact length,
                // rebase the following value's initial requirement at the actual key end.
                if (!logRecord.DataHeader.ValueIsInline)
                {
                    var valueExtentIsDiscovery = !isLegacy && !logRecord.ValueIsExactSize;
                    SetDynamicRecordReadThrough(recordStreamConsumed + valueLength, valueExtentIsDiscovery);
                }

                if (logRecord.DataHeader.ValueIsOverflow)
                {
                    // A headered overflow value (ValueIsExactSize clear) carries its exact length (and any DMA alignment
                    // padding) in a leading ChunkHeader; a headerless (ValueIsExactSize set) value has its exact length in its objectId size hint.
                    var valueIsExactSize = isLegacy || logRecord.ValueIsExactSize;
                    var exactValueLength = isLegacy ? checked((int)valueLength) : logRecord.ValueObjectIdSizeHint;
                    logRecord.ValueOverflow = ReadOverflow(valueIsExactSize, exactValueLength);
                }
                else if (logRecord.DataHeader.ValueIsObject)
                {
                    // A headered object (data length > cutoff, ValueIsExactSize clear) is [prefix][hdr][chunk]…; the reader strips headers/padding
                    // and follows the continuation chain in ReadObjectData. A headerless object (ValueIsExactSize set) is a plain dense stream. The
                    // deserializer self-terminates in both cases.
                    if (!isLegacy && !logRecord.ValueIsExactSize)
                    {
                        objectChunked = true;
                        objectPrefixRemaining = ObjectLogWriter<TStoreFunctions>.ObjectHeaderlessPrefixLen;
                        objectChunkRemaining = 0;
                        objectFirstHeaderRead = false;
                    }
                    DoDeserialize(ref logRecord);
                    objectChunked = false;
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

        /// <summary>Recovery Pass 1 (index build) helper: read ONLY this record's overflow key from the object log and return its hash code,
        /// without reading the value or touching the transient <see cref="ObjectIdMap"/> (which is not populated until Pass 2, so
        /// <see cref="LogRecord.Key"/> cannot resolve an overflow key during Pass 1). Mirrors the overflow-key branch of
        /// <see cref="ReadRecordObjects{TKey}(ref LogRecord, TKey, int)"/>: the key bytes are read into a temporary pinned buffer and hashed via
        /// the store's comparer (which hashes key bytes only). The caller must have set up the read-ahead via <see cref="OnBeginReadRecords"/> at
        /// the record's object-log start position, and the record must have an overflow key.</summary>
        internal long ReadOverflowKeyHashCodeForRecovery(in LogRecord logRecord, int segmentSizeBits)
        {
            Debug.Assert(logRecord.DataHeader.KeyIsOverflow, "Record must have an overflow key");
            if (readBuffers is null)
                throw new TsavoriteException("ReadBuffers are required to ReadOverflowKeyHashCodeForRecovery");

            var positionWord = logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out _);
            recordStartPosition = new ObjectLogFilePositionInfo(positionWord, segmentSizeBits);
            if (!readBuffers.OnBeginRecord(recordStartPosition))
                throw new TsavoriteException("ReadOverflowKeyHashCodeForRecovery found no data available in ReadBuffers");

            // Reset per-record object-stream tracking; capture the low 3 bits of the record's object-log start offset for the first-header 8-align.
            recordStreamConsumed = 0;
            objectRecordStartOffsetLow3 = (int)(recordStartPosition.Offset & 7);

            var keyIsExactSize = logRecord.HasReuseObjectIdForSize || logRecord.KeyIsExactSize;
            var exactKeyLength = logRecord.HasReuseObjectIdForSize ? keyLength : logRecord.KeyObjectIdSizeHint;
            var overflow = ReadOverflow(keyIsExactSize, exactKeyLength);
            fixed (byte* keyPtr = overflow.Span)
            {
                var key = ConditionallyHoistedKey.CreatePinned(keyPtr, overflow.Length);
                return storeFunctions.GetKeyHashCode64(key);
            }
        }

        /// <summary>Recovery snapshot-copy helper that walks this record's object-log framing (overflow key, then overflow or
        /// object value), following the ChunkHeader chain to its exact on-disk extent and self-extending the read-ahead as chunks are consumed,
        /// WITHOUT materializing the objects. Every raw byte consumed is tee'd to <paramref name="sink"/> (see <see cref="verbatimCopySink"/>) so
        /// the record's object bytes are copied byte-exact into the main object-log. This positions the read-ahead ring at
        /// <paramref name="snapshotPositionWord"/> itself. Returns the exact object-log bytes consumed (key + value).</summary>
        internal ulong CopyRecordObjectsFollowingFraming(in LogRecord logRecord, ulong snapshotPositionWord, int keyLength, ulong valueLength, int segmentSizeBits,
            ObjectLogWriter<TStoreFunctions> sink)
        {
            Debug.Assert(logRecord.DataHeader.RecordHasObjects, "Record must have objects");
            if (readBuffers is null)
                throw new TsavoriteException("ReadBuffers are required to CopyRecordObjectsFollowingFraming");

            recordStartPosition = new ObjectLogFilePositionInfo(snapshotPositionWord, segmentSizeBits);
            var initialLength = logRecord.DataHeader.KeyIsOverflow ? (ulong)keyLength : valueLength;
            var initialEnd = recordStartPosition;
            initialEnd.Advance(initialLength);
            readBuffers.SetDynamicReadThrough(initialEnd,
                isDiscoveryWindow: logRecord.DataHeader.KeyIsOverflow ? !logRecord.KeyIsExactSize : !logRecord.ValueIsExactSize);
            if (!readBuffers.OnBeginRecord(recordStartPosition))
                throw new TsavoriteException("CopyRecordObjectsFollowingFraming found no data available in ReadBuffers");
            recordStreamConsumed = 0;
            objectRecordStartOffsetLow3 = (int)(recordStartPosition.Offset & 7);

            var dataHeader = logRecord.DataHeader;
            var rented = ArrayPool<byte>.Shared.Rent(IStreamBuffer.BufferSize);
            verbatimCopySink = sink;
            try
            {
                var discard = new Span<byte>(rented, 0, IStreamBuffer.BufferSize);

                if (dataHeader.KeyIsOverflow)
                {
                    var actualKeyLength = logRecord.KeyIsExactSize ? logRecord.KeyObjectIdSizeHint : ReadOverflowHeaderAndSetEndpoint();
                    DrainRawStream((ulong)actualKeyLength, discard);
                }

                if (!dataHeader.ValueIsInline)
                    SetDynamicRecordReadThrough(recordStreamConsumed + valueLength, isDiscoveryWindow: !logRecord.ValueIsExactSize);

                if (dataHeader.ValueIsOverflow)
                {
                    var actualValueLength = logRecord.ValueIsExactSize ? logRecord.ValueObjectIdSizeHint : ReadOverflowHeaderAndSetEndpoint();
                    DrainRawStream((ulong)actualValueLength, discard);
                }
                else if (dataHeader.ValueIsObject)
                {
                    if (logRecord.ValueIsExactSize)
                    {
                        DrainRawStream((ulong)logRecord.ValueObjectIdSizeHint, discard);
                    }
                    else
                    {
                        objectChunked = true;
                        objectFollowToEnd = true;
                        objectCurrentChunkContinues = true;   // "not yet at the final chunk"; overwritten as each data chunk's header is read
                        objectPrefixRemaining = ObjectLogWriter<TStoreFunctions>.ObjectHeaderlessPrefixLen;
                        objectChunkRemaining = 0;
                        objectFirstHeaderRead = false;
                        try
                        {
                            // ReadObjectData strips headers/padding and follows the continuation chain; in copy-to-end mode it self-terminates
                            // after the final (non-continuing) data chunk. Drive it with a full discard buffer until it returns short.
                            while (ReadObjectData(discard, default) == discard.Length)
                            { }
                        }
                        finally
                        {
                            objectChunked = false;
                            objectFollowToEnd = false;
                        }
                    }
                }
            }
            finally
            {
                verbatimCopySink = null;
                ArrayPool<byte>.Shared.Return(rented);
            }
            return recordStreamConsumed;
        }

        /// <summary>Consume exactly <paramref name="count"/> raw object-log bytes (tee'd verbatim to <see cref="verbatimCopySink"/> during a
        /// recovery copy), reading through <paramref name="discard"/> in buffer-sized slices; the data itself is not retained.</summary>
        void DrainRawStream(ulong count, Span<byte> discard)
        {
            while (count > 0)
            {
                var want = (int)Math.Min(count, (ulong)discard.Length);
                var n = ReadRawStream(discard.Slice(0, want));
                if (n == 0)
                    throw new TsavoriteException("Unexpected end of snapshot object-log data while copying record objects during recovery");
                count -= (ulong)n;
            }
        }

        /// <summary>Read a framed overflow header, set the component's exact absolute endpoint, skip DMA padding, and return its payload length.</summary>
        int ReadOverflowHeaderAndSetEndpoint()
        {
            var componentStart = recordStreamConsumed;
            var currentLength = ReadOverflowChunkHeader(out var alignmentPadding);
            SetDynamicRecordReadThrough(componentStart + ChunkHeader.TotalSize + (ulong)alignmentPadding + (uint)currentLength,
                isDiscoveryWindow: false);

            // Skip any O_DIRECT alignment padding between the header and the sector-aligned data start (0 on the buffered write path).
            if (alignmentPadding > 0)
                SkipReadBytes(alignmentPadding);
            return currentLength;
        }

        /// <summary>Convert a record-relative exclusive endpoint to its absolute object-log address and replace the ring's current framing
        /// demand. The same method handles both speculative discovery windows and authoritative endpoints parsed from headers.</summary>
        void SetDynamicRecordReadThrough(ulong recordRelativeEnd, bool isDiscoveryWindow)
        {
            var end = recordStartPosition;
            end.Advance(recordRelativeEnd);
            readBuffers.SetDynamicReadThrough(end, isDiscoveryWindow);
        }

        /// <summary>Read the 8-byte <see cref="ChunkHeader"/> that precedes an overflow (key or value) with a leading header, returning the
        /// full overflow length from <see cref="ChunkHeader.currentLength"/> and its O_DIRECT alignment padding via <paramref name="alignmentPadding"/>.
        /// <para>This submits no separate IO for the header: it is included in the component's initial read extent
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
            if (buffer is null || (!buffer.HasData && !buffer.WaitForDataAvailable()))
                return 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();   // IDevice does not support cancellation, so just check this here

                var copyLength = buffer.AvailableLength;
                if (copyLength > destinationSpanAppend.Length)
                    copyLength = destinationSpanAppend.Length;

                if (copyLength > 0)
                {
                    var consumed = buffer.AvailableSpan.Slice(0, copyLength);
                    consumed.CopyTo(destinationSpanAppend);
                    verbatimCopySink?.Write(consumed);
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

        /// <summary>Copy only data already present in the current ring buffer. Do not wait for or submit additional ring IO when that
        /// buffer is exhausted; a large overflow direct read will supply the remainder into the final allocation.</summary>
        int ReadRawStreamAvailable(Span<byte> destinationSpan)
        {
            var buffer = readBuffers.GetCurrentBuffer();
            if (buffer is null || !buffer.HasData)
                return 0;

            // One ring buffer is the maximum initial discovery window (4 MB). The direct read bypasses later ring demand,
            // and RepositionAfterDirectRead drains submitted read-ahead before resetting the ring.
            var copyLength = Math.Min(buffer.AvailableLength, destinationSpan.Length);
            buffer.AvailableSpan.Slice(0, copyLength).CopyTo(destinationSpan);
            buffer.currentPosition += copyLength;
            recordStreamConsumed += (uint)copyLength;
            return copyLength;
        }

        /// <summary>Read one overflow key or value. Exact-size components allocate their final array and copy the known byte count from the
        /// ring. Framed components first parse the authoritative payload length and alignment padding. Small payloads then copy from the
        /// ring; large payloads allocate the final array with sector slack, copy bytes already present in the current buffer, direct-read
        /// the aligned remainder into that array, and reset ring consumption at the payload end.</summary>
        OverflowByteArray ReadOverflow(bool isExactSize, int initialLength)
        {
            if (isExactSize)
            {
                var exactOverflow = new OverflowByteArray(initialLength, startOffset: 0, endOffset: 0, zeroInit: false);
                if (Read(exactOverflow.Span) != initialLength)
                    throw new TsavoriteException($"Expected {initialLength} headerless overflow bytes");
                return exactOverflow;
            }

            var length = ReadOverflowHeaderAndSetEndpoint();
            if (length <= ObjectLogWriter<TStoreFunctions>.MaxCopySpanLen)
            {
                var bufferedOverflow = new OverflowByteArray(length, startOffset: 0, endOffset: 0, zeroInit: false);
                if (Read(bufferedOverflow.Span) != length)
                    throw new TsavoriteException($"Expected {length} framed overflow bytes");
                return bufferedOverflow;
            }

            var payloadPosition = recordStartPosition;
            payloadPosition.Advance(recordStreamConsumed);
            var sectorSize = (int)readBuffers.SectorSize;
            var overflow = new OverflowByteArray(length + (3 * sectorSize), startOffset: 0, endOffset: 0, zeroInit: false);
            var handle = overflow.Pin();
            try
            {
                var allocationAddress = (nuint)handle.AddrOfPinnedObject();
                var desiredResidue = (int)(payloadPosition.Offset % (uint)sectorSize);
                var allocationResidue = (int)((allocationAddress + 8) % (uint)sectorSize);
                var startOffset = sectorSize + ((desiredResidue - allocationResidue + sectorSize) % sectorSize);
                overflow.SetAlignedReadOffsets(startOffset, (3 * sectorSize) - startOffset);

                var copied = ReadRawStreamAvailable(overflow.Span);
                if (copied < length)
                {
                    var directPosition = payloadPosition;
                    directPosition.Advance((ulong)copied);
                    var leadingBytes = (int)(directPosition.Offset % (uint)sectorSize);
                    directPosition.Offset -= (uint)leadingBytes;

                    var payloadEnd = payloadPosition;
                    payloadEnd.Advance((ulong)length);
                    var directEndAddress = (payloadEnd.CurrentAddress + (uint)sectorSize - 1) & ~((ulong)(uint)sectorSize - 1);
                    var directLength = directEndAddress - directPosition.CurrentAddress;
                    var destination = handle.AddrOfPinnedObject() + overflow.StartOffset + copied - leadingBytes;
                    readBuffers.ReadDirect(directPosition, destination, (long)directLength);

                    recordStreamConsumed += (uint)(length - copied);
                    readBuffers.RepositionAfterDirectRead(payloadEnd);
                }
                return overflow;
            }
            finally
            {
                handle.Free();
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
                    // Copy-to-end mode has no deserializer to stop it: once the final (non-continuing) data chunk is fully consumed, the object
                    // is complete -- ending here avoids reading the next record's bytes as a spurious header. The exact-buffer-boundary case,
                    // where the last data chunk continues into a trailing zero-length terminal chunk, still stops via AdvanceToNextObjectChunk.
                    if (objectFollowToEnd && objectFirstHeaderRead && !objectCurrentChunkContinues)
                        break;
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
                // The first ChunkHeader is 8-aligned in the object log; skip the padding written after the headerless prefix.
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
                    var chunkStart = recordStartPosition;
                    chunkStart.Advance(recordStreamConsumed);
                    if ((ulong)objectChunkRemaining > chunkStart.RemainingSizeInSegment)
                        throw new TsavoriteException($"Object chunk length {objectChunkRemaining} crosses segment {chunkStart.SegmentId} from offset {chunkStart.Offset}");
                    // Remember this chunk's continuation flag so copy-to-end mode can stop after the final (non-continuing) data chunk.
                    objectCurrentChunkContinues = continues;
                    var chunkEnd = recordStreamConsumed + (ulong)objectChunkRemaining;
                    // A continuation opens a normal 4 MB discovery window at the following header. A final chunk tightens the logical
                    // requirement to its exact endpoint; already-submitted physical over-read remains reusable by later components of this record.
                    if (continues)
                        SetDynamicRecordReadThrough(chunkEnd + ChunkHeader.TotalSize, isDiscoveryWindow: false);
                    SetDynamicRecordReadThrough(continues ? chunkEnd + (ulong)IStreamBuffer.BufferSize : chunkEnd,
                        isDiscoveryWindow: continues);
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
            // headerless one), not the deserialized data length, so recovery reconstructs the correct on-disk footprint and objectId size hint.
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