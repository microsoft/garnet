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
            if (!readBuffers.OnBeginRecord(new ObjectLogFilePositionInfo(positionWord, segmentSizeBits)))
                throw new TsavoriteException("ReadRecordObjects found no data available in ReadBuffers");

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
                    // Overflow value v2.2 encoding: a value with the has-header bit set carries its exact length (and any DMA alignment
                    // padding) in a leading ChunkHeader; a headerless (isExactSize) value has its exact length in the RDH ValueLength field.
                    var encodedValue = (uint)logRecord.DataHeader.GetValueLengthRaw();
                    var actualValueLength = RecordDataHeader.FlushValueHasHeader(encodedValue)
                        ? ReadOverflowHeaderAndExtend((long)valueLength)
                        : RecordDataHeader.FlushValueExactByteSize(encodedValue);
                    logRecord.ValueOverflow = new OverflowByteArray(actualValueLength, startOffset: 0, endOffset: 0, zeroInit: false);
                    _ = Read(logRecord.ValueOverflow.Span);
                }
                else if (logRecord.DataHeader.ValueIsObject)
                {
                    // Chunked (multi-buffer, >= sentinel) and headerless (< sentinel, exact) objects both size their read-ahead from the RDH
                    // ValueLength encoding decoded in GetObjectLogRecordStartPositionAndLengths; the object stream carries no per-chunk framing
                    // and the deserializer self-terminates, so nothing extra is parsed here.
                    DoDeserialize(ref logRecord);
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
            // This is called by valueObjectSerializer.Deserialize() to read up to destinationSpan.Length bytes.
            // It is also currently called internally for Overflow.
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

        void DoDeserialize(ref LogRecord logRecord)
        {
            deserializedLength = 0;
            inDeserialize = true;

            // If we haven't yet instantiated the serializer do so now.
            if (valueObjectSerializer is null)
            {
                pinnedMemoryStream = new(this);
                valueObjectSerializer = storeFunctions.CreateValueObjectSerializer();
                valueObjectSerializer.BeginDeserialize(pinnedMemoryStream);
            }

            valueObjectSerializer.Deserialize(out var valueObject);
            logRecord.SetDeserializedValueObject(valueObject, deserializedLength);
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