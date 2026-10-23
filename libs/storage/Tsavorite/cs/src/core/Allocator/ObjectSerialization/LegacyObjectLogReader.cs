// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Reads out-of-line record components from a downlevel (version 7) object log: a dense file in which a record's overflow key, then its
    /// overflow value or serialized object, follow one another with no framing, at the position and with the exact lengths the record carries.
    /// </summary>
    /// <remarks>Used only to recover a version 7 checkpoint, whose objects recovery re-serializes into the current object log. The
    /// <see cref="CircularDiskReadBuffer"/> owns device-read submission and sector alignment; this class owns the logical stream position within
    /// a record, converting its exact lengths into absolute endpoints so the ring reads what each component needs.</remarks>
    internal unsafe class LegacyObjectLogReader<TStoreFunctions> : IStreamBuffer
        where TStoreFunctions : IStoreFunctions
    {
        IObjectSerializer<IHeapObject> valueObjectSerializer;
        PinnedMemoryStream<LegacyObjectLogReader<TStoreFunctions>> pinnedMemoryStream;

        /// <summary>The circular buffer we cycle through for object-log deserialization.</summary>
        readonly CircularDiskReadBuffer readBuffers;

        /// <summary>The <see cref="IStoreFunctions"/> implementation to use</summary>
        internal readonly TStoreFunctions storeFunctions;

        /// <summary>Object-log bytes consumed since the record began (key and value).</summary>
        ulong recordStreamConsumed;

        /// <summary>Bytes the value deserializer may still consume for the current record; -1 when no object is being read. This keeps a
        /// deserializer that asks for more than it wrote from consuming the following record's bytes.</summary>
        long objectDataBudget = -1;

        /// <summary>Current record's object-log start position, used to convert record-relative lengths to absolute read endpoints.</summary>
        LegacyObjectLogPosition recordStartPosition;

        /// <inheritdoc/>
        public bool IsForWrite => false;

        /// <summary>Create a reader over <paramref name="readBuffers"/>.</summary>
        public LegacyObjectLogReader(CircularDiskReadBuffer readBuffers, TStoreFunctions storeFunctions)
        {
            this.readBuffers = readBuffers ?? throw new ArgumentNullException(nameof(readBuffers));
            this.storeFunctions = storeFunctions ?? throw new ArgumentNullException(nameof(storeFunctions));
        }

        /// <summary>
        /// Called when one or more records with Objects have been read, before their components are read.
        /// </summary>
        /// <param name="filePosition">The initial file position to read</param>
        /// <param name="totalLength">Initial object-log extent for the first record(s); record lengths extend it as records are consumed.</param>
        /// <param name="hardReadEndPosition">Exclusive durable end in the same object-log address space as <paramref name="filePosition"/>, or a
        /// position whose word is <see cref="ObjectLogPositionInfo.NotSet"/> for no bound.</param>
        internal void OnBeginReadRecords(LegacyObjectLogPosition filePosition, ulong totalLength, LegacyObjectLogPosition hardReadEndPosition)
            => readBuffers.OnBeginReadRecords(filePosition, totalLength, hardReadEndPosition);

        /// <summary>
        /// Called when the records begun by <see cref="OnBeginReadRecords"/> have been read.
        /// </summary>
        internal void OnEndReadRecords() => readBuffers.OnEndReadRecords();

        /// <inheritdoc/>
        public void FlushAndReset(CancellationToken cancellationToken = default) => throw new InvalidOperationException("FlushAndReset is not supported for LegacyObjectLogReader");

        /// <inheritdoc/>
        public void Write(ReadOnlySpan<byte> data, CancellationToken cancellationToken = default) => throw new InvalidOperationException("Write is not supported for LegacyObjectLogReader");

        /// <summary>
        /// Read the overflow key and the overflow or object value of <paramref name="logRecord"/> and store them in its <see cref="ObjectIdMap"/>.
        /// </summary>
        /// <param name="logRecord">A downlevel disk-image record.</param>
        /// <param name="requestedKey">The requested key, if not ReadAtAddress; a record whose overflow key does not match it is not read further.</param>
        /// <param name="segmentSizeBits">The object log's segment size bits, at which its position words split.</param>
        /// <param name="checkpointVersion">The downlevel checkpoint version, which selects the split-length decode.</param>
        /// <returns>False if <paramref name="requestedKey"/> is set and the record's overflow key does not match it; otherwise true</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public bool ReadRecordObjects<TKey>(ref LogRecord logRecord, TKey requestedKey, int segmentSizeBits, int checkpointVersion)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            Debug.Assert(logRecord.DataHeader.RecordHasObjects, "Inline records should have been checked by the caller");
            Debug.Assert(logRecord.IsDownlevelObjectLogRecord(checkpointVersion), "The legacy reader decodes only downlevel records");

            // A downlevel record carries its exact component lengths, split between the RDH and the objectId slots.
            var positionWord = logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength, checkpointVersion);
            recordStartPosition = new LegacyObjectLogPosition(positionWord, segmentSizeBits);
            var initialEnd = recordStartPosition;
            initialEnd.Advance(logRecord.DataHeader.KeyIsOverflow ? (ulong)keyLength : valueLength);
            readBuffers.SetDynamicReadThrough(initialEnd);
            if (!readBuffers.OnBeginRecord(recordStartPosition))
                throw new TsavoriteException("ReadRecordObjects found no data available in ReadBuffers");
            recordStreamConsumed = 0;

            var keyWasSet = false;
            try
            {
                if (logRecord.DataHeader.KeyIsOverflow)
                {
                    // This assignment also allocates the slot in ObjectIdMap, overwriting the key length high bits the objectId slot held on disk.
                    logRecord.KeyOverflow = ReadOverflow(keyLength);
                    if (!requestedKey.IsEmpty && !storeFunctions.KeysEqual(requestedKey, logRecord))
                        return false;
                    keyWasSet = true;
                }

                if (!logRecord.DataHeader.ValueIsInline)
                    SetDynamicRecordReadThrough(recordStreamConsumed + valueLength);

                if (logRecord.DataHeader.ValueIsOverflow)
                    logRecord.ValueOverflow = ReadOverflow(checked((int)valueLength));
                else if (logRecord.DataHeader.ValueIsObject)
                {
                    // Bound the deserializer to exactly the object's bytes. An object that serialized to no bytes carries no type information,
                    // so record its absence rather than invoking the deserializer on an empty stream.
                    objectDataBudget = (long)valueLength;
                    try
                    {
                        if (objectDataBudget == 0)
                            logRecord.SetDeserializedValueObject(null);
                        else
                            DoDeserialize(ref logRecord);
                    }
                    finally
                    {
                        objectDataBudget = -1;
                    }
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

        /// <summary>Recovery Pass 1 (index build) helper: read ONLY this record's overflow key and return its hash code, without reading the value
        /// or touching the transient <see cref="ObjectIdMap"/>, which is not populated until Pass 2. The caller must have set up the read-ahead via
        /// <see cref="OnBeginReadRecords"/> at the record's object-log start position, and the record must have an overflow key.</summary>
        internal long ReadOverflowKeyHashCodeForRecovery(in LogRecord logRecord, int segmentSizeBits, int checkpointVersion)
        {
            Debug.Assert(logRecord.DataHeader.KeyIsOverflow, "Record must have an overflow key");
            Debug.Assert(logRecord.IsDownlevelObjectLogRecord(checkpointVersion), "The legacy reader decodes only downlevel records");

            var positionWord = logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out _, checkpointVersion);
            recordStartPosition = new LegacyObjectLogPosition(positionWord, segmentSizeBits);
            if (!readBuffers.OnBeginRecord(recordStartPosition))
                throw new TsavoriteException("ReadOverflowKeyHashCodeForRecovery found no data available in ReadBuffers");
            recordStreamConsumed = 0;

            var overflow = ReadOverflow(keyLength);
            fixed (byte* keyPtr = overflow.Span)
            {
                var key = ConditionallyHoistedKey.CreatePinned(keyPtr, overflow.Length);
                return storeFunctions.GetKeyHashCode64(key);
            }
        }

        /// <summary>Convert a record-relative exclusive endpoint to its absolute object-log address and make it the ring's read demand.</summary>
        void SetDynamicRecordReadThrough(ulong recordRelativeEnd)
        {
            var end = recordStartPosition;
            end.Advance(recordRelativeEnd);
            readBuffers.SetDynamicReadThrough(end);
        }

        /// <inheritdoc/>
        public int Read(Span<byte> destinationSpan, CancellationToken cancellationToken = default)
        {
            // Nothing left for this component: report end-of-stream rather than consuming another record's bytes from the ring.
            if (objectDataBudget == 0)
                return 0;

            // A component cannot serve more than it wrote; clamp so an over-reading deserializer sees end-of-stream at its own boundary.
            if (objectDataBudget > 0 && destinationSpan.Length > objectDataBudget)
                destinationSpan = destinationSpan.Slice(0, (int)objectDataBudget);

            var bytesRead = ReadRawStream(destinationSpan, cancellationToken);
            if (objectDataBudget > 0)
                objectDataBudget -= bytesRead;
            return bytesRead;
        }

        /// <summary>Read up to <paramref name="destinationSpan"/>.Length raw bytes from the object-log read-ahead ring.</summary>
        int ReadRawStream(Span<byte> destinationSpan, CancellationToken cancellationToken = default)
        {
            var prevCopyLength = 0;
            var destinationSpanAppend = destinationSpan;

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
                    buffer.AvailableSpan.Slice(0, copyLength).CopyTo(destinationSpanAppend);
                    buffer.currentPosition += copyLength;
                    recordStreamConsumed += (uint)copyLength;
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

        /// <summary>Read one overflow key or value of exactly <paramref name="length"/> bytes into its final array.</summary>
        OverflowByteArray ReadOverflow(int length)
        {
            var overflow = new OverflowByteArray(length, startOffset: 0, endOffset: 0, zeroInit: false);
            if (length > 0 && Read(overflow.Span) != length)
                throw new TsavoriteException($"Expected {length} overflow bytes");
            return overflow;
        }

        void DoDeserialize(ref LogRecord logRecord)
        {
            // If we haven't yet instantiated the serializer do so now.
            if (valueObjectSerializer is null)
            {
                pinnedMemoryStream = new(this);
                valueObjectSerializer = storeFunctions.CreateValueObjectSerializer();
                valueObjectSerializer.BeginDeserialize(pinnedMemoryStream);
            }

            valueObjectSerializer.Deserialize(out var valueObject);
            logRecord.SetDeserializedValueObject(valueObject);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            pinnedMemoryStream?.Dispose();
            valueObjectSerializer?.EndDeserialize();
        }
    }
}