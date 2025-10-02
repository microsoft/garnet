﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
        /// Called when one or more records are to be read via ReadAsync.
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

        /// <inheritdoc/>
        public void FlushAndReset(CancellationToken cancellationToken = default) => throw new InvalidOperationException("FlushAndReset is not supported for DiskStreamReadBuffer");

        /// <inheritdoc/>
        public void Write(ReadOnlySpan<byte> data, CancellationToken cancellationToken = default) => throw new InvalidOperationException("Write is not supported for DiskStreamReadBuffer");

        /// <summary>
        /// Get the object log entries for Overflow Keys and Values and Object Values for the record at <paramref name="physicalAddress"/>. We create the log record here,
        /// because we are calling this over a pages from iterator frames or Restore.
        /// </summary>
        /// <param name="physicalAddress">Pointer to the initial record read from disk, either from iterator or Restore.</param>
        /// <param name="recordSize">Number of bytes available at <paramref name="physicalAddress"/></param>
        /// <param name="requestedKey">The requested key, if not ReadAtAddress; we will compare to see if it matches the record.</param>
        /// <param name="transientObjectIdMap">The <see cref="ObjectIdMap"/> to place Overflow and Object Keys and Values in.</param>
        /// <param name="segmentSizeBits">Number of bits in segment size</param>
        /// <param name="logRecord">The output <see cref="LogRecord"/>, which has its Key and Value ObjectIds filled in in the log record.</param>
        /// <returns>False if requestedKey is set and we read an Overflow key and it did not match; otherwise true</returns>
        public bool ReadRecordObjects(long physicalAddress, int recordSize, ReadOnlySpan<byte> requestedKey, ObjectIdMap transientObjectIdMap, int segmentSizeBits, out LogRecord logRecord)
        {
            logRecord = new LogRecord(physicalAddress, transientObjectIdMap);
            Debug.Assert(logRecord.GetInlineRecordSizes().actualSize <= recordSize, $"RecordSize ({recordSize}) is less than required LogRecord size ({logRecord.GetInlineRecordSizes().actualSize})");
            return logRecord.Info.RecordIsInline || ReadRecordObjects(ref logRecord, requestedKey, segmentSizeBits);
        }

        /// <summary>
        /// Get the object log entries for Overflow Keys and Values and Object Values for the record in <paramref name="diskLogRecord"/>, which came
        /// from the initial IO operation.
        /// </summary>
        /// <param name="diskLogRecord">The initial record read from disk from Pending IO, so it is of size <see cref="IStreamBuffer.InitialIOSize"/> or less.</param>
        /// <param name="requestedKey">The requested key, if not ReadAtAddress; we will compare to see if it matches the record.</param>
        /// <param name="segmentSizeBits">Number of bits in segment size</param>
        /// <returns>False if requestedKey is set and we read an Overflow key and it did not match; otherwise true</returns>
        public bool ReadRecordObjects(ref DiskLogRecord diskLogRecord, ReadOnlySpan<byte> requestedKey, int segmentSizeBits)
            => diskLogRecord.logRecord.Info.RecordIsInline || ReadRecordObjects(ref diskLogRecord.logRecord, requestedKey, segmentSizeBits);

        /// <summary>
        /// Get the object log entries for Overflow Keys and Values and Object Values for the <paramref name="logRecord"/>:
        /// <list type="bullet">
        /// <item>If there is an Overflow key, read it and if we have a <paramref name="requestedKey"/> compare it and return false if it does not match.
        ///     Otherwise, store the Key Overflow in the transient <see cref="ObjectIdMap"/> in <paramref name="logRecord"/>.
        ///     If we don't have <paramref name="requestedKey"/>, this is either ReadAtAddress (which is an implicit match) or Scan or Restore.</item>
        /// <item>If we have an Overflow or Object value, read and store it in the transient <see cref="ObjectIdMap"/> in <paramref name="logRecord"/>.</item>
        /// </list>
        /// </summary>
        /// <param name="logRecord">The initial record read from disk from Pending IO, so it is of size <see cref="IStreamBuffer.InitialIOSize"/> or less.</param>
        /// <param name="requestedKey">The requested key, if not ReadAtAddress; we will compare to see if it matches the record.</param>
        /// <param name="segmentSizeBits">Number of bits in segment size</param>
        /// <returns>False if requestedKey is set and we read an Overflow key and it did not match; otherwise true</returns>
        public bool ReadRecordObjects(ref LogRecord logRecord, ReadOnlySpan<byte> requestedKey, int segmentSizeBits)
        {
            Debug.Assert(!logRecord.Info.RecordIsInline, $"Inline records should have been checked by the caller");

            var positionWord = logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength);
            readBuffers.OnBeginRecord(new ObjectLogFilePositionInfo(positionWord, segmentSizeBits));

            // TODO: Optimize the reading of large internal sector-aligned parts of Overflow Keys and Values to read directly into the overflow, similar to how ObjectLogWriter writes
            //       directly from overflow. This requires changing the read-ahead in CircularDiskReadBuffer.OnBeginReadRecords and the "backfill" in CircularDiskReadBuffer.MoveToNextBuffer.

            // Note: Similar logic to this is in DiskLogRecord.Deserialize.
            if (logRecord.Info.KeyIsOverflow)
            {
                // This assignment also allocates the slot in ObjectIdMap. The varbyte length info should be unchanged from ObjectIdSize.
                logRecord.KeyOverflow = new OverflowByteArray(keyLength, startOffset:0, endOffset:0, zeroInit:false);
                _ = Read(logRecord.KeyOverflow.Span);
                if (!requestedKey.IsEmpty && !storeFunctions.KeysEqual(requestedKey, logRecord.KeyOverflow.Span))
                    return false;
            }

            if (logRecord.Info.ValueIsOverflow)
            {
                // This assignment also allocates the slot in ObjectIdMap. The varbyte length info should be unchanged from ObjectIdSize.
                logRecord.ValueOverflow = new OverflowByteArray((int)valueLength, startOffset: 0, endOffset: 0, zeroInit: false);
                _ = Read(logRecord.ValueOverflow.Span);

                // If value is overflow, there's no object to read
                return true;
            }

            // This assignment also allocates the slot in ObjectIdMap and updates the varbyte length to be ObjectIdSize.
            logRecord.ValueObject = DoDeserialize();
            return true;
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

        IHeapObject DoDeserialize()
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
            OnDeserializeComplete(valueObject);
            return valueObject;
        }

        void OnDeserializeComplete(IHeapObject valueObject)
        {
            if (valueObject.SerializedSizeIsExact)
                Debug.Assert(valueObject.SerializedSize == (long)deserializedLength, $"valueObject.SerializedSize(Exact) {valueObject.SerializedSize} != deserializedLength {deserializedLength}");
            else
                valueObject.SerializedSize = (long)deserializedLength;

            // TODO add size tracking; do not track deserialization size changes if we are deserializing to a frame

            inDeserialize = false;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            pinnedMemoryStream?.Dispose();
            valueObjectSerializer?.EndDeserialize();
        }
    }
}
