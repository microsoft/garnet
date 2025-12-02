// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>A wrapper around LogRecord for retrieval from disk or carrying through pending operations</summary>
    public unsafe struct DiskLogRecord : ISourceLogRecord, IDisposable
    {
        /// <summary>The <see cref="LogRecord"/>> around the record data.</summary>
        internal LogRecord logRecord;

        /// <summary>The buffer containing the record data, from either disk IO or a copy from a LogRecord that is carried through pending operations
        /// such as Compact or ConditionalCopyToTail. The <see cref="LogRecord"/> contains its <see cref="SectorAlignedMemory.GetValidPointer()"/>
        /// as its <see cref="LogRecord.physicalAddress"/>.</summary>
        /// <remarks>We always own the record buffer; it is either transferred to us, or allocated as a copy of the record memory. However, it may be
        ///  null if we transferred it out.</remarks>
        SectorAlignedMemory recordBuffer;

        /// <summary>The action to perform when disposing the contained LogRecord; the objects may have been transferred.</summary>
        internal Action<IHeapObject> objectDisposer;

        public override readonly string ToString()
            => $"logRec [{logRecord}], recordBuffer [{recordBuffer}], objDisp [{objectDisposer}]";

        /// <summary>
        /// Constructor taking the record buffer and out-of-line objects. Private; use either CopyFrom or TransferFrom.
        /// </summary>
        /// <param name="recordBuffer">The record buffer, either from IO or a copy for pending operations such as Compact or ConditionalCopyToTail.</param>
        /// <param name="transientObjectIdMap">The <see cref="ObjectIdMap"/> to hold the objects for the <see cref="LogRecord"/> for the lifetime of this <see cref="DiskLogRecord"/>.</param>
        /// <param name="keyOverflow">The key overflow byte[] wrapper, if any</param>
        /// <param name="valueOverflow">The value overflow byte[] wrapper, if any</param>
        /// <param name="valueObject">The value object, if any</param>
        /// <param name="objectDisposer">The action to invoke when disposing the value object if it is present when we dispose the <see cref="LogRecord"/></param>
        /// <remarks>We always own the record buffer; it is either transferred to us by TransferFrom, or allocated as a copy of the record memory by CopyFrom</remarks>
        private DiskLogRecord(SectorAlignedMemory recordBuffer, ObjectIdMap transientObjectIdMap, OverflowByteArray keyOverflow,
            OverflowByteArray valueOverflow, IHeapObject valueObject, Action<IHeapObject> objectDisposer)
        {
            this.recordBuffer = recordBuffer;
            this.objectDisposer = objectDisposer;
            logRecord = new((long)recordBuffer.GetValidPointer(), transientObjectIdMap);
            if (!keyOverflow.IsEmpty)
                logRecord.KeyOverflow = keyOverflow;
            if (!valueOverflow.IsEmpty)
                logRecord.ValueOverflow = valueOverflow;
            else if (valueObject is not null)
                logRecord.ValueObject = valueObject;
        }

        /// <summary>
        /// Constructs the <see cref="DiskLogRecord"/> from an already-constructed LogRecord (e.g. from <see cref="IAllocator{TStoreFunctions}.CreateRemappedLogRecordOverPinnedTransientMemory"/> which
        /// has transient ObjectIds if it has objects).
        /// </summary>
        internal DiskLogRecord(in LogRecord memoryLogRecord, Action<IHeapObject> objectDisposer)
        {
            logRecord = memoryLogRecord;
            this.objectDisposer = objectDisposer;
        }

        /// <summary>
        /// Transfers a transient inline record buffer and creates our contained <see cref="LogRecord"/> from it. Private; use either CopyFrom or TransferFrom.
        /// </summary>
        /// <param name="recordBuffer">The record buffer, either from IO or a copy for pending operations such as Compact or ConditionalCopyToTail.</param>
        /// <param name="transientObjectIdMap">The <see cref="ObjectIdMap"/> to hold the objects for the <see cref="LogRecord"/> for the lifetime of this <see cref="DiskLogRecord"/>.</param>
        /// <remarks>We always own the record buffer; it is either transferred to us, or allocated as a copy of the record memory</remarks>
        private DiskLogRecord(SectorAlignedMemory recordBuffer, ObjectIdMap transientObjectIdMap)
        {
            this.recordBuffer = recordBuffer;
            logRecord = new((long)recordBuffer.GetValidPointer(), transientObjectIdMap);
        }

        /// <summary>
        /// Creates a <see cref="DiskLogRecord"/> from an already-constructed LogRecord (e.g. from <see cref="IAllocator{TStoreFunctions}.CreateRemappedLogRecordOverPinnedTransientMemory"/> which
        /// has transient ObjectIds if it has objects).
        /// </summary>
        internal static DiskLogRecord CreateFromTransientLogRecord(in LogRecord memoryLogRecord, Action<IHeapObject> objectDisposer) => new(memoryLogRecord, objectDisposer);

        /// <summary>
        /// Allocates <see cref="recordBuffer"/> and copies the LogRecord's record memory into it; any out-of-line objects are shallow-copied.
        /// </summary>
        /// <param name="logRecord">The <see cref="LogRecord"/> to copy</param>
        /// <param name="bufferPool">The buffer pool to allocate from</param>
        /// <param name="transientObjectIdMap">The <see cref="ObjectIdMap"/> to hold the objects for the <see cref="LogRecord"/> for the lifetime of this <see cref="DiskLogRecord"/>.</param>
        /// <param name="objectDisposer">The action to invoke when disposing the value object if it is present when we dispose the <see cref="LogRecord"/></param>
        internal static DiskLogRecord CopyFrom(in LogRecord logRecord, SectorAlignedBufferPool bufferPool, ObjectIdMap transientObjectIdMap, Action<IHeapObject> objectDisposer)
        {
            var recordBuffer = AllocateBuffer(in logRecord, bufferPool);
            return new DiskLogRecord(recordBuffer, transientObjectIdMap,
                logRecord.Info.KeyIsOverflow ? logRecord.KeyOverflow : default,
                logRecord.Info.ValueIsOverflow ? logRecord.ValueOverflow : default,
                logRecord.Info.ValueIsObject ? logRecord.ValueObject : default, objectDisposer);
        }

        /// <summary>
        /// Copies a LogRecord with no out-of-line objects into our contained <see cref="LogRecord"/>.
        /// </summary>
        /// <param name="recordBuffer">The record buffer, either from IO or a copy for pending operations such as Compact or ConditionalCopyToTail.</param>
        /// <param name="transientObjectIdMap">The <see cref="ObjectIdMap"/> to hold the objects for the <see cref="LogRecord"/> for the lifetime of this <see cref="DiskLogRecord"/>.</param>
        /// <param name="keyOverflow">The key overflow byte[] wrapper, if any</param>
        /// <param name="valueOverflow">The value overflow byte[] wrapper, if any</param>
        /// <param name="valueObject">The value object, if any</param>
        /// <param name="objectDisposer">The action to invoke when disposing the value object if it is present when we dispose the <see cref="LogRecord"/></param>
        internal static DiskLogRecord TransferFrom(ref SectorAlignedMemory recordBuffer, ObjectIdMap transientObjectIdMap, OverflowByteArray keyOverflow,
            OverflowByteArray valueOverflow, IHeapObject valueObject, Action<IHeapObject> objectDisposer)
        {
            var diskLogRecord = new DiskLogRecord(recordBuffer, transientObjectIdMap, keyOverflow, valueOverflow, valueObject, objectDisposer);
            recordBuffer = default;     // Transfer ownership to us
            return diskLogRecord;
        }

        internal static DiskLogRecord TransferFrom(ref DiskLogRecord src)
        {
            var diskLogRecord = new DiskLogRecord(in src.logRecord, src.objectDisposer) { recordBuffer = src.recordBuffer };
            src = default; // Transfer ownership to us, and make sure we don't try to clear the logRecord
            return diskLogRecord;
        }

        /// <summary>
        /// Transfers a transient inline record buffer and creates our contained <see cref="LogRecord"/> from it.
        /// </summary>
        /// <param name="recordBuffer">The record buffer, either from IO or a copy for pending operations such as Compact or ConditionalCopyToTail.</param>
        /// <param name="transientObjectIdMap">The <see cref="ObjectIdMap"/> to hold the objects for the <see cref="LogRecord"/> for the lifetime of this <see cref="DiskLogRecord"/>.</param>
        internal static DiskLogRecord TransferFrom(ref SectorAlignedMemory recordBuffer, ObjectIdMap transientObjectIdMap)
        {
            var diskLogRecord = new DiskLogRecord(recordBuffer, transientObjectIdMap);
            recordBuffer = default;     // Transfer ownership to us
            return diskLogRecord;
        }

        private static SectorAlignedMemory AllocateBuffer(in LogRecord logRecord, SectorAlignedBufferPool bufferPool)
        {
            var allocatedSize = RoundUp(logRecord.ActualSize, Constants.kRecordAlignment);
            var recordBuffer = bufferPool.Get(allocatedSize);
            logRecord.RecordSpan.CopyTo(recordBuffer.RequiredValidSpan);
            return recordBuffer;
        }

        public void Dispose()
        {
            logRecord.Dispose(objectDisposer);
            logRecord = default;

            recordBuffer?.Return();
            recordBuffer = default;
            objectDisposer = default;
        }

        #region ISourceLogRecord
        /// <inheritdoc/>
        public readonly bool IsPinnedKey => logRecord.Info.KeyIsInline;

        /// <inheritdoc/>
        public readonly byte* PinnedKeyPointer => logRecord.PinnedKeyPointer;

        /// <inheritdoc/>
        public OverflowByteArray KeyOverflow
        {
            readonly get => logRecord.KeyOverflow;
            set => logRecord.KeyOverflow = value;
        }

        /// <inheritdoc/>
        public readonly bool IsPinnedValue => logRecord.Info.ValueIsInline;

        /// <inheritdoc/>
        public readonly byte* PinnedValuePointer => logRecord.PinnedValuePointer;

        /// <inheritdoc/>
        public OverflowByteArray ValueOverflow
        {
            readonly get => logRecord.ValueOverflow;
            set => logRecord.ValueOverflow = value;
        }

        /// <inheritdoc/>
        public readonly byte RecordType => logRecord.IsSet ? logRecord.RecordType : default;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> Namespace => logRecord.IsSet ? logRecord.Namespace : default;

        /// <inheritdoc/>
        public readonly ObjectIdMap ObjectIdMap => logRecord.objectIdMap;

        /// <inheritdoc/>
        public readonly bool IsSet => logRecord.IsSet;

        /// <inheritdoc/>
        public ref RecordInfo InfoRef => ref logRecord.InfoRef;
        /// <inheritdoc/>
        public readonly RecordInfo Info => logRecord.Info;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> Key => logRecord.Key;

        /// <inheritdoc/>
        public readonly Span<byte> ValueSpan => logRecord.ValueSpan;

        /// <inheritdoc/>
        public readonly IHeapObject ValueObject => logRecord.ValueObject;

        /// <inheritdoc/>
        public readonly long ETag => logRecord.IsSet ? logRecord.ETag : LogRecord.NoETag;

        /// <inheritdoc/>
        public readonly long Expiration => logRecord.Expiration;

        /// <inheritdoc/>
        public readonly void ClearValueIfHeap(Action<IHeapObject> disposer) { }  // Nothing to do here; we dispose the object in the pending operation or iteration completion

        /// <inheritdoc/>
        public readonly bool IsMemoryLogRecord => false;

        /// <inheritdoc/>
        public readonly unsafe ref LogRecord AsMemoryLogRecordRef() => throw new TsavoriteException("DiskLogRecord cannot be returned as MemoryLogRecord");

        /// <inheritdoc/>
        public readonly bool IsDiskLogRecord => true;

        /// <inheritdoc/>
        public readonly unsafe ref DiskLogRecord AsDiskLogRecordRef() => ref Unsafe.AsRef(in this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordFieldInfo GetRecordFieldInfo() => logRecord.GetRecordFieldInfo();

        /// <inheritdoc/>
        public readonly int AllocatedSize => logRecord.AllocatedSize;

        /// <inheritdoc/>
        public readonly int ActualSize => logRecord.ActualSize;
        #endregion //ISourceLogRecord

        #region Serialization to and from expanded record format
        /// <summary>
        /// Serialize a log record (which may be in-memory <see cref="LogRecord"/> or IO'd <see cref="DiskLogRecord"/>) to the <see cref="SpanByteAndMemory"/>
        /// <paramref name="output"/> in inline-expanded format, with the Overflow Keys and Values and Object Values serialized inline to the Key and Value spans.
        /// The serialized layout is:
        /// <list type="bullet">
        ///     <item>Inline portion of the LogRecord: RecordInfo, IndicatorWord, Key and Value data (each of 4 byte length, which restores to object Id), optionals (ETag, Expiration, ObjectLogPosition, ...)</item>
        ///     <item>Key data, if key is Overflow</item>
        ///     <item>Value data, if value is Overflow or Object</item>
        /// </list>
        /// </summary>
        /// <param name="srcLogRecord">The Source log record to be serialized to inline form</param>
        /// <param name="maxHeapAllocationSize">The size of heap allocation to make in <paramref name="output"/>.<see cref="SpanByteAndMemory.Memory"/>
        ///     if the record is larger than the inline <paramref name="output"/>.<see cref="SpanByteAndMemory.SpanByte"/>. This can happen because:
        ///     <list type="bullet">
        ///         <item>The known record size (due to inline data and Overflow (whose length is known in advance)) is larger than the 
        ///             <paramref name="output"/>.<see cref="SpanByteAndMemory.SpanByte"/> size</item>
        ///         <item>The Object Value serialization (if any) surpasses the <paramref name="output"/>.<see cref="SpanByteAndMemory.SpanByte"/> size
        ///             (we don't know how big an object is until we serialize it)</item>
        ///     </list></param>
        /// <param name="valueObjectSerializer">The serializer for the value object, if there is one (ignored if not)</param>
        /// <param name="memoryPool">The memory pool to use to allocate <paramref name="maxHeapAllocationSize"/> bytes into 
        ///     <paramref name="output"/>.<see cref="SpanByteAndMemory.Memory"/> if we overflow the <paramref name="output"/>.<see cref="SpanByteAndMemory.SpanByte"/>.</param>
        /// <param name="output">The output to receive the serialized logRecord data, either in <paramref name="output"/>.<see cref="SpanByteAndMemory.SpanByte"/>
        ///   or <paramref name="output"/>.<see cref="SpanByteAndMemory.Memory"/></param>
        /// <remarks>
        /// This is used for migration and replication, and output.SpanByteAndMemory is a span of the remaining space in the network buffer.
        /// <list type="bullet">
        ///     <item>If <paramref name="output"/>.<see cref="SpanByteAndMemory.IsSpanByte"/>, it points directly to the network buffer so we include the length prefix in the output.</item>
        ///     <item><see cref="SpanByteAndMemory.Memory"/> is allocated if needed; in that case the caller will flush the network buffer and retry with the full length.</item>
        ///     <item>The record stream is prefixed with the int length of the stream. RespReadUtils.GetSerializedRecordSpan sets up for deserialization from the network buffer.</item>
        /// </list>
        /// </remarks>
        /// <returns>The total number of bytes in the output.</returns>
        public static int Serialize<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, int maxHeapAllocationSize, IObjectSerializer<IHeapObject> valueObjectSerializer,
            MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.IsMemoryLogRecord)
                return SerializeLogRecord(in srcLogRecord.AsMemoryLogRecordRef(), maxHeapAllocationSize, valueObjectSerializer, memoryPool, ref output);

            if (!srcLogRecord.IsDiskLogRecord)
                throw new TsavoriteException("Unknown TSourceLogRecord type");
            return SerializeLogRecord(in srcLogRecord.AsDiskLogRecordRef().logRecord, maxHeapAllocationSize, valueObjectSerializer, memoryPool, ref output);
        }

        // TODO: long value sizes (larger than the network buffer) are currently not supported; need to create a chunked protocol that will write incrementally to a PinnedMemoryStream
        //       specialized on a new ReplicaStreamBuffer that wraps the network buffer's PinnedSpanByte and the FlushAndReset callback does the network buffer flush and reset (with
        //       extra work to make this send to multiple replicas) and updates the output available length. Currently using maxHeapAllocationSize to get around not yet having this.

        static int SerializeLogRecord(in LogRecord logRecord, int maxHeapAllocationSize, IObjectSerializer<IHeapObject> valueObjectSerializer, MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
        {
            // TotalSize includes the length prefix, which is included in the output stream if we can write directly to the SpanByte, which is a span in the
            // network buffer. In case of significant shrinkage, calculate this AllocatedSize separately rather than logRecord.GetInlineRecordSizes().allocatedSize.
            var alignedInlineRecordSize = RoundUp(logRecord.ActualSize, Constants.kRecordAlignment);
            var estimatedTotalSize = alignedInlineRecordSize + sizeof(int); // Include the record-size prefix in case we can use the SpanByte directly (see DirectCopyInlinePortionOfRecord)

            var heapSize = 0;
            if (logRecord.Info.RecordIsInline)
            {
                // estimatedTotalSize is accurate here.
                DirectCopyInlinePortionOfRecord(in logRecord, alignedInlineRecordSize, estimatedTotalSize, maxHeapAllocationSize, memoryPool, ref output);
            }
            else
            {
                var estimatedRecordHeapSize = logRecord.Info.KeyIsOverflow ? logRecord.KeyOverflow.Length : 0;
                if (logRecord.Info.ValueIsOverflow)
                    estimatedRecordHeapSize += logRecord.ValueOverflow.Length;
                else if (logRecord.Info.ValueIsObject)
                {
                    // We don't know this size exactly so use a small value. Either we'll fit in inline and then have to adjust if we go beyond that, or we
                    // will just allocate max here and then if we go beyond that, we'll throw the capacity-exceeded exception. This is where "estimated" comes in.
                    estimatedRecordHeapSize += 1 << 4;
                }

                estimatedTotalSize += estimatedRecordHeapSize;

                DirectCopyInlinePortionOfRecord(in logRecord, alignedInlineRecordSize, estimatedTotalSize, maxHeapAllocationSize, memoryPool, ref output);
                heapSize = SerializeHeapObjects(in logRecord, alignedInlineRecordSize, estimatedRecordHeapSize, valueObjectSerializer, ref output);
            }
            return alignedInlineRecordSize + heapSize;
        }

        /// <summary>
        /// Directly copies a record in inline format to the SpanByteAndMemory. Allocates <see cref="SpanByteAndMemory.Memory"/> if needed.
        /// </summary>
        /// <remarks>If <paramref name="output"/>.<see cref="SpanByteAndMemory.IsSpanByte"/>, it points directly to the network buffer so we include the length prefix in the output.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void DirectCopyInlinePortionOfRecord<TSourceLogRecord>(in TSourceLogRecord logRecord, int alignedInlineRecordSize, int estimatedTotalSize, int maxHeapAllocationSize,
            MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
            where TSourceLogRecord : ISourceLogRecord
        {
            // See if we have enough space in the SpanByte and, if not, if we would fit in maxHeapAllocationSize.
            // For SpanByte the recordSize must include the length prefix, which is included in the output stream
            // if we can write directly to the SpanByte, which is a span in the network buffer.
            if (!output.IsSpanByte || estimatedTotalSize > output.SpanByte.Length + sizeof(int) || logRecord.Info.ValueIsObject)
            {
                var allocationSizeToUse = logRecord.Info.ValueIsObject ? maxHeapAllocationSize : estimatedTotalSize + sizeof(int);
                if (estimatedTotalSize > allocationSizeToUse)
                    throw new TsavoriteException($"estimatedRecordSize ({estimatedTotalSize}) exceeds max allocated heap size (to use: {allocationSizeToUse}; max: {maxHeapAllocationSize})");
                output.EnsureHeapMemorySize(allocationSizeToUse, memoryPool);
            }

            // We must reset the LogRecord's filler size, because we truncated the record down to the (rounded-up) ActualSize if it had been shrunken.
            var newFillerLength = alignedInlineRecordSize - logRecord.ActualSize;
            if (output.IsSpanByte)
            {
                // TotalSize includes the length prefix. If there is a SpanByte it is a span in the network buffer, so we include the prefix length in the output stream.
                var outPtr = output.SpanByte.ToPointer();
                *(int*)outPtr = alignedInlineRecordSize;
                outPtr += sizeof(int);
                Buffer.MemoryCopy((byte*)logRecord.PhysicalAddress, outPtr, alignedInlineRecordSize, alignedInlineRecordSize);
                new LogRecord((long)outPtr).SetRecordAndFillerLength(alignedInlineRecordSize, newFillerLength);
            }
            else
            {
                // Do not include the length prefix in the output stream; this is done by the caller before writing the stream to the network buffer.
                fixed (byte* outPtr = output.MemorySpan)
                {
                    Buffer.MemoryCopy((byte*)logRecord.PhysicalAddress, outPtr, alignedInlineRecordSize, alignedInlineRecordSize);
                    new LogRecord((long)outPtr).SetRecordAndFillerLength(alignedInlineRecordSize, newFillerLength);
                }
            }
        }

        private static int SerializeHeapObjects(in LogRecord logRecord, int inlineRecordSize, int heapSize, IObjectSerializer<IHeapObject> valueObjectSerializer, ref SpanByteAndMemory output)
        {
            if (logRecord.Info.RecordIsInline)
                return inlineRecordSize;

            // Serialize Key then Value, just like the Object log file. And this will be easy to modify for future chunking of multi-networkBuffer Keys and Values.
            var outputOffset = (ulong)inlineRecordSize;
            if (logRecord.Info.KeyIsOverflow)
            {
                var overflow = logRecord.KeyOverflow;
                overflow.ReadOnlySpan.CopyTo(output.Span.Slice(inlineRecordSize));
                outputOffset += (ulong)overflow.Length;
            }

            var valueObjectLength = 0UL;
            if (logRecord.Info.ValueIsOverflow)
            {
                var overflow = logRecord.ValueOverflow;
                overflow.ReadOnlySpan.CopyTo(output.Span.Slice(inlineRecordSize + (int)outputOffset));
                outputOffset += (ulong)overflow.Length;
            }
            else
            {
                Debug.Assert(logRecord.Info.ValueIsObject, "Expected ValueIsObject to be true");
                if (output.IsSpanByte)
                    valueObjectLength = DoSerialize(logRecord.ValueObject, valueObjectSerializer, output.SpanByte.ToPointer() + outputOffset, output.Length);
                else
                {
                    fixed (byte* ptr = output.MemorySpan.Slice(inlineRecordSize))
                        valueObjectLength = DoSerialize(logRecord.ValueObject, valueObjectSerializer, ptr, output.Length);
                }
                outputOffset += valueObjectLength;
            }

            // Create a temp LogRecord over the output data so we can store the lengths in serialized format, using the offset to the serialized
            // part of the buffer as a fake file offset (implicitly for segment 0).
            var fakeFilePos = new ObjectLogFilePositionInfo((ulong)inlineRecordSize, segSizeBits: 0);
            if (output.IsSpanByte)
            {
                var serializedLogRecord = new LogRecord((long)output.SpanByte.ToPointer());
                serializedLogRecord.SetObjectLogRecordStartPositionAndLength(fakeFilePos, valueObjectLength);
            }
            else
            {
                fixed (byte* ptr = output.MemorySpan.Slice(0, inlineRecordSize))
                {
                    var serializedLogRecord = new LogRecord((long)ptr, logRecord.objectIdMap);
                    serializedLogRecord.SetObjectLogRecordStartPositionAndLength(fakeFilePos, valueObjectLength);
                    serializedLogRecord = new LogRecord((long)ptr);     // Reset to clear objectIdMap because it may be the one in the main log and we pass in a transient one when deserializing
                }
            }

            return (int)outputOffset - inlineRecordSize;

            static ulong DoSerialize(IHeapObject valueObject, IObjectSerializer<IHeapObject> valueObjectSerializer, byte* destPtr, int destLength)
            {
                var stream = new UnmanagedMemoryStream(destPtr, destLength, destLength, FileAccess.ReadWrite);
                valueObjectSerializer.BeginSerialize(stream);
                valueObjectSerializer.Serialize(valueObject);
                valueObjectSerializer.EndSerialize();
                var valueLength = (ulong)stream.Position;
                return valueLength;
            }
        }

        /// <summary>
        /// Deserialize from a <see cref="PinnedSpanByte"/> over a stream of bytes created by <see cref="Serialize"/>.
        /// </summary>
        public static DiskLogRecord Deserialize<TStoreFunctions>(PinnedSpanByte recordSpan, IObjectSerializer<IHeapObject> valueObjectSerializer, ObjectIdMap transientObjectIdMap,
            TStoreFunctions storeFunctions)
            where TStoreFunctions : IStoreFunctions
        {
            // Serialize() did not change the state of the KeyIsInline/ValueIsInline/ValueIsObject bits, but it did change the value at the ObjectId
            // location to be serialized length. Create a transient logRecord to decode these and restore the objectId values.
            var ptr = recordSpan.ToPointer();
            var serializedLogRecord = new LogRecord((long)ptr, transientObjectIdMap);
            if (serializedLogRecord.Info.RecordIsInline)
                return new(serializedLogRecord, obj => { });
            var offset = serializedLogRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength);

            // Note: Similar logic to this is in ObjectLogReader.ReadObjects.
            var keyWasSet = false;
            try
            {
                if (serializedLogRecord.Info.KeyIsOverflow)
                {
                    // This assignment also allocates the slot in ObjectIdMap. The RecordDataHeader length info should be unchanged from ObjectIdSize.
                    serializedLogRecord.KeyOverflow = new OverflowByteArray(keyLength, startOffset: 0, endOffset: 0, zeroInit: false);
                    recordSpan.ReadOnlySpan.Slice((int)offset, keyLength).CopyTo(serializedLogRecord.KeyOverflow.Span);
                    offset += (uint)keyLength;
                    keyWasSet = true;
                }

                if (serializedLogRecord.Info.ValueIsOverflow)
                {
                    // This assignment also allocates the slot in ObjectIdMap. The RecordDataHeader length info should be unchanged from ObjectIdSize.
                    serializedLogRecord.ValueOverflow = new OverflowByteArray((int)valueLength, startOffset: 0, endOffset: 0, zeroInit: false);
                    recordSpan.ReadOnlySpan.Slice((int)offset, (int)valueLength).CopyTo(serializedLogRecord.KeyOverflow.Span);
                }
                else
                {
                    var stream = new UnmanagedMemoryStream(ptr + offset, (int)valueLength);
                    valueObjectSerializer.BeginDeserialize(stream);
                    valueObjectSerializer.Deserialize(out var valueObject);
                    serializedLogRecord.ValueObject = valueObject;
                    valueObjectSerializer.EndDeserialize();
                }
                return new(serializedLogRecord, obj => storeFunctions.DisposeValueObject(obj, DisposeReason.DeserializedFromDisk));
            }
            catch
            {
                serializedLogRecord.OnDeserializationError(keyWasSet);
                throw;
            }
        }

        /// <summary>
        /// Return the serialized size of the contained logRecord.
        /// </summary>
        public readonly int GetSerializedSize() => logRecord.GetSerializedSize();

        #endregion Serialization to and from expanded record format
    }
}