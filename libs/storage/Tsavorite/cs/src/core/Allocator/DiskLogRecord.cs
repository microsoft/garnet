// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;
    using static VarbyteLengthUtility;

    /// <summary>A wrapper around LogRecord for retrieval from disk or carrying through pending operations</summary>
    public unsafe struct DiskLogRecord : ISourceLogRecord, IDisposable
    {
        /// <summary>The <see cref="LogRecord"/>> around the record data.</summary>
        LogRecord logRecord;

        /// <summary>The buffer containing the record data, from either disk IO or a copy from a LogRecord that is carried through pending operations
        /// such as Compact or ConditionalCopyToTail. The <see cref="LogRecord"/> contains its <see cref="SectorAlignedMemory.GetValidPointer()"/>
        /// as its <see cref="LogRecord.physicalAddress"/>.</summary>
        /// <remarks>We always own the record buffer; it is either transferred to us, or allocated as a copy of the record memory. However, it may be
        ///  null if we transferred it out.</remarks>
        SectorAlignedMemory recordBuffer;

        /// <summary>The action to perform when disposing the contained LogRecord; the objects may have been transferred.</summary>
        Action<IHeapObject> objectDisposer;

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
        /// Constructs the <see cref="DiskLogRecord"/> from an already-constructed LogRecord (e.g. from <see cref="IAllocator{TStoreFunctions}.CreateRemappedLogRecordOverTransientMemory"/> which
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
        /// Creates a <see cref="DiskLogRecord"/> from an already-constructed LogRecord (e.g. from <see cref="IAllocator{TStoreFunctions}.CreateRemappedLogRecordOverTransientMemory"/> which
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
            src.recordBuffer = default; // Transfer ownership to us
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
            var allocatedSize = RoundUp(logRecord.GetInlineRecordSizes().actualSize, Constants.kRecordAlignment);
            var recordBuffer = bufferPool.Get(allocatedSize);
            logRecord.RecordSpan.CopyTo(recordBuffer.RequiredValidSpan);
            return recordBuffer;
        }

        public void Dispose()
        {
            _ = logRecord.ClearKeyIfOverflow();
            _ = logRecord.ClearValueIfHeap(objectDisposer);
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
        public readonly long ETag => logRecord.ETag;

        /// <inheritdoc/>
        public readonly long Expiration => logRecord.Expiration;

        /// <inheritdoc/>
        public readonly bool ClearValueIfHeap(Action<IHeapObject> disposer) => false;  // Nothing to do here; we dispose the object in the pending operation or iteration completion

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

        #endregion //ISourceLogRecord

        #region Serialization to and from expanded record format
        /// <summary>
        /// Serialize a log record (which may be in-memory <see cref="LogRecord"/> or IO'd <see cref="DiskLogRecord"/>) to the <see cref="SpanByteAndMemory"/>
        /// <paramref name="output"/> in inline-expanded format, with the Overflow Keys and Values and Object Values serialized inline to the Key and Value spans.
        /// The record stream is prefixed with the int length of the streawm.
        /// </summary>
        /// <remarks>
        /// This is used for migration and replication, and output.SpanByteAndMemory is a span of the remaining space in the network buffer.
        /// This allocates <see cref="SpanByteAndMemory.Memory"/> if needed; in that case the caller will flush the network buffer and retry with the full length.
        /// </remarks>
        /// <remarks>If <paramref name="output"/>.<see cref="SpanByteAndMemory.IsSpanByte"/>, it points directly to the network buffer so we include the length prefix in the output.</remarks>
        public static void Serialize<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, IObjectSerializer<IHeapObject> valueSerializer, MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.IsMemoryLogRecord)
                SerializeLogRecord(in srcLogRecord.AsMemoryLogRecordRef(), valueSerializer, memoryPool, ref output);
            else
            {
                if (!srcLogRecord.IsDiskLogRecord)
                    throw new TsavoriteException("Unknown TSourceLogRecord type");
                SerializeLogRecord(in srcLogRecord.AsDiskLogRecordRef().logRecord, valueSerializer, memoryPool, ref output);
            }

            static void SerializeLogRecord(in LogRecord logRecord, IObjectSerializer<IHeapObject> valueSerializer, MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
            {
                if (logRecord.Info.RecordIsInline)
                    _ = DirectCopyInlinePortionOfRecord(in logRecord, heapSize: 0, memoryPool, ref output);
                else
                {
                    // TODO: long value sizes (larger than the network buffer) are currently not supported; need to create a chunked protocol that will write incrementally to the
                    //       network buffer, possibly using a callback to tell the network buffer to flush and reset and update the output available length.
                    if (!logRecord.ValueObject.SerializedSizeIsExact)
                        throw new TsavoriteException("Currently we do not support in-memory serialization of objects that do not support SerializedSizeIsExact");

                    var heapSize = logRecord.Info.KeyIsOverflow ? logRecord.KeyOverflow.Length : 0;
                    if (logRecord.Info.ValueIsOverflow)
                        heapSize += logRecord.ValueOverflow.Length;
                    else if (logRecord.Info.ValueIsObject)
                        heapSize += (int)logRecord.ValueObject.SerializedSize;
                    var inlineRecordSize = DirectCopyInlinePortionOfRecord(in logRecord, heapSize, memoryPool, ref output);
                    SerializeHeapObjects(in logRecord, inlineRecordSize, valueSerializer, ref output);
                }
            }
        }

        /// <summary>
        /// Directly copies a record in inline varbyte format to the SpanByteAndMemory. Allocates <see cref="SpanByteAndMemory.Memory"/> if needed.
        /// </summary>
        /// <remarks>If <paramref name="output"/>.<see cref="SpanByteAndMemory.IsSpanByte"/>, it points directly to the network buffer so we include the length prefix in the output.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int DirectCopyInlinePortionOfRecord(in LogRecord logRecord, int heapSize, MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
        {
            // TotalSize includes the length prefix, which is included in the output stream if we can write directly to the SpanByte,
            // which is a span in the network buffer.
            var recordSize = logRecord.ActualRecordSize;
            var totalSize = recordSize + sizeof(int) + heapSize;
            if (output.IsSpanByte && output.SpanByte.TotalSize >= totalSize)
            {
                var outPtr = output.SpanByte.ToPointer();
                *(int*)outPtr = recordSize;
                Buffer.MemoryCopy((byte*)logRecord.physicalAddress, outPtr + sizeof(int), recordSize, recordSize);
            }
            else
            {
                // Do not include the length prefix in the output stream; this is done by the caller before writing the stream, from the SpanByte.Length we set here.
                totalSize -= sizeof(int);
                output.EnsureHeapMemorySize(recordSize, memoryPool);
                fixed (byte* outPtr = output.MemorySpan)
                    Buffer.MemoryCopy((byte*)logRecord.physicalAddress, outPtr, recordSize, recordSize);
            }

            output.Length = totalSize;
            return totalSize;
        }

        private static void SerializeHeapObjects(in LogRecord logRecord, int inlineRecordSize, IObjectSerializer<IHeapObject> valueSerializer, ref SpanByteAndMemory output)
        {
            // Serialize Key then Value, just like the Object log file. And this will be ready for future chunking of long Values.
        }

        /// <summary>
        /// Deserialize from a <see cref="PinnedSpanByte"/> over a stream of bytes created by <see cref="Serialize"/>.
        /// </summary>
        /// <param name="recordSpan"></param>
        public DiskLogRecord Deserialize(PinnedSpanByte recordSpan)
        {
            // Serialize() did not change the state of the KeyIsInline/ValueIsInline/ValueIsObject bits. 


            TODO("make sure to include space for the objectLog pointer if ValueIsObject");
        }

        #endregion Serialization to and from expanded record format
    }
}