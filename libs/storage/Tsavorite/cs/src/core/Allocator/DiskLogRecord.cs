// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        LogRecord logRecord;

        /// <summary>The buffer containing the record data, from either disk IO or a copy from a LogRecord that is carried through pending operations
        /// such as Compact or ConditionalCopyToTail. The <see cref="logRecord"/> contains its <see cref="SectorAlignedMemory.GetValidPointer()"/>
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
        /// <param name="objectIdMap">The <see cref="ObjectIdMap"/> to hold the objects for the <see cref="logRecord"/>.</param>
        /// <param name="keyOverflow">The key overflow byte[] wrapper, if any</param>
        /// <param name="valueOverflow">The value overflow byte[] wrapper, if any</param>
        /// <param name="valueObject">The value object, if any</param>
        /// <param name="objectDisposer">The action to invoke when disposing the value object if it is present when we dispose the <see cref="logRecord"/></param>
        /// <remarks>We always own the record buffer; it is either transferred to us by TransferFrom, or allocated as a copy of the record memory by CopyFrom</remarks>
        private DiskLogRecord(SectorAlignedMemory recordBuffer, ObjectIdMap objectIdMap, OverflowByteArray keyOverflow,
            OverflowByteArray valueOverflow, IHeapObject valueObject, Action<IHeapObject> objectDisposer)
        {
            this.recordBuffer = recordBuffer;
            this.objectDisposer = objectDisposer;
            logRecord = new((long)recordBuffer.GetValidPointer(), objectIdMap);
            if (!keyOverflow.IsEmpty)
                logRecord.KeyOverflow = keyOverflow;
            if (!valueOverflow.IsEmpty)
                logRecord.ValueOverflow = valueOverflow;
            else if (valueObject is not null)
                logRecord.ValueObject = valueObject;
        }

        /// <summary>
        /// Constructs the <see cref="DiskLogRecord"/> from an already-constructed LogRecord (which is assumed to have transient ObjectIds if it has objects).
        /// </summary>
        internal DiskLogRecord(in LogRecord memoryLogRecord, Action<IHeapObject> objectDisposer)
        {
            logRecord = memoryLogRecord;
            this.objectDisposer = objectDisposer;
        }

        /// <summary>
        /// Copies a LogRecord with no out-of-line objects into our contained <see cref="logRecord"/>. Private; use either CopyFrom or TransferFrom.
        /// </summary>
        /// <param name="recordBuffer">The record buffer, either from IO or a copy for pending operations such as Compact or ConditionalCopyToTail.</param>
        /// <remarks>We always own the record buffer; it is either transferred to us, or allocated as a copy of the record memory</remarks>
        private DiskLogRecord(SectorAlignedMemory recordBuffer)
        {
            this.recordBuffer = recordBuffer;
            logRecord = new((long)recordBuffer.GetValidPointer(), objectIdMap: default);
        }

        /// <summary>
        /// Allocates <see cref="recordBuffer"/> and copies the LogRecord's record memory into it; any out-of-line objects are shallow-copied.
        /// </summary>
        /// <param name="logRecord">The <see cref="LogRecord"/> to copy</param>
        /// <param name="bufferPool">The buffer pool to allocate from</param>
        /// <param name="objectIdMap">The <see cref="ObjectIdMap"/> to hold the objects for the <see cref="logRecord"/>.</param>
        /// <param name="objectDisposer">The action to invoke when disposing the value object if it is present when we dispose the <see cref="logRecord"/></param>
        internal static DiskLogRecord CopyFrom(ref LogRecord logRecord, SectorAlignedBufferPool bufferPool, ObjectIdMap objectIdMap, Action<IHeapObject> objectDisposer)
        {
            var recordBuffer = AllocateBuffer(logRecord, bufferPool);
            return new DiskLogRecord(recordBuffer, objectIdMap,
                logRecord.Info.KeyIsOverflow ? logRecord.KeyOverflow : default,
                logRecord.Info.ValueIsOverflow ? logRecord.ValueOverflow : default,
                logRecord.Info.ValueIsObject ? logRecord.ValueObject : default, objectDisposer);
        }

        /// <summary>
        /// Copies a LogRecord with no out-of-line objects into our contained <see cref="logRecord"/>.
        /// </summary>
        /// <param name="recordBuffer">The record buffer, either from IO or a copy for pending operations such as Compact or ConditionalCopyToTail.</param>
        /// <param name="objectIdMap">The <see cref="ObjectIdMap"/> to hold the objects for the <see cref="logRecord"/>.</param>
        /// <param name="keyOverflow">The key overflow byte[] wrapper, if any</param>
        /// <param name="valueOverflow">The value overflow byte[] wrapper, if any</param>
        /// <param name="valueObject">The value object, if any</param>
        /// <param name="objectDisposer">The action to invoke when disposing the value object if it is present when we dispose the <see cref="logRecord"/></param>
        internal static DiskLogRecord TransferFrom(ref SectorAlignedMemory recordBuffer, ObjectIdMap objectIdMap, OverflowByteArray keyOverflow,
            OverflowByteArray valueOverflow, IHeapObject valueObject, Action<IHeapObject> objectDisposer)
        {
            var diskLogRecord = new DiskLogRecord(recordBuffer, objectIdMap, keyOverflow, valueOverflow, valueObject, objectDisposer);
            recordBuffer = default;     // Transfer ownership to us
            return diskLogRecord;
        }

        /// <summary>
        /// Transfers a LogRecord with no out-of-line objects into our contained <see cref="logRecord"/>.
        /// </summary>
        /// <param name="recordBuffer">The record buffer, either from IO or a copy for pending operations such as Compact or ConditionalCopyToTail.</param>
        internal static DiskLogRecord TransferFrom(ref SectorAlignedMemory recordBuffer)
        {
            var diskLogRecord = new DiskLogRecord(recordBuffer);
            recordBuffer = default;     // Transfer ownership to us
            return diskLogRecord;
        }

        private static SectorAlignedMemory AllocateBuffer(LogRecord logRecord, SectorAlignedBufferPool bufferPool)
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
        public readonly bool IsMemoryLogRecord => false;

        /// <inheritdoc/>
        public readonly unsafe ref LogRecord AsMemoryLogRecordRef() => throw new InvalidOperationException("Cannot cast a DiskLogRecord to a memory LogRecord.");

        /// <inheritdoc/>
        public readonly bool IsDiskLogRecord => true;

        /// <inheritdoc/>
        public readonly unsafe ref DiskLogRecord AsDiskLogRecordRef() => ref Unsafe.AsRef(in this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordFieldInfo GetRecordFieldInfo() => logRecord.GetRecordFieldInfo();

        #endregion //ISourceLogRecord
    }
}