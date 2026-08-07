// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Tsavorite.core.Allocator.ObjectSerialization;

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

        public override readonly string ToString()
        {
            return $"logRec [{logRecord}], recordBuffer [{recordBuffer?.ToString() ?? "<null>"}]";
        }

        /// <summary>
        /// Constructor taking the record buffer and out-of-line objects. Private; use either CopyFrom or TransferFrom.
        /// </summary>
        /// <param name="recordBuffer">The record buffer, either from IO or a copy for pending operations such as Compact or ConditionalCopyToTail.</param>
        /// <param name="transientObjectIdMap">The <see cref="ObjectIdMap"/> to hold the objects for the <see cref="LogRecord"/> for the lifetime of this <see cref="DiskLogRecord"/>.</param>
        /// <param name="keyOverflow">The key overflow byte[] wrapper, if any</param>
        /// <param name="valueOverflow">The value overflow byte[] wrapper, if any</param>
        /// <param name="valueObject">The value object, if any</param>
        /// <remarks>We always own the record buffer; it is either transferred to us by TransferFrom, or allocated as a copy of the record memory by CopyFrom</remarks>
        private DiskLogRecord(SectorAlignedMemory recordBuffer, ObjectIdMap transientObjectIdMap, OverflowByteArray keyOverflow,
            OverflowByteArray valueOverflow, IHeapObject valueObject)
        {
            this.recordBuffer = recordBuffer;
            logRecord = new((long)recordBuffer.GetValidPointer(), transientObjectIdMap);

            // Assign any out-of-line fields. This will put them into transientObjectIdMap.
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
        internal DiskLogRecord(in LogRecord memoryLogRecord)
        {
            logRecord = memoryLogRecord;
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
        internal static DiskLogRecord CreateFromTransientLogRecord(in LogRecord memoryLogRecord) => new(memoryLogRecord);

        /// <summary>
        /// Allocates <see cref="recordBuffer"/> and copies the LogRecord's record memory into it; any out-of-line objects are shallow-copied.
        /// </summary>
        /// <param name="logRecord">The <see cref="LogRecord"/> to copy</param>
        /// <param name="bufferPool">The buffer pool to allocate from</param>
        /// <param name="transientObjectIdMap">The <see cref="ObjectIdMap"/> to hold the objects for the <see cref="LogRecord"/> for the lifetime of this <see cref="DiskLogRecord"/>.</param>
        internal static DiskLogRecord CopyFrom(in LogRecord logRecord, SectorAlignedBufferPool bufferPool, ObjectIdMap transientObjectIdMap)
        {
            // Allocate from ActualSize roundup here because the value may have been shrunk.
            var allocatedSize = RoundUp(logRecord.ActualSize, Constants.kRecordAlignment);
            var recordBuffer = bufferPool.Get(allocatedSize);

            // Copy the inline portion of the logRecord.
            logRecord.RecordSpan.CopyTo(recordBuffer.RequiredValidSpan);

            return new DiskLogRecord(recordBuffer, transientObjectIdMap,
                logRecord.DataHeader.KeyIsOverflow ? logRecord.KeyOverflow : default,
                logRecord.DataHeader.ValueIsOverflow ? logRecord.ValueOverflow : default,
                logRecord.DataHeader.ValueIsObject ? logRecord.ValueObject : default);
        }

        /// <summary>
        /// Copies a LogRecord with no out-of-line objects into our contained <see cref="LogRecord"/>.
        /// </summary>
        /// <param name="recordBuffer">The record buffer, either from IO or a copy for pending operations such as Compact or ConditionalCopyToTail.</param>
        /// <param name="transientObjectIdMap">The <see cref="ObjectIdMap"/> to hold the objects for the <see cref="LogRecord"/> for the lifetime of this <see cref="DiskLogRecord"/>.</param>
        /// <param name="keyOverflow">The key overflow byte[] wrapper, if any</param>
        /// <param name="valueOverflow">The value overflow byte[] wrapper, if any</param>
        /// <param name="valueObject">The value object, if any</param>
        internal static DiskLogRecord TransferFrom(ref SectorAlignedMemory recordBuffer, ObjectIdMap transientObjectIdMap, OverflowByteArray keyOverflow,
            OverflowByteArray valueOverflow, IHeapObject valueObject)
        {
            var diskLogRecord = new DiskLogRecord(recordBuffer, transientObjectIdMap, keyOverflow, valueOverflow, valueObject);
            recordBuffer = default;     // Transfer ownership to us
            return diskLogRecord;
        }

        internal static DiskLogRecord TransferFrom(ref DiskLogRecord srcDiskLogRecord, SectorAlignedBufferPool bufferPool)
        {
            DiskLogRecord diskLogRecord;
            if (srcDiskLogRecord.recordBuffer is not null)
                diskLogRecord = new DiskLogRecord(in srcDiskLogRecord.logRecord) { recordBuffer = srcDiskLogRecord.recordBuffer };
            else
            {
                // Deep copy. This is necessary when srcDiskLogRecord does not own its recordBuffer, because the underlying memory
                // may be freed or reused--e.g. if it is from an iterator frame.
                diskLogRecord = CopyFrom(in srcDiskLogRecord.logRecord, bufferPool, srcDiskLogRecord.logRecord.objectIdMap);
            }

            srcDiskLogRecord = default;              // Transfer ownership to us, and make sure we don't try to clear the logRecord
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

        public void Dispose()
        {
            if (logRecord.IsSet)
            {
                // Pure cleanup: clear the inner LogRecord's heap-field slots and release the record buffer.
                // The IHeapObject owned by this DiskLogRecord (if any) is disposed via the store-level
                // IRecordTriggers.OnDisposeDiskRecord trigger, which callers must invoke before this
                // Dispose(). The allocator's OnDisposeDiskRecord forwards to that trigger.
                logRecord.Dispose();
            }
            logRecord = default;

            recordBuffer?.Return();
            recordBuffer = default;
        }

        #region ISourceLogRecord
        /// <inheritdoc/>
        public readonly bool IsPinnedKey => logRecord.DataHeader.KeyIsInline;

        /// <inheritdoc/>
        public readonly byte* PinnedKeyPointer => logRecord.PinnedKeyPointer;

        /// <inheritdoc/>
        public OverflowByteArray KeyOverflow
        {
            readonly get => logRecord.KeyOverflow;
            set => logRecord.KeyOverflow = value;
        }

        /// <inheritdoc/>
        public readonly bool IsPinnedValue => logRecord.DataHeader.ValueIsInline;

        /// <inheritdoc/>
        public readonly byte* PinnedValuePointer => logRecord.PinnedValuePointer;

        /// <inheritdoc/>
        public OverflowByteArray ValueOverflow
        {
            readonly get => logRecord.ValueOverflow;
            set => logRecord.ValueOverflow = value;
        }

        /// <inheritdoc/>
        public readonly SpanByteAndMemory ValueSpanByteAndMemory
        {
            get
            {
                // For an inline value, the underlying SpanByte points into this DiskLogRecord's
                // recordBuffer (a SectorAlignedMemory rented from a pool). That buffer is returned
                // to the pool when this DiskLogRecord is disposed -- typically as part of pending-
                // completion cleanup, immediately after the read callback returns, or when a scan
                // iterator advances. To keep the contract uniform with in-memory LogRecord (where
                // SpanByte is stable for the unsafe context), copy the bytes into a pooled
                // IMemoryOwner so the returned SpanByteAndMemory remains valid past disposal.
                if (logRecord.IsPinnedValue)
                {
                    var span = logRecord.ValueSpan;
                    var owner = MemoryPool<byte>.Shared.Rent(span.Length);
                    span.CopyTo(owner.Memory.Span);
                    return new SpanByteAndMemory(owner, span.Length);
                }

                // Overflow values come back as a no-copy BorrowedMemoryOwner around the underlying
                // GC-managed byte[]. The byte[] stays rooted via the Memory<byte> reference inside
                // the owner, so it survives DiskLogRecord disposal without an extra copy.
                return logRecord.ValueSpanByteAndMemory;
            }
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
        // Implemented directly (rather than relying on the ISourceLogRecord default) so that calls
        // through the ISourceLogRecord generic constraint resolve here instead of boxing to dispatch
        // the default interface method.
        public readonly long PhysicalAddress => logRecord.PhysicalAddress;

        /// <inheritdoc/>
        public ref RecordInfo InfoRef => ref logRecord.InfoRef;
        /// <inheritdoc/>
        public readonly RecordInfo Info => logRecord.Info;
        /// <inheritdoc/>
        public readonly RecordDataHeader DataHeader => logRecord.DataHeader;

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
        public readonly void ClearValueIfHeap() { }  // Nothing to do here; we dispose the object in the pending operation or iteration completion

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

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long CalculateHeapMemorySize() => logRecord.CalculateHeapMemorySize();
        #endregion //ISourceLogRecord

        #region IKey
        /// <inheritdoc/>
        public readonly bool IsPinned => IsPinnedKey;

        /// <inheritdoc/>
        // Implemented directly (rather than relying on the IKey default) so that calls through the
        // IKey generic constraint resolve here instead of boxing to dispatch the default interface method.
        public readonly bool IsEmpty => false;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> KeyBytes => Key;

        /// <inheritdoc/>
        public readonly bool HasNamespace => logRecord.HasNamespace;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> NamespaceBytes => logRecord.NamespaceBytes;
        #endregion


        #region Serialization to and from expanded record format
        /// <summary>
        /// Serialize a log record (in-memory <see cref="LogRecord"/> or IO'd <see cref="DiskLogRecord"/>) that is too large for a
        /// single network send buffer as a sequence of chunks, for the migration / replication chunked path (the small-record
        /// fast path - the whole record in one buffer - is handled by the caller). The record is written through
        /// <paramref name="chunker"/> (a reused network-mode <see cref="ChunkedObjectSerializer{TContext}"/>) as one continuous
        /// byte stream:
        /// <list type="bullet">
        ///     <item>Inline portion of the LogRecord: RecordInfo, IndicatorWord, inline Key/Value (each 4-byte length restoring to
        ///         an object Id), optionals (ETag, Expiration, ObjectLogPosition, ...)</item>
        ///     <item>Key data, if the key is Overflow, preceded by its 4-byte length</item>
        ///     <item>Value data, if the value is Overflow, preceded by its 4-byte length; or the streamed object serialization
        ///         (no length prefix), if the value is an Object</item>
        /// </list>
        /// The chunker's consumer frames each drained span as a chunk record (see <c>GarnetClientSession.TryWriteChunkedRecordSpan</c>)
        /// with a continuation flag that is clear only on the record's last chunk. The receiver (<c>ChunkedRecordReassembler</c>)
        /// routes the stream by component using <see cref="GetChunkedRecordInlineSize"/>, the record header, and the overflow length
        /// prefixes, populating the overflow key/value buffers directly and streaming any object value, then builds the record via
        /// <see cref="Deserialize"/> (fully-inline) or <see cref="CompleteDeserializeChunkedRecord"/> (out-of-line components).
        /// </summary>
        /// <typeparam name="TSourceLogRecord">The source log record type.</typeparam>
        /// <typeparam name="TContext">Per-record caller state threaded through the chunker to its consumer on every drain.</typeparam>
        /// <param name="srcLogRecord">The source log record to serialize.</param>
        /// <param name="valueObjectSerializer">Serializer used to stream an object value into the chunk stream (ignored if the value is not an object).</param>
        /// <param name="chunker">The reused chunk writer (network manual mode); its consumer frames and sends each drained span.</param>
        /// <param name="context">Per-record caller state passed to the chunker's consumer on every drain.</param>
        public static void Serialize<TSourceLogRecord, TContext>(in TSourceLogRecord srcLogRecord, IObjectSerializer<IHeapObject> valueObjectSerializer,
                ChunkedObjectSerializer<TContext> chunker, TContext context)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.IsMemoryLogRecord)
                SerializeChunked(in srcLogRecord.AsMemoryLogRecordRef(), valueObjectSerializer, chunker, context);
            else if (srcLogRecord.IsDiskLogRecord)
                SerializeChunked(in srcLogRecord.AsDiskLogRecordRef().logRecord, valueObjectSerializer, chunker, context);
            else
                throw new TsavoriteException("Unknown TSourceLogRecord type");
        }

        static void SerializeChunked<TContext>(in LogRecord logRecord, IObjectSerializer<IHeapObject> valueObjectSerializer, ChunkedObjectSerializer<TContext> chunker, TContext context)
        {
            var dataHeader = logRecord.DataHeader;
            var alignedInlineRecordSize = RoundUp(logRecord.ActualSize, Constants.kRecordAlignment);

            chunker.BeginSerialize(context);

            // Emit the inline portion directly from the record's native memory. These bytes are the aligned component image; the
            // receiver locates the overflow components at RoundUp(ActualSize), derived from the RDH component lengths and so
            // filler-independent. The RDH filler word is emitted as-is: the receiver applies the record with a semantic Upsert
            // that sizes the destination from the key/value field info and never reads the wire filler.
            chunker.WriteBytes(new ReadOnlySpan<byte>((byte*)logRecord.physicalAddress, alignedInlineRecordSize));

            if (!dataHeader.RecordIsInline)
            {
                // Each overflow key/value is preceded by its 4-byte length so the receiver allocates the overflow buffer up front
                // and populates it directly from the stream. An object value is streamed with no prefix (length derived).
                if (dataHeader.KeyIsOverflow)
                {
                    var keySpan = logRecord.KeyOverflow.ReadOnlySpan;
                    WriteChunkedLengthPrefix(chunker, keySpan.Length);
                    chunker.WriteBytes(keySpan);
                }

                if (dataHeader.ValueIsOverflow)
                {
                    var valueSpan = logRecord.ValueOverflow.ReadOnlySpan;
                    WriteChunkedLengthPrefix(chunker, valueSpan.Length);
                    chunker.WriteBytes(valueSpan);
                }
                else if (dataHeader.ValueIsObject)
                {
                    // Stream the object into the chunk buffer; it drains to the consumer as it fills, so the whole serialized
                    // form is never materialized at once.
                    var stream = chunker.GetStream();
                    valueObjectSerializer.BeginSerialize(stream);
                    valueObjectSerializer.Serialize(logRecord.ValueObject);
                    valueObjectSerializer.EndSerialize();
                }
            }

            chunker.EndSerialize();
        }

        // Write a 4-byte little-endian length prefix for an overflow key/value on the chunked send path.
        static void WriteChunkedLengthPrefix<TContext>(ChunkedObjectSerializer<TContext> chunker, int length)
        {
            Span<byte> prefix = stackalloc byte[sizeof(int)];
            BinaryPrimitives.WriteInt32LittleEndian(prefix, length);
            chunker.WriteBytes(prefix);
        }

        /// <summary>
        /// Serialize only the <b>inline portion</b> of a record into <paramref name="output"/> (growing its heap memory as needed),
        /// compacted to <c>RoundUp(ActualSize)</c>. The overflow key/value/object bytes themselves are NOT written
        /// here: the migration caller captures them separately (see <c>Garnet.server.MigrationChunkWriterAccumulator</c>) and
        /// assembles and sends the whole record out of epoch, prefixing each overflow key/value with its 4-byte length (an object
        /// value is the tail, sent with no prefix; the receiver derives its length).
        /// </summary>
        /// <remarks>Must run while holding the store epoch: it copies the source record's in-memory image.</remarks>
        /// <returns>The length of the inline portion written (the offset at which the overflow key/value would follow).</returns>
        public static int SerializeInlinePortionForMigration<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.IsMemoryLogRecord)
                return SerializeInlinePortion(in srcLogRecord.AsMemoryLogRecordRef(), memoryPool, ref output);
            if (!srcLogRecord.IsDiskLogRecord)
                throw new TsavoriteException("Unknown TSourceLogRecord type");
            return SerializeInlinePortion(in srcLogRecord.AsDiskLogRecordRef().logRecord, memoryPool, ref output);
        }

        static int SerializeInlinePortion(in LogRecord logRecord, MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
        {
            var alignedInlineRecordSize = RoundUp(logRecord.ActualSize, Constants.kRecordAlignment);

            // Copy the inline portion directly; the receiver locates the overflow key/value at RoundUp(ActualSize), so the emitted
            // image stays compacted to that size. The overflow key/value bytes are captured separately by the migration
            // accumulator and assembled with their 4-byte length prefixes by the sender.
            DirectCopyInlinePortionOfRecord(in logRecord, alignedInlineRecordSize, estimatedTotalSize: alignedInlineRecordSize,
                maxHeapAllocationSize: alignedInlineRecordSize, memoryPool, ref output);

            return alignedInlineRecordSize;
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
            if (!output.IsSpanByte || estimatedTotalSize + sizeof(int) > output.SpanByte.Length || logRecord.DataHeader.ValueIsObject)
            {
                var allocationSizeToUse = logRecord.DataHeader.ValueIsObject ? maxHeapAllocationSize : estimatedTotalSize + sizeof(int);
                if (estimatedTotalSize > allocationSizeToUse)
                    throw new TsavoriteException($"estimatedRecordSize ({estimatedTotalSize}) exceeds max allocated heap size (to use: {allocationSizeToUse}; max: {maxHeapAllocationSize})");
                output.EnsureHeapMemorySize(allocationSizeToUse, memoryPool);
            }

            // Rewrite the filler word to the rounding remainder so the copied image is a self-consistent LogRecord of length
            // RoundUp(ActualSize) (a shrunk source record can carry a larger explicit filler). This is needed by the in-memory
            // RENAME consumer, which builds a live LogRecord over this buffer; wire receivers ignore the filler and size the
            // destination from the key/value field info.
            var newFillerLength = alignedInlineRecordSize - logRecord.ActualSize;
            if (output.IsSpanByte)
            {
                // TotalSize includes the length prefix. If there is a SpanByte it is a span in the network buffer, so we include the prefix length in the output stream.
                var outPtr = output.SpanByte.ToPointer();
                *(int*)outPtr = alignedInlineRecordSize;
                outPtr += sizeof(int);
                Buffer.MemoryCopy((byte*)logRecord.PhysicalAddress, outPtr, alignedInlineRecordSize, alignedInlineRecordSize);
                new LogRecord((long)outPtr).SetFillerLength(newFillerLength);
            }
            else
            {
                // Do not include the length prefix in the output stream; this is done by the caller before writing the stream to the network buffer.
                fixed (byte* outPtr = output.MemorySpan)
                {
                    Buffer.MemoryCopy((byte*)logRecord.PhysicalAddress, outPtr, alignedInlineRecordSize, alignedInlineRecordSize);
                    new LogRecord((long)outPtr).SetFillerLength(newFillerLength);
                }
            }
        }

        /// <summary>
        /// Deserialize a whole record image (the non-chunked <c>MigrationRecordSpanType.LogRecord</c> path) created by
        /// <see cref="SerializeInlinePortionForMigration"/> + the migration accumulator: a fully-inline record, or an inline
        /// portion followed by an overflow key and either an overflow value or an object value. An object value is the tail of
        /// the image, so its length is derived from <paramref name="recordSpan"/> (the sender leaves the RDH object length zero);
        /// the object deserializes directly from the buffer with no chunk reassembly.
        /// </summary>
        /// <param name="recordSpan">The whole record bytes.</param>
        /// <param name="valueObjectSerializer">Serializer used to deserialize an object value.</param>
        /// <param name="transientObjectIdMap">Transient object-id map for the deserialized record's overflow/object slots.</param>
        /// <param name="storeFunctions">The store functions.</param>
        public static DiskLogRecord Deserialize<TStoreFunctions>(PinnedSpanByte recordSpan, IObjectSerializer<IHeapObject> valueObjectSerializer, ObjectIdMap transientObjectIdMap,
            TStoreFunctions storeFunctions)
            where TStoreFunctions : IStoreFunctions
        {
            // Serialize() did not change the KeyIsInline/ValueIsInline/ValueIsObject bits. A non-inline record's out-of-line
            // components follow the inline portion (compacted to RoundUp(ActualSize)): each overflow key/value is preceded by its
            // 4-byte length; an object value is the tail (its length derived from the record span). Create a transient logRecord to
            // decode the layout and populate the overflow/object slots.
            var ptr = recordSpan.ToPointer();
            var serializedLogRecord = new LogRecord((long)ptr, transientObjectIdMap);
            if (serializedLogRecord.DataHeader.RecordIsInline)
                return new(serializedLogRecord);

            var dataHeader = serializedLogRecord.DataHeader;
            var offset = RoundUp(serializedLogRecord.ActualSize, Constants.kRecordAlignment);

            // Note: Similar logic to this is in ObjectLogReader.ReadObjects.
            var keyWasSet = false;
            try
            {
                if (dataHeader.KeyIsOverflow)
                {
                    var keyLength = BinaryPrimitives.ReadInt32LittleEndian(recordSpan.ReadOnlySpan.Slice(offset));
                    offset += sizeof(int);
                    // This assignment also allocates the slot in ObjectIdMap. The RecordDataHeader length info stays ObjectIdSize.
                    serializedLogRecord.KeyOverflow = new OverflowByteArray(keyLength, startOffset: 0, endOffset: 0, zeroInit: false);
                    recordSpan.ReadOnlySpan.Slice(offset, keyLength).CopyTo(serializedLogRecord.KeyOverflow.Span);
                    offset += keyLength;
                    keyWasSet = true;
                }

                if (dataHeader.ValueIsOverflow)
                {
                    var valueLength = BinaryPrimitives.ReadInt32LittleEndian(recordSpan.ReadOnlySpan.Slice(offset));
                    offset += sizeof(int);
                    // This assignment also allocates the slot in ObjectIdMap. The RecordDataHeader length info stays ObjectIdSize.
                    serializedLogRecord.ValueOverflow = new OverflowByteArray(valueLength, startOffset: 0, endOffset: 0, zeroInit: false);
                    recordSpan.ReadOnlySpan.Slice(offset, valueLength).CopyTo(serializedLogRecord.ValueOverflow.Span);
                }
                else if (dataHeader.ValueIsObject)
                {
                    // The object value is the remainder of the record image (no length prefix), so deserialize it from the tail.
                    var objectValueLength = recordSpan.Length - offset;
                    var stream = new UnmanagedMemoryStream(ptr + offset, objectValueLength);
                    valueObjectSerializer.BeginDeserialize(stream);
                    valueObjectSerializer.Deserialize(out var valueObject);
                    serializedLogRecord.ValueObject = valueObject;
                    valueObjectSerializer.EndDeserialize();
                }
                return new(serializedLogRecord);
            }
            catch
            {
                serializedLogRecord.OnDeserializationError(keyWasSet);
                throw;
            }
        }

        /// <summary>
        /// Compute a chunked record's inline-portion size (<c>RoundUp(ActualSize)</c>) from the start of its inline header. The
        /// chunked migration/replication receiver (<c>ChunkedRecordReassembler</c>) uses this to know how many bytes make up the
        /// inline portion; it reads the component kinds (overflow key/value, object value) directly from the buffer's
        /// <see cref="RecordDataHeader"/>. <paramref name="header"/> must cover at least <see cref="ChunkedRecordHeaderSize"/> bytes.
        /// </summary>
        /// <param name="header">A prefix of the reassembled record covering at least its fixed header (<see cref="ChunkedRecordHeaderSize"/> bytes).</param>
        public static int GetChunkedRecordInlineSize(ReadOnlySpan<byte> header)
        {
            Debug.Assert(header.Length >= Constants.FixedHeaderSize, "header must cover at least the fixed record header");
            fixed (byte* ptr = header)
                return RoundUp(new LogRecord((long)ptr).ActualSize, Constants.kRecordAlignment);
        }

        /// <summary>The fixed record header size (RecordInfo + RecordDataHeader); the chunked receiver needs this many bytes to read
        /// the inline size (<see cref="GetChunkedRecordInlineSize"/>) and the component kinds from the <see cref="RecordDataHeader"/>.</summary>
        public static int ChunkedRecordHeaderSize => Constants.FixedHeaderSize;

        /// <summary>
        /// Build a <see cref="DiskLogRecord"/> from a chunked record whose out-of-line components were reassembled by the receiver
        /// (see <c>ChunkedRecordReassembler</c>) directly into their final buffers: the pre-populated overflow key and/or value,
        /// and/or the already-deserialized object value (which can exceed 2 GB). <paramref name="headerSpan"/> holds only the
        /// inline portion (its length is <see cref="GetChunkedRecordInlineSize"/>) - the overflow bytes are NOT in it. Assigns
        /// each present component directly (no re-allocation or copy). The RDH length fields are left untouched (the out-of-line
        /// lengths rode in 4-byte wire length prefixes, which the receiver already consumed).
        /// </summary>
        /// <param name="headerSpan">The record's inline portion.</param>
        /// <param name="keyOverflow">The pre-populated overflow key (used only when the RDH marks the key overflow).</param>
        /// <param name="valueOverflow">The pre-populated overflow value (used only when the RDH marks the value overflow).</param>
        /// <param name="valueObject">The already-deserialized object value (used only when the RDH marks the value an object).</param>
        /// <param name="transientObjectIdMap">Transient object-id map for the deserialized record's overflow/object slots.</param>
        public static DiskLogRecord CompleteDeserializeChunkedRecord(PinnedSpanByte headerSpan, OverflowByteArray keyOverflow, OverflowByteArray valueOverflow,
            IHeapObject valueObject, ObjectIdMap transientObjectIdMap)
        {
            var ptr = headerSpan.ToPointer();
            var serializedLogRecord = new LogRecord((long)ptr, transientObjectIdMap);
            Debug.Assert(!serializedLogRecord.DataHeader.RecordIsInline, "CompleteDeserializeChunkedRecord is only for a non-inline record");

            var keyWasSet = false;
            try
            {
                if (serializedLogRecord.DataHeader.KeyIsOverflow)
                {
                    // Assign the pre-populated overflow key directly (allocates the ObjectIdMap slot; no re-alloc/copy).
                    serializedLogRecord.KeyOverflow = keyOverflow;
                    keyWasSet = true;
                }

                if (serializedLogRecord.DataHeader.ValueIsOverflow)
                    serializedLogRecord.ValueOverflow = valueOverflow; // assign the pre-populated overflow value directly
                else if (serializedLogRecord.DataHeader.ValueIsObject)
                    serializedLogRecord.ValueObject = valueObject;     // assign the already-deserialized object value

                return new(serializedLogRecord);
            }
            catch
            {
                serializedLogRecord.OnDeserializationError(keyWasSet);
                throw;
            }
        }

        #endregion Serialization to and from expanded record format
    }
}