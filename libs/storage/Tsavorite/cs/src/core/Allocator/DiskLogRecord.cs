// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
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
        /// fast path — the whole record in one buffer — is handled by the caller). The record is written through
        /// <paramref name="chunker"/> (a reused network-mode <see cref="ChunkedObjectSerializer{TContext}"/>) as one continuous
        /// byte stream, in the same layout as the non-chunked path:
        /// <list type="bullet">
        ///     <item>Inline portion of the LogRecord: RecordInfo, IndicatorWord, inline Key/Value (each 4-byte length restoring to
        ///         an object Id), optionals (ETag, Expiration, ObjectLogPosition, ...)</item>
        ///     <item>Key data, if the key is Overflow</item>
        ///     <item>Value data, if the value is Overflow; or the streamed object serialization, if the value is an Object</item>
        /// </list>
        /// The chunker's consumer frames each drained span as a chunk record (see <c>GarnetClientSession.TryWriteChunkedRecordSpan</c>)
        /// with a continuation flag that is clear only on the record's last chunk; the receiver reassembles the chunks and calls
        /// <see cref="Deserialize"/>. A streamed object value's length is not known when the inline portion is emitted, so its RDH
        /// value-length is left zero here and the receiver supplies it to <see cref="Deserialize"/> via the value-length override.
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
            var actualSize = logRecord.ActualSize;
            var alignedInlineRecordSize = RoundUp(actualSize, Constants.kRecordAlignment);

            chunker.BeginSerialize(context);

            // Emit the inline portion. Copy it to a scratch buffer so we can reset the filler length (the record may have been
            // shrunk) and, for a non-inline record, encode the overflow key/value lengths into the RDH. For a streamed object
            // value the length is not yet known, so its RDH value-length is left zero (SetObjectLogRecordStartPositionAndLength
            // ignores the passed length for an overflow value and uses it verbatim for an object value); the receiver derives it.
            var rented = ArrayPool<byte>.Shared.Rent(alignedInlineRecordSize);
            try
            {
                fixed (byte* scratch = rented)
                {
                    Buffer.MemoryCopy((byte*)logRecord.physicalAddress, scratch, alignedInlineRecordSize, alignedInlineRecordSize);
                    var scratchRecord = new LogRecord((long)scratch, logRecord.objectIdMap);
                    scratchRecord.SetFillerLength(alignedInlineRecordSize - actualSize);
                    if (!dataHeader.RecordIsInline)
                    {
                        var fakeFilePos = new ObjectLogFilePositionInfo((ulong)alignedInlineRecordSize, segSizeBits: 0);
                        scratchRecord.SetObjectLogRecordStartPositionAndLength(fakeFilePos, valueObjectLength: 0);
                    }
                }

                // WriteBytes from the managed array (outside the fixed) so a drain's blocking flush does not hold the pin.
                chunker.WriteBytes(new ReadOnlySpan<byte>(rented, 0, alignedInlineRecordSize));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }

            if (!dataHeader.RecordIsInline)
            {
                if (dataHeader.KeyIsOverflow)
                    chunker.WriteBytes(logRecord.KeyOverflow.ReadOnlySpan);

                if (dataHeader.ValueIsOverflow)
                {
                    chunker.WriteBytes(logRecord.ValueOverflow.ReadOnlySpan);
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

        /// <summary>
        /// Serialize a whole log record into <paramref name="output"/> (growing its heap memory as needed) in the non-chunked
        /// layout that <see cref="Deserialize"/> reads. Used by the migration read path, which must serialize while holding the
        /// store epoch (a migrating key is not locked and may be concurrently updated) and therefore cannot stream to the
        /// network there; the caller sends the resulting buffer out of epoch (whole if it fits a send buffer, else sliced into
        /// chunks). Unlike the chunked <see cref="Serialize"/>, an object value's length is known here, so its RDH value-length
        /// is encoded normally.
        /// </summary>
        /// <typeparam name="TSourceLogRecord">The source log record type.</typeparam>
        /// <param name="srcLogRecord">The source log record to serialize.</param>
        /// <param name="valueObjectSerializer">Serializer for an object value (ignored if the value is not an object).</param>
        /// <param name="memoryPool">Memory pool used to grow <paramref name="output"/> if needed.</param>
        /// <param name="output">Receives the serialized record (in <see cref="SpanByteAndMemory.Memory"/> when it must grow).</param>
        /// <returns>The serialized record length (no length prefix).</returns>
        public static int SerializeToBuffer<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, IObjectSerializer<IHeapObject> valueObjectSerializer,
                MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.IsMemoryLogRecord)
                return SerializeLogRecordToBuffer(in srcLogRecord.AsMemoryLogRecordRef(), valueObjectSerializer, memoryPool, ref output);
            if (!srcLogRecord.IsDiskLogRecord)
                throw new TsavoriteException("Unknown TSourceLogRecord type");
            return SerializeLogRecordToBuffer(in srcLogRecord.AsDiskLogRecordRef().logRecord, valueObjectSerializer, memoryPool, ref output);
        }

        static int SerializeLogRecordToBuffer(in LogRecord logRecord, IObjectSerializer<IHeapObject> valueObjectSerializer, MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
        {
            var dataHeader = logRecord.DataHeader;
            var alignedInlineRecordSize = RoundUp(logRecord.ActualSize, Constants.kRecordAlignment);
            if (dataHeader.RecordIsInline)
            {
                DirectCopyInlinePortionOfRecord(in logRecord, alignedInlineRecordSize, estimatedTotalSize: alignedInlineRecordSize,
                    maxHeapAllocationSize: alignedInlineRecordSize, memoryPool, ref output);
                return alignedInlineRecordSize;
            }

            // Not inline: determine the heap component sizes (serializing an object value up front, into a growable stream, so
            // its length is known), size the output for the whole record, then lay out the inline portion, overflow key, and value.
            var keyLength = dataHeader.KeyIsOverflow ? logRecord.KeyOverflow.Length : 0;

            var valueLength = 0;
            byte[] objectBuffer = null;
            if (dataHeader.ValueIsOverflow)
            {
                valueLength = logRecord.ValueOverflow.Length;
            }
            else
            {
                Debug.Assert(dataHeader.ValueIsObject, "Expected ValueIsObject to be true");
                using var stream = new MemoryStream();
                valueObjectSerializer.BeginSerialize(stream);
                valueObjectSerializer.Serialize(logRecord.ValueObject);
                valueObjectSerializer.EndSerialize();
                objectBuffer = stream.GetBuffer();
                valueLength = (int)stream.Length;
            }

            var totalSize = alignedInlineRecordSize + keyLength + valueLength;

            // Grows output to the whole-record size and copies the inline portion into it.
            DirectCopyInlinePortionOfRecord(in logRecord, alignedInlineRecordSize, estimatedTotalSize: totalSize,
                maxHeapAllocationSize: totalSize, memoryPool, ref output);

            var outputSpan = output.Span;
            var offset = alignedInlineRecordSize;
            if (dataHeader.KeyIsOverflow)
            {
                logRecord.KeyOverflow.ReadOnlySpan.CopyTo(outputSpan.Slice(offset, keyLength));
                offset += keyLength;
            }
            if (dataHeader.ValueIsOverflow)
                logRecord.ValueOverflow.ReadOnlySpan.CopyTo(outputSpan.Slice(offset, valueLength));
            else
                new ReadOnlySpan<byte>(objectBuffer, 0, valueLength).CopyTo(outputSpan.Slice(offset, valueLength));

            // Encode the overflow/object lengths into the inline portion's RDH so Deserialize can restore them.
            var fakeFilePos = new ObjectLogFilePositionInfo((ulong)alignedInlineRecordSize, segSizeBits: 0);
            fixed (byte* ptr = outputSpan)
            {
                var serializedLogRecord = new LogRecord((long)ptr, logRecord.objectIdMap);
                serializedLogRecord.SetObjectLogRecordStartPositionAndLength(fakeFilePos, (ulong)valueLength);
            }

            return totalSize;
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

            // We must reset the LogRecord's filler size, because we truncated the record down to the (rounded-up) ActualSize if it had been shrunken.
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
        /// Deserialize from a <see cref="PinnedSpanByte"/> over a stream of bytes created by <see cref="Serialize"/>.
        /// </summary>
        /// <param name="recordSpan">The reassembled record bytes.</param>
        /// <param name="valueObjectSerializer">Serializer used to deserialize an object value.</param>
        /// <param name="transientObjectIdMap">Transient object-id map for the deserialized record's overflow/object slots.</param>
        /// <param name="storeFunctions">The store functions.</param>
        /// <param name="objectValueLengthOverride">When non-negative, the object value's serialized length to use instead of the
        ///     one encoded in the RDH. The chunked network path (see <see cref="Serialize"/>) streams the object value and so
        ///     cannot encode its length up front; the receiver derives it and passes it here. Only valid for an object value.</param>
        public static DiskLogRecord Deserialize<TStoreFunctions>(PinnedSpanByte recordSpan, IObjectSerializer<IHeapObject> valueObjectSerializer, ObjectIdMap transientObjectIdMap,
            TStoreFunctions storeFunctions, int objectValueLengthOverride = -1)
            where TStoreFunctions : IStoreFunctions
        {
            // Serialize() did not change the state of the KeyIsInline/ValueIsInline/ValueIsObject bits, but it did change the value at the ObjectId
            // location to be serialized length. Create a transient logRecord to decode these and restore the objectId values.
            var ptr = recordSpan.ToPointer();
            var serializedLogRecord = new LogRecord((long)ptr, transientObjectIdMap);
            if (serializedLogRecord.DataHeader.RecordIsInline)
                return new(serializedLogRecord);
            var offset = serializedLogRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength);

            if (objectValueLengthOverride >= 0)
            {
                // The chunked sender left an object value's RDH length as zero (it is streamed, so unknown up front); use the length the receiver derived.
                Debug.Assert(serializedLogRecord.DataHeader.ValueIsObject, "objectValueLengthOverride is only valid for an object value");
                valueLength = (ulong)objectValueLengthOverride;
            }
            // Note: Similar logic to this is in ObjectLogReader.ReadObjects.
            var keyWasSet = false;
            try
            {
                if (serializedLogRecord.DataHeader.KeyIsOverflow)
                {
                    // This assignment also allocates the slot in ObjectIdMap. The RecordDataHeader length info should be unchanged from ObjectIdSize.
                    serializedLogRecord.KeyOverflow = new OverflowByteArray(keyLength, startOffset: 0, endOffset: 0, zeroInit: false);
                    recordSpan.ReadOnlySpan.Slice((int)offset, keyLength).CopyTo(serializedLogRecord.KeyOverflow.Span);
                    offset += (uint)keyLength;
                    keyWasSet = true;
                }

                if (serializedLogRecord.DataHeader.ValueIsOverflow)
                {
                    // This assignment also allocates the slot in ObjectIdMap. The RecordDataHeader length info should be unchanged from ObjectIdSize.
                    serializedLogRecord.ValueOverflow = new OverflowByteArray((int)valueLength, startOffset: 0, endOffset: 0, zeroInit: false);
                    recordSpan.ReadOnlySpan.Slice((int)offset, (int)valueLength).CopyTo(serializedLogRecord.ValueOverflow.Span);
                }
                else
                {
                    var stream = new UnmanagedMemoryStream(ptr + offset, (int)valueLength);
                    valueObjectSerializer.BeginDeserialize(stream);
                    valueObjectSerializer.Deserialize(out var valueObject);
                    serializedLogRecord.ValueObject = valueObject;
                    valueObjectSerializer.EndDeserialize();
                }

                // Restore raw RDH KeyLength/ValueLength fields to ObjectIdSize for in-memory invariant (R11.5).
                serializedLogRecord.OnObjectReadComplete();
                return new(serializedLogRecord);
            }
            catch
            {
                serializedLogRecord.OnDeserializationError(keyWasSet);
                throw;
            }
        }

        /// <summary>
        /// Deserialize a record reassembled from <see cref="Serialize"/>'s chunk stream (migration / replication chunked path).
        /// A streamed object value's length is not encoded in the RDH (the chunked <see cref="Serialize"/> cannot know it up
        /// front), so it is derived here from the reassembled record size — the object value is the record's last component, so
        /// its length is <c>total − inlinePortion − overflowKey</c> — and passed to <see cref="Deserialize"/> as the value-length
        /// override. When the RDH length is present (the migration buffer path encodes it), it must agree.
        /// </summary>
        public static DiskLogRecord DeserializeChunked<TStoreFunctions>(PinnedSpanByte recordSpan, IObjectSerializer<IHeapObject> valueObjectSerializer,
                ObjectIdMap transientObjectIdMap, TStoreFunctions storeFunctions)
            where TStoreFunctions : IStoreFunctions
        {
            var objectValueLengthOverride = -1;
            var logRecord = new LogRecord((long)recordSpan.ToPointer());
            if (!logRecord.DataHeader.RecordIsInline && logRecord.DataHeader.ValueIsObject)
            {
                // Derive the object value length from the reassembled size (it is the record's last component). An overflow key's
                // length is encoded in the RDH; an overflow value never coexists with an object value.
                _ = logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var rdhValueLength);
                var inlineRecordSize = RoundUp(logRecord.ActualSize, Constants.kRecordAlignment);
                objectValueLengthOverride = recordSpan.Length - inlineRecordSize - keyLength;
                Debug.Assert(objectValueLengthOverride >= 0, "Chunked record: derived object value length is negative");
                Debug.Assert(rdhValueLength == 0 || (long)rdhValueLength == objectValueLengthOverride,
                    "Chunked record: RDH-encoded object length disagrees with the reassembled size");
            }

            return Deserialize(recordSpan, valueObjectSerializer, transientObjectIdMap, storeFunctions, objectValueLengthOverride);
        }

        /// <summary>
        /// Return the serialized size of the contained logRecord.
        /// </summary>
        public readonly int GetSerializedSize() => logRecord.GetSerializedSize();

        #endregion Serialization to and from expanded record format
    }
}