// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>The record on the disk: header, optional fields, key, value</summary>
    /// <remarks>The space is laid out as:
    ///     <list>
    ///     <item>[RecordInfo][SerializedRecordLength][ETag?][Expiration?][key Span][value Span]</item>
    ///     </list>
    /// This lets us get to the optional fields for comparisons without loading the full record (GetIOSize should cover the space for optionals).
    /// </remarks>
    public unsafe struct DiskLogRecord : ISourceLogRecord, IDisposable
    {
        /// <summary>The length of the serialized data.</summary>
        internal const int SerializedRecordLengthSize = sizeof(long);

        /// <summary>The physicalAddress in the log.</summary>
        internal long physicalAddress;

        /// <summary>The deserialized ValueObject if this is a disk record for the Object Store. Held directly; does not use <see cref="ObjectIdMap"/>.</summary>
        internal IHeapObject valueObject;

        /// <summary>If this is non-null, it must be freed on <see cref="Dispose()"/>.</summary>
        internal SectorAlignedMemory allocatedBuffer;

        /// <summary>Constructor that takes a physical address, which may come from a <see cref="SectorAlignedMemory"/> or some other allocation
        /// that will have at least the lifetime of this <see cref="DiskLogRecord"/>. This <see cref="DiskLogRecord"/> does not own the memory allocation
        /// so will not free it on <see cref="Dispose()"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DiskLogRecord(long physicalAddress)
        {
            this.physicalAddress = physicalAddress;
            InfoRef.ClearBitsForDiskImages();
        }

        /// <summary>Constructor that takes a <see cref="SectorAlignedMemory"/> from which it obtains the physical address.
        /// This <see cref="DiskLogRecord"/> owns the memory allocation and must free it on <see cref="Dispose()"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DiskLogRecord(SectorAlignedMemory allocatedBuffer)
            : this((long)allocatedBuffer.GetValidPointer())
        {
            this.allocatedBuffer = allocatedBuffer;
        }

        /// <summary>A ref to the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref RecordInfo GetInfoRef(long physicalAddress) => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);

        /// <summary>Fast access returning a copy of the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RecordInfo GetInfo(long physicalAddress) => *(RecordInfo*)physicalAddress;

        /// <summary>Serialized length of the record</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetSerializedLength(long physicalAddress) => *(long*)(physicalAddress + RecordInfo.GetLength());

        /// <summary>If true, this DiskLogRecord owns the buffer and must free it on <see cref="Dispose"/></summary>
        public readonly bool OwnsMemory => allocatedBuffer is not null;

        #region ISourceLogRecord
        /// <inheritdoc/>
        public readonly bool ValueIsObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(Info.ValueIsObject == valueObject is not null, $"Mismatch between Info.ValueIsObject ({Info.ValueIsObject}) and valueObject is not null {valueObject is not null}");
                return Info.ValueIsObject;
            }
        }

        public void Dispose()
        {
            allocatedBuffer?.Dispose();
            allocatedBuffer = null;
        }

        /// <inheritdoc/>
        public bool IsPinnedValue => Info.ValueIsInline;

        /// <inheritdoc/>
        public byte* PinnedValuePointer => IsPinnedValue ? (byte*)ValueAddress : null;

        /// <inheritdoc/>
        public readonly bool IsSet => physicalAddress != 0;
        /// <inheritdoc/>
        public readonly ref RecordInfo InfoRef => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);
        /// <inheritdoc/>
        public readonly RecordInfo Info => *(RecordInfo*)physicalAddress;
        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> Key => SpanByte.FromLengthPrefixedPinnedPointer((byte*)KeyAddress);
        /// <inheritdoc/>
        public bool IsPinnedKey => true;

        /// <inheritdoc/>
        public byte* PinnedKeyPointer => (byte*)KeyAddress;

        /// <inheritdoc/>
        public readonly Span<byte> ValueSpan => ValueIsObject ? throw new TsavoriteException("DiskLogRecord with ValueIsObject does not support Span<byte> values") : SpanByte.FromLengthPrefixedPinnedPointer((byte*)ValueAddress);
        /// <inheritdoc/>
        public readonly IHeapObject ValueObject => ValueIsObject ? valueObject : throw new TsavoriteException("This DiskLogRecord has a Span Value");
        
        /// <inheritdoc/>
        public readonly long ETag => Info.HasETag ? *(long*)GetETagAddress() : LogRecord.NoETag;

        /// <inheritdoc/>
        public readonly long Expiration => Info.HasExpiration ? *(long*)GetExpirationAddress() : 0;

        /// <inheritdoc/>
        public readonly void ClearValueObject(Action<IHeapObject> disposer) { }  // Nothing done here; we dispose the object in the pending operation completion

        /// <inheritdoc/>
        public readonly bool AsLogRecord(out LogRecord logRecord)
        {
            logRecord = default;
            return false;
        }

        /// <inheritdoc/>
        public readonly bool AsDiskLogRecord(out DiskLogRecord diskLogRecord)
        {
            diskLogRecord = this;
            return true;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordFieldInfo GetRecordFieldInfo() => new()
            {
                KeyDataSize = Key.Length,
                ValueDataSize = ValueIsObject ? ObjectIdMap.ObjectIdSize : (Info.ValueIsOverflow ? SpanField.OverflowInlineSize : SpanField.GetTotalSizeOfInlineField(ValueAddress)),
                ValueIsObject = ValueIsObject,
                HasETag = Info.HasETag,
                HasExpiration = Info.HasExpiration
            };
        #endregion //ISourceLogRecord

        public readonly long SerializedRecordLength => GetSerializedLength(physicalAddress);

        readonly long KeyAddress => physicalAddress + RecordInfo.GetLength() + SerializedRecordLengthSize + ETagLen + ExpirationLen;

        internal readonly long ValueAddress => KeyAddress + SpanField.GetTotalSizeOfInlineField(KeyAddress);

        private readonly int InlineValueLength => ValueIsObject ? ObjectIdMap.ObjectIdSize : SpanField.GetTotalSizeOfInlineField(ValueAddress);
        public readonly int OptionalLength => (Info.HasETag ? LogRecord.ETagSize : 0) + (Info.HasExpiration ? LogRecord.ExpirationSize : 0);

        private readonly int ETagLen => Info.HasETag ? LogRecord.ETagSize : 0;
        private readonly int ExpirationLen => Info.HasExpiration ? LogRecord.ExpirationSize : 0;

        private readonly long GetETagAddress() => physicalAddress + RecordInfo.GetLength() + SerializedRecordLengthSize;
        private readonly long GetExpirationAddress() => GetETagAddress() + ETagLen;

        /// <summary>The initial size to IO from disk when reading a record; by default a single page. If we don't get the full record,
        /// at least we'll get the SerializedRecordLength and can read the full record using that.</summary>
        public static int InitialIOSize => 4 * 1024;

        internal static ReadOnlySpan<byte> GetContextRecordKey(ref AsyncIOContext ctx) => new DiskLogRecord((long)ctx.record.GetValidPointer()).Key;

        internal static ReadOnlySpan<byte> GetContextRecordValue(ref AsyncIOContext ctx) => new DiskLogRecord((long)ctx.record.GetValidPointer()).ValueSpan;

        #region Serialized Record Creation
        /// <summary>
        /// Serialize for RUMD operations, called by PendingContext; these also have TInput, TOutput, and TContext, which are handled by PendingContext.
        /// </summary>
        /// <param name="key">Record key</param>
        /// <param name="valueSpan">Record value as a Span, if Upsert</param>
        /// <param name="valueObject">Record value as an object, if Upsert</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Serialize(ReadOnlySpan<byte> key, ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, SectorAlignedBufferPool bufferPool)
            => Serialize(key, valueSpan, valueObject, bufferPool, ref allocatedBuffer);

        /// <summary>
        /// Serialize for RUMD operations, called by PendingContext; these also have TInput, TOutput, and TContext, which are handled by PendingContext.
        /// </summary>
        /// <remarks>This overload may be called either directly for a caller who owns the <paramref name="allocatedRecord"/>, or with this.allocatedRecord.</remarks>
        /// <param name="key">Record key</param>
        /// <param name="valueSpan">Record value as a Span, if Upsert</param>
        /// <param name="valueObject">Record value as an object, if Upsert</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Serialize(ReadOnlySpan<byte> key, ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord)
        {
            // Value length prefix on the disk is a long, as it may be an object.
            long valueSize = (valueObject is not null && valueSpan.Length > 0)
                ? valueSize = valueSpan.Length
                : 0;

            var recordSize = RecordInfo.GetLength()
                + SerializedRecordLengthSize                                            // Total record length on disk; used in IO
                + 0                                                                     // OptionalSize; ETag and Expiration are not supplied here (they are in the Input)
                + sizeof(int) + key.Length                                              // Key; length prefix is an int
                + valueSize;

            var ptr = SerializeCommonFields(key, recordSize, bufferPool, ref allocatedRecord);

            // For RUMD ops we never serialize the object, just carry it through the pending IO sequence.
            if (valueSize > 0)
                SerializeValue(ptr, valueSpan);
            else
                this.valueObject = valueObject;
        }

        /// <summary>
        /// Serialize for Compact, Pending Operations, etc. There is no associated TInput, TOutput, TContext for these.
        /// </summary>
        /// <param name="logRecord">The log record. This may be either in-memory or from disk IO</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        /// <param name="valueSerializer">Serializer for the value object; if null, do not serialize (carry the valueObject (if any) through from the logRecord instead)</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Serialize(ref readonly LogRecord logRecord, SectorAlignedBufferPool bufferPool, IObjectSerializer<IHeapObject> valueSerializer)
            => Serialize(in logRecord, bufferPool, valueSerializer, ref allocatedBuffer);

        /// <summary>
        /// Serialize for Compact, Pending Operations, etc. There is no associated TInput, TOutput, TContext for these.
        /// </summary>
        /// <param name="logRecord">The log record. This may be either in-memory or from disk IO</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        /// <param name="valueSerializer">Serializer for the value object; if null, do not serialize (carry the valueObject (if any) through from the logRecord instead)</param>
        /// <remarks>This overload may be called either directly for a caller who owns the <paramref name="allocatedRecord"/>, or with this.allocatedRecord.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Serialize(ref readonly LogRecord logRecord, SectorAlignedBufferPool bufferPool, IObjectSerializer<IHeapObject> valueSerializer, ref SectorAlignedMemory allocatedRecord)
        {
            // If we have an object and we don't serialize it, we don't need to allocate space for it.
            // Value length prefix on the disk is a long, as it may be an object.
            var valueSize = logRecord.ValueIsObject
                ? (valueSerializer is not null ? sizeof(long) + logRecord.ValueObject.Size : 0)
                : sizeof(long) + logRecord.ValueSpan.Length;

            var recordSize = RecordInfo.GetLength()
                + SerializedRecordLengthSize                                            // Total record length on disk; used in IO
                + logRecord.OptionalSize                                                // ETag and Expiration
                + sizeof(int) + logRecord.Key.Length                                    // Key; length prefix is an int
                + valueSize;

            var ptr = SerializeCommonFields(in logRecord, recordSize, bufferPool, ref allocatedRecord);

            if (!logRecord.ValueIsObject)
                SerializeValue(ptr, logRecord.ValueSpan);
            else if (valueSerializer is not null)
            {
                var stream = new UnmanagedMemoryStream(ptr, logRecord.ValueObject.Size);
                valueSerializer.BeginSerialize(stream);
                var valueObject = logRecord.ValueObject;
                valueSerializer.Serialize(valueObject);
                valueSerializer.EndSerialize();
            }
            else
                valueObject = logRecord.ValueObject;
        }

        /// <summary>
        /// Serialize for Compact, Pending Operations, etc.
        /// </summary>
        /// <param name="logRecord">The log record. This may be either in-memory or from disk IO</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        /// <param name="valueSerializer">Serializer for the value object; if null, do not serialize (carry the valueObject (if any) through from the logRecord instead)</param>
        /// <remarks>This overload converts from LogRecord to DiskLogRecord.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static DiskLogRecord CreateAndSerialize(ref readonly LogRecord logRecord, SectorAlignedBufferPool bufferPool, IObjectSerializer<IHeapObject> valueSerializer)
        {
            var diskLogRecord = new DiskLogRecord();
            diskLogRecord.Serialize(in logRecord, bufferPool, valueSerializer);
            return diskLogRecord;
        }

        /// <summary>
        /// Serialize for Compact, Pending Operations, etc.
        /// </summary>
        /// <param name="diskLogRecord">The log record. This may be either in-memory or from disk IO</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        /// <param name="valueSerializer">Serializer for the value object; if null, do not serialize (carry the valueObject (if any) through from the logRecord instead)</param>
        /// <remarks>This overload is a "shim" between the TSourceLogRecord generic type argument and DiskLogRecord.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static DiskLogRecord CreateAndSerialize(ref readonly DiskLogRecord diskLogRecord, SectorAlignedBufferPool bufferPool, IObjectSerializer<IHeapObject> valueSerializer)
            => diskLogRecord;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe byte* SerializeCommonFields(ReadOnlySpan<byte> key, long recordSize, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord)
        {
            bufferPool.EnsureSize(ref allocatedRecord, (int)recordSize);                // TODO: handle chunked operations on large objects
            physicalAddress = (long)allocatedRecord.GetValidPointer();
            var ptr = (byte*)physicalAddress;

            *(RecordInfo*)ptr = default;
            InfoRef.SetKeyIsInline();
            ptr += RecordInfo.GetLength();

            *(long*)ptr = recordSize;
            ptr += SerializedRecordLengthSize;

            return SerializeKey(ref ptr, key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe byte* SerializeCommonFields(ref readonly LogRecord logRecord, long recordSize, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord)
        {
            bufferPool.EnsureSize(ref allocatedRecord, (int)recordSize);                // TODO: handle chunked operations on large objects
            physicalAddress = (long)allocatedRecord.GetValidPointer();
            var ptr = (byte*)physicalAddress;

            *(RecordInfo*)ptr = logRecord.Info;
            InfoRef.SetKeyIsInline();
            ptr += RecordInfo.GetLength();

            *(long*)ptr = recordSize;
            ptr += SerializedRecordLengthSize;

            if (logRecord.Info.HasETag)
            {
                *(long*)ptr = logRecord.ETag;
                InfoRef.SetHasETag();
                ptr += LogRecord.ETagSize;
            }

            if (logRecord.Info.HasExpiration)
            {
                *(long*)ptr = logRecord.Expiration;
                InfoRef.SetHasExpiration();
                ptr += LogRecord.ExpirationSize;
            }

            return SerializeKey(ref ptr, logRecord.Key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte* SerializeKey(ref byte* ptr, ReadOnlySpan<byte> key)
        {
            *(int*)ptr = key.Length;
            ptr += SpanField.FieldLengthPrefixSize;
            key.CopyTo(new Span<byte>(ptr, key.Length));
            return ptr + key.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void SerializeValue(byte* ptr, ReadOnlySpan<byte> value)
        {
            if (value.Length > 0)
            {
                *(long*)ptr = value.Length;
                ptr += sizeof(long);
                value.CopyTo(new Span<byte>(ptr, value.Length));
            }
        }

        /// <summary>
        /// Deserialize the current value span to a <see cref="valueObject"/> valueObject.
        /// </summary>
        /// <param name="valueSerializer">Serializer for the value object; if null, do not serialize (carry the valueObject (if any) through from the logRecord instead)</param>
        /// <remarks>This overload converts from LogRecord to DiskLogRecord.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DeserializeValueObject(IObjectSerializer<IHeapObject> valueSerializer)
        {
            if (valueObject is not null)
                return;
            var valueAddress = ValueAddress;
            var stream = new UnmanagedMemoryStream((byte*)SpanField.GetInlineDataAddress(valueAddress), GetSerializedLength(valueAddress));
            valueSerializer.BeginDeserialize(stream);
            valueSerializer.Deserialize(out valueObject);
            valueSerializer.EndDeserialize();
        }

        /// <summary>
        /// Clone from a temporary <see cref="DiskLogRecord"/> (having no <see cref="SectorAlignedMemory"/>) to a longer-lasting one.
        /// </summary>
        /// <param name="inputDiskLogRecord"></param>
        /// <param name="bufferPool"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CloneFrom(ref DiskLogRecord inputDiskLogRecord, SectorAlignedBufferPool bufferPool)
        {
            Debug.Assert(!inputDiskLogRecord.IsSet, "inputDiskLogRecord is not set");

            // If the source has a Value object we don't need to allocate space for the serialized value.
            // Larger-than-int serialized values should always be value objects and thus we should have a value object.
            // This cloning thus lets us release the original diskLogRecord, keeping only the (hopefully much) smaller key (and optionals).
            var recordSize = inputDiskLogRecord.ValueIsObject
                ? inputDiskLogRecord.SerializedRecordLength
                : RecordInfo.GetLength()
                    + SerializedRecordLengthSize                                            // Total record length on disk; used in IO
                    + OptionalLength                                                        // OptionalSize
                    + sizeof(int) + Key.Length                                              // Key; length prefix is an int
                    + 0;                                                                    // No value length allocation here.

            Debug.Assert(recordSize < int.MaxValue, $"recordSize too large: {recordSize}");

            if (allocatedBuffer is not null)
                allocatedBuffer.pool.EnsureSize(ref allocatedBuffer, (int)recordSize);      // TODO: handle chunked operations on large objects
            else
                allocatedBuffer = bufferPool.Get((int)recordSize);

            Buffer.MemoryCopy((void*)inputDiskLogRecord.physicalAddress, (void*)physicalAddress, recordSize, recordSize);
            valueObject = inputDiskLogRecord.valueObject;
        }

        /// <summary>
        /// Transfer memory ownership from a temporary <see cref="DiskLogRecord"/> to a longer-lasting one.
        /// </summary>
        /// <remarks>This is separate from <see cref="CloneFrom"/> to ensure the caller is prepared to handle the implications of the transfer</remarks>
        /// <param name="diskLogRecord"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Transfer(ref DiskLogRecord diskLogRecord)
        {
            Debug.Assert(diskLogRecord.IsSet, "inputDiskLogRecord is not set");
            Debug.Assert(diskLogRecord.allocatedBuffer is not null, "inputDiskLogRecord does not own its memory");

            if (allocatedBuffer is not null)
                allocatedBuffer.Return();
            allocatedBuffer = diskLogRecord.allocatedBuffer;
            diskLogRecord.allocatedBuffer = null;   // Transfers ownership
            physicalAddress = (long)allocatedBuffer.GetValidPointer();
        }
        #endregion //Serialized Record Creation

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            var valueString = ValueIsObject ? ValueObject.ToString() : ValueSpan.ToString();

            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasETag {bstr(Info.HasETag)}:{ETag} | HasExpiration {bstr(Info.HasExpiration)}:{Expiration}";
        }
    }
}