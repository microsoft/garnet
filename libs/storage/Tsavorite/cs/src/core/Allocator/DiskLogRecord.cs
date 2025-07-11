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
    using static Utility;

    /// <summary>A record from the disk or network buffer. Because we read in pieces, we isolate the individual pieces so we don't rely on having the full record in memory.</summary>
    /// <remarks>This struct is used as a temporary holding space for pending operations (both holding onto keys on the query side, and the results on the completion side) and iteration.</remarks>
    public unsafe struct DiskLogRecord : ISourceLogRecord, IDisposable
    {
        /// <summary>The physicalAddress of the record data. May come from <see cref="recordBuffer"/> or from an iterator frame. In either case, it is the full record</summary>
        internal long physicalAddress;
        /// <summary>If this is non-null, it is the full record; it is the basis for physicalAddress and must be Return()ed on <see cref="Dispose()"/>.</summary>
        SectorAlignedMemory recordBuffer;

        /// <summary>The record header.</summary>
        RecordInfo recordInfo;

        /// <summary>If not empty, the key is an overflow key.</summary>
        readonly OverflowByteArray keyOverflow;
        /// <summary>If not empty, the key is part of either <see cref="keyBuffer"/> or <see cref="recordBuffer"/>.</summary>
        readonly PinnedSpanByte keySpan;
        /// <summary>We may have had an initial read of just the key, but not the full value. In that case we hung onto the key memory here (and it must be Return()ed)
        /// and may also have allocated <see cref="recordBuffer"/>.</summary>
        readonly SectorAlignedMemory keyBuffer;

        /// <summary>If not empty, the value is an overflow value.</summary>
        readonly OverflowByteArray valueOverflow;
        /// <summary>If not empty, the value is part of <see cref="recordBuffer"/>.</summary>
        readonly PinnedSpanByte valueSpan;
        /// <summary>If non-null, this is the deserialized ValueObject. Held directly; does not use <see cref="ObjectIdMap"/></summary>
        readonly IHeapObject valueObject;

        /// <summary>If Info.HasETag, this is the ETag.</summary>
        readonly long eTag;
        /// <summary>If Info.HasExpiration, this is the Expiration.</summary>
        readonly long expiration;

        /// <summary>Constructor that takes a physical address, which may come from a <see cref="SectorAlignedMemory"/> or some other allocation
        /// that will have at least the lifetime of this <see cref="DiskLogRecord"/>. This <see cref="DiskLogRecord"/> does not own the memory
        /// allocation so will not free it on <see cref="Dispose()"/>.</summary>
        /// <param name="physicalAddress">Physical address of the record; may have come from a <see cref="SectorAlignedMemory"/> buffer</param>
        /// <param name="offsetToKeyStart">Offset to start of key data</param>
        /// <param name="keyLength">Length of key data</param>
        /// <param name="valueLength">Length of value data. This is an int; a long length would mean <paramref name="valueObject"/> is not null</param>
        /// <param name="valueObject"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DiskLogRecord(long physicalAddress, int offsetToKeyStart, int keyLength, int valueLength, IHeapObject valueObject = null)
        {
            this.physicalAddress = physicalAddress;

            // We assume we have read this directly and not yet processed its components other than possibly deserialize the value object,
            // so do that now to avoid "if" checks later.
            var ptr = (byte*)physicalAddress;
            recordInfo = *(RecordInfo*)ptr;
            recordInfo.ClearBitsForDiskImages();

            // If there is a value object it should already have been deserialized
            Debug.Assert(recordInfo.ValueIsObject == valueObject is not null, $"recordInfo.ValueIsObject ({recordInfo.ValueIsObject} does not match (valueObject is not null) {valueObject is not null}");

            ptr += offsetToKeyStart;
            keySpan = PinnedSpanByte.FromPinnedPointer(ptr, keyLength);
            ptr += keyLength;

            if (valueObject is not null)
                this.valueObject = valueObject;
            else
                valueSpan = PinnedSpanByte.FromPinnedPointer(ptr, valueLength);
            ptr += valueLength;

            if (recordInfo.HasETag)
            {
                eTag = *(long*)ptr;
                ptr += LogRecord.ETagSize;
            }
            if (recordInfo.HasExpiration)
                expiration = *(long*)ptr;
        }

        /// <summary>Static factory that takes a <see cref="SectorAlignedMemory"/> from which it obtains the physical address.
        /// This <see cref="DiskLogRecord"/> owns the memory allocation and must free it on <see cref="Dispose()"/>.</summary>
        internal static DiskLogRecord Transfer(ref SectorAlignedMemory allocatedRecord, int offsetToKeyStart, int keyLength, int valueLength, IHeapObject valueObject = null)
        {
            if (allocatedRecord is null)
                throw new ArgumentNullException(nameof(allocatedRecord), "Cannot transfer a null SectorAlignedMemory to DiskLogRecord");
            var diskLogRecord = new DiskLogRecord((long)allocatedRecord.GetValidPointer(), offsetToKeyStart, keyLength, valueLength, valueObject)
                { recordBuffer = allocatedRecord };
            allocatedRecord = default;
            return diskLogRecord;
        }

        internal DiskLogRecord(RecordInfo recordInfo, ref SectorAlignedMemory keyBuffer, OverflowByteArray keyOverflow, ref SectorAlignedMemory valueBuffer,
            OverflowByteArray valueOverflow, long eTag, long expiration, IHeapObject valueObject) : this()
        {
            this.recordInfo = recordInfo;
            
            if (keyBuffer is not null)
            {
                Debug.Assert(keyOverflow.IsEmpty, "Must have only one of keyBuffer or keyOverflow");
                this.keyBuffer = keyBuffer;     // Transfer ownership to us
                keyBuffer = default;
                keySpan = PinnedSpanByte.FromPinnedPointer(keyBuffer.GetValidPointer(), keyBuffer.required_bytes);
            }
            else
            {
                Debug.Assert(!keyOverflow.IsEmpty, "Must have either keyBuffer or keyOverflow");
                Debug.Assert(keyBuffer is null, "Must have only one of keyBuffer or keyOverflow");
                this.keyOverflow = keyOverflow;
            }

            if (valueObject is not null)
            {
                Debug.Assert(valueBuffer is null && valueOverflow.IsEmpty, "Must have only one of valueBuffer, valueOverflow, or valueObject");
                this.valueObject = valueObject;
            }
            if (valueBuffer is not null)
            {
                Debug.Assert(valueObject is null && valueOverflow.IsEmpty, "Must have only one of valueBuffer, valueOverflow, or valueObject");
                recordBuffer = valueBuffer;     // Transfer ownership to us
                valueBuffer = default;
                valueSpan = PinnedSpanByte.FromPinnedPointer(valueBuffer.GetValidPointer(), valueBuffer.required_bytes);
            }
            else
            {
                Debug.Assert(!valueOverflow.IsEmpty, "Must have valueBuffer, valueObject, or valueOverflow");
                Debug.Assert(valueObject is null && valueBuffer is null, "Must have only one of valueBuffer, valueOverflow, or valueObject");
                this.valueOverflow = valueOverflow;
            }
            this.eTag = eTag;
            this.expiration = expiration;
        }

        /// <summary>A ref to the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref RecordInfo GetInfoRef(long physicalAddress) => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);

        /// <summary>Fast access returning a copy of the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RecordInfo GetInfo(long physicalAddress) => *(RecordInfo*)physicalAddress;

        /// <summary>Serialized length of the record</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetSerializedLength()
            => Info.IsNull ? RecordInfo.GetLength() : RoundUp(GetOptionalStartAddress() + OptionalLength - physicalAddress, Constants.kRecordAlignment);

        /// <summary>If true, this DiskLogRecord owns the buffer and must free it on <see cref="Dispose"/></summary>
        public readonly bool OwnsRecordBuffer => recordBuffer is not null;

        /// <summary>This contains the leading byte which are the indicators, plus the up-to-int length for the key, and then some or all of the length for the value.</summary>
        internal readonly long IndicatorAddress => physicalAddress + RecordInfo.GetLength();

        internal readonly (int length, long dataAddress) KeyInfo
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var address = IndicatorAddress;
                if (Info.RecordIsInline)    // For inline, the key length int starts at the same offset as IndicatorAddress
                    return (*(int*)address, address + LogField.InlineLengthPrefixSize);

                var (keyLengthBytes, valueLengthBytes) = DeconstructIndicatorByte(*(byte*)address);

                var ptr = (byte*)++address;  // Move past the indicator byte; the next bytes are key length
                var keyLength = ReadVarBytes(keyLengthBytes, ref ptr);

                // Move past the key and value length bytes to the start of the key
                return ((int)keyLength, address + keyLengthBytes + valueLengthBytes);
            }
        }

        internal readonly (long length, long dataAddress) ValueInfo
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var keyInfo = KeyInfo;
                if (Info.RecordIsInline)    // For inline records the value length is an int, stored immediately after key data
                {
                    var valueLengthAddress = keyInfo.dataAddress + keyInfo.length;
                    return (*(int*)valueLengthAddress, valueLengthAddress + sizeof(int));
                }

                var (keyLengthBytes, valueLengthBytes) = DeconstructIndicatorByte(*(byte*)IndicatorAddress);

                var ptr = (byte*)IndicatorAddress + 1 + keyLengthBytes;   // Skip over the key length bytes; the value length bytes are immediately after (before the key data)
                var valueLength = ReadVarBytes(valueLengthBytes, ref ptr);
                return (valueLength, keyInfo.dataAddress + keyInfo.length); // Value data (without length prefix) starts immediately after key data
            }
        }

        public void Dispose()
        {
            recordBuffer?.Return();
            recordBuffer = null;
            keyBuffer?.Return();
            keyBuffer = null;
        }

        #region ISourceLogRecord
        /// <inheritdoc/>
        public readonly bool IsPinnedKey => true;

        /// <inheritdoc/>
        public readonly byte* PinnedKeyPointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return IsPinnedKey ? (byte*)KeyInfo.dataAddress : null; }
        }

        /// <inheritdoc/>
        public readonly bool IsPinnedValue => !Info.ValueIsObject;  // We store all bytes inline, but we don't set ValueIsInline, per discussion in SerializeCommonVarByteFields.

        /// <inheritdoc/>
        public readonly byte* PinnedValuePointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return IsPinnedValue ? (byte*)ValueInfo.dataAddress : null; }
        }

        /// <inheritdoc/>
        public readonly bool IsSet => physicalAddress != 0;
        /// <inheritdoc/>
        public readonly ref RecordInfo InfoRef => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);
        /// <inheritdoc/>
        public readonly RecordInfo Info => *(RecordInfo*)physicalAddress;
        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> Key
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var (length, dataAddress) = KeyInfo;
                return new((byte*)dataAddress, length);
            }
        }

        /// <inheritdoc/>
        public readonly Span<byte> ValueSpan
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (Info.ValueIsObject)
                    throw new TsavoriteException("DiskLogRecord with Info.ValueIsObject does not support Span<byte> values");
                var (length, dataAddress) = ValueInfo;
                return new((byte*)dataAddress, length);    // TODO: handle long value length
            }
        }

        /// <inheritdoc/>
        public readonly IHeapObject ValueObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (!Info.ValueIsObject)
                    throw new TsavoriteException("DiskLogRecord without Info.ValueIsObject does not allow ValueObject");
                Debug.Assert(valueObject is not null, "Should have deserialized valueObject by this point, or received it directly from LogRecord or PendingContext");
                return valueObject;
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly ReadOnlySpan<byte> AsReadOnlySpan()
        {
            var valueInfo = ValueInfo;
            if (valueInfo.length == 0)
                throw new TsavoriteException("RecordSpan is not valid for records with unserialized ValueObjects");
            return new((byte*)physicalAddress, (int)GetSerializedLength()); // TODO: Handle long object sizes
        }

        /// <inheritdoc/>
        public readonly long ETag => Info.HasETag ? *(long*)GetETagAddress() : LogRecord.NoETag;

        /// <inheritdoc/>
        public readonly long Expiration => Info.HasExpiration ? *(long*)GetExpirationAddress() : 0;

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
        public readonly RecordFieldInfo GetRecordFieldInfo() => new()
        {
            KeyDataSize = Key.Length,
            ValueDataSize = Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : ValueInfo.length, // TODO: Handle long object sizes
            ValueIsObject = Info.ValueIsObject,
            HasETag = Info.HasETag,
            HasExpiration = Info.HasExpiration
        };
        #endregion //ISourceLogRecord

        public readonly int OptionalLength => ETagLen + ExpirationLen;

        private readonly int ETagLen => Info.HasETag ? LogRecord.ETagSize : 0;
        private readonly int ExpirationLen => Info.HasExpiration ? LogRecord.ExpirationSize : 0;

        public static int GetOptionalLength(RecordInfo info) => (info.HasETag ? LogRecord.ETagSize : 0) + (info.HasExpiration ? LogRecord.ExpirationSize : 0);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetOptionalStartAddress()
        {
            var (length, dataAddress) = ValueInfo;
            return dataAddress + length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetETagAddress() => GetOptionalStartAddress();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetExpirationAddress() => GetETagAddress() + ETagLen;

        #region Serialized Record Creation
        /// <summary>
        /// Serialize for Read or RMW operations, called by PendingContext; these have no Value but have TInput, TOutput, and TContext, which are handled by PendingContext.
        /// </summary>
        /// <param name="key">Record key</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SerializeForPendingReadOrRMW(ReadOnlySpan<byte> key, SectorAlignedBufferPool bufferPool)
            => SerializeForPendingReadOrRMW(key, bufferPool, ref recordBuffer);

        /// <summary>
        /// Serialize for Read or RMW operations, called by PendingContext; these have no Value but have TInput, TOutput, and TContext, which are handled by PendingContext.
        /// </summary>
        /// <remarks>This overload may be called either directly for a caller who owns the <paramref name="allocatedRecord"/>, or with this.allocatedRecord.</remarks>
        /// <param name="key">Record key</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        /// <param name="allocatedRecord">The allocated record; may be owned by this instance, or owned by the caller for reuse</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SerializeForPendingReadOrRMW(ReadOnlySpan<byte> key, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord)
        {
            // OptionalSize (ETag and Expiration) is not considered here; those are specified in the Input, which is serialized separately by PendingContext.

            long recordSize;
            byte* ptr;

            // Value is a span so we can use RecordIsInline format, so both key and value are int length.
            recordSize = RecordInfo.GetLength() + key.TotalSize() + LogField.InlineLengthPrefixSize;

            if (allocatedRecord is not null)
                allocatedRecord.pool.EnsureSize(ref allocatedRecord, (int)recordSize);
            else
                allocatedRecord = bufferPool.Get((int)recordSize);

            physicalAddress = (long)allocatedRecord.GetValidPointer();
            ptr = (byte*)physicalAddress;

            *(RecordInfo*)ptr = default;
            ptr += RecordInfo.GetLength();

            InfoRef.SetKeyIsInline();
            *(int*)ptr = key.Length;
            ptr += LogField.InlineLengthPrefixSize;
            key.CopyTo(new Span<byte>(ptr, key.Length));
            ptr += key.Length;

            InfoRef.SetValueIsInline();
            *(int*)ptr = 0;

            allocatedRecord.available_bytes = (int)recordSize;
            return;
        }

        /// <summary>
        /// Serialize for Compact, Scan, Conditional Pending Operations, Migration, Replication, etc. The logRecord comes from the in-memory log;
        /// there is no associated TInput, TOutput, TContext, and any object is just reference-copied to the destination.
        /// </summary>
        /// <param name="logRecord">The log record. This may be either in-memory or from disk IO</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Serialize(in LogRecord logRecord, SectorAlignedBufferPool bufferPool)
            => Serialize(in logRecord, bufferPool, ref recordBuffer);

        /// <summary>
        /// Serialize for Compact, Scan, Conditional Pending Operations, Migration, Replication, etc. The logRecord comes from the in-memory log;
        /// there is no associated TInput, TOutput, TContext, and any object is just reference-copied to the destination.
        /// </summary>
        /// <param name="logRecord">The log record. This may be either in-memory or from disk IO</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        /// <param name="allocatedRecord">The allocated record; may be owned by this instance, or owned by the caller for reuse</param>
        /// <remarks>This overload may be called either directly for a caller who owns the <paramref name="allocatedRecord"/>, or with this.allocatedRecord.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Serialize(in LogRecord logRecord, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord)
        {
            // TODO: Value Overflow allocations should be carried over as objects as well; see also AsyncGetFromDiskCallback.
            // We won't optimize for huge keys.
            // TODO the tests that used to pass valueObjectSerializer should use normal MemoryStream-based serialization
            // and then compare the lengths.
            // TODO optimize layout for LogRecord, so we have the same layout.

            if (logRecord.Info.RecordIsInline)
            {
                DirectCopyRecord(logRecord.ActualRecordSize, logRecord.physicalAddress, bufferPool, ref allocatedRecord);
                return;
            }

            // Record is not inline so we must use the varbyte format: create the indicator byte and space-optimized length representation.

            // If we have an object we don't serialize it so we don't need to allocate space for it.
            // Value length prefix on the disk is up to a long, as it may be an object.
            var valueLength = !logRecord.Info.ValueIsObject
                ? logRecord.ValueSpan.Length
                : 0;

            var indicatorByte = CreateIndicatorByte(logRecord.Key.Length, valueLength, out var keyLengthByteCount, out var valueLengthByteCount);

            var recordSize = RecordInfo.GetLength()
                + 1 // indicator byte
                + keyLengthByteCount + logRecord.Key.Length
                + valueLengthByteCount + valueLength
                + logRecord.OptionalLength;

            // This writes the value length, but not value data
            EnsureAllocation(recordSize, bufferPool, ref allocatedRecord);
            var ptr = SerializeCommonVarByteFields(logRecord.Info, indicatorByte, logRecord.Key, keyLengthByteCount, valueLength, valueLengthByteCount, allocatedRecord.GetValidPointer());

            // Set the value
            if (!logRecord.Info.ValueIsObject)
                logRecord.ValueSpan.CopyTo(new Span<byte>(ptr, valueLength));
            else
            {
                valueObject = logRecord.ValueObject;
                InfoRef.SetValueIsObject();
            }
            ptr += valueLength;
            CopyOptionals(in logRecord, ref ptr);

            allocatedRecord.available_bytes = recordSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe byte* SerializeCommonVarByteFields(RecordInfo recordInfo, byte indicatorByte, ReadOnlySpan<byte> key, int keyLengthByteCount,
                long valueLength, int valueLengthByteCount, byte* ptr)
        {
            // RecordInfo is a stack copy so we can modify it here. Write the key "inline" status; keys are always inline in DiskLogRecord, as they cannot
            // be object. Value, however, does not set ValueIsInline for varbyte records, because then Info.RecordIsInline would be true. Instead we clear
            // ValueIsInline and use ValueIsObject to determine whether the serialized bytes are string or object, and whether DeserializeValueObject can
            // be used. WARNING: ToString() may AV when stepping through here in the debugger, until we have the lengths correctly set.
            recordInfo.SetKeyIsInline();
            recordInfo.ClearValueIsInline();
            *(RecordInfo*)ptr = recordInfo;
            ptr += RecordInfo.GetLength();

            // Set the indicator and lengths
            *ptr++ = indicatorByte;
            WriteVarBytes(key.Length, keyLengthByteCount, ref ptr);
            WriteVarBytes(valueLength, valueLengthByteCount, ref ptr);

            // Copy the key but not the value; the caller does that.
            key.CopyTo(new Span<byte>(ptr, key.Length));

            // Return the pointer to the value data space (immediately following the key data space; the value length was already written above).
            return ptr + key.Length;
        }

        /// <summary>
        /// Deserialize the current value span to a <see cref="valueObject"/> valueObject.
        /// </summary>
        /// <param name="valueSerializer">Serializer for the value object; if null, do not serialize (carry the valueObject (if any) through from the logRecord instead)</param>
        /// <remarks>This overload converts from LogRecord to DiskLogRecord.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IHeapObject DeserializeValueObject(IObjectSerializer<IHeapObject> valueSerializer)
        {
            if (valueObject is not null)
                return valueObject;
            if (!Info.ValueIsObject)
                return valueObject = default;

            var (length, dataAddress) = ValueInfo;
            var stream = new UnmanagedMemoryStream((byte*)dataAddress, length);
            valueSerializer.BeginDeserialize(stream);
            valueSerializer.Deserialize(out valueObject);
            valueSerializer.EndDeserialize();
            return valueObject;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DirectCopyRecord(long recordSize, long srcPhysicalAddress, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord)
        {
            EnsureAllocation(recordSize, bufferPool, ref allocatedRecord);
            Buffer.MemoryCopy((byte*)srcPhysicalAddress, (byte*)physicalAddress, recordSize, recordSize);
            allocatedRecord.available_bytes = (int)recordSize;
        }

        /// <summary>
        /// Directly copies a record in varbyte format to the SpanByteAndMemory. Allocates <see cref="SpanByteAndMemory.Memory"/> if needed.
        /// </summary>
        /// <remarks>If <paramref name="output"/>.<see cref="SpanByteAndMemory.IsSpanByte"/>, it points directly to the network buffer so we include the length prefix in the output.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void DirectCopyRecord(long srcPhysicalAddress, long srcRecordSize, ref SpanByteAndMemory output, MemoryPool<byte> memoryPool)
        {
            // TotalSize includes the length prefix, which is included in the output stream if we can write to the SpanByte.
            if (output.IsSpanByte && output.SpanByte.TotalSize >= (int)srcRecordSize)     // TODO: long value sizes
            {
                var outPtr = output.SpanByte.ToPointer();
                *(int*)outPtr = (int)srcRecordSize;
                Buffer.MemoryCopy((byte*)srcPhysicalAddress, outPtr + sizeof(int), output.SpanByte.Length, srcRecordSize);
                output.SpanByte.Length = (int)srcRecordSize;
                return;
            }

            // Do not include the length prefix in the output stream; this is done by the caller before writing the stream, from the SpanByte.Length we set here.
            output.EnsureHeapMemorySize((int)srcRecordSize + sizeof(int), memoryPool);
            fixed (byte* outPtr = output.MemorySpan)
            {
                Buffer.MemoryCopy((byte*)srcPhysicalAddress, outPtr, srcRecordSize, srcRecordSize);
                output.Length = (int)srcRecordSize;
            }
        }

        /// <summary>
        /// Serializes a record in varbyte format to the SpanByteAndMemory. Allocates <see cref="SpanByteAndMemory.Memory"/> if needed.
        /// </summary>
        /// <remarks>If <paramref name="output"/>.<see cref="SpanByteAndMemory.IsSpanByte"/>, it points directly to the network buffer so we include the length prefix in the output.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long SerializeVarbyteRecord<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, long srcPhysicalAddress, IObjectSerializer<IHeapObject> valueSerializer,
                ref SpanByteAndMemory output, MemoryPool<byte> memoryPool)
            where TSourceLogRecord : ISourceLogRecord
        {
            // If we have an object and we don't serialize it, we don't need to allocate space for it.
            // Value length prefix on the disk is a long, as it may be an object.
            var valueLength = !srcLogRecord.Info.ValueIsObject
                ? srcLogRecord.ValueSpan.Length
                : srcLogRecord.ValueObject.SerializedSize;

            var indicatorByte = CreateIndicatorByte(srcLogRecord.Key.Length, valueLength, out var keyLengthByteCount, out var valueLengthByteCount);

            var recordSize = RecordInfo.GetLength()
                + 1 // indicator byte
                + keyLengthByteCount + srcLogRecord.Key.Length
                + valueLengthByteCount + valueLength
                + (srcLogRecord.Info.HasETag ? LogRecord.ETagSize : 0)
                + (srcLogRecord.Info.HasExpiration ? LogRecord.ExpirationSize : 0);

            // Copy in varbyte format. If the object was not serialized in srcLogRecord we need to revise the length which is varbyte, so we can't just Buffer copy the whole record.
            var serializedSize = 0L;

            // TotalSize includes the length prefix, which is included in the output stream if we can write to the SpanByte.
            if (output.IsSpanByte && output.SpanByte.TotalSize >= (int)recordSize)     // TODO: long value sizes
            {
                var outPtr = output.SpanByte.ToPointer();
                *(int*)outPtr = (int)recordSize;
                serializedSize = SerializeVarbyteRecordToPinnedPointer(in srcLogRecord, outPtr + sizeof(int), indicatorByte, keyLengthByteCount, valueLength, valueLengthByteCount, valueSerializer);
                output.SpanByte.Length = (int)serializedSize;
            }
            else
            {
                // Do not include the length prefix in the output stream; this is done by the caller before writing the stream, from the SpanByte.Length we set here.
                output.EnsureHeapMemorySize((int)recordSize + sizeof(int), memoryPool);
                fixed (byte* outPtr = output.MemorySpan)
                {
                    serializedSize = SerializeVarbyteRecordToPinnedPointer(in srcLogRecord, outPtr, indicatorByte, keyLengthByteCount, valueLength, valueLengthByteCount, valueSerializer);
                    output.Length = (int)recordSize;
                }
            }
            Debug.Assert(serializedSize == recordSize, $"Serialized size {serializedSize} does not match expected size {recordSize}");
            return serializedSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long SerializeVarbyteRecordToPinnedPointer<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, byte* ptr, byte indicatorByte,
                int keyLengthByteCount, long valueLength, int valueLengthByteCount, IObjectSerializer<IHeapObject> valueSerializer)
            where TSourceLogRecord : ISourceLogRecord
        {
            var dstRecordInfoPtr = (RecordInfo*)ptr;
            ptr = SerializeCommonVarByteFields(srcLogRecord.Info, indicatorByte, srcLogRecord.Key, keyLengthByteCount, valueLength, valueLengthByteCount, ptr);

            // If the srcLogRecord has an object already serialized to its ValueSpan, then we will not be here; LogRecord does not serialize values to the ValueSpan,
            // and the DiskLogRecord case where valueInfo.length > 0 has already been tested for in the caller.
            if (!srcLogRecord.Info.ValueIsObject)
                srcLogRecord.ValueSpan.CopyTo(new Span<byte>(ptr, (int)valueLength));
            else
            {
                var stream = new UnmanagedMemoryStream(ptr, srcLogRecord.ValueObject.SerializedSize, srcLogRecord.ValueObject.SerializedSize, FileAccess.ReadWrite);
                valueSerializer.BeginSerialize(stream);
                valueSerializer.Serialize(srcLogRecord.ValueObject);
                valueSerializer.EndSerialize();
            }
            ptr += valueLength;

            if (srcLogRecord.Info.HasETag)
            {
                dstRecordInfoPtr->SetHasETag();
                *(long*)ptr = srcLogRecord.ETag;
                ptr += LogRecord.ETagSize;
            }

            if (srcLogRecord.Info.HasExpiration)
            {
                dstRecordInfoPtr->SetHasExpiration();
                *(long*)ptr = srcLogRecord.Expiration;
                ptr += LogRecord.ExpirationSize;
            }

            return ptr - (byte*)dstRecordInfoPtr;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureAllocation(long recordSize, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord)
        {
            var allocatedSize = RoundUp((int)recordSize, Constants.kRecordAlignment);
            if (allocatedRecord is not null)
                allocatedRecord.pool.EnsureSize(ref allocatedRecord, allocatedSize);
            else
                allocatedRecord = bufferPool.Get(allocatedSize);
            physicalAddress = (long)allocatedRecord.GetValidPointer();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly void CopyOptionals<TSourceLogRecord>(ref readonly TSourceLogRecord logRecord, ref byte* ptr)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (logRecord.Info.HasETag)
            {
                InfoRef.SetHasETag();
                *(long*)ptr = logRecord.ETag;
                ptr += LogRecord.ETagSize;
            }

            if (logRecord.Info.HasExpiration)
            {
                InfoRef.SetHasExpiration();
                *(long*)ptr = logRecord.Expiration;
                ptr += LogRecord.ExpirationSize;
            }
        }

        /// <summary>
        /// The record is directly copyable if it has a serialized value; in that case it is in linear format and any deserialized object can be ignored.
        /// </summary>
        public readonly bool IsDirectlyCopyable => ValueInfo.length > 0;

        /// <summary>
        /// Clone from a temporary <see cref="DiskLogRecord"/> (having no overflow <see cref="SectorAlignedMemory"/>) to this instance's <see cref="SectorAlignedMemory"/>.
        /// </summary>
        /// <param name="inputDiskLogRecord"></param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        /// <param name="preferDeserializedObject">If true, prefer the deserialized object over copying the serialized value; this saves space for pending operations</param>
        public void CloneFrom(ref DiskLogRecord inputDiskLogRecord, SectorAlignedBufferPool bufferPool, bool preferDeserializedObject)
        {
            Debug.Assert(inputDiskLogRecord.IsSet, "inputDiskLogRecord is not set");

            if (!inputDiskLogRecord.Info.ValueIsObject || !preferDeserializedObject)
            {
                Debug.Assert(inputDiskLogRecord.ValueInfo.length > 0, "inputDiskLogRecord value length should be > 0");
                DirectCopyRecord(inputDiskLogRecord.GetSerializedLength(), inputDiskLogRecord.physicalAddress, bufferPool, ref recordBuffer);
                return;
            }

            // The source DiskLogRecord has a Value object rather than serialized bytes so it is in varbyte format. Copy everything up to the end of the key; ignore
            // the serialized value data even if the source source still has it. inputDiskLogRecord can Return() its recordBuffer, releasing any serialized value data.
            var (length, dataAddress) = inputDiskLogRecord.KeyInfo;
            var partialRecordSize = dataAddress + length - inputDiskLogRecord.physicalAddress;
            var allocatedRecordSize = partialRecordSize + inputDiskLogRecord.OptionalLength;

            if (recordBuffer is not null)
                recordBuffer.pool.EnsureSize(ref recordBuffer, (int)allocatedRecordSize);       // TODO handle 'long' valuelength
            else
                recordBuffer = bufferPool.Get((int)allocatedRecordSize);                        // TODO handle 'long' valuelength
            physicalAddress = (long)recordBuffer.GetValidPointer();

            Buffer.MemoryCopy((void*)inputDiskLogRecord.physicalAddress, (void*)physicalAddress, partialRecordSize, partialRecordSize);

            // Clear the value length in the indicator byte, as we did not copy any serialized data.
            var ptr = (byte*)physicalAddress + RecordInfo.GetLength();
            *ptr = (byte)(*ptr & ~kValueLengthBitMask);

            // Set the Value
            InfoRef.SetValueIsObject();
            valueObject = inputDiskLogRecord.valueObject;

            // Set the Optionals
            ptr = (byte*)physicalAddress + partialRecordSize;
            CopyOptionals(ref inputDiskLogRecord, ref ptr);
            recordBuffer.available_bytes = (int)allocatedRecordSize;
        }

        /// <summary>
        /// Transfer memory ownership from a temporary <see cref="DiskLogRecord"/> to a longer-lasting one.
        /// </summary>
        /// <remarks>This is separate from <see cref="CloneFrom(ref DiskLogRecord, SectorAlignedBufferPool, bool)"/> to ensure the caller
        /// is prepared to handle the implications of the transfer</remarks>
        /// <param name="inputDiskLogRecord"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void TransferFrom(ref DiskLogRecord inputDiskLogRecord)
        {
            Debug.Assert(inputDiskLogRecord.IsSet, "inputDiskLogRecord is not set");
            Debug.Assert(inputDiskLogRecord.recordBuffer is not null, "inputDiskLogRecord does not own its memory");

            recordBuffer?.Return();
            recordBuffer = inputDiskLogRecord.recordBuffer;
            inputDiskLogRecord.recordBuffer = null;   // Transfers ownership
            physicalAddress = (long)recordBuffer.GetValidPointer();
        }

        /// <summary>
        /// Serialize a log record to the <see cref="SpanByteAndMemory"/> <paramref name="output"/> in DiskLogRecord format, with the objectValue
        /// serialized to the Value span if it is not already there in the source (there will be no ValueObject in the result).
        /// Allocates <see cref="SpanByteAndMemory.Memory"/> if needed. This is used for migration.
        /// </summary>
        /// <remarks>If <paramref name="output"/>.<see cref="SpanByteAndMemory.IsSpanByte"/>, it points directly to the network buffer so we include the length prefix in the output.</remarks>
        public static void Serialize<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, IObjectSerializer<IHeapObject> valueSerializer, ref SpanByteAndMemory output, MemoryPool<byte> memoryPool)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.IsMemoryLogRecord)
            {
                ref var inMemoryLogRecord = ref srcLogRecord.AsMemoryLogRecordRef();
                if (inMemoryLogRecord.Info.RecordIsInline)
                    DirectCopyRecord(inMemoryLogRecord.physicalAddress, inMemoryLogRecord.ActualRecordSize, ref output, memoryPool);
                else
                    _ = SerializeVarbyteRecord(in inMemoryLogRecord, inMemoryLogRecord.physicalAddress, valueSerializer, ref output, memoryPool);
                return;
            }

            if (!srcLogRecord.IsDiskLogRecord)
                throw new TsavoriteException("Unknown TSourceLogRecord type");
            ref var diskLogRecord = ref srcLogRecord.AsDiskLogRecordRef();
            Debug.Assert(diskLogRecord.Info.KeyIsInline, "DiskLogRecord key should always be inline");

            var valueInfo = diskLogRecord.ValueInfo;

            // Either the value is present in serialized byte form or there should be an object. If there is no object we have nothing to serialize
            if (diskLogRecord.Info.RecordIsInline
                || (diskLogRecord.Info.KeyIsInline && (valueInfo.length > 0 || diskLogRecord.valueObject is null)))
            {
                DirectCopyRecord(diskLogRecord.physicalAddress, diskLogRecord.GetSerializedLength(), ref output, memoryPool);
                return;
            }
            _ = SerializeVarbyteRecord(in diskLogRecord, diskLogRecord.physicalAddress, valueSerializer, ref output, memoryPool);
        }
        #endregion //Serialized Record Creation

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            var valueString = Info.ValueIsObject ? $"obj:{ValueObject}" : ValueSpan.ToString();
            var eTag = Info.HasETag ? ETag.ToString() : "-";
            var expiration = Info.HasExpiration ? Expiration.ToString() : "-";

            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasETag {bstr(Info.HasETag)}:{eTag} | HasExpiration {bstr(Info.HasExpiration)}:{expiration}";
        }
    }
}