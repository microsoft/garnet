// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>The record on the disk: header, optional fields, key, value</summary>
    /// <remarks>The space is laid out as one of:
    ///     <list>
    ///     <item>Identical to <see cref="LogRecord"/></item>
    ///     <item>As a "varbyte" encoding:
    ///         <list>
    ///             <item>[RecordInfo][Indicator byte][KeyLength varbytes][ValueLength varbytes][key Span][value Span][ETag?][Expiration?][filler bytes--rounded up to 8 byte boundary]</item>
    ///             <item>Indicator byte: 3 bits for version, 2 bits for key length, 3 bits for value length</item>
    ///             <item>Key and Value Length varbytes: from 1-4 bytes for Key and 1-8 bytes for Value</item>
    ///         </list>
    ///     </item>
    ///     </list>
    /// This lets us get to the optional fields for comparisons without loading the full record (GetIOSize should cover the space for optionals).
    /// </remarks>
    public unsafe struct DiskLogRecord : ISourceLogRecord, IDisposable
    {
        /// <summary>The initial size to IO from disk when reading a record; by default a single page. If we don't get the full record,
        /// at least we'll likely get the full Key and value length, and can read the full record using that.</summary>
        /// <remarks>Must be a power of 2</remarks>
        public static int InitialIOSize => 4 * 1024;

        /// <summary>The physicalAddress in the log.</summary>
        internal long physicalAddress;

        /// <summary>The deserialized ValueObject if this is a disk record for the Object Store. Held directly; does not use <see cref="ObjectIdMap"/>.</summary>
        internal IHeapObject valueObject;

        /// <summary>If this is non-null, it must be freed on <see cref="Dispose()"/>.</summary>
        internal SectorAlignedMemory recordBuffer;

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
        internal DiskLogRecord(SectorAlignedMemory allocatedRecord)
            : this((long)allocatedRecord.GetValidPointer())
        {
            recordBuffer = allocatedRecord;
        }

        /// <summary>Constructor that takes an <see cref="AsyncIOContext"/> from which it obtains the physical address and ValueObject.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DiskLogRecord(ref AsyncIOContext ctx)
            : this((long)ctx.record.GetValidPointer())
        {
            valueObject = ctx.ValueObject;
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
            => RoundUp(GetOptionalStartAddress() + OptionalLength - physicalAddress, Constants.kRecordAlignment);

        /// <summary>Whether the record is complete (full serialized length has been read)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool IsComplete(SectorAlignedMemory recordBuffer, out bool hasFullKey, out int requiredBytes)
        {
            hasFullKey = false;

            // Check for RecordInfo and either indicator byte or key length; FieldLengthPrefixSize is the larger.
            if (recordBuffer.available_bytes < RecordInfo.GetLength() + SpanField.FieldLengthPrefixSize)
            {
                requiredBytes = InitialIOSize;
                return false;
            }

            // Now we can tell if this is a fully inline vs. varbyte record.
            long physicalAddress = (long)recordBuffer.GetValidPointer();
            var address = physicalAddress;
            if ((*(RecordInfo*)address).RecordIsInline)
            {
                address += RecordInfo.GetLength();
                address += sizeof(int) + *(int*)address;                            // Inline key length is after RecordInfo; position address after the Key (but don't dereference it yet!)
                requiredBytes = (int)(address + sizeof(int) - physicalAddress);     // Include value length int in the calculation
                if (recordBuffer.available_bytes < requiredBytes)
                {
                    // We have the RecordInfo and key length, but not the full key data. Get another page.
                    requiredBytes = RoundUp(requiredBytes, InitialIOSize);
                    return false;
                }

                // We have the full Key and the value length available.
                hasFullKey = true;
                requiredBytes += *(int*)address;
            }
            else
            {
                // We are in varbyte format. We need to check the indicator byte for the key and value length.
                address += RecordInfo.GetLength();      // Point to indicator byte
                var keyLengthBytes = (int)((*(long*)address & kKeyLengthBitMask) >> 3);
                var valueLengthBytes = (int)(*(long*)address & kValueLengthBitMask);

                requiredBytes = RecordInfo.GetLength() + 1 + keyLengthBytes + valueLengthBytes; // Include the indicator byte in the calculation
                if (recordBuffer.available_bytes < requiredBytes)
                    return false;

                var ptr = (byte*)++address;  // Move past the indicator byte; the next bytes are key length, then value length
                var keyLength = ReadVarBytes(keyLengthBytes, ref ptr);
                var valueLength = ReadVarBytes(valueLengthBytes, ref ptr);

                requiredBytes += (int)keyLength;
                hasFullKey = recordBuffer.available_bytes >= requiredBytes;
                requiredBytes += (int)valueLength;      // TODO: Handle long values
            }

            requiredBytes += OptionalLength;
            return recordBuffer.available_bytes >= requiredBytes;
        }

        /// <summary>If true, this DiskLogRecord owns the buffer and must free it on <see cref="Dispose"/></summary>
        public readonly bool OwnsMemory => recordBuffer is not null;

        // Indicator bits for version and varlen int. Since the record is always aligned to 8 bytes, we can use long operations (on values only in
        // the low byte) which are faster than byte or int.
#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter
        const long kVersionBitMask = 7 << 5;        // 3 bits for version
        const long kKeyLengthBitMask = 3 << 3;      // 2 bits for the number of bytes for the key length (this is limited to 512MB)
        const long kValueLengthBitMask = 7;         // 3 bits for the number of bytes for the value length
#pragma warning restore IDE1006 // Naming Styles

        const long CurrentVersion = 0 << 5;         // Initial version

        /// <summary>This contains the leading byte which are the indicators, plus the up-to-int length for the key, and then some or all of the length for the value.</summary>
        readonly long IndicatorAddress => physicalAddress + RecordInfo.GetLength();

        /// <summary>Version of the variable-length byte encoding for key and value lengths. There is no version info for <see cref="RecordInfo.RecordIsInline"/>
        /// records as these are image-identical to LogRecord. TODO: Include a major version for this in the Recovery version-compatibility detection</summary>
        readonly long VarByteVersion => (*(long*)IndicatorAddress & kVersionBitMask) >> 6;

        readonly (int length, long dataAddress) KeyInfo
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var address = IndicatorAddress;
                if (Info.RecordIsInline)    // For inline, the key length int starts at the same offset as IndicatorAddress
                    return (*(int*)address, address + SpanField.FieldLengthPrefixSize);

                var keyLengthBytes = (int)((*(long*)address & kKeyLengthBitMask) >> 3);
                var valueLengthBytes = (int)(*(long*)address & kValueLengthBitMask);

                byte* ptr = (byte*)++address;  // Move past the indicator byte; the next bytes are key length
                var keyLength = ReadVarBytes(keyLengthBytes, ref ptr);

                // Move past the key and value length bytes to the start of the key
                return ((int)keyLength, address + keyLengthBytes + valueLengthBytes);
            }
        }

        readonly (long length, long dataAddress) ValueInfo
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var keyInfo = KeyInfo;

                if (Info.RecordIsInline)    // For inline values the length is an int.
                    return (*(int*)keyInfo.length, keyInfo.dataAddress);

                var address = IndicatorAddress;
                var keyLengthBytes = (int)((*(long*)address & kKeyLengthBitMask) >> 3);
                var valueLengthBytes = (int)(*(long*)address & kValueLengthBitMask);

                byte* ptr = (byte*)IndicatorAddress + 1 + keyLengthBytes;   // Skip over the key length bytes; the value length bytes are immediately after (before the key data)
                var valueLength = ReadVarBytes(valueLengthBytes, ref ptr);
                return (valueLength, keyInfo.dataAddress + keyInfo.length); // Value data (without length prefix) starts immediately after key data
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static byte CreateIndicatorByte(int keyLength, long valueLength, out int keyByteCount, out int valueByteCount)
        {
            keyByteCount = GetByteCount(keyLength);
            valueByteCount = GetByteCount(valueLength);
            return (byte)(CurrentVersion            // Already shifted
                | ((long)keyByteCount << 3)         // Shift key into position
                | (long)valueByteCount);            // Value does not need to be shifted
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static int GetByteCount(long num)
        {
            var result = 0;
            do
            {
                num >>= 8;
                ++result;
            } while (num > 0);
            return result;
        }

        static void WriteVarBytes(long value, int len, ref byte* ptr)
        {
            for (; len > 0; --len)
            {
                *ptr++ = (byte)(value & 0xFF);
                value >>= 8;
            }
            Debug.Assert(value == 0, "len too short");
        }

        static long ReadVarBytes(int len, ref byte* ptr)
        {
            long value = 0;
            for (; len > 0; --len)
                value = (value << 8) + *ptr++;
            return value;
        }

        public void Dispose()
        {
            recordBuffer?.Dispose();
            recordBuffer = null;
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
        public readonly bool IsPinnedValue => Info.ValueIsInline;

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
                return new((byte*)dataAddress, (int)length);    // TODO: handle long value length
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
            ValueDataSize = Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : (int)ValueInfo.length,
            ValueIsObject = Info.ValueIsObject,
            HasETag = Info.HasETag,
            HasExpiration = Info.HasExpiration
        };
        #endregion //ISourceLogRecord

        public readonly int OptionalLength => ETagLen + ExpirationLen;

        private readonly int ETagLen => Info.HasETag ? LogRecord.ETagSize : 0;
        private readonly int ExpirationLen => Info.HasExpiration ? LogRecord.ExpirationSize : 0;

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
        /// Serialize for RUMD operations, called by PendingContext; these also have TInput, TOutput, and TContext, which are handled by PendingContext.
        /// </summary>
        /// <param name="key">Record key</param>
        /// <param name="valueSpan">Record value as a Span, if Upsert</param>
        /// <param name="valueObject">Record value as an object, if Upsert</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SerializeForPendingOperation(ReadOnlySpan<byte> key, ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, SectorAlignedBufferPool bufferPool)
            => SerializeForPendingRUMD(key, valueSpan, valueObject, bufferPool, ref recordBuffer);

        /// <summary>
        /// Serialize for RUMD operations, called by PendingContext; these also have TInput, TOutput, and TContext, which are handled by PendingContext.
        /// </summary>
        /// <remarks>This overload may be called either directly for a caller who owns the <paramref name="allocatedRecord"/>, or with this.allocatedRecord.</remarks>
        /// <param name="key">Record key</param>
        /// <param name="valueSpan">Record value as a Span, if Upsert</param>
        /// <param name="valueObject">Record value as an object, if Upsert</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        /// <param name="allocatedRecord">The allocated record; may be owned by this instance, or owned by the caller for reuse</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SerializeForPendingRUMD(ReadOnlySpan<byte> key, ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord)
        {
            // OptionalSize (ETag and Expiration) is not considered here; those are specified in the Input, which is serialized separately by PendingContext.

            long recordSize;
            byte* ptr;
            if (!valueSpan.IsEmpty)
            {
                // Value is a span so we can use RecordIsInline format, so both key and value are int length.
                recordSize = RecordInfo.GetLength() + key.TotalSize() + valueSpan.TotalSize();

                allocatedRecord.pool.EnsureSize(ref allocatedRecord, (int)recordSize);
                physicalAddress = (long)allocatedRecord.GetValidPointer();
                ptr = (byte*)physicalAddress;

                *(RecordInfo*)ptr = default;
                ptr += RecordInfo.GetLength();

                InfoRef.SetKeyIsInline();
                *(int*)ptr = key.Length;
                ptr += SpanField.FieldLengthPrefixSize;
                key.CopyTo(new Span<byte>(ptr, key.Length));
                ptr += key.Length;

                InfoRef.SetValueIsInline();
                *(long*)ptr = valueSpan.Length;
                ptr += sizeof(long);
                valueSpan.CopyTo(new Span<byte>(ptr, valueSpan.Length));
            }

            // Value is an object so we use the varbyte format. For RUMD ops we don't serialize the object; we just carry it through the pending IO sequence.
            // However since we check for RecordIsInline to read the record fields, we must use the indicator byte. Do so with a 0 value length.
            var valueLength = 0;
            var indicatorByte = CreateIndicatorByte(key.Length, valueLength, out var keyLengthByteCount, out var valueLengthByteCount);
            Debug.Assert(valueLengthByteCount == 1, $"valueByteCount should be 1 but was {valueLengthByteCount}");

            recordSize = RecordInfo.GetLength()
                + 1 // indicator byte
                + keyLengthByteCount + key.Length
                + valueLengthByteCount; // serialized value length is 0, so one byte

            // This writes the value length, but not value data
            ptr = SerializeCommonVarByteFields(recordInfo:default, indicatorByte, key, keyLengthByteCount, valueLength, valueLengthByteCount, recordSize, bufferPool, ref allocatedRecord);

            // Set the value
            InfoRef.SetValueIsObject();
            this.valueObject = valueObject;
        }

        /// <summary>
        /// Serialize for Compact, Scan, Conditional Pending Operations, Migration, Replication, etc. The logRecord comes from the log or disk; there is no associated TInput, TOutput, TContext.
        /// </summary>
        /// <param name="logRecord">The log record. This may be either in-memory or from disk IO</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        /// <param name="valueSerializer">Serializer for the value object; if null, do not serialize (carry the valueObject (if any) through from the logRecord instead)</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Serialize(ref readonly LogRecord logRecord, SectorAlignedBufferPool bufferPool, IObjectSerializer<IHeapObject> valueSerializer)
            => Serialize(in logRecord, bufferPool, valueSerializer, ref recordBuffer);

        /// <summary>
        /// Serialize for Compact, Pending Operations, etc. There is no associated TInput, TOutput, TContext for these as it is just a direct copy of data.
        /// </summary>
        /// <param name="logRecord">The log record. This may be either in-memory or from disk IO</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        /// <param name="valueSerializer">Serializer for the value object; if null, do not serialize (carry the valueObject (if any) through from the logRecord instead)</param>
        /// <param name="allocatedRecord">The allocated record; may be owned by this instance, or owned by the caller for reuse</param>
        /// <remarks>This overload may be called either directly for a caller who owns the <paramref name="allocatedRecord"/>, or with this.allocatedRecord.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Serialize(ref readonly LogRecord logRecord, SectorAlignedBufferPool bufferPool, IObjectSerializer<IHeapObject> valueSerializer, ref SectorAlignedMemory allocatedRecord)
        {
            if (logRecord.Info.RecordIsInline)
            {
                DirectCopyRecord(logRecord.GetInlineRecordSizes().actualSize, logRecord.physicalAddress, ref allocatedRecord);
                return;
            }

            // Record is not inline so we must use the varbyte format: create the indicator byte and space-optimized length representation.

            // If we have an object and we don't serialize it, we don't need to allocate space for it.
            // Value length prefix on the disk is a long, as it may be an object.
            var valueLength = !logRecord.Info.ValueIsObject
                ? logRecord.ValueSpan.Length
                : (valueSerializer is not null ? logRecord.ValueObject.Size : 0);

            var indicatorByte = CreateIndicatorByte(logRecord.Key.Length, valueLength, out var keyLengthByteCount, out var valueLengthByteCount);

            var recordSize = RecordInfo.GetLength()
                + 1 // indicator byte
                + keyLengthByteCount + logRecord.Key.Length
                + valueLengthByteCount + valueLength
                + logRecord.OptionalLength;

            // This writes the value length, but not value data
            var ptr = SerializeCommonVarByteFields(logRecord.Info, indicatorByte, logRecord.Key, keyLengthByteCount, valueLength, valueLengthByteCount, recordSize, bufferPool, ref allocatedRecord);

            // Set the value
            if (!logRecord.Info.ValueIsObject)
            {
                InfoRef.SetValueIsInline();
                logRecord.ValueSpan.CopyTo(new Span<byte>(ptr, (int)valueLength));
            }
            else if (valueSerializer is not null)
            {
                InfoRef.SetValueIsInline();
                var stream = new UnmanagedMemoryStream(ptr, logRecord.ValueObject.Size);
                valueSerializer.BeginSerialize(stream);
                var valueObject = logRecord.ValueObject;
                valueSerializer.Serialize(valueObject);
                valueSerializer.EndSerialize();
            }
            else
            {
                InfoRef.SetValueIsObject();
                valueObject = logRecord.ValueObject;
            }
            ptr += valueLength;

            CopyOptionals(in logRecord, ref ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe byte* SerializeCommonVarByteFields(RecordInfo recordInfo, byte indicatorByte, ReadOnlySpan<byte> key, int keyLengthByteCount,
                long valueLength, int valueLengthByteCount, long recordSize, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord)
        {
            allocatedRecord.pool.EnsureSize(ref allocatedRecord, (int)recordSize);
            physicalAddress = (long)allocatedRecord.GetValidPointer();
            var ptr = (byte*)physicalAddress;

            *(RecordInfo*)ptr = recordInfo;
            ptr += RecordInfo.GetLength();

            *ptr++ = indicatorByte;

            InfoRef.SetKeyIsInline();
            WriteVarBytes(key.Length, keyLengthByteCount, ref ptr);

            // Copy the value length (the number of bytes is encoded in the indicator byte, written above), but not the actual value; the caller does that.
            WriteVarBytes(valueLength, valueLengthByteCount, ref ptr);

            key.CopyTo(new Span<byte>(ptr, key.Length));

            // Return the pointer to the value data space (immediately following the key data space, with no value length prefix as the value was already written).
            return ptr + key.Length;
        }

        /// <summary>
        /// Deserialize the current value span to a <see cref="valueObject"/> valueObject.
        /// </summary>
        /// <param name="valueSerializer">Serializer for the value object; if null, do not serialize (carry the valueObject (if any) through from the logRecord instead)</param>
        /// <remarks>This overload converts from LogRecord to DiskLogRecord.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IHeapObject DeserializeValueObject(IObjectSerializer<IHeapObject> valueSerializer)
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
        private void DirectCopyRecord(long recordSize, long srcPhysicalAddress, ref SectorAlignedMemory allocatedRecord)
        {
            allocatedRecord.pool.EnsureSize(ref allocatedRecord, RoundUp((int)recordSize, Constants.kRecordAlignment));
            physicalAddress = (long)allocatedRecord.GetValidPointer();
            Buffer.MemoryCopy((byte*)srcPhysicalAddress, (byte*)physicalAddress, recordSize, recordSize);
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
        /// Clone from a temporary <see cref="DiskLogRecord"/> (having no <see cref="SectorAlignedMemory"/>) to a longer-lasting one.
        /// </summary>
        /// <param name="inputDiskLogRecord"></param>
        /// <param name="bufferPool"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CloneFrom(ref DiskLogRecord inputDiskLogRecord, SectorAlignedBufferPool bufferPool)
        {
            Debug.Assert(!inputDiskLogRecord.IsSet, "inputDiskLogRecord is not set");

            if (!inputDiskLogRecord.Info.ValueIsObject)
            {
                // The record is in inline format, so we can just copy the whole record.
                DirectCopyRecord(inputDiskLogRecord.GetSerializedLength(), inputDiskLogRecord.physicalAddress, ref recordBuffer);
                return;
            }

            // The source DiskLogRecord has a Value object so it is in varbyte format. Copy everything up to the end of the key; ignore the serialized
            // value data even if the source source still has it. inputDiskLogRecord can Return() its recordBuffer, releasing any serialized value data.
            var (length, dataAddress) = KeyInfo;
            var partialRecordSize = dataAddress + length - inputDiskLogRecord.physicalAddress;
            var allocatedRecordSize = partialRecordSize + OptionalLength;

            if (recordBuffer is not null)
                recordBuffer.pool.EnsureSize(ref recordBuffer, (int)allocatedRecordSize);       // TODO handle 'long' valuelength
            else
                recordBuffer = bufferPool.Get((int)allocatedRecordSize);                        // TODO handle 'long' valuelength

            Buffer.MemoryCopy((void*)inputDiskLogRecord.physicalAddress, (void*)physicalAddress, partialRecordSize, partialRecordSize);

            // Clear the value length in the indicator byte, as we did not copy the serialized data.
            var ptr = (byte*)physicalAddress + RecordInfo.GetLength();
            *ptr = (byte)(*ptr & ~kValueLengthBitMask);

            // Set the Value
            InfoRef.SetValueIsObject();
            valueObject = inputDiskLogRecord.valueObject;

            // Set the Optionals
            ptr = (byte*)physicalAddress + partialRecordSize;
            CopyOptionals(ref inputDiskLogRecord, ref ptr);
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
            Debug.Assert(diskLogRecord.recordBuffer is not null, "inputDiskLogRecord does not own its memory");

            recordBuffer?.Return();
            recordBuffer = diskLogRecord.recordBuffer;
            diskLogRecord.recordBuffer = null;   // Transfers ownership
            physicalAddress = (long)recordBuffer.GetValidPointer();
        }
        #endregion //Serialized Record Creation

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            var valueString = Info.ValueIsObject ? $"obj:{ValueObject}" : ValueSpan.ToString();

            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasETag {bstr(Info.HasETag)}:{ETag} | HasExpiration {bstr(Info.HasExpiration)}:{Expiration}";
        }
    }
}