// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
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
        /// that will have at least the lifetime of this <see cref="DiskLogRecord"/>. This <see cref="DiskLogRecord"/> does not own the memory
        /// allocation so will not free it on <see cref="Dispose()"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DiskLogRecord(long physicalAddress, int length) : this(physicalAddress)
        {
            if (!IsComplete(physicalAddress, length, out _, out _))
                throw new TsavoriteException("DiskLogRecord is not complete");
        }

        /// <summary>Constructor that takes a physical address, which may come from a <see cref="SectorAlignedMemory"/> or some other allocation
        /// that will have at least the lifetime of this <see cref="DiskLogRecord"/> and is known to contain the entire record (as there is no length
        /// supplied). This <see cref="DiskLogRecord"/> does not own the memory allocation so will not free it on <see cref="Dispose()"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DiskLogRecord(long physicalAddress)
        {
            this.physicalAddress = physicalAddress;
            InfoRef.ClearBitsForDiskImages();
        }

        /// <summary>Constructor that takes a <see cref="SectorAlignedMemory"/> from which it obtains the physical address.
        /// This <see cref="DiskLogRecord"/> owns the memory allocation and must free it on <see cref="Dispose()"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DiskLogRecord(SectorAlignedMemory allocatedRecord) : this((long)allocatedRecord.GetValidPointer())
        {
            recordBuffer = allocatedRecord;
            if (!IsComplete(allocatedRecord, out _, out _))
                throw new TsavoriteException("DiskLogRecord is not complete");
        }

        /// <summary>Constructor that takes an <see cref="AsyncIOContext"/> from which it obtains the physical address and ValueObject.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DiskLogRecord(ref AsyncIOContext ctx) : this(ctx.record)
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

        /// <summary>Called by IO to determine whether the record is complete (full serialized length has been read)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsComplete(SectorAlignedMemory recordBuffer, out bool hasFullKey, out int requiredBytes)
            => IsComplete((long)recordBuffer.GetValidPointer(), recordBuffer.available_bytes, out hasFullKey, out requiredBytes);

        private static bool IsComplete(long physicalAddress, int availableBytes, out bool hasFullKey, out int requiredBytes)
        {
            hasFullKey = false;

            // Check for RecordInfo and either indicator byte or key length; InlineLengthPrefixSize is the larger.
            if (availableBytes < RecordInfo.GetLength() + LogField.InlineLengthPrefixSize)
            {
                requiredBytes = InitialIOSize;
                return false;
            }

            // Now we can tell if this is a fully inline vs. varbyte record.
            var address = physicalAddress;
            if ((*(RecordInfo*)address).RecordIsInline)
            {
                address += RecordInfo.GetLength();
                address += sizeof(int) + *(int*)address;                            // Inline key length is after RecordInfo; position address after the Key (but don't dereference it yet!)
                requiredBytes = (int)(address + sizeof(int) - physicalAddress);     // Include value length int in the calculation
                if (availableBytes < requiredBytes)
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
                var keyLengthBytes = (int)((*(byte*)address & kKeyLengthBitMask) >> 3);
                var valueLengthBytes = (int)(*(byte*)address & kValueLengthBitMask);

                requiredBytes = RecordInfo.GetLength() + 1 + keyLengthBytes + valueLengthBytes; // Include the indicator byte in the calculation
                if (availableBytes < requiredBytes)
                    return false;

                var ptr = (byte*)++address;  // Move past the indicator byte; the next bytes are key length, then value length
                var keyLength = ReadVarBytes(keyLengthBytes, ref ptr);
                var valueLength = ReadVarBytes(valueLengthBytes, ref ptr);

                requiredBytes += (int)keyLength;
                hasFullKey = availableBytes >= requiredBytes;
                requiredBytes += (int)valueLength;      // TODO: Handle long values
            }

            var info = *(RecordInfo*)physicalAddress;
            var eTagLen = info.HasETag ? LogRecord.ETagSize : 0;
            var expirationLen = info.HasExpiration ? LogRecord.ExpirationSize : 0;

            requiredBytes += eTagLen + expirationLen;
            return availableBytes >= requiredBytes;
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

        const long CurrentVersion = 0 << 5;         // Initial version is 0; shift will always be 5

        /// <summary>This contains the leading byte which are the indicators, plus the up-to-int length for the key, and then some or all of the length for the value.</summary>
        internal readonly long IndicatorAddress => physicalAddress + RecordInfo.GetLength();

        /// <summary>Version of the variable-length byte encoding for key and value lengths. There is no version info for <see cref="RecordInfo.RecordIsInline"/>
        /// records as these are image-identical to LogRecord. TODO: Include a major version for this in the Recovery version-compatibility detection</summary>
        internal readonly long Version => (*(byte*)IndicatorAddress & kVersionBitMask) >> 6;

        internal readonly (int length, long dataAddress) KeyInfo
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var address = IndicatorAddress;
                if (Info.RecordIsInline)    // For inline, the key length int starts at the same offset as IndicatorAddress
                    return (*(int*)address, address + LogField.InlineLengthPrefixSize);

                var keyLengthBytes = (int)((*(byte*)address & kKeyLengthBitMask) >> 3) + 1;
                var valueLengthBytes = (int)(*(byte*)address & kValueLengthBitMask) + 1;

                byte* ptr = (byte*)++address;  // Move past the indicator byte; the next bytes are key length
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

                var address = IndicatorAddress;
                var keyLengthBytes = (int)((*(byte*)address & kKeyLengthBitMask) >> 3) + 1;     // add 1 due to 0-based
                var valueLengthBytes = (int)(*(byte*)address & kValueLengthBitMask) + 1;        // add 1 due to 0-based

                byte* ptr = (byte*)IndicatorAddress + 1 + keyLengthBytes;   // Skip over the key length bytes; the value length bytes are immediately after (before the key data)
                var valueLength = ReadVarBytes(valueLengthBytes, ref ptr);
                return (valueLength, keyInfo.dataAddress + keyInfo.length); // Value data (without length prefix) starts immediately after key data
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte CreateIndicatorByte(int keyLength, long valueLength, out int keyByteCount, out int valueByteCount)
        {
            keyByteCount = GetByteCount(keyLength);
            valueByteCount = GetByteCount(valueLength);
            return (byte)(CurrentVersion                // Already shifted
                | ((long)(keyByteCount - 1) << 3)       // Shift key into position; subtract 1 for 0-based
                | (long)(valueByteCount - 1));          // Value does not need to be shifted; subtract 1 for 0-based
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetByteCount(long num)
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
            for (var ii = 0; ii < len; ++ii)
                value |= (long)*ptr++ << (ii * 8);
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
        public readonly ReadOnlySpan<byte> RecordSpan
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var valueInfo = ValueInfo;
                if (valueInfo.length == 0)
                    throw new TsavoriteException("RecordSpan is not valid for records with unserialized ValueObjects");
                return new((byte*)physicalAddress, (int)GetSerializedLength()); // TODO: Handle long object sizes
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
        /// Serialize for Compact, Scan, Conditional Pending Operations, Migration, Replication, etc. The logRecord comes from the in-memory log; there is no associated TInput, TOutput, TContext.
        /// </summary>
        /// <param name="logRecord">The log record. This may be either in-memory or from disk IO</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        /// <param name="valueSerializer">Serializer for the value object; if null, do not serialize (carry the valueObject (if any) through from the logRecord instead)</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Serialize(ref readonly LogRecord logRecord, SectorAlignedBufferPool bufferPool, IObjectSerializer<IHeapObject> valueSerializer)
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
        public void Serialize(ref readonly LogRecord logRecord, SectorAlignedBufferPool bufferPool, IObjectSerializer<IHeapObject> valueSerializer, ref SectorAlignedMemory allocatedRecord)
        {
            if (logRecord.Info.RecordIsInline)
            {
                DirectCopyRecord(logRecord.ActualRecordSize, logRecord.physicalAddress, bufferPool, ref allocatedRecord);
                return;
            }

            // Record is not inline so we must use the varbyte format: create the indicator byte and space-optimized length representation.

            // If we have an object and we don't serialize it, we don't need to allocate space for it.
            // Value length prefix on the disk is a long, as it may be an object.
            var valueLength = !logRecord.Info.ValueIsObject
                ? logRecord.ValueSpan.Length
                : (valueSerializer is not null ? logRecord.ValueObject.DiskSize : 0);

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
                logRecord.ValueSpan.CopyTo(new Span<byte>(ptr, (int)valueLength));
            else
            {
                if (valueSerializer is not null)
                {
                    var stream = new UnmanagedMemoryStream(ptr, logRecord.ValueObject.DiskSize, logRecord.ValueObject.DiskSize, FileAccess.ReadWrite);
                    valueSerializer.BeginSerialize(stream);
                    valueSerializer.Serialize(logRecord.ValueObject);
                    valueSerializer.EndSerialize();
                }
                else
                    valueObject = logRecord.ValueObject;
                InfoRef.SetValueIsObject();
            }
            ptr += valueLength;
            CopyOptionals(in logRecord, ref ptr);

            allocatedRecord.available_bytes = (int)recordSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe byte* SerializeCommonVarByteFields(RecordInfo recordInfo, byte indicatorByte, ReadOnlySpan<byte> key, int keyLengthByteCount,
                long valueLength, int valueLengthByteCount, long recordSize, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord)
        {
            EnsureAllocation(recordSize, bufferPool, ref allocatedRecord);
            return SerializeCommonVarByteFields(recordInfo, indicatorByte, key, keyLengthByteCount, valueLength, valueLengthByteCount, allocatedRecord.GetValidPointer());
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
        private static long SerializeVarbyteRecord<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, long srcPhysicalAddress, IObjectSerializer<IHeapObject> valueSerializer,
                ref SpanByteAndMemory output, MemoryPool<byte> memoryPool)
            where TSourceLogRecord : ISourceLogRecord
        {
            // If we have an object and we don't serialize it, we don't need to allocate space for it.
            // Value length prefix on the disk is a long, as it may be an object.
            var valueLength = !srcLogRecord.Info.ValueIsObject
                ? srcLogRecord.ValueSpan.Length
                : srcLogRecord.ValueObject.DiskSize;

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
                serializedSize = SerializeVarbyteRecordToPinnedPointer(ref srcLogRecord, outPtr + sizeof(int), indicatorByte, keyLengthByteCount, valueLength, valueLengthByteCount, valueSerializer);
                output.SpanByte.Length = (int)serializedSize;
            }
            else
            {
                // Do not include the length prefix in the output stream; this is done by the caller before writing the stream, from the SpanByte.Length we set here.
                output.EnsureHeapMemorySize((int)recordSize + sizeof(int), memoryPool);
                fixed (byte* outPtr = output.MemorySpan)
                {
                    serializedSize = SerializeVarbyteRecordToPinnedPointer(ref srcLogRecord, outPtr, indicatorByte, keyLengthByteCount, valueLength, valueLengthByteCount, valueSerializer);
                    output.Length = (int)recordSize;
                }
            }
            Debug.Assert(serializedSize == recordSize, $"Serialized size {serializedSize} does not match expected size {recordSize}");
            return serializedSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long SerializeVarbyteRecordToPinnedPointer<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, byte* ptr, byte indicatorByte,
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
                var stream = new UnmanagedMemoryStream(ptr, srcLogRecord.ValueObject.DiskSize, srcLogRecord.ValueObject.DiskSize, FileAccess.ReadWrite);
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
            => CloneFrom(ref inputDiskLogRecord, bufferPool, ref recordBuffer, preferDeserializedObject);

        /// <summary>
        /// Clone from a temporary <see cref="DiskLogRecord"/> (having no overflow <see cref="SectorAlignedMemory"/>) to a longer-lasting one.
        /// </summary>
        /// <param name="inputDiskLogRecord"></param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        /// <param name="allocatedRecord">The allocated record; may be owned by this instance, or owned by the caller for reuse</param>
        /// <param name="preferDeserializedObject">If true, prefer the deserialized object over copying the serialized value; this saves space for pending operations</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CloneFrom(ref DiskLogRecord inputDiskLogRecord, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord, bool preferDeserializedObject)
        {
            Debug.Assert(inputDiskLogRecord.IsSet, "inputDiskLogRecord is not set");

            if (!inputDiskLogRecord.Info.ValueIsObject || !preferDeserializedObject)
            {
                Debug.Assert(inputDiskLogRecord.ValueInfo.length > 0, "inputDiskLogRecord value length should be > 0");
                DirectCopyRecord(inputDiskLogRecord.GetSerializedLength(), inputDiskLogRecord.physicalAddress, bufferPool, ref allocatedRecord);
                return;
            }

            // The source DiskLogRecord has a Value object rather than serialized bytes so it is in varbyte format. Copy everything up to the end of the key; ignore
            // the serialized value data even if the source source still has it. inputDiskLogRecord can Return() its recordBuffer, releasing any serialized value data.
            var (length, dataAddress) = inputDiskLogRecord.KeyInfo;
            var partialRecordSize = dataAddress + length - inputDiskLogRecord.physicalAddress;
            var allocatedRecordSize = partialRecordSize + inputDiskLogRecord.OptionalLength;

            if (allocatedRecord is not null)
                allocatedRecord.pool.EnsureSize(ref allocatedRecord, (int)allocatedRecordSize);       // TODO handle 'long' valuelength
            else
                allocatedRecord = bufferPool.Get((int)allocatedRecordSize);                        // TODO handle 'long' valuelength
            physicalAddress = (long)allocatedRecord.GetValidPointer();

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
            allocatedRecord.available_bytes = (int)allocatedRecordSize;
        }

        /// <summary>
        /// Transfer memory ownership from a temporary <see cref="DiskLogRecord"/> to a longer-lasting one.
        /// </summary>
        /// <remarks>This is separate from <see cref="CloneFrom(ref DiskLogRecord, SectorAlignedBufferPool, ref SectorAlignedMemory, bool)"/> to ensure the caller
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
        public static void Serialize<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, IObjectSerializer<IHeapObject> valueSerializer, ref SpanByteAndMemory output, MemoryPool<byte> memoryPool)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.AsLogRecord(out var logRecord))
            {
                if (logRecord.Info.RecordIsInline)
                    DirectCopyRecord(logRecord.physicalAddress, logRecord.ActualRecordSize, ref output, memoryPool);
                else
                    _ = SerializeVarbyteRecord(ref logRecord, logRecord.physicalAddress, valueSerializer, ref output, memoryPool);
                return;
            }

            if (!srcLogRecord.AsDiskLogRecord(out var diskLogRecord))
                throw new TsavoriteException("Unknown TSourceLogRecord type");
            Debug.Assert(diskLogRecord.Info.KeyIsInline, "DiskLogRecord key should always be inline");

            var valueInfo = diskLogRecord.ValueInfo;

            // Either the value is present in serialized byte form or there should be an object. If there is no object we have nothing to serialize
            if (diskLogRecord.Info.RecordIsInline
                || (diskLogRecord.Info.KeyIsInline && (valueInfo.length > 0 || diskLogRecord.valueObject is null)))
            {
                DirectCopyRecord(diskLogRecord.physicalAddress, diskLogRecord.GetSerializedLength(), ref output, memoryPool);
                return;
            }
            _ = SerializeVarbyteRecord(ref diskLogRecord, diskLogRecord.physicalAddress, valueSerializer, ref output, memoryPool);
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