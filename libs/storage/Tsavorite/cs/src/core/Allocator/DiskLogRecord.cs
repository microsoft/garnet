// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>A non-Generic form of the in-memory <see cref="DiskLogRecord{TValue}"/> that provides access to <see cref="RecordInfo"/>.
    /// Useful in quick recordInfo-testing operations</summary>
    public unsafe struct DiskLogRecord
    {
        /// <summary>The length of the serialized data.</summary>
        internal const int SerializedRecordLengthSize = sizeof(long);

        /// <summary>A ref to the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref RecordInfo GetInfoRef(long physicalAddress) => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);

        /// <summary>Fast access returning a copy of the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RecordInfo GetInfo(long physicalAddress) => *(RecordInfo*)physicalAddress;

        /// <summary>Serialized length of the record</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetSerializedLength(long physicalAddress) => *(long*)(physicalAddress + RecordInfo.GetLength());
    }

    /// <summary>The record on the disk: header, optional fields, key, value</summary>
    /// <remarks>The space is laid out as:
    ///     <list>
    ///     <item>[RecordInfo][SerializedRecordLength][ETag?][Expiration?][key SpanByte][value SpanByte]</item>
    ///     </list>
    /// This lets us get to the optional fields for comparisons without loading the full record (GetIOSize should cover the space for optionals).
    /// </remarks>
    public unsafe struct DiskLogRecord<TValue> : ISourceLogRecord<TValue>
    {
        /// <summary>The physicalAddress in the log.</summary>
        internal readonly long physicalAddress;

        /// <summary>The deserialized ValueObject if this is a disk record for the Object Store.</summary>
        internal TValue valueObject;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DiskLogRecord(long physicalAddress)
        {
            this.physicalAddress = physicalAddress;
            InfoRef.ClearBitsForDiskImages();
        }

        #region IReadOnlyRecord
        /// <inheritdoc/>
        public readonly bool ValueIsObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(Info.ValueIsObject == valueObject is IHeapObject, $"Mismatch between Info.ValueIsObject ({Info.ValueIsObject}) and valueObject is IHeapObject {valueObject is IHeapObject}");
                return Info.ValueIsObject;
            }
        }

        /// <inheritdoc/>
        public readonly bool IsSet => physicalAddress != 0;
        /// <inheritdoc/>
        public readonly ref RecordInfo InfoRef => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);
        /// <inheritdoc/>
        public readonly RecordInfo Info => *(RecordInfo*)physicalAddress;
        /// <inheritdoc/>
        public readonly SpanByte Key => SpanByte.FromLengthPrefixedPinnedPointer((byte*)KeyAddress);
        /// <inheritdoc/>
        public readonly SpanByte ValueSpan => ValueIsObject ? throw new TsavoriteException("DiskLogRecord with ValueIsObject does not support SpanByte values") : SpanByte.FromLengthPrefixedPinnedPointer((byte*)ValueAddress);
        /// <inheritdoc/>
        public readonly TValue ValueObject => ValueIsObject ? valueObject : throw new TsavoriteException("This DiskLogRecord has a non-object Value");
        
        /// <inheritdoc/>
        public TValue GetReadOnlyValue()
        {
            if (ValueIsObject)
                return valueObject;

            var sb = ValueSpan;
            return Unsafe.As<SpanByte, TValue>(ref sb);
        }

        /// <inheritdoc/>
        public readonly long ETag => Info.HasETag ? *(long*)GetETagAddress() : 0;

        /// <inheritdoc/>
        public readonly long Expiration => Info.HasExpiration ? *(long*)GetExpirationAddress() : 0;

        /// <inheritdoc/>
        public readonly void ClearValueObject(Action<TValue> disposer) { }  // Nothing done here; we dispose the object in the pending operation completion

        /// <inheritdoc/>
        public readonly LogRecord<TValue> AsLogRecord() => throw new TsavoriteException("DiskLogRecord cannot be converted to AsLogRecord");
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordFieldInfo GetRecordFieldInfo() => new()
            {
                KeyTotalSize = Key.TotalSize,
                ValueTotalSize = ValueIsObject ? ObjectIdMap.ObjectIdSize : ValueSpan.TotalSize,
                ValueIsObject = ValueIsObject,
                HasETag = Info.HasETag,
                HasExpiration = Info.HasExpiration
            };
        #endregion //IReadOnlyRecord

        public readonly long SerializedRecordLength => DiskLogRecord.GetSerializedLength(physicalAddress);

        readonly long KeyAddress => physicalAddress + RecordInfo.GetLength() + DiskLogRecord.SerializedRecordLengthSize + ETagLen + ExpirationLen;

        internal readonly long ValueAddress => KeyAddress + SpanField.GetTotalSizeOfInlineField(KeyAddress);

        private readonly int InlineValueLength => ValueIsObject ? ObjectIdMap.ObjectIdSize : SpanField.GetTotalSizeOfInlineField(ValueAddress);
        public readonly int OptionalLength => (Info.HasETag ? LogRecord.ETagSize : 0) + (Info.HasExpiration ? LogRecord.ExpirationSize : 0);

        private readonly int ETagLen => Info.HasETag ? LogRecord.ETagSize : 0;
        private readonly int ExpirationLen => Info.HasExpiration ? LogRecord.ExpirationSize : 0;

        private readonly long GetETagAddress() => physicalAddress + RecordInfo.GetLength() + DiskLogRecord.SerializedRecordLengthSize;
        private readonly long GetExpirationAddress() => GetETagAddress() + ETagLen;

        /// <summary>The initial size to IO from disk when reading a record. If we don't get the full record, at least we'll get the SerializedRecordLength
        /// and can read the full record using that.</summary>
        public static int GetEstimatedIOSize(int sectorSize, bool isObjectAllocator) => 
            RoundUp(RecordInfo.GetLength()
                + DiskLogRecord.SerializedRecordLengthSize                          // Total record length on disk; used in IO
                + LogRecord.ETagSize + LogRecord.ExpirationSize                     // Optionals, included in the estimate
                + sizeof(int) + (1 << LogSettings.kDefaultMaxInlineKeySizeBits)     // Key; length prefix is an int
                + sizeof(long) + (1 << LogSettings.kDefaultMaxInlineValueSizeBits)  // Value; length prefix is a long as it may be an object
                + (isObjectAllocator ? sectorSize : 0)                              // Additional read for object value; TODO adjust this for balance between wasted initial IO and reduction in secondary IO
            , sectorSize);

        internal static SpanByte GetContextRecordKey(ref AsyncIOContext<TValue> ctx) => new DiskLogRecord<TValue>((long)ctx.record.GetValidPointer()).Key;

        internal static SpanByte GetContextRecordValue(ref AsyncIOContext<TValue> ctx) => new DiskLogRecord<TValue>((long)ctx.record.GetValidPointer()).ValueSpan;

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            var valueString = ValueIsObject ? ValueObject.ToString() : ValueSpan.ToString();

            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasETag {bstr(Info.HasETag)}:{ETag} | HasExpiration {bstr(Info.HasExpiration)}:{Expiration}";
        }
    }
}