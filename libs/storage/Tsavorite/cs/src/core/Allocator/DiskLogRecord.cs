// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>A non-Generic form of the in-memory <see cref="DiskLogRecord{TValue}"/> that provides access to <see cref="RecordInfo"/>.
    /// Useful in quick recordInfo-testing operations</summary>
    public unsafe struct DiskLogRecord
    {
        /// <inheritdoc/>
        public readonly ref RecordInfo GetInfoRef(long physicalAddress) => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);
        /// <inheritdoc/>
        public readonly RecordInfo GetInfo(long physicalAddress) => *(RecordInfo*)physicalAddress;
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
        public readonly bool IsObjectRecord => valueObject is not null;
        /// <inheritdoc/>
        public readonly bool IsSet => true;
        /// <inheritdoc/>
        public readonly ref RecordInfo InfoRef => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);
        /// <inheritdoc/>
        public readonly RecordInfo Info => *(RecordInfo*)physicalAddress;
        /// <inheritdoc/>
        public readonly SpanByte Key => *(SpanByte*)KeyAddress;
        /// <inheritdoc/>
        public readonly SpanByte ValueSpan => IsObjectRecord ? throw new TsavoriteException("Object LogRecord does not have SpanByte values") : *(SpanByte*)ValueAddress;
        /// <inheritdoc/>
        public readonly TValue ValueObject => IsObjectRecord ? valueObject : throw new TsavoriteException("SpanByte LogRecord does not have Object values");
        /// <inheritdoc/>
        public ref TValue GetReadOnlyValueRef()
        {
            if (IsObjectRecord)
#pragma warning disable CS9084 // Struct member returns 'this' or other instance members by reference; OK here because we only use it on direct calls where the stack remains intact
                return ref valueObject;
#pragma warning restore CS9084
            return ref Unsafe.AsRef<TValue>((void*)ValueAddress);
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
                KeySize = Key.TotalSize,
                ValueSize = IsObjectRecord ? ObjectIdMap.ObjectIdSize : ValueSpan.TotalSize,
                HasETag = Info.HasETag,
                HasExpiration = Info.HasExpiration
            };
        #endregion //IReadOnlyRecord

        const int SerializedRecordLengthSize = sizeof(long);

        public readonly long SerializedRecordLength => *(long*)(physicalAddress + RecordInfo.GetLength());

        readonly long KeyAddress => physicalAddress + RecordInfo.GetLength() + SerializedRecordLengthSize + ETagLen + ExpirationLen;

        internal readonly long ValueAddress => KeyAddress + SpanField.InlineSize(KeyAddress);

        private readonly int InlineValueLength => IsObjectRecord ? ObjectIdMap.ObjectIdSize : SpanField.InlineSize(ValueAddress);
        public readonly int OptionalLength => (Info.HasETag ? LogRecord.ETagSize : 0) + (Info.HasExpiration ? LogRecord.ExpirationSize : 0);

        private readonly int ETagLen => Info.HasETag ? LogRecord.ETagSize : 0;
        private readonly int ExpirationLen => Info.HasExpiration ? LogRecord.ExpirationSize : 0;

        private readonly long GetETagAddress() => physicalAddress + RecordInfo.GetLength() + SerializedRecordLengthSize;
        private readonly long GetExpirationAddress() => GetETagAddress() + ETagLen;

        /// <summary>The size to IO from disk when reading a record. Keys and Values are SpanByte on disk and we reuse the max inline key size
        /// for both key and value for this estimate. They are prefaced by the full record length and optionals (ETag, Expiration) which we include in the estimate.</summary>
        public static int GetIOSize(int sectorSize) => RoundUp(RecordInfo.GetLength() + SerializedRecordLengthSize + sizeof(long) * 2 + sizeof(int) * 2 + (1 << LogSettings.kMaxInlineKeySizeBits) * 2, sectorSize);

        internal static SpanByte GetContextRecordKey(ref AsyncIOContext<TValue> ctx) => new DiskLogRecord<TValue>((long)ctx.record.GetValidPointer()).Key;

        internal static SpanByte GetContextRecordValue(ref AsyncIOContext<TValue> ctx) => new DiskLogRecord<TValue>((long)ctx.record.GetValidPointer()).ValueSpan;

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            var valueString = IsObjectRecord ? ValueObject.ToString() : ValueSpan.ToString();

            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasETag {bstr(Info.HasETag)}:{ETag} | HasExpiration {bstr(Info.HasExpiration)}:{Expiration}";
        }
    }
}