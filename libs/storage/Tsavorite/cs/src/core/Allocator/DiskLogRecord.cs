// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>The record on the disk: header, optional fields, key, value</summary>
    /// <remarks>The space is laid out as:
    ///     <list>
    ///     <item>[RecordInfo][FullRecordLength][ETag?][Expiration?][key SpanByte][value SpanByte]</item>
    ///     </list>
    /// This lets us get to the optional fields for comparisons without loading the full record (GetIOSize should cover the space for optionals).
    /// </remarks>
    public unsafe struct DiskLogRecord : ISourceLogRecord
    {
        /// <summary>The physicalAddress in the log.</summary>
        internal readonly long physicalAddress;

        /// <summary>The deserialized ValueObject if this is a disk record for the Object Store.</summary>
        internal IHeapObject valueObject;

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
        public readonly SpanByte ValueSpan => valueObject is not null ? throw new TsavoriteException("Object LogRecord does not have SpanByte values") : *(SpanByte*)ValueAddress;
        /// <inheritdoc/>
        public readonly IHeapObject ValueObject => valueObject;
        /// <inheritdoc/>
        public readonly ref TValue GetValueRef<TValue>() => ref Unsafe.AsRef<TValue>((void*)ValueAddress);
        /// <inheritdoc/>
        public readonly long ETag => Info.HasETag ? *(long*)GetETagAddress() : 0;
        /// <inheritdoc/>
        public readonly long Expiration => Info.HasExpiration ? *(long*)GetExpirationAddress() : 0;
        /// <inheritdoc/>
        public readonly LogRecord AsLogRecord() => throw new TsavoriteException("DiskLogRecord cannot be converted to AsLogRecord");
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

        const int FullRecordLenSize = sizeof(int);


        // TODO: LOgRec ActualRecordSize ignores params bc it is already encoded into the key/value
        //      Disk/PendingCOntext need to calc on the fly tho
        // Add KeySize to GetRecordFieldInfo bc it is needed for PendingContext to avoid special-case handling 

        readonly long KeyAddress => physicalAddress + RecordInfo.GetLength() + FullRecordLenSize + ETagLen + ExpirationLen;

        internal readonly long ValueAddress => KeyAddress + Key.TotalInlineSize;

        private readonly int ValueLength => IsObjectRecord ? ObjectIdMap.ObjectIdSize : ValueSpan.TotalInlineSize;
        public readonly int OptionalLength => (Info.HasETag ? LogRecord.ETagSize : 0) + (Info.HasExpiration ? LogRecord.ExpirationSize : 0);

        private readonly int ETagLen => Info.HasETag ? LogRecord.ETagSize : 0;
        private readonly int ExpirationLen => Info.HasExpiration ? LogRecord.ExpirationSize : 0;

        private readonly long GetETagAddress() => physicalAddress + RecordInfo.GetLength() + FullRecordLenSize;
        private readonly long GetExpirationAddress() => GetETagAddress() + ETagLen;

        /// <summary>The size to IO from disk when reading a record. Keys and Values are SpanByte on disk and we reuse the max inline key size
        /// for both key and value for this estimate. They prefaced by the full record length and optionals (ETag, Expiration) which we include in the estimate.</summary>
        public static int GetIOSize(int sectorSize) => RoundUp(RecordInfo.GetLength() + FullRecordLenSize + sizeof(long) * 2 + sizeof(int) * 2 + (1 << LogSettings.kMaxInlineKeySizeBits) * 2, sectorSize);

        internal static SpanByte GetContextRecordKey<TValue>(ref AsyncIOContext<TValue> ctx) => new DiskLogRecord((long)ctx.record.GetValidPointer()).Key;

        internal static SpanByte GetContextRecordValue<TValue>(ref AsyncIOContext<TValue> ctx) => new DiskLogRecord((long)ctx.record.GetValidPointer()).ValueSpan;

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            var valueString = IsObjectRecord ? ValueObject.ToString() : ValueSpan.ToString();

            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasETag {bstr(Info.HasETag)}:{ETag} | HasExpiration {bstr(Info.HasExpiration)}:{Expiration}";
        }
    }
}