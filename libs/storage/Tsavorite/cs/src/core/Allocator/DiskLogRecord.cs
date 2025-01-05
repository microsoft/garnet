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
    public unsafe struct DiskLogRecord : IReadOnlyLogRecord
    {
        /// <summary>The physicalAddress in the log.</summary>
        internal readonly long physicalAddress;

        /// <summary>The deserialized ValueObject if this is a disk record for the Object Store.</summary>
        internal IHeapObject valueObject;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DiskLogRecord(long physicalAddress) => this.physicalAddress = physicalAddress;

        #region IReadOnlyRecord
        /// <inheritdoc/>
        public readonly ref RecordInfo InfoRef => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);
        /// <inheritdoc/>
        public readonly RecordInfo Info => *(RecordInfo*)physicalAddress;
        /// <inheritdoc/>
        public readonly SpanByte Key => *(SpanByte*)(physicalAddress + RecordInfo.GetLength() + FullRecordLenSize + ETagLen + ExpirationLen);
        /// <inheritdoc/>
        public readonly SpanByte ValueSpan => throw new TsavoriteException("ObjectLogRecord does not have SpanByte values");
        /// <inheritdoc/>
        public readonly IHeapObject ValueObject => ValueObject;
        /// <inheritdoc/>
        public readonly long ETag => Info.HasETag ? *(long*)GetETagAddress() : 0;
        /// <inheritdoc/>
        public readonly long Expiration => Info.HasExpiration ? *(long*)GetExpirationAddress() : 0;
        #endregion //IReadOnlyRecord

        const int FullRecordLenSize = sizeof(int);

        /// <summary>The used length of the record</summary>
        public readonly int FullRecordLen => *(int*)physicalAddress + RecordInfo.GetLength();
        /// <summary>The used length of the record rounded up to record-alignment boundary</summary>
        public readonly int AlignedFullRecordLen => RoundUp(FullRecordLen, Constants.kRecordAlignment);

        readonly long KeyAddress => physicalAddress + RecordInfo.GetLength() + FullRecordLenSize + ETagLen + ExpirationLen;

        internal readonly long ValueAddress => KeyAddress + Key.TotalInlineSize;

        /// <summary>The value; unlike in-memory, this is always an inline stream of bytes, but not a SpanByte; to avoid redundantly storing length,
        /// we calculate the SpanByte length from FullRecordLen, because Value is the last field in the record.</summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        public readonly SpanByte Value => new(FullRecordLen - (int)(ValueAddress - physicalAddress), (IntPtr)ValueAddress);

        private readonly int ETagLen => Info.HasETag ? LogRecord.ETagSize : 0;
        private readonly int ExpirationLen => Info.HasExpiration ? LogRecord.ExpirationSize : 0;

        private readonly long GetETagAddress() => physicalAddress + RecordInfo.GetLength() + FullRecordLenSize;
        private readonly long GetExpirationAddress() => GetETagAddress() + ETagLen;

        /// <summary>The size to IO from disk when reading a record. Keys and Values are SpanByte on disk and we reuse the max inline key size
        /// for both key and value for this estimate. They prefaced by the full record length and optionals (ETag, Expiration) which we include in the estimate.</summary>
        public static int GetIOSize(int sectorSize) => RoundUp(RecordInfo.GetLength() + FullRecordLenSize + sizeof(long) * 2 + sizeof(int) * 2 + (1 << LogSettings.kMaxInlineKeySizeBits) * 2, sectorSize);

        internal static SpanByte GetContextRecordKey(ref AsyncIOContext<SpanByte, SpanByte> ctx) => new DiskLogRecord((long)ctx.record.GetValidPointer()).Key;

        internal static SpanByte GetContextRecordValue(ref AsyncIOContext<SpanByte, SpanByte> ctx) => new DiskLogRecord((long)ctx.record.GetValidPointer()).Value;

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";

            return $"ri {Info} | key {Key.ToShortString(20)} | val {Value.ToShortString(20)} | HasETag {bstr(Info.HasETag)}:{ETag} | HasExpiration {bstr(Info.HasExpiration)}:{Expiration}";
        }
    }
}