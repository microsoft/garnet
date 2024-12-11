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
    ///     <item>[RecordInfo][FullRecordLength][DBId?][ETag?][Expiration?][key SpanByte][value SpanByte]</item>
    ///     </list>
    /// This lets us get to the optional fields for comparisons without loading the full record (GetAverageRecordSize should cover the space for optionals).
    /// </remarks>
    public unsafe struct DiskRecord(long physicalAddress)
    {
        /// <summary>The physicalAddress in the log.</summary>
        internal readonly long physicalAddress = physicalAddress;

        /// <summary>A ref to the record header</summary>
        public readonly ref RecordInfo InfoRef => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);

        /// <summary>Fast access returning a copy of the record header</summary>
        public readonly RecordInfo Info => *(RecordInfo*)physicalAddress;

        const int FullRecordLenSize = sizeof(int);

        /// <summary>The used length of the record</summary>
        public readonly int FullRecordLen => *(int*)physicalAddress + RecordInfo.GetLength();
        /// <summary>The used length of the record rounded up to record-alignment boundary</summary>
        public readonly int AlignedFullRecordLen => RoundUp(FullRecordLen, Constants.kRecordAlignment);

        readonly long KeyAddress => physicalAddress + RecordInfo.GetLength() + FullRecordLenSize + DBIdLen + ETagLen + ExpirationLen;

        /// <summary>The key; unlike in-memory, this is always an inline serialized spanbyte.</summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        public readonly SpanByte Key => *(SpanByte*)(physicalAddress + RecordInfo.GetLength() + FullRecordLenSize + DBIdLen + ETagLen + ExpirationLen);

        internal readonly long ValueAddress => KeyAddress + Key.TotalInlineSize;

        /// <summary>The value; unlike in-memory, this is always an inline stream of bytes, but not a SpanByte; to avoid redundantly storing length,
        /// we calculate the SpanByte length from FullRecordLen, because Value is the last field in the record.</summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        public readonly SpanByte Value => new(FullRecordLen - (int)(ValueAddress - physicalAddress), (IntPtr)ValueAddress);

        private readonly int DBIdLen => Info.HasDBId ? 1 : 0;
        private readonly int ETagLen => Info.HasETag ? Constants.ETagSize : 0;
        private readonly int ExpirationLen => Info.HasExpiration ? Constants.ExpirationSize : 0;

        private readonly long GetDBIdAddress() => physicalAddress + RecordInfo.GetLength() + FullRecordLenSize;
        private readonly long GetETagAddress() => GetDBIdAddress() + DBIdLen + ETagLen + ExpirationLen;
        private readonly long GetExpirationAddress() => GetETagAddress() + ETagLen + ExpirationLen;

        public readonly int GetDBId() => Info.HasDBId ? *(byte*)GetDBIdAddress() : 0;
        public readonly long GetETag() => Info.HasETag ? *(long*)GetETagAddress() : 0;
        public readonly long GetExpiration() => Info.HasExpiration ? *(long*)GetExpirationAddress() : 0;

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";

            return $"ri {Info} | key {Key.ToShortString(20)} | val {Value.ToShortString(20)} | HasDBId {bstr(Info.HasDBId)}:{GetDBId()} | HasETag {bstr(Info.HasETag)}:{GetETag()} | HasExpiration {bstr(Info.HasExpiration)}:{GetExpiration()}";
        }
    }
}