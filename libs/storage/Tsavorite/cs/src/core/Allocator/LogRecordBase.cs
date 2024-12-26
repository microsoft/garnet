// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>The in-memory record on the log: header, key, value, and optional fields</summary>
    /// <remarks>The space is laid out as:
    ///     <list>
    ///     <item>[RecordInfo][SpanByte key][Value Id or SpanByte][DBId?][ETag?][Expiration?][FillerLen]</item>
    ///     </list>
    /// This lets us get to the key without intermediate computations to account for the optional fields.
    /// Some methods have both member and static versions for ease of access and possibly performance gains.
    /// </remarks>
    public unsafe struct LogRecordBase
    {
        /// <summary>Number of bytes required to store a DBId</summary>
        public const int DBIdSize = sizeof(byte);
        /// <summary>Number of bytes required to store an ETag</summary>
        public const int ETagSize = sizeof(long);
        /// <summary>Number of bytes required to store an Expiration</summary>
        public const int ExpirationSize = sizeof(long);
        /// <summary>Number of bytes required to store the FillerLen</summary>
        internal const int FillerLenSize = sizeof(int);

        /// <summary>The physicalAddress in the log.</summary>
        internal readonly long physicalAddress;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LogRecordBase(long physicalAddress) => this.physicalAddress = physicalAddress;

        /// <summary>A ref to the record header</summary>
        public readonly ref RecordInfo InfoRef => ref GetInfoRef(physicalAddress);
        /// <summary>A ref to the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref RecordInfo GetInfoRef(long physicalAddress) => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);

        /// <summary>Fast access returning a copy of the record header</summary>
        public readonly RecordInfo Info => GetInfo(physicalAddress);
        /// <summary>Fast access returning a copy of the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RecordInfo GetInfo(long physicalAddress) => *(RecordInfo*)physicalAddress;

        /// <summary>The key:
        ///     <list type="bullet">
        ///     <item>If serialized, then the key is inline in this record (i.e. is below the overflow size).</item>
        ///     <item>If not serialized, then it is a pointer to the key in OverflowKeySpace.</item>
        ///     </list>
        /// </summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        public readonly SpanByte Key => GetKey(physicalAddress);
        /// <summary>The key:
        ///     <list type="bullet">
        ///     <item>If serialized, then the key is inline in this record (i.e. is below the overflow size).</item>
        ///     <item>If not serialized, then it is a pointer to the key in OverflowKeySpace.</item>
        ///     </list>
        /// </summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SpanByte GetKey(long physicalAddress) => *(SpanByte*)(physicalAddress + RecordInfo.GetLength());

        /// <summary>The address of the value</summary>
        internal readonly int ValueAddress => GetValueAddress(physicalAddress);
        /// <summary>The address of the value</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetValueAddress(long physicalAddress) => RecordInfo.GetLength() + GetKey(physicalAddress).TotalInlineSize;

        private readonly int DBIdLen => Info.HasDBId ? DBIdSize : 0;
        private readonly int ETagLen => Info.HasETag ? ETagSize : 0;
        private readonly int ExpirationLen => Info.HasExpiration ? ExpirationSize : 0;

        /// <summary>The total size of the main-log (inline) portion of the record, not including extra value length.</summary>
        public readonly int GetRecordSize(int valueLen) => RecordInfo.GetLength() + Key.TotalInlineSize + valueLen + DBIdLen + ETagLen + ExpirationLen;
        public readonly (int actualSize, int allocatedSize) GetRecordSizes(int valueLen)
        {
            var actualSize = RecordInfo.GetLength() + Key.TotalInlineSize + valueLen + DBIdLen + ETagLen + ExpirationLen;
            return (actualSize, actualSize + GetFillerLen(valueLen));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long GetOptionalStartAddress(int valueLen) => physicalAddress + RecordInfo.GetLength() + Key.TotalInlineSize + valueLen;

        public readonly int OptionalLength => DBIdLen + ETagLen + ExpirationLen;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetDBIdAddress(int valueLen) => GetOptionalStartAddress(valueLen);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetETagAddress(int valueLen) => GetDBIdAddress(valueLen) + DBIdLen;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetExpirationAddress(int valueLen) => GetETagAddress(valueLen) + ETagLen;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetFillerLenAddress(int valueLen) => GetExpirationAddress(valueLen) + ExpirationLen;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly int GetDBId(int valueLen) => Info.HasDBId ? *(byte*)GetDBIdAddress(valueLen) : 0;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetETag(int valueLen) => Info.HasETag ? *(long*)GetETagAddress(valueLen) : 0;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetExpiration(int valueLen) => Info.HasExpiration ? *(long*)GetExpirationAddress(valueLen) : 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetFillerLen(int valueLen)
        {
            if (Info.HasFiller)
                return *(int*)GetFillerLenAddress(valueLen);

            // Filler includes extra space opened up by removing ETag or Expiration. If there is no Filler, we may still have a couple bytes (< Constants.FillerLenSize) due to RoundUp of record size.
            var recSize = GetRecordSize(valueLen);
            return RoundUp(recSize, Constants.kRecordAlignment) - recSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly bool TrySetETag(int valueLen, long eTag)
        {
            if (Info.HasETag)
            {
                *(long*)GetETagAddress(valueLen) = eTag;
                return true;
            }

            // We're adding an ETag where there wasn't one before.
            const int growth = ETagSize;
            var recordLen = GetRecordSize(valueLen);
            var maxLen = recordLen + GetFillerLen(valueLen);
            var availableSpace = maxLen - recordLen;
            if (availableSpace < growth)
                return false;

            // Start at FillerLen address
            var address = physicalAddress + ValueAddress + valueLen + OptionalLength;
            var extraLen = availableSpace - growth;

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            //      - We must do this here in case there is not enough room for filler after the growth.
            if (Info.HasFiller)
                *(int*)address = 0;
            //  - Preserve Expiration if present; set ETag; re-enter Expiration if present
            var expiration = 0L;
            if (Info.HasExpiration)
            {
                address -= ExpirationSize;
                expiration = *(long*)address;
            }
            *(long*)address = eTag;
            InfoRef.SetHasETag();
            if (Info.HasExpiration)
            {
                address += ETagSize;
                *(long*)address = expiration;
                address += ExpirationLen;
            }
            //  - Set the new (reduced) ExtraValueLength if there is still space for it.
            if (extraLen >= FillerLenSize)
                *(int*)address = extraLen;
            else
                InfoRef.ClearHasFiller();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly void RemoveETag(int valueLen)
        {
            if (!Info.HasETag)
                return;

            const int growth = -ETagSize;
            var recordLen = GetRecordSize(valueLen);
            var maxLen = recordLen + GetFillerLen(valueLen);
            var availableSpace = maxLen - recordLen;

            // Start at FillerLen address
            var address = physicalAddress + ValueAddress + valueLen + OptionalLength;
            var extraLen = availableSpace + growth;

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            if (Info.HasFiller)
                *(int*)address = 0;
            //  - Move Expiration, if present, up to cover ETag; then clear the ETag bit
            var expiration = 0L;
            if (Info.HasExpiration)
            {
                address -= ExpirationSize;
                expiration = *(long*)address;
                *(long*)address = 0L;  // To ensure zero-init
            }
            address -= ETagSize;
            if (Info.HasExpiration)
            {
                *(long*)address = expiration;
                address += ExpirationSize;
            }
            InfoRef.ClearHasETag();
            //  - Set the new (increased) ExtraValueLength if there is space for it.
            if (extraLen >= FillerLenSize)
                *(int*)address = extraLen;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly bool TrySetExpiration(int valueLen, long expiration)
        {
            if (Info.HasExpiration)
            {
                *(long*)GetExpirationAddress(valueLen) = expiration;
                return true;
            }

            // We're adding an Expiration where there wasn't one before.
            const int growth = ExpirationSize;
            var recordLen = GetRecordSize(valueLen);
            var maxLen = recordLen + GetFillerLen(valueLen);
            var availableSpace = maxLen - recordLen;
            if (availableSpace < growth)
                return false;

            // Start at FillerLen address
            var address = physicalAddress + ValueAddress + valueLen + OptionalLength;
            var extraLen = availableSpace - growth;

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            //      - We must do this here in case there is not enough room for filler after the growth.
            if (Info.HasFiller)
                *(int*)address = 0;
            //  - Set Expiration
            *(long*)address = expiration;
            address += ExpirationLen;
            //  - Set the new (reduced) ExtraValueLength if there is still space for it.
            if (extraLen >= FillerLenSize)
                *(int*)address = extraLen;
            else
                InfoRef.ClearHasFiller();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly void RemoveExpiration(int valueLen)
        {
            if (!Info.HasExpiration)
                return;

            const int growth = -ETagSize;
            var recordLen = GetRecordSize(valueLen);
            var maxLen = recordLen + GetFillerLen(valueLen);
            var availableSpace = maxLen - recordLen;

            // Start at FillerLen address
            var address = physicalAddress + ValueAddress + valueLen + OptionalLength;
            var extraLen = availableSpace + growth;

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            if (Info.HasFiller)
                *(int*)address = 0;
            //  - Remove Expiration and clear the Expiration bit
            address -= ExpirationSize;
            *(long*)address = 0;
            InfoRef.ClearHasExpiration();
            //  - Set the new (increased) ExtraValueLength if there is space for it.
            if (extraLen >= FillerLenSize)
                *(int*)address = extraLen;
        }

        public readonly string ToString(int valueLen, string valueString)
        {
            static string bstr(bool value) => value ? "T" : "F";

            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasDBId {bstr(Info.HasDBId)}:{GetDBId(valueLen)} | HasETag {bstr(Info.HasETag)}:{GetETag(valueLen)} | HasExpiration {bstr(Info.HasExpiration)}:{GetExpiration(valueLen)}";
        }
    }
}