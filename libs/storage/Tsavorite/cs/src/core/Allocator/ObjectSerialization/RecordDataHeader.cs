// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    /// <summary>
    /// The header describing the data layout of the record. The record must be pinned.
    /// The layout is a fixed 8 bytes:
    /// <list type="bullet">
    ///     <item>Byte 0 (IndicatorByte):
    ///         <list type="bullet">
    ///             <item>Bits 0-1: FillerLength encoding. Interpreted only if <see cref="RecordInfo.HasFiller"/> is set. Values 0-2 mean
    ///                 1-3 filler bytes (the indicator alone encodes the length). Value 3 means the actual filler length is stored as an
    ///                 int at the start of the filler region (at <c>recordBase + nonFiller</c>, where <c>nonFiller</c> is the sum of all
    ///                 non-filler components).</item>
    ///             <item>Bits 2-7: Unused (reserved for future use).</item>
    ///         </list>
    ///     </item>
    ///     <item>Byte 1 (Namespace): single-byte namespace value, or an indicator that ExtendedNamespace bytes precede the key data.
    ///         Bit 7 is <see cref="ExtendedNamespaceIndicatorBit"/>; if set, bits 0-6 give the number of ExtendedNamespace bytes.</item>
    ///     <item>Byte 2 (RecordType): caller-defined record type byte.</item>
    ///     <item>Bytes 3-4 (KeyLength): fixed 16-bit little-endian unsigned integer.
    ///         Max valid value <see cref="LogSettings.MaxInlineKeySizeLimit"/> (0xFFFE); 0xFFFF reserved as a future sentinel slot
    ///         (no sentinel logic in this iteration). For overflow keys, this stores <see cref="ObjectIdMap.ObjectIdSize"/>; the
    ///         object holds the true key length.</item>
    ///     <item>Bytes 5-7 (ValueLength): fixed 24-bit little-endian unsigned integer.
    ///         Max valid value <see cref="LogSettings.MaxInlineValueSizeLimit"/> (0xFFFFFE); 0xFFFFFF reserved as a future sentinel
    ///         slot (no sentinel logic in this iteration). For overflow/object values, this stores <see cref="ObjectIdMap.ObjectIdSize"/>;
    ///         the object holds the true value length.</item>
    /// </list>
    /// The full record layout (from the start of the <see cref="RecordInfo"/>) is:
    /// <list type="bullet">
    ///     <item><see cref="RecordInfo"/> (8 bytes)</item>
    ///     <item>RecordDataHeader (8 bytes)</item>
    ///     <item>Namespace extra data, if any (length <see cref="ExtendedNamespaceLength"/>)</item>
    ///     <item>Key data bytes (length <see cref="GetKeyLength"/>)</item>
    ///     <item>Value data bytes (length <see cref="GetValueLength"/>); for overflow/object values this is <see cref="ObjectIdMap.ObjectIdSize"/></item>
    ///     <item>Optional fields, in this order if present: ETag, Expiration, ObjectLogPosition</item>
    ///     <item>Filler, if any (size encoded as described above). For the explicit-int encoding the int sits at the start of the filler region.</item>
    /// </list>
    /// Total record length is not stored in this header; it is derived from the components.
    ///</summary>
    public unsafe struct RecordDataHeader : IKey
    {
        /// <summary>The total size of the RecordDataHeader, in bytes.</summary>
        public const int Size = 8;

        /// <summary>Offset of the IndicatorByte (FillerLength + unused bits).</summary>
        internal const int IndicatorByteOffset = 0;

        /// <summary>Offset of the Namespace byte.</summary>
        internal const int NamespaceOffsetInHeader = 1;

        /// <summary>Offset of the RecordType byte.</summary>
        internal const int RecordTypeOffsetInHeader = 2;

        /// <summary>Offset of the 16-bit KeyLength field (bytes 3-4).</summary>
        internal const int KeyLengthOffset = 3;

        /// <summary>Offset of the 24-bit ValueLength field (bytes 5-7).</summary>
        internal const int ValueLengthOffset = 5;

        /// <summary>Mask for the 24-bit ValueLength field (used when reading/writing as a 32-bit word).</summary>
        internal const uint ValueLengthMask = 0x00FFFFFFu;

        // FillerLength encoding occupies bits 0-1 of the IndicatorByte.
        internal const int kFillerLengthIndicatorBits = 2;
        internal const int kFillerLengthIndicatorBitMask = (1 << kFillerLengthIndicatorBits) - 1;   // = 0x3
        internal const int kFillerLengthIndicatorShift = 0;

        /// <summary>If the <see cref="ExtendedNamespaceIndicatorBit"/> is not set, then the <see cref="NamespaceIndicatorMask"/> bits
        /// contain the full namespace as a single byte; otherwise those bits are the length of the extended namespace data preceding the key data.</summary>
        internal const byte ExtendedNamespaceIndicatorBit = 1 << 7;
        /// <summary>If the <see cref="ExtendedNamespaceIndicatorBit"/> is not set, then the <see cref="NamespaceIndicatorMask"/> bits
        /// contain the full namespace as a single byte; otherwise those bits are the length of the extended namespace data preceding the key data.</summary>
        internal const byte NamespaceIndicatorMask = ExtendedNamespaceIndicatorBit - 1;

        /// <summary>Pointer to the first byte of the header (the IndicatorByte).</summary>
        internal byte* HeaderPtr;

        /// <inheritdoc/>
        public override readonly string ToString() => ToString("na", "na");

        internal readonly string ToString(string keyString, string valueString)
        {
            if (HeaderPtr == null)
                return "<empty>";
            var recordInfo = *RecordInfoPtr;
            var keyLength = GetKeyLength();
            var valueLength = GetValueLength();
            var keyAddress = (long)HeaderPtr + Size + ExtendedNamespaceLength;
            var valueAddress = keyAddress + keyLength;
            var fillerLength = GetFillerLength(recordInfo, out var recordLength);
            var fillerLenStr = recordInfo.HasFiller ? fillerLength.ToString() : "na";

            return $"rec l:{recordLength}"
                 + $" | key o:{keyAddress - (long)RecordInfoPtr}/l:{keyLength} {keyString}"
                 + $" | val o:{valueAddress - (long)RecordInfoPtr}/l:{valueLength}, {valueString}"
                 + $" | filLen {fillerLenStr} Namespace b:{NamespaceByte}/x:{ExtendedNamespaceLength}, RecordType {RecordType}";
        }

        internal RecordDataHeader(byte* indicatorPtr) => HeaderPtr = indicatorPtr;

        private readonly RecordInfo* RecordInfoPtr => (RecordInfo*)(HeaderPtr - RecordInfo.Size);

        internal readonly int ExtendedNamespaceLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var nameSpace = *(HeaderPtr + NamespaceOffsetInHeader);
                return (nameSpace & ExtendedNamespaceIndicatorBit) == 0 ? 0 : nameSpace & NamespaceIndicatorMask;
            }
        }

        /// <summary>Get or set the Namespace byte. Throws an exception if out of range or if there is a conflicting specification for extended-length nameSpace.</summary>
        public readonly byte NamespaceByte
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var nameSpace = *(HeaderPtr + NamespaceOffsetInHeader);
                if ((nameSpace & ExtendedNamespaceIndicatorBit) != 0)
                    ThrowTsavoriteException("Cannot get NamespaceByte when ExtendedNamespaceFlag is set");
                return nameSpace;
            }
            set
            {
                if (value > sbyte.MaxValue)
                    ThrowTsavoriteException($"NamespaceByte value {value} exceeds max allowable {sbyte.MaxValue}");
                *(HeaderPtr + NamespaceOffsetInHeader) = value;
            }
        }

        /// <summary>Get or set the RecordType byte.</summary>
        public readonly byte RecordType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => *(HeaderPtr + RecordTypeOffsetInHeader);
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => *(HeaderPtr + RecordTypeOffsetInHeader) = value;
        }

        #region IKey

        /// <inheritdoc/>
        public readonly bool IsPinned => true;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> KeyBytes
        {
            get
            {
                var ptr = HeaderPtr - RecordInfo.Size;
                return new ReadOnlySpan<byte>(ptr + GetOffsetToKeyStart(), GetKeyLength());
            }
        }

        /// <inheritdoc/>
        public readonly bool HasNamespace
        {
            get
            {
                var nameSpace = *(HeaderPtr + NamespaceOffsetInHeader);
                return nameSpace != 0;
            }
        }

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> NamespaceBytes
        {
            get
            {
                Debug.Assert(HasNamespace, "Should call if !HasNamespace");

                var nameSpacePtr = (HeaderPtr + NamespaceOffsetInHeader);

                if (((*nameSpacePtr) & ExtendedNamespaceIndicatorBit) == 0)
                {
                    // Single byte namespace
                    return new ReadOnlySpan<byte>(nameSpacePtr, 1);
                }
                else
                {
                    ThrowTsavoriteException($"Extended namespaces not yet implemented");
                    return default;
                }
            }
        }

        #endregion

        /// <summary>
        /// Initialize the RecordDataHeader for a newly allocated (or revivified) record.
        /// Writes the indicator byte, namespace, record type, KeyLength, ValueLength, and filler.
        /// </summary>
        /// <returns>The data-header size (constant <see cref="Size"/>).</returns>
        internal readonly int Initialize(ref RecordInfo recordInfo, in RecordSizeInfo sizeInfo, out long keyAddress, out long namespaceAddress, out long valueAddress)
        {
            var keyLength = sizeInfo.InlineKeySize;
            var valueLength = sizeInfo.InlineValueSize;
            var recordLength = sizeInfo.AllocatedInlineRecordSize;

            Debug.Assert(keyLength <= LogSettings.MaxInlineKeySizeLimit, $"keyLength {keyLength} exceeds {LogSettings.MaxInlineKeySizeLimit}");
            Debug.Assert(valueLength <= LogSettings.MaxInlineValueSizeLimit, $"valueLength {valueLength} exceeds {LogSettings.MaxInlineValueSizeLimit}");

            // Zero the IndicatorByte; SetFillerLength below sets the filler bits.
            *HeaderPtr = 0;

            // Namespace byte: single byte value, or indicator + length for extended.
            var extendedNamespaceSize = sizeInfo.FieldInfo.ExtendedNamespaceSize;
            namespaceAddress = (long)HeaderPtr + NamespaceOffsetInHeader;
            *(byte*)namespaceAddress = (byte)(extendedNamespaceSize > 0 ? (ExtendedNamespaceIndicatorBit | (extendedNamespaceSize & NamespaceIndicatorMask)) : 0);

            // RecordType byte.
            *(HeaderPtr + RecordTypeOffsetInHeader) = sizeInfo.FieldInfo.RecordType;

            // KeyLength: 16-bit at offset 3.
            *(ushort*)(HeaderPtr + KeyLengthOffset) = (ushort)keyLength;

            // ValueLength: 24-bit at offset 5. Writing 4 bytes here would overflow into byte 8 (past RDH), so RMW-preserve byte 8.
            var vlPtr = (uint*)(HeaderPtr + ValueLengthOffset);
            *vlPtr = (*vlPtr & ~ValueLengthMask) | ((uint)valueLength & ValueLengthMask);

            // Calculate and store the filler length, if any. Filler includes any space for optionals that won't have been set this early in
            // the initialization process. If sizeInfo indicates the record is not inline, that won't have been reflected in RecordInfo yet
            // and thus not in optionals, but we need to reserve the ObjectLogPosition space and not let it be part of FillerLength.
            var nonFillerAtInit = RecordInfo.Size + Size + extendedNamespaceSize + keyLength + valueLength + sizeInfo.ObjectLogPositionSize;
            SetFillerLengthAtNonFiller(ref recordInfo, nonFiller: nonFillerAtInit, fillerLength: recordLength - nonFillerAtInit);

            keyAddress = (long)RecordInfoPtr + GetOffsetToKeyStart();
            valueAddress = keyAddress + keyLength;

            return Size;
        }

        /// <summary>
        /// Initialize a record retrieved from revivification. Clears optionals and filler indicator, resets namespace/recordType to 0,
        /// and updates <paramref name="sizeInfo"/> to reflect the previously-allocated record length.
        /// </summary>
        internal readonly void InitializeForRevivification(ref RecordInfo recordInfo, ref RecordSizeInfo sizeInfo)
        {
            Debug.Assert(recordInfo.Invalid, "Expected record to be Invalid in InitializeForRevivification");
            Debug.Assert(recordInfo.KeyIsInline, "Expected Key to be inline in InitializeForRevivification");
            Debug.Assert(recordInfo.ValueIsInline, "Expected Value to be inline in InitializeForRevivification");
            Debug.Assert(!recordInfo.HasETag && !recordInfo.HasExpiration, "Expected no optionals in InitializeForRevivification");

            var recordLength = GetAllocatedRecordSize();
            Debug.Assert(sizeInfo.AllocatedInlineRecordSize <= recordLength, "Cannot exceed previous Record size in InitializeForRevivification");

            // No optionals; clear filler indicator (it will be set correctly when the record is re-initialized).
            recordInfo.ClearHasFiller();
            *HeaderPtr = 0;

            *(HeaderPtr + NamespaceOffsetInHeader) = 0;
            *(HeaderPtr + RecordTypeOffsetInHeader) = 0;

            // Preserve previously-allocated record length so LogRecord.InitializeRecord reuses the same span.
            sizeInfo.AllocatedInlineRecordSize = recordLength;
            sizeInfo.SetIsRevivifiedRecord();
        }

        /// <summary>Get the offset of the key, relative to the <see cref="RecordInfo"/> start.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetOffsetToKeyStart() => RecordInfo.Size + Size + ExtendedNamespaceLength;

        /// <summary>Get the inline KeyLength stored in the header.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetKeyLength() => *(ushort*)(HeaderPtr + KeyLengthOffset);

        /// <summary>Get the inline ValueLength stored in the header (24-bit, masked to <see cref="ValueLengthMask"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetValueLength() => (int)(*(uint*)(HeaderPtr + ValueLengthOffset) & ValueLengthMask);

        /// <summary>
        /// Set the inline ValueLength stored in the header (24-bit). Preserves byte 8 (the byte past the end of the header).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly void SetValueLength(int valueLength)
        {
            Debug.Assert(valueLength >= 0 && valueLength <= LogSettings.MaxInlineValueSizeLimit,
                $"valueLength {valueLength} out of range [0, {LogSettings.MaxInlineValueSizeLimit}]");
            var vlPtr = (uint*)(HeaderPtr + ValueLengthOffset);
            *vlPtr = (*vlPtr & ~ValueLengthMask) | ((uint)valueLength & ValueLengthMask);
        }

        /// <summary>Compute the non-filler component sum (everything except filler bytes), from the current header state. The caller must ensure
        /// the header fields and <paramref name="recordInfo"/> are in their post-update state (e.g., HasETag set if ETag was just added).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetNonFillerSize(RecordInfo recordInfo)
            => RecordInfo.Size + Size + ExtendedNamespaceLength + GetKeyLength() + GetValueLength() + recordInfo.GetOptionalSize();

        /// <summary>Get the filler length given a precomputed <paramref name="nonFiller"/> total. The caller is responsible for
        /// ensuring <see cref="RecordInfo.HasFiller"/> is set when calling this; otherwise the result is undefined.</summary>
        /// <remarks>For the explicit-int encoding the int sits at <c>recordBase + nonFiller</c> (the start of the filler region).</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly int GetExplicitOrInlineFiller(int nonFiller)
        {
            var fillerLen = (*HeaderPtr >> kFillerLengthIndicatorShift) & kFillerLengthIndicatorBitMask;
            return fillerLen < 3 ? fillerLen + 1 : *(int*)((long)RecordInfoPtr + nonFiller);
        }

        /// <summary>Get the current filler length encoded in the header. Returns 0 if no filler is set.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetFillerLength(RecordInfo recordInfo)
            => recordInfo.HasFiller ? GetExplicitOrInlineFiller(GetNonFillerSize(recordInfo)) : 0;

        /// <summary>Get the filler length and the derived total record length in one pass.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetFillerLength(RecordInfo recordInfo, out int recordLength)
        {
            var nonFiller = GetNonFillerSize(recordInfo);
            if (!recordInfo.HasFiller)
            {
                recordLength = nonFiller;
                return 0;
            }
            var filler = GetExplicitOrInlineFiller(nonFiller);
            recordLength = nonFiller + filler;
            return filler;
        }

        /// <summary>
        /// Set the filler length. Computes the non-filler component sum from the current header state; the caller must ensure
        /// the header (KeyLength, ValueLength, optionals) reflects the post-update state. For the explicit-int encoding the int
        /// is written at <c>recordBase + nonFiller</c> (the start of the filler region).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly void SetFillerLength(ref RecordInfo recordInfo, int fillerLength)
            => SetFillerLengthAtNonFiller(ref recordInfo, GetNonFillerSize(recordInfo), fillerLength);

        /// <summary>
        /// Set the filler length, with a caller-provided non-filler component sum (an optimization when the caller has already computed it).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly void SetFillerLengthAtNonFiller(ref RecordInfo recordInfo, int nonFiller, int fillerLength)
        {
            Debug.Assert(fillerLength >= 0, $"Filler length {fillerLength} must be nonnegative");

            if (fillerLength > 0)
            {
                recordInfo.SetHasFiller();
                if (fillerLength <= 3)
                {
                    // Store only the indicator bits; we don't have space for the full int. Mask out previous bits in the filler space first.
                    *HeaderPtr = (byte)((*HeaderPtr & ~(kFillerLengthIndicatorBitMask << kFillerLengthIndicatorShift)) | ((fillerLength - 1) << kFillerLengthIndicatorShift));
                }
                else
                {
                    // Store the indicator bits as 3, and the filler length in the int at recordBase + nonFiller (start of filler region).
                    // 3 is "all bits set" in the filler space, so no need to mask out previous bits there.
                    *HeaderPtr |= 3 << kFillerLengthIndicatorShift;
                    *(int*)((long)RecordInfoPtr + nonFiller) = fillerLength;
                }
            }
            else
            {
                recordInfo.ClearHasFiller();
                *HeaderPtr = (byte)(*HeaderPtr & ~(kFillerLengthIndicatorBitMask << kFillerLengthIndicatorShift));
            }
        }

        /// <summary>
        /// Compute a CONSERVATIVE estimate of the record's allocated size, derived from the header fields without reading the filler int.
        /// For non-revivified records where filler &lt; <see cref="Constants.kRecordAlignment"/>, this equals the actual allocated size.
        /// For records with larger filler (e.g., reserved-space records during initialization or revivified records), this is a lower bound;
        /// callers that need the exact size on such records must use <see cref="GetAllocatedRecordSize"/> on a fully-loaded buffer.
        /// </summary>
        /// <remarks>This is intended for partial-load scenarios (e.g., disk reads where the filler int might not yet be in the buffer).</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetRecordLengthEstimate(RecordInfo recordInfo)
            => RoundUp(GetNonFillerSize(recordInfo), Constants.kRecordAlignment);

        /// <summary>Get the derived total record length.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetRecordLength()
        {
            _ = GetFillerLength(*RecordInfoPtr, out var recordLength);
            return recordLength;
        }

        /// <summary>Returns the inline key length and value length stored in the header.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (int keyLength, int valueLength) GetKVLengths(RecordInfo recordInfo)
            => (GetKeyLength(), GetValueLength());

        /// <summary>
        /// Get the inline key/value lengths along with the optional sizes, filler length, derived total record length,
        /// and value address, in a single pass.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (int keyLength, int valueLength) GetKVLengths(RecordInfo recordInfo, out int recordLength, out int eTagLen, out int expirationLen, out int objectLogPositionLen, out int fillerLen, out long valueAddress)
        {
            eTagLen = recordInfo.HasETag ? LogRecord.ETagSize : 0;
            expirationLen = recordInfo.HasExpiration ? LogRecord.ExpirationSize : 0;
            objectLogPositionLen = recordInfo.RecordIsInline ? 0 : LogRecord.ObjectLogPositionSize;

            var keyLength = GetKeyLength();
            var valueLength = GetValueLength();
            var extendedNsLen = ExtendedNamespaceLength;

            var nonFiller = RecordInfo.Size + Size + extendedNsLen + keyLength + valueLength + eTagLen + expirationLen + objectLogPositionLen;
            fillerLen = recordInfo.HasFiller ? GetExplicitOrInlineFiller(nonFiller) : 0;
            recordLength = nonFiller + fillerLen;

            valueAddress = (long)RecordInfoPtr + RecordInfo.Size + Size + extendedNsLen + keyLength;
            return (keyLength, valueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (int keyLength, long keyAddress) GetKeyFieldInfo()
        {
            var keyLength = GetKeyLength();
            var keyAddress = (long)(HeaderPtr + Size + ExtendedNamespaceLength);
            return (keyLength, keyAddress);
        }

        /// <summary>
        /// Gets the value field information for an in-memory or on-disk with object size changes to value length restored (objects have been read).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (long valueLength, long valueAddress) GetValueFieldInfo(RecordInfo recordInfo)
        {
            var (keyLength, keyAddress) = GetKeyFieldInfo();
            return (GetValueLength(), keyAddress + keyLength);
        }

        /// <summary>
        /// Gets the value field information returning <paramref name="keyLength"/> as well.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (long valueLength, long valueAddress) GetValueFieldInfo(RecordInfo recordInfo, out int keyLength)
        {
            long keyAddress;
            (keyLength, keyAddress) = GetKeyFieldInfo();
            return (GetValueLength(), keyAddress + keyLength);
        }

        /// <summary>Get the full allocated record size (the total record extent, including any filler).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetAllocatedRecordSize() => GetRecordLength();

        /// <summary>Get the actual (used) record size: total record size minus any filler.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetActualRecordSize(RecordInfo recordInfo)
        {
            var fillerLen = GetFillerLength(recordInfo, out var recordLength);
            return recordLength - fillerLen;
        }
    }
}
