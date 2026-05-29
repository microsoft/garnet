// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    /// <summary>
    /// Fixed 8-byte header describing the data layout of the record. Atomic assignment is guaranteed on 64-bit systems because
    /// the entire header fits in a single aligned <see cref="ulong"/>.
    /// <para>Layout (byte 0 is lowest address, little-endian):</para>
    /// <list type="bullet">
    ///     <item>Byte 0: IndicatorByte — bit flags: [Unused5:7][Unused4:6][HasFiller:5][HasETag:4][HasExpiration:3][ValueIsObject:2][ValueIsInline:1][KeyIsInline:0]</item>
    ///     <item>Bits 8–11: Unused0..Unused3 (4 unused bits between IndicatorByte and KeyLength). Together with Unused4..Unused5 at bits 6–7, all 6 unused bits form a contiguous group at bits 6–11.</item>
    ///     <item>Bits 12–23: KeyLength (12 bits). The <see cref="KeyLength"/> property returns this raw
    ///         value for inline keys; for overflow keys it returns <see cref="ObjectIdMap.ObjectIdSize"/>. The OverflowByteArray
    ///         already carries the length, so mirroring it in the header would be extra work with no consumer.</item>
    ///     <item>Bits 24–47: ValueLength (24 bits, byte-aligned). The <see cref="ValueLength"/> property returns this raw value for inline
    ///         values; for overflow/object values it returns <see cref="ObjectIdMap.ObjectIdSize"/>. The OverflowByteArray /
    ///         IHeapObject already carries the length, so mirroring it in the header would be extra work with no consumer.</item>
    ///     <item>Bits 48–55: RecordType byte; interpreted by caller.</item>
    ///     <item>Bits 56–63: Namespace byte (with encoding indicating if there are many extra namespace bytes; if so, they precede the Key data bytes).</item>
    /// </list>
    /// <para>Disk-write paths (<see cref="LogRecord.SetObjectLogRecordStartPositionAndLength"/>) temporarily write the actual length (or the
    /// sentinel — <see cref="LogSettings.KeyLengthSentinel"/> = 0xFFF / <see cref="LogSettings.ValueLengthSentinel"/> = 0xFFFFFF) into the
    /// KeyLength/ValueLength field before flushing. After read-back (<see cref="LogRecord.OnObjectReadComplete"/>) we reset those fields
    /// to ObjectIdSize so the in-memory invariant holds.</para>
    /// <para>RecordLength is no longer stored; it is derived: <c>AlignedSum = RoundUp(RecordInfo.Size + Size + ExtendedNamespaceLength + KeyLength + ValueLength + OptionalSize, kRecordAlignment)</c>.
    /// If <see cref="HasFiller"/>, an explicit filler int is stored at <c>record_base + AlignedSum</c>.</para>
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct RecordDataHeader : IKey
    {
#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter

        // ── IndicatorByte bit layout (byte 0) ──────────────────────────────────────
        const int kKeyIsInlineBit = 0;
        const int kValueIsInlineBit = 1;
        const int kValueIsObjectBit = 2;
        const int kHasExpirationBit = 3;
        const int kHasETagBit = 4;
        const int kHasFillerBit = 5;
        const int kUnused4Bit = 6;
        const int kUnused5Bit = 7;

        // Unused0..Unused3 sit just above the IndicatorByte, so all 6 unused bits (Unused0..Unused5) are contiguous at bits 6–11.
        const int kUnused0Bit = 8;
        const int kUnused1Bit = 9;
        const int kUnused2Bit = 10;
        const int kUnused3Bit = 11;

        const ulong kHasFillerMask = 1UL << kHasFillerBit;
        const ulong kHasETagMask = 1UL << kHasETagBit;
        const ulong kHasExpirationMask = 1UL << kHasExpirationBit;
        const ulong kValueIsObjectMask = 1UL << kValueIsObjectBit;
        const ulong kValueIsInlineMask = 1UL << kValueIsInlineBit;
        const ulong kKeyIsInlineMask = 1UL << kKeyIsInlineBit;

        const ulong kUnused0Mask = 1UL << kUnused0Bit;
        const ulong kUnused1Mask = 1UL << kUnused1Bit;
        const ulong kUnused2Mask = 1UL << kUnused2Bit;
        const ulong kUnused3Mask = 1UL << kUnused3Bit;
        const ulong kUnused4Mask = 1UL << kUnused4Bit;
        const ulong kUnused5Mask = 1UL << kUnused5Bit;

        // ── Field positions within the 8-byte word ─────────────────────────────────
        // IndicatorByte:  bits  0–7   (byte 0; bits 6–7 are Unused4/Unused5)
        // Unused0..3:     bits  8–11  (just above IndicatorByte; contiguous with Unused4/5 above for one 6-bit unused group at bits 6–11)
        // KeyLength:      bits 12–23  (12 bits)
        // ValueLength:    bits 24–47  (24 bits, byte-aligned at byte 3)
        // RecordType:     bits 48–55  (byte 6)
        // Namespace:      bits 56–63  (byte 7)
        const int kKeyLengthShift = 12;
        const int kValueLengthShift = 24;
        const int kRecordTypeShift = 48;
        const int kNamespaceShift = 56;

        const ulong kKeyLengthMask = 0xFFFUL << kKeyLengthShift;       // 12 bits
        const ulong kValueLengthMask = 0xFFFFFFUL << kValueLengthShift; // 24 bits
        const ulong kRecordTypeMask = 0xFFUL << kRecordTypeShift;
        const ulong kNamespaceMask = 0xFFUL << kNamespaceShift;
        const ulong kIndicatorByteMask = 0xFFUL;

        /// <summary>Mask for extracting a single byte from the word.</summary>
        const ulong ByteMask = 0xFFUL;

        /// <summary>Mask for extracting the 12-bit KeyLength value (before shifting).</summary>
        const ulong kKeyLengthValueMask = 0xFFFUL;

        /// <summary>Mask for extracting the 24-bit ValueLength value (before shifting).</summary>
        const ulong kValueLengthValueMask = 0xFFFFFFUL;

#pragma warning restore IDE1006 // Naming Styles

        /// <summary>The fixed size of the RecordDataHeader in bytes.</summary>
        public const int Size = 8;

        /// <summary>The bit position of the extended-namespace indicator (bit 7 of the namespace byte). The full byte may be split as:
        /// <list type="bullet">
        ///     <item>If bit at this position is 0, the lower 7 bits hold the namespace value itself (single-byte namespace).</item>
        ///     <item>If bit at this position is 1, the lower 7 bits hold the length of the extended-namespace data preceding the key.</item>
        /// </list>
        /// Use <c>1 &lt;&lt; ExtendedNamespaceIndicatorBit</c> to obtain the mask, or <see cref="NamespaceIndicatorMask"/> for the value bits.</summary>
        internal const byte ExtendedNamespaceIndicatorBit = 7;
        /// <summary>Mask covering the lower 7 bits of the namespace byte (the value bits, excluding the extended-namespace indicator bit).</summary>
        internal const byte NamespaceIndicatorMask = (1 << ExtendedNamespaceIndicatorBit) - 1;

        /// <summary>Offset of the nameSpace byte in the header (byte 7).</summary>
        internal const byte NamespaceOffsetInHeader = 7;
        /// <summary>Offset of the recordType byte in the header (byte 6).</summary>
        internal const byte RecordTypeOffsetInHeader = 6;

        /// <summary>The 8-byte word backing all fields. All access MUST go through this word to ensure atomic reads/writes.</summary>
        [FieldOffset(0)]
        internal ulong word;

        // ── IndicatorByte bit accessors ────────────────────────────────────────────

        /// <summary>Whether the record has an explicit filler region (int stored at record_base + AlignedSum).</summary>
        public readonly bool HasFiller => (word & kHasFillerMask) != 0;
        /// <summary>Set the HasFiller bit.</summary>
        public void SetHasFiller() => word |= kHasFillerMask;
        /// <summary>Clear the HasFiller bit.</summary>
        public void ClearHasFiller() => word &= ~kHasFillerMask;

        /// <summary>Whether the record has an ETag optional field.</summary>
        public readonly bool HasETag => (word & kHasETagMask) != 0;
        /// <summary>Set the HasETag bit.</summary>
        public void SetHasETag() => word |= kHasETagMask;
        /// <summary>Clear the HasETag bit.</summary>
        public void ClearHasETag() => word &= ~kHasETagMask;

        /// <summary>Whether the record has an Expiration optional field.</summary>
        public readonly bool HasExpiration => (word & kHasExpirationMask) != 0;
        /// <summary>Set the HasExpiration bit.</summary>
        public void SetHasExpiration() => word |= kHasExpirationMask;
        /// <summary>Clear the HasExpiration bit.</summary>
        public void ClearHasExpiration() => word &= ~kHasExpirationMask;

        /// <summary>Whether the value is a serialized object (managed heap reference via ObjectIdMap).</summary>
        public readonly bool ValueIsObject => (word & kValueIsObjectMask) != 0;
        /// <summary>Set the ValueIsObject bit; also clears ValueIsInline.</summary>
        public void SetValueIsObject() => word = (word & ~kValueIsInlineMask) | kValueIsObjectMask;

        /// <summary>Whether the value data is stored inline in the record.</summary>
        public readonly bool ValueIsInline => (word & kValueIsInlineMask) != 0;
        /// <summary>Set the ValueIsInline bit; also clears ValueIsObject.</summary>
        public void SetValueIsInline() => word = (word & ~kValueIsObjectMask) | kValueIsInlineMask;
        /// <summary>Clear the ValueIsInline bit.</summary>
        public void ClearValueIsInline() => word &= ~kValueIsInlineMask;

        /// <summary>Whether the key data is stored inline in the record.</summary>
        public readonly bool KeyIsInline => (word & kKeyIsInlineMask) != 0;
        /// <summary>Set the KeyIsInline bit.</summary>
        public void SetKeyIsInline() => word |= kKeyIsInlineMask;
        /// <summary>Clear the KeyIsInline bit.</summary>
        public void ClearKeyIsInline() => word &= ~kKeyIsInlineMask;
        /// <summary>Whether the key is overflow (not inline).</summary>
        public readonly bool KeyIsOverflow => !KeyIsInline;
        /// <summary>Set the key to overflow (clear KeyIsInline).</summary>
        public void SetKeyIsOverflow() => word &= ~kKeyIsInlineMask;

        /// <summary>Whether the value is overflow (not inline and not object).</summary>
        public readonly bool ValueIsOverflow => !ValueIsInline && !ValueIsObject;
        /// <summary>Set the value to overflow (clear both ValueIsInline and ValueIsObject).</summary>
        public void SetValueIsOverflow() => word &= ~(kValueIsInlineMask | kValueIsObjectMask);

        // Unused0..Unused5 are placeholder bits with no semantic meaning yet; exposed only for diagnostic ToString output.
        internal readonly bool Unused0 => (word & kUnused0Mask) != 0;
        internal readonly bool Unused1 => (word & kUnused1Mask) != 0;
        internal readonly bool Unused2 => (word & kUnused2Mask) != 0;
        internal readonly bool Unused3 => (word & kUnused3Mask) != 0;
        internal readonly bool Unused4 => (word & kUnused4Mask) != 0;
        internal readonly bool Unused5 => (word & kUnused5Mask) != 0;

        /// <summary>Set both key and value to inline.</summary>
        public void SetKeyAndValueInline() => word = (word & ~kValueIsObjectMask) | kKeyIsInlineMask | kValueIsInlineMask;

        /// <summary>Whether the record is fully inline (both key and value).</summary>
        public readonly bool RecordIsInline => (word & (kKeyIsInlineMask | kValueIsInlineMask)) == (kKeyIsInlineMask | kValueIsInlineMask);

        /// <summary>Whether the record has any objects (key overflow, value overflow, or value object).</summary>
        public readonly bool RecordHasObjects => (word & (kKeyIsInlineMask | kValueIsInlineMask)) != (kKeyIsInlineMask | kValueIsInlineMask);

        /// <summary>Whether the record has any optional fields (ETag or Expiration).</summary>
        public readonly bool HasOptionalFields => (word & (kHasETagMask | kHasExpirationMask)) != 0;

        /// <summary>Whether the record has optional fields or requires ObjectLogPosition (i.e., is not fully inline).</summary>
        public readonly bool HasOptionalOrObjectFields => (word & (kKeyIsInlineMask | kValueIsInlineMask | kHasETagMask | kHasExpirationMask)) != (kKeyIsInlineMask | kValueIsInlineMask);

        /// <summary>Get the total size of optional fields (ETag + Expiration + ObjectLogPosition if applicable).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetOptionalSize()
        {
            var size = HasETag ? LogRecord.ETagSize : 0;
            if (HasExpiration)
                size += LogRecord.ExpirationSize;
            if (!RecordIsInline)
                size += LogRecord.ObjectLogPositionSize;
            return size;
        }

        /// <summary>Initialize the DataHeader for a new record: sets KeyIsInline and ValueIsInline; zeroes everything else.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeForNewRecord()
        {
            // Zero everything except the inline bits.
            word = kKeyIsInlineMask | kValueIsInlineMask;
        }

        // ── Field accessors via ulong word bit manipulation ────────────────────────

        /// <summary>The effective KeyLength for record-length calculations.
        /// <para>For inline keys, returns the raw 12-bit value at bits 8–19. For overflow keys, returns <see cref="ObjectIdMap.ObjectIdSize"/>
        /// (the OverflowByteArray already carries the length, so mirroring the raw value in the header would be additional work with no consumer
        /// in the in-memory path).</para>
        /// <para>The setter always writes the raw 12-bit value. The disk-write path uses it to temporarily store the actual length or sentinel
        /// (<see cref="LogSettings.KeyLengthSentinel"/>) for serialization; <see cref="LogRecord.OnObjectReadComplete"/> restores ObjectIdSize on read-back.</para>
        /// <para>For disk-serialization paths that need to READ the raw stored value (not the effective length), use <see cref="GetKeyLengthRaw"/>.</para></summary>
        internal int KeyLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => KeyIsInline ? (int)((word >> kKeyLengthShift) & kKeyLengthValueMask) : ObjectIdMap.ObjectIdSize;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Debug.Assert((uint)value <= kKeyLengthValueMask, $"KeyLength {value} exceeds 12-bit max");
                word = (word & ~kKeyLengthMask) | (((ulong)value & kKeyLengthValueMask) << kKeyLengthShift);
            }
        }

        /// <summary>Read the raw 12-bit value stored in the KeyLength field, without the inline check. Used by disk-serialization paths
        /// where the field may hold a sentinel or actual length (not the effective <see cref="KeyLength"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetKeyLengthRaw() => (int)((word >> kKeyLengthShift) & kKeyLengthValueMask);

        /// <summary>The effective ValueLength for record-length calculations.
        /// <para>For inline values, returns the raw 24-bit value at bits 20–43. For overflow or object values, returns <see cref="ObjectIdMap.ObjectIdSize"/>
        /// (the OverflowByteArray / IHeapObject already carries the length, so mirroring the raw value in the header would be additional work with no
        /// consumer in the in-memory path).</para>
        /// <para>The setter always writes the raw 24-bit value. The disk-write path uses it to temporarily store the actual length or sentinel
        /// (<see cref="LogSettings.ValueLengthSentinel"/>) for serialization; <see cref="LogRecord.OnObjectReadComplete"/> restores ObjectIdSize on read-back.</para>
        /// <para>For disk-serialization paths that need to READ the raw stored value (not the effective length), use <see cref="GetValueLengthRaw"/>.</para></summary>
        internal int ValueLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => ValueIsInline ? (int)((word >> kValueLengthShift) & kValueLengthValueMask) : ObjectIdMap.ObjectIdSize;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Debug.Assert((uint)value <= kValueLengthValueMask, $"ValueLength {value} exceeds 24-bit max");
                word = (word & ~kValueLengthMask) | (((ulong)value & kValueLengthValueMask) << kValueLengthShift);
            }
        }

        /// <summary>Read the raw 24-bit value stored in the ValueLength field, without the inline check. Used by disk-serialization paths
        /// where the field may hold a sentinel or actual length (not the effective <see cref="ValueLength"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetValueLengthRaw() => (int)((word >> kValueLengthShift) & kValueLengthValueMask);

        internal readonly int ExtendedNamespaceLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var nameSpace = (byte)((word >> kNamespaceShift) & ByteMask);
                return (nameSpace & (1 << ExtendedNamespaceIndicatorBit)) == 0 ? 0 : nameSpace & NamespaceIndicatorMask;
            }
        }

        /// <summary>Get or set the Namespace byte. Throws an exception if out of range or if there is a conflicting specification for extended-length nameSpace.</summary>
        public byte NamespaceByte
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get
            {
                var nameSpace = (byte)((word >> kNamespaceShift) & ByteMask);
                if ((nameSpace & (1 << ExtendedNamespaceIndicatorBit)) != 0)
                    ThrowTsavoriteException("Cannot get NamespaceByte when ExtendedNamespaceFlag is set");
                return nameSpace;
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                if (value > sbyte.MaxValue)
                    ThrowTsavoriteException($"NamespaceByte value {value} exceeds max allowable {sbyte.MaxValue}");
                word = (word & ~kNamespaceMask) | ((ulong)value << kNamespaceShift);
            }
        }

        /// <summary>Set the raw namespace byte (including extended namespace indicator).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetNamespaceByteRaw(byte value)
        {
            word = (word & ~kNamespaceMask) | ((ulong)value << kNamespaceShift);
        }

        /// <summary>Get or set the RecordType byte.</summary>
        public byte RecordType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => (byte)((word >> kRecordTypeShift) & ByteMask);
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => word = (word & ~kRecordTypeMask) | ((ulong)value << kRecordTypeShift);
        }

        // ── RecordLength derivation (no longer stored) ─────────────────────────────
        //
        // For perf, callers that need multiple of {unalignedSum, alignedSum, totalFiller, recordLength} should call
        // GetRecordLengths(recordBaseAddress, out ...) once instead of calling the individual getters multiple times,
        // because each individual getter recomputes the unaligned/aligned sum. The unaligned/aligned/filler/record-length
        // chain depends on multiple header fields, so the redundant work compounds quickly when called in a loop.

        /// <summary>The unaligned sum of all record components: RecordInfo + DataHeader + ExtendedNamespace + Key + Value + Optionals.
        /// <para>NOTE: For perf, prefer <see cref="GetRecordLengths"/> if you also need aligned sum, filler, or record length —
        /// it computes everything in one pass.</para></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetUnalignedComponentSum()
            => RecordInfo.Size + Size + ExtendedNamespaceLength + KeyLength + ValueLength + GetOptionalSize();

        /// <summary>Aligned sum (rounded up to kRecordAlignment). See perf note on <see cref="GetUnalignedComponentSum"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetAlignedComponentSum()
            => RoundUp(GetUnalignedComponentSum(), Constants.kRecordAlignment);

        /// <summary>
        /// Compute all record-length derivations in a single pass. Prefer this over multiple individual getters when you need
        /// more than one of {unalignedSum, alignedSum, implicitFiller, explicitFiller, recordLength}.
        /// </summary>
        /// <param name="recordBaseAddress">Physical address of the start of the RecordInfo.</param>
        /// <param name="unalignedSum">Sum of all record components (no alignment padding).</param>
        /// <param name="alignedSum">Aligned sum (= recordLength if there is no explicit filler).</param>
        /// <param name="implicitFiller">Bytes of padding from alignment alone (0..kRecordAlignment-1).</param>
        /// <param name="explicitFiller">Bytes of padding stored as an int at <c>recordBase + alignedSum</c> (only if <see cref="HasFiller"/>; multiple of kRecordAlignment).</param>
        /// <returns>The total allocated record length (alignedSum + explicitFiller).</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly unsafe int GetRecordLengths(long recordBaseAddress, out int unalignedSum, out int alignedSum, out int implicitFiller, out int explicitFiller)
        {
            unalignedSum = RecordInfo.Size + Size + ExtendedNamespaceLength + KeyLength + ValueLength + GetOptionalSize();
            alignedSum = RoundUp(unalignedSum, Constants.kRecordAlignment);
            implicitFiller = alignedSum - unalignedSum;
            explicitFiller = HasFiller ? *(int*)(recordBaseAddress + alignedSum) : 0;
            return alignedSum + explicitFiller;
        }

        /// <summary>Get the total allocated record length, including any filler.
        /// <para>NOTE: For perf, prefer <see cref="GetRecordLengths"/> if you also need other related values.</para></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly unsafe int GetRecordLength(long recordBaseAddress)
        {
            var alignedSum = GetAlignedComponentSum();
            return HasFiller ? alignedSum + *(int*)(recordBaseAddress + alignedSum) : alignedSum;
        }

        // ── Filler helpers ─────────────────────────────────────────────────────────

        /// <summary>Get the explicit filler length. Zero if HasFiller is not set.
        /// <para>NOTE: For perf, prefer <see cref="GetRecordLengths"/> if you also need other related values.</para></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly unsafe int GetExplicitFillerLength(long recordBaseAddress)
            => HasFiller ? *(int*)(recordBaseAddress + GetAlignedComponentSum()) : 0;

        /// <summary>Get the total filler length (implicit + explicit).
        /// <para>NOTE: For perf, prefer <see cref="GetRecordLengths"/> if you also need other related values.</para></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly unsafe int GetTotalFillerLength(long recordBaseAddress)
        {
            var unalignedSum = GetUnalignedComponentSum();
            var alignedSum = RoundUp(unalignedSum, Constants.kRecordAlignment);
            var explicitFiller = HasFiller ? *(int*)(recordBaseAddress + alignedSum) : 0;

            Debug.Assert(explicitFiller % Constants.kRecordAlignment == 0, $"Reading Explicit filler: {explicitFiller} is not a multiple of kRecordAlignment");
            return alignedSum - unalignedSum + explicitFiller;
        }

        /// <summary>Set the filler for a record given the total filler bytes available (allocatedRecordLength - unalignedSum).
        /// Computes implicit and explicit portions, writes the explicit int if needed, and sets/clears HasFiller.</summary>
        /// <param name="recordBaseAddress">Physical address of the start of the RecordInfo.</param>
        /// <param name="totalFiller">Total filler bytes = allocatedRecordLength - unalignedSum. Must be non-negative.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void SetFiller(long recordBaseAddress, int totalFiller)
        {
            Debug.Assert(totalFiller >= 0, $"Total filler {totalFiller} must be non-negative");

            // Compute implicit filler inline (avoid double-computing GetAlignedComponentSum via the helper methods).
            var unalignedSum = GetUnalignedComponentSum();
            var alignedSum = RoundUp(unalignedSum, Constants.kRecordAlignment);
            var implicitFiller = alignedSum - unalignedSum;
            var explicitFiller = totalFiller - implicitFiller;
            Debug.Assert(explicitFiller >= 0, $"Explicit filler {explicitFiller} must be non-negative");

            if (explicitFiller > 0)
            {
                Debug.Assert(explicitFiller % Constants.kRecordAlignment == 0, $"Explicit filler {explicitFiller} must be a multiple of kRecordAlignment");
                SetHasFiller();
                *(int*)(recordBaseAddress + alignedSum) = explicitFiller;
            }
            else
                ClearHasFiller();
        }

        // ── Key and Value field info ───────────────────────────────────────────────

        /// <summary>Get the offset of the key data, relative to the RecordInfo start.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetOffsetToKeyStart() => RecordInfo.Size + Size + ExtendedNamespaceLength;

        /// <summary>Get the key length and key data address.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (int keyLength, long keyAddress) GetKeyFieldInfo(long recordBaseAddress)
            => (KeyLength, recordBaseAddress + GetOffsetToKeyStart());

        /// <summary>Get the value length and value data address.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (int valueLength, long valueAddress) GetValueFieldInfo(long recordBaseAddress)
            => (ValueLength, recordBaseAddress + GetOffsetToKeyStart() + KeyLength);

        /// <summary>Get all KV lengths, optional sizes, filler, and value address in a single pass for perf.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (int keyLength, int valueLength) GetKVLengths(long recordBaseAddress, out int eTagLen, out int expirationLen, out int objectLogPositionLen, out int fillerLen, out long valueAddress)
        {
            eTagLen = HasETag ? LogRecord.ETagSize : 0;
            expirationLen = HasExpiration ? LogRecord.ExpirationSize : 0;
            objectLogPositionLen = RecordIsInline ? 0 : LogRecord.ObjectLogPositionSize;

            var keyLength = KeyLength;
            var valueLength = ValueLength;
            fillerLen = GetTotalFillerLength(recordBaseAddress);

            valueAddress = recordBaseAddress + GetOffsetToKeyStart() + keyLength;
            return (keyLength, valueLength);
        }

        // ── Create a temp intermediate version for  ───────────────────────────────────────────────

        // ── Initialize ─────────────────────────────────────────────────────────────

        /// <summary>Initialize the DataHeader for a new or revivified record. Sets the field lengths, namespace, recordType, filler,
        /// and the default inline bits (<see cref="KeyIsInline"/> + <see cref="ValueIsInline"/>).
        /// <para>NOTE: Setting the inline bits here is the default for new records. The caller may then transition to
        /// <see cref="KeyIsOverflow"/> / <see cref="ValueIsOverflow"/> / <see cref="ValueIsObject"/> as appropriate based on <paramref name="sizeInfo"/>
        /// (via the <c>SetXxx</c> methods on the DataHeader). RDH owns the inline bits — callers that previously initialized them
        /// by assigning <c>RecordInfo.InitialValid</c> no longer touch those bits, so this method has to provide the default state.</para></summary>
        /// <param name="recordBaseAddress">Physical address of the start of the RecordInfo.</param>
        /// <param name="sizeInfo">Record size information.</param>
        /// <param name="keyAddress">Output: physical address of key data.</param>
        /// <param name="namespaceAddress">Output: physical address of namespace byte.</param>
        /// <param name="valueAddress">Output: physical address of value data.</param>
        /// <returns>The fixed header length (always <see cref="Size"/>).</returns>
        internal int Initialize(in RecordSizeInfo sizeInfo, out long keyAddress, out long namespaceAddress, out long valueAddress, long recordBaseAddress)
        {
            var keyLength = sizeInfo.InlineKeySize;
            var valueLength = sizeInfo.InlineValueSize;
            var extendedNamespaceSize = sizeInfo.FieldInfo.ExtendedNamespaceSize;
            var namespaceByte = (byte)(extendedNamespaceSize > 0 ? ((1 << ExtendedNamespaceIndicatorBit) | (extendedNamespaceSize & NamespaceIndicatorMask)) : 0);
            var recordType = sizeInfo.FieldInfo.RecordType;

            // Single atomic write of the non-filler fields. Set the default inline bits (KeyIsInline + ValueIsInline);
            // clear the other indicator bits (HasETag/HasExpiration/HasFiller/ValueIsObject/Unused); then set KeyLength,
            // ValueLength, Namespace, RecordType. Caller transitions inline bits to overflow/object as needed.
            word = kKeyIsInlineMask | kValueIsInlineMask
                 | (((ulong)keyLength & kKeyLengthValueMask) << kKeyLengthShift)
                 | (((ulong)valueLength & kValueLengthValueMask) << kValueLengthShift)
                 | ((ulong)namespaceByte << kNamespaceShift)
                 | ((ulong)recordType << kRecordTypeShift);

            // Note: We do not set ETag and Expiration here, as that may confuse ISessionFunctions into thinking those values have actually been set.
            // This is deferred to TrySetContentLengths, which should be first in the chain of calls that includes TrySetETag and/or TrySetExpiration.

            // Calculate and set filler (separate update because explicit filler is also written to recordBaseAddress + alignedSum).
            var unalignedSum = RecordInfo.Size + Size + extendedNamespaceSize + keyLength + valueLength + sizeInfo.ObjectLogPositionSize;
            var totalFiller = sizeInfo.AllocatedInlineRecordSize - unalignedSum;
            if (totalFiller > 0)
                SetFiller(recordBaseAddress, totalFiller);

            namespaceAddress = recordBaseAddress + RecordInfo.Size + NamespaceOffsetInHeader;
            keyAddress = recordBaseAddress + RecordInfo.Size + Size + extendedNamespaceSize;
            valueAddress = keyAddress + keyLength;

            return Size;
        }

        /// <summary>Prepare the header for revivification: clear filler, namespace, and recordType; preserve record length via sizeInfo update.</summary>
        internal void InitializeForRevivification(ref RecordSizeInfo sizeInfo, long recordBaseAddress)
        {
            Debug.Assert(KeyIsInline, "Expected Key to be inline in InitializeForRevivification");
            Debug.Assert(ValueIsInline, "Expected Value to be inline in InitializeForRevivification");
            Debug.Assert(!HasETag && !HasExpiration, "Expected no optionals in InitializeForRevivification");

            var recordLength = GetRecordLength(recordBaseAddress);
            Debug.Assert(sizeInfo.AllocatedInlineRecordSize <= recordLength, "Cannot exceed previous Record size in InitializeForRevivification");

            // Clear filler, namespace, recordType; keep inline bits and key/value lengths
            ClearHasFiller();
            SetNamespaceByteRaw(0);
            RecordType = 0;

            // Ensure the AllocatedInlineRecordSize retains recordLength when LogRecord.InitializeRecord is called
            sizeInfo.AllocatedInlineRecordSize = recordLength;
            sizeInfo.SetIsRevivifiedRecord();
        }

        // ── IKey implementation ────────────────────────────────────────────────────

        #region IKey

        /// <inheritdoc/>
        public readonly bool IsPinned => true;

        /// <inheritdoc/>
        public readonly unsafe ReadOnlySpan<byte> KeyBytes
        {
            get
            {
                // The struct IS the header; it starts at DataHeaderAddress = recordBase + RecordInfo.Size.
                var recordBase = (long)Unsafe.AsPointer(ref Unsafe.AsRef(in this)) - RecordInfo.Size;
                var keyLength = KeyLength;
                var keyStartPtr = (byte*)(recordBase + GetOffsetToKeyStart());
                return new ReadOnlySpan<byte>(keyStartPtr, keyLength);
            }
        }

        /// <inheritdoc/>
        public readonly bool HasNamespace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (byte)((word >> kNamespaceShift) & ByteMask) != 0;
        }

        /// <inheritdoc/>
        public readonly unsafe ReadOnlySpan<byte> NamespaceBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(HasNamespace, "Should not call if !HasNamespace");
                var nameSpace = (byte)((word >> kNamespaceShift) & ByteMask);
                if ((nameSpace & (1 << ExtendedNamespaceIndicatorBit)) == 0)
                {
                    // Single byte namespace — return a span over the namespace byte in the word
                    var ptr = (byte*)Unsafe.AsPointer(ref Unsafe.AsRef(in this)) + NamespaceOffsetInHeader;
                    return new ReadOnlySpan<byte>(ptr, 1);
                }
                else
                {
                    ThrowTsavoriteException("Extended namespaces not yet implemented");
                    return default;
                }
            }
        }

        #endregion

        // ── ToString ───────────────────────────────────────────────────────────────

        /// <inheritdoc/>
        public override readonly string ToString() => ToString("na", "na", 0);

        internal readonly string ToString(string keyString, string valueString, long recordBaseAddress)
        {
            if (word == 0)
                return "<empty>";
            static string bstr(bool value) => value ? "T" : "F";
            static string bstr01(bool value) => value ? "1" : "0";
            var keyLength = KeyLength;
            var valueLength = ValueLength;

            string recordLenStr = "na", fillerLenStr = "na";
            if (recordBaseAddress != 0)
            {
                var recordLen = GetRecordLengths(recordBaseAddress, out var unalignedSum, out var alignedSum, out var implicitFiller, out var explicitFiller);
                recordLenStr = $"act: {alignedSum}, all: {recordLen}";
                if (HasFiller)
                    fillerLenStr = $"[i:{implicitFiller} + e:{explicitFiller} = {implicitFiller + explicitFiller}]";
            }

            var keyStr = KeyIsInline ? "inl" : "ovf";
            var valStr = ValueIsInline ? "inl" : (ValueIsObject ? "obj" : "ovf");

            // Unused0..Unused5 printed in declared order (Unused0 leftmost), no separator since only 6 bits.
            var unusedStr = $"{bstr01(Unused0)}{bstr01(Unused1)}{bstr01(Unused2)}{bstr01(Unused3)}{bstr01(Unused4)}{bstr01(Unused5)}";

            return $"rec l:{recordLenStr}"
                 + $" | key {keyStr}/l:{keyLength} {keyString}"
                 + $" | val {valStr}/l:{valueLength}, {valueString}"
                 + $" | ETag {bstr(HasETag)}, Expir {bstr(HasExpiration)}"
                 + $" | fil {fillerLenStr} Ns:{(byte)((word >> kNamespaceShift) & ByteMask)}/x:{ExtendedNamespaceLength}, RT:{RecordType}"
                 + $" | Unused0-5 {unusedStr}";
        }
    }
}