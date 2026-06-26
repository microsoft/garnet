// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    /// <summary>
    /// Fixed 8-byte header describing the data layout of the record. Atomic assignment is guaranteed on 64-bit systems because
    /// the entire header fits in a single aligned <see cref="ulong"/>.
    /// <para>Layout (low bit to high bit):</para>
    /// <list type="bullet">
    ///     <item>Bit 0: <see cref="KeyIsInline"/></item>
    ///     <item>Bit 1: <see cref="ValueIsInline"/></item>
    ///     <item>Bit 2: <see cref="ValueIsObject"/></item>
    ///     <item>Bit 3: <see cref="HasExpiration"/></item>
    ///     <item>Bit 4: <see cref="HasETag"/></item>
    ///     <item>Bit 5: Unused1 (reserved for future use, e.g. version toggle).</item>
    ///     <item>Bits 6–13: <see cref="FillerWords"/> (8-bit count of 8-byte filler words AFTER the implicit alignment padding).
    ///         The total explicit filler in bytes is <c>FillerWords &lt;&lt; Constants.kRecordAlignmentShift</c>; the filler bytes themselves live at
    ///         <c>recordBase + alignedSum .. recordBase + alignedSum + (FillerWords &lt;&lt; Constants.kRecordAlignmentShift)</c> and are never read.
    ///         Maximum representable explicit filler is <see cref="MaxFillerWords"/> * <see cref="Constants.kRecordAlignment"/> = 2040 bytes. Records that need more
    ///         filler are <i>split</i>: the original record retains <see cref="RecordSplitRetainFillerWords"/> * <see cref="Constants.kRecordAlignment"/> = 512 bytes
    ///         of filler and the excess is placed in a new invalid record (see <see cref="SetFiller"/>).</item>
    ///     <item>Bits 14–23: <see cref="KeyLength"/>. The property returns this raw value for inline keys; for overflow keys
    ///         it returns <see cref="ObjectIdMap.ObjectIdSize"/>. The OverflowByteArray already carries the length, so mirroring it
    ///         in the header would be extra work with no consumer.</item>
    ///     <item>Bits 24–47: <see cref="ValueLength"/>. The property returns this raw value for inline values; for
    ///         overflow/object values it returns <see cref="ObjectIdMap.ObjectIdSize"/>. The OverflowByteArray / IHeapObject
    ///         already carries the length, so mirroring it in the header would be extra work with no consumer.</item>
    ///     <item>Bits 48–55: <see cref="RecordType"/> byte; interpreted by caller. (Byte-aligned at byte 6.)</item>
    ///     <item>Bits 56–63: Namespace byte (with encoding indicating if there are many extra namespace bytes; if so, they precede
    ///         the Key data bytes). (Byte-aligned at byte 7.)</item>
    /// </list>
    /// <para>Disk-write paths (<see cref="LogRecord.SetObjectLogRecordStartPositionAndLength"/>) write the low <see cref="kKeyLengthBits"/> /
    /// <see cref="kValueLengthBits"/> bits of the on-disk overflow/object length into the RDH KeyLength/ValueLength field, with the next
    /// 32 bits stored in the objectId slot at keyAddress/valueAddress. After read-back (<see cref="LogRecord.OnObjectReadComplete"/>)
    /// the raw RDH fields are restored to <see cref="ObjectIdMap.ObjectIdSize"/> so the runtime "non-inline → property returns
    /// ObjectIdSize" invariant holds.</para>
    /// <para>RecordLength is no longer stored; it is derived from the header alone:
    /// <c>alignedSum = RoundUp(Constants.FixedHeaderSize + ExtendedNamespaceLength + KeyLength + ValueLength + OptionalSize, kRecordAlignment)</c>;
    /// <c>recordLength = alignedSum + (FillerWords &lt;&lt; 3)</c>. Because everything that defines record length is in this 8-byte
    /// word, a single atomic write to <c>word</c> publishes a fully-consistent new record layout.</para>
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct RecordDataHeader
    {
#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter

        // ── Indicator bits (bits 0-5) ──────────────────────────────────────────────
        const int kKeyIsInlineBit = 0;
        const int kValueIsInlineBit = 1;
        const int kValueIsObjectBit = 2;
        const int kHasExpirationBit = 3;
        const int kHasETagBit = 4;
        const int kUnused1Bit = 5;

        const ulong kKeyIsInlineMask = 1UL << kKeyIsInlineBit;
        const ulong kValueIsInlineMask = 1UL << kValueIsInlineBit;
        const ulong kValueIsObjectMask = 1UL << kValueIsObjectBit;
        const ulong kHasExpirationMask = 1UL << kHasExpirationBit;
        const ulong kHasETagMask = 1UL << kHasETagBit;
        const ulong kUnused1Mask = 1UL << kUnused1Bit;

        // ── FillerWords field (bits 6-13, 8 bits) ──────────────────────────────────
        const int kFillerWordsShift = 6;
        const int kFillerWordsBits = 8;
        const ulong kFillerWordsValueMask = (1UL << kFillerWordsBits) - 1;        // 0xFF
        const ulong kFillerWordsMask = kFillerWordsValueMask << kFillerWordsShift;

        /// <summary>Maximum value of the <see cref="FillerWords"/> field — represents up to <c>MaxFillerWords * Constants.kRecordAlignment</c> = 2040 bytes
        /// of explicit filler. Records that need more filler are split (see <see cref="SetFiller"/>).</summary>
        internal const int MaxFillerWords = (1 << kFillerWordsBits) - 1;          // 255

        /// <summary>Number of bits in the <see cref="RecordSplitRetainFillerWords"/> constant (chosen so the retained filler stays well under
        /// <see cref="MaxFillerWords"/> but is still a meaningful amount of in-place headroom for future re-growth).</summary>
        const int kRecordSplitRetainFillerWordsBits = 6;

        /// <summary>When splitting an over-filled record, the original record retains this many filler words
        /// (= <c>RecordSplitRetainFillerWords * Constants.kRecordAlignment</c> = 512 bytes). The remainder becomes a new invalid record.</summary>
        internal const int RecordSplitRetainFillerWords = 1 << kRecordSplitRetainFillerWordsBits;     // 64

        // ── KeyLength field (low kKeyLengthBits bits after FillerWords) ─────────────
        const int kKeyLengthShift = 14;
        internal const int kKeyLengthBits = 10;
        internal const ulong kKeyLengthLowBitsMask = (1UL << kKeyLengthBits) - 1;       // The bit mask at the low bit positions of the shifted ulong
        const ulong kKeyLengthMask = kKeyLengthLowBitsMask << kKeyLengthShift;

        // ── ValueLength field (low kValueLengthBits bits after KeyLength) ───────────
        const int kValueLengthShift = kKeyLengthShift + kKeyLengthBits;
        internal const int kValueLengthBits = 24;
        internal const ulong kValueLengthLowBitsMask = (1UL << kValueLengthBits) - 1;   // The bit mask at the low bit positions of the shifted ulong
        const ulong kValueLengthMask = kValueLengthLowBitsMask << kValueLengthShift;

        // ── RecordType byte (byte 6 of word; must be byte-aligned: requires kValueLengthShift + kValueLengthBits == 48) ─
        const int kRecordTypeShift = kValueLengthShift + kValueLengthBits;
        const ulong kRecordTypeMask = 0xFFUL << kRecordTypeShift;

        // ── Namespace byte (byte 7 of word) ────────────────────────────────────────
        const int kNamespaceShift = kRecordTypeShift + 8;
        const ulong kNamespaceMask = 0xFFUL << kNamespaceShift;

        /// <summary>Mask for extracting a single byte from the word.</summary>
        const ulong ByteMask = 0xFFUL;

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

        // ── Indicator-bit accessors ────────────────────────────────────────────────

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

        /// <summary>Unused future-toggle bit. Exposed only for diagnostic ToString output.</summary>
        internal readonly bool Unused1 => (word & kUnused1Mask) != 0;

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

        // ── FillerWords accessor ───────────────────────────────────────────────────

        /// <summary>The number of 8-byte filler words BEYOND the implicit-alignment padding. The number of explicit filler bytes is
        /// <c>FillerWords &lt;&lt; 3</c>; total filler is <c>implicitFiller + (FillerWords &lt;&lt; 3)</c>.</summary>
        internal int FillerWords
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => (int)((word >> kFillerWordsShift) & kFillerWordsValueMask);
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Debug.Assert((uint)value <= MaxFillerWords, $"FillerWords {value} exceeds {MaxFillerWords}");
                word = (word & ~kFillerWordsMask) | (((ulong)value & kFillerWordsValueMask) << kFillerWordsShift);
            }
        }

        /// <summary>Whether the record has any explicit filler beyond alignment padding (i.e., <see cref="FillerWords"/> != 0).
        /// Provided for diagnostic and back-compat use; most callers should read <see cref="FillerWords"/> directly.</summary>
        public readonly bool HasFiller => (word & kFillerWordsMask) != 0;

        // ── Optional/object size helper ────────────────────────────────────────────

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

        /// <summary>Initialize the DataHeader for a new record: currently, do nothing. Callers must subsequently invoke
        /// <see cref="Initialize"/> to publish the full record state (lengths, inline/overflow/object bits, filler).
        /// <para>Between <c>InitializeForNewRecord</c> and <c>Initialize</c>, the RDH is either zero already from log
        /// allocation, or has been retrieved from a prior allocation (revivification or retry of failed CAS) and thus must
        /// retain the original length information, as the record content may not be zero-initialized. If RDH is zero then
        /// scanner length-walks see a min-length record (<see cref="Constants.FixedHeaderSize"/> = 16 bytes) so they advance
        /// safely past the partially-allocated slot. See <see cref="GetRecordLength"/> for the zero-RDH guard.</para></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeForNewRecord() { }

        // ── Field accessors via ulong word bit manipulation ────────────────────────

        /// <summary>The effective KeyLength for record-length calculations.
        /// <para>For inline keys, returns the raw <see cref="kKeyLengthBits"/>-bit value. For overflow keys, returns <see cref="ObjectIdMap.ObjectIdSize"/>
        /// (the OverflowByteArray already carries the length, so mirroring the raw value in the header would be additional work with no consumer
        /// in the in-memory path).</para>
        /// <para>The setter always writes the raw <see cref="kKeyLengthBits"/>-bit value. The disk-write path uses it to temporarily store the LOW <see cref="kKeyLengthBits"/> bits of
        /// the on-disk overflow key length (the next 32 bits live in the objectId slot at keyAddress); after read-back,
        /// <see cref="LogRecord.OnObjectReadComplete"/> restores ObjectIdSize so the runtime invariant holds.</para>
        /// <para>For disk-serialization paths that need to READ the raw stored value (not the effective length), use <see cref="GetKeyLengthRaw"/>.</para></summary>
        internal int KeyLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => KeyIsInline ? (int)((word >> kKeyLengthShift) & kKeyLengthLowBitsMask) : ObjectIdMap.ObjectIdSize;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Debug.Assert((uint)value <= kKeyLengthLowBitsMask, $"KeyLength {value} exceeds {kKeyLengthBits}-bit max");
                word = (word & ~kKeyLengthMask) | (((ulong)value & kKeyLengthLowBitsMask) << kKeyLengthShift);
            }
        }

        /// <summary>Read the raw value stored in the KeyLength field, without the inline check. Used by disk-serialization paths
        /// where the field may hold the low <see cref="kKeyLengthBits"/> bits of the on-disk overflow length (not the effective <see cref="KeyLength"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetKeyLengthRaw() => (int)((word >> kKeyLengthShift) & kKeyLengthLowBitsMask);

        /// <summary>The effective ValueLength for record-length calculations.
        /// <para>For inline values, returns the raw <see cref="kValueLengthBits"/>-bit value. For overflow or object values, returns <see cref="ObjectIdMap.ObjectIdSize"/>
        /// (the OverflowByteArray / IHeapObject already carries the length, so mirroring the raw value in the header would be additional work with no
        /// consumer in the in-memory path).</para>
        /// <para>The setter always writes the raw <see cref="kValueLengthBits"/>-bit value. The disk-write path uses it to temporarily store the LOW <see cref="kValueLengthBits"/> bits of
        /// the on-disk overflow/object value length (the next 32 bits live in the objectId slot at valueAddress); after read-back,
        /// <see cref="LogRecord.OnObjectReadComplete"/> restores ObjectIdSize so the runtime invariant holds.</para>
        /// <para>For disk-serialization paths that need to READ the raw stored value (not the effective length), use <see cref="GetValueLengthRaw"/>.</para></summary>
        internal int ValueLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => ValueIsInline ? (int)((word >> kValueLengthShift) & kValueLengthLowBitsMask) : ObjectIdMap.ObjectIdSize;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Debug.Assert((uint)value <= kValueLengthLowBitsMask, $"ValueLength {value} exceeds {kValueLengthBits}-bit max");
                word = (word & ~kValueLengthMask) | (((ulong)value & kValueLengthLowBitsMask) << kValueLengthShift);
            }
        }

        /// <summary>Read the raw value stored in the ValueLength field, without the inline check. Used by disk-serialization paths
        /// where the field may hold the low <see cref="kValueLengthBits"/> bits of the on-disk overflow/object length (not the effective <see cref="ValueLength"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetValueLengthRaw() => (int)((word >> kValueLengthShift) & kValueLengthLowBitsMask);

        internal readonly int ExtendedNamespaceLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var nameSpace = (byte)((word >> kNamespaceShift) & ByteMask);
                return (nameSpace & (1 << ExtendedNamespaceIndicatorBit)) == 0 ? 0 : nameSpace & NamespaceIndicatorMask;
            }
        }

        /// <summary>Get or the Namespace byte. Set is not implemented as this is immutable after construction; see <see cref="SetNamespaceByteRaw"/>.</summary>
        public readonly byte NamespaceByte
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var nameSpace = (byte)((word >> kNamespaceShift) & ByteMask);
                if ((nameSpace & (1 << ExtendedNamespaceIndicatorBit)) != 0)
                    ThrowTsavoriteException("Cannot get NamespaceByte when ExtendedNamespaceFlag is set");
                return nameSpace;
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
        // GetRecordLengths(out ...) once instead of calling the individual getters multiple times, because each individual
        // getter recomputes the unaligned/aligned sum. The unaligned/aligned/filler/record-length chain depends on multiple
        // header fields, so the redundant work compounds quickly when called in a loop.
        //
        // Note: with FillerWords stored in the header word itself, NONE of these helpers need a recordBaseAddress argument
        // — the explicit filler length is read directly from the FillerWords field, not from a stored int in the record body.

        /// <summary>The unaligned sum of all record components: RecordInfo + DataHeader + ExtendedNamespace + Key + Value + Optionals.
        /// <para>NOTE: For perf, prefer <see cref="GetRecordLengths"/> if you also need aligned sum, filler, or record length —
        /// it computes everything in one pass.</para></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetUnalignedComponentSum()
            => Constants.FixedHeaderSize + ExtendedNamespaceLength + KeyLength + ValueLength + GetOptionalSize();

        /// <summary>Aligned sum (rounded up to kRecordAlignment). See perf note on <see cref="GetUnalignedComponentSum"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetAlignedComponentSum()
            => RoundUp(GetUnalignedComponentSum(), Constants.kRecordAlignment);

        /// <summary>
        /// Compute all record-length derivations in a single pass. Prefer this over multiple individual getters when you need
        /// more than one of {unalignedSum, alignedSum, implicitFiller, explicitFiller, recordLength}.
        /// </summary>
        /// <param name="unalignedSum">Sum of all record components (no alignment padding).</param>
        /// <param name="alignedSum">Aligned sum (= recordLength if there is no explicit filler).</param>
        /// <param name="implicitFiller">Bytes of padding from alignment alone (0..kRecordAlignment-1).</param>
        /// <param name="explicitFiller">Bytes of padding read from the <see cref="FillerWords"/> field (always a multiple of 8).</param>
        /// <returns>The total allocated record length (alignedSum + explicitFiller).</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetRecordLengths(out int unalignedSum, out int alignedSum, out int implicitFiller, out int explicitFiller)
        {
            // Zero-RDH guard: a freshly-allocated record that has not yet been Initialize()d has word == 0
            // (no indicator bits, no lengths, no filler, no namespace, no recordType). Scanner length-walks must step past
            // it as a min-length record (Constants.FixedHeaderSize = 16 bytes) until Initialize publishes the real layout.
            // We test the full word (not just the key/value-length bitfields) so a degenerate but valid record
            // with KeyLength=0+ValueLength=0+nonzero indicator/filler/optional/namespace bits is NOT mistaken for unInitialized.
            if (word == 0)
            {
                unalignedSum = Constants.FixedHeaderSize;
                alignedSum = unalignedSum;
                implicitFiller = 0;
                explicitFiller = 0;
                return alignedSum;
            }

            unalignedSum = Constants.FixedHeaderSize + ExtendedNamespaceLength + KeyLength + ValueLength + GetOptionalSize();
            alignedSum = RoundUp(unalignedSum, Constants.kRecordAlignment);
            implicitFiller = alignedSum - unalignedSum;
            explicitFiller = FillerWords << Constants.kRecordAlignmentShift;
            return alignedSum + explicitFiller;
        }

        /// <summary>Get the total allocated record length, including any filler.
        /// <para>NOTE: For perf, prefer <see cref="GetRecordLengths"/> if you also need other related values.</para></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetRecordLength()
        {
            // Zero-RDH guard: see comment in GetRecordLengths. Full-word test rejects degenerate-but-valid records with zero K/V lengths.
            return word == 0 ? Constants.FixedHeaderSize : GetAlignedComponentSum() + (FillerWords << Constants.kRecordAlignmentShift);
        }

        // ── Filler helpers ─────────────────────────────────────────────────────────

        /// <summary>Get the explicit filler length in bytes (= <c>FillerWords &lt;&lt; 3</c>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetExplicitFillerLength() => FillerWords << Constants.kRecordAlignmentShift;

        /// <summary>Get the total filler length (implicit + explicit).
        /// <para>NOTE: For perf, prefer <see cref="GetRecordLengths"/> if you also need other related values.</para></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetTotalFillerLength()
        {
            var unalignedSum = GetUnalignedComponentSum();
            var alignedSum = RoundUp(unalignedSum, Constants.kRecordAlignment);
            return alignedSum - unalignedSum + (FillerWords << Constants.kRecordAlignmentShift);
        }

        /// <summary>Set the filler for a record given the total filler bytes available (allocatedRecordLength - unalignedSum).
        /// Computes implicit and explicit portions and writes <see cref="FillerWords"/>.
        /// <para>If the computed FillerWords value exceeds <see cref="MaxFillerWords"/> (255), the record is split: this RDH retains
        /// <see cref="RecordSplitRetainFillerWords"/> (64) filler words and the excess becomes a new invalid record placed at
        /// <c>recordBase + alignedSum + (RecordSplitRetainFillerWords &lt;&lt; Constants.kRecordAlignmentShift)</c>. The new record's RecordInfo (with Invalid set)
        /// and RDH (inline keys/values, no optionals) are written BEFORE this RDH's FillerWords is updated; this ordering ensures a
        /// concurrent scanner that reads our OLD RDH will jump over the new (invalid) record (effectively as part of the old record's
        /// allocated extent), while a scanner that reads our NEW RDH will see the new invalid record as its own next-record entry and
        /// will properly skip it (because Invalid is set).</para>
        /// <para>This record splitting is safe to do without any kind of additional locking, because it is still part of the current
        /// record that we have locked. To make this splitting safe for concurrent scanners, the newly split-off record's RecordInfo
        /// and RecordDataHeader must be set before the original record's RDH is updated; this ensures that a concurrent scanner will
        /// see a valid record if it reads the new RDH, and if it still has the old RDH, it will just jump to the end of the original
        /// record, which effectively just jumps over the new invalid record.</para>
        /// <para>TODO: REVIVIFICATION — if revivification is active when a split occurs, the newly split-off record should be sent to
        /// <c>TryTransferToFreeList</c> so the free-record pool can absorb it.</para>
        /// </summary>
        /// <param name="recordBaseAddress">Physical address of the start of the RecordInfo (only used when a split is required).</param>
        /// <param name="totalFiller">Total filler bytes = allocatedRecordLength - unalignedSum. Must be non-negative.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetFiller(long recordBaseAddress, int totalFiller)
        {
            var fillerWords = ComputeFillerWordsOrSplit(recordBaseAddress, totalFiller);
            word = (word & ~kFillerWordsMask) | (((ulong)fillerWords & kFillerWordsValueMask) << kFillerWordsShift);
        }

        /// <summary>Compute the <see cref="FillerWords"/> value for a given total filler size, performing record-splitting
        /// if the explicit filler would exceed <see cref="MaxFillerWords"/>.
        /// <para>Does NOT mutate <see cref="word"/> — returns the computed FillerWords value for the caller to fold into a
        /// larger atomic word write (e.g. <see cref="Initialize"/> publishes indicator bits, lengths, namespace, recordType,
        /// AND filler in a single 8-byte word write).</para>
        /// <para>May write to memory at <c>recordBaseAddress + alignedSum + retainedExplicitFiller</c> if a split occurs,
        /// publishing the new invalid record's RecordInfo + RDH before returning. The caller MUST then perform its own
        /// publish of this RDH (typically as part of the surrounding atomic word write) so concurrent scanners see the
        /// split-off record as either part of this extent (old RDH) or as a separate invalid entry (new RDH).</para>
        /// </summary>
        /// <param name="recordBaseAddress">Physical address of the start of the RecordInfo (only used if a split is required).</param>
        /// <param name="totalFiller">Total filler bytes = allocatedRecordLength - unalignedSum. Must be non-negative.</param>
        /// <returns>The FillerWords value (0..MaxFillerWords) for the caller to encode into the RDH word.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ComputeFillerWordsOrSplit(long recordBaseAddress, int totalFiller)
        {
            Debug.Assert(totalFiller >= 0, $"Total filler {totalFiller} must be non-negative");

            var unalignedSum = GetUnalignedComponentSum();
            var alignedSum = RoundUp(unalignedSum, Constants.kRecordAlignment);
            var implicitFiller = alignedSum - unalignedSum;
            var explicitFiller = totalFiller - implicitFiller;
            Debug.Assert(explicitFiller >= 0, $"Explicit filler {explicitFiller} must be non-negative");
            Debug.Assert((explicitFiller & (Constants.kRecordAlignment - 1)) == 0, $"Explicit filler {explicitFiller} must be a multiple of kRecordAlignment");

            var fillerWords = explicitFiller >> Constants.kRecordAlignmentShift;
            if (fillerWords > MaxFillerWords)
                fillerWords = SplitOverflowingFiller(recordBaseAddress, alignedSum, explicitFiller);
            return fillerWords;
        }

        /// <summary>
        /// Handle the case where computed <see cref="FillerWords"/> would exceed <see cref="MaxFillerWords"/>: split off the excess into a
        /// new invalid record placed AFTER this record's retained filler. Returns the <see cref="RecordSplitRetainFillerWords"/> value that
        /// the caller should write into this record's <see cref="FillerWords"/> field.
        /// <para>The new split-off record's RecordInfo and RDH are written here, BEFORE the caller updates this record's <see cref="FillerWords"/>.
        /// This ordering is critical for concurrent-scanner safety: a scanner that reads our OLD (pre-split) RDH will treat the entire
        /// pre-split extent as one record and step over the new invalid record without inspecting it; a scanner that reads our NEW
        /// (post-split) RDH will encounter the new invalid record as a separate entry and will properly skip it (because Invalid is set).</para>
        /// </summary>
        private static unsafe int SplitOverflowingFiller(long recordBaseAddress, int alignedSum, int explicitFiller)
        {
            var retainedExplicitFiller = RecordSplitRetainFillerWords << Constants.kRecordAlignmentShift;        // 512 bytes
            var newRecordBytes = explicitFiller - retainedExplicitFiller;          // must be > 0 since fillerWords > MaxFillerWords > RecordSplitRetainFillerWords
            Debug.Assert(newRecordBytes >= Constants.FixedHeaderSize, $"Split-off region {newRecordBytes} is smaller than RecordInfo + RDH ({Constants.FixedHeaderSize})");
            Debug.Assert((newRecordBytes & (Constants.kRecordAlignment - 1)) == 0, $"Split-off region {newRecordBytes} must be a multiple of kRecordAlignment");

            var newRecordAddress = recordBaseAddress + alignedSum + retainedExplicitFiller;

            // The new record holds: RecordInfo + RDH + (the rest as inline "value" bytes; no key, no optionals).
            // If the rest doesn't fit in the ValueLength field + 8-bit FillerWords*8, recursively split via SetFiller.
            var newInnerBytes = newRecordBytes - Constants.FixedHeaderSize;        // bytes available for value + filler
            int newValueLength = newInnerBytes <= LogSettings.MaxInlineValueSizeLimit ? newInnerBytes : LogSettings.MaxInlineValueSizeLimit;
            var newRemainingFiller = newInnerBytes - newValueLength;

            // Step 1: Write the new record's RecordInfo (Invalid set) FIRST.
            var newRecInfo = RecordInfo.InitialValid;
            newRecInfo.SetInvalid();
            *(RecordInfo*)newRecordAddress = newRecInfo;

            // Step 2: Build and write the new record's RDH (inline keys/values, no optionals, KeyLength=0, ValueLength as computed).
            //   Then, if there's leftover filler, recursively call SetFiller on the new record's RDH.
            var newRDH = new RecordDataHeader
            {
                word = kKeyIsInlineMask | kValueIsInlineMask
                     | (((ulong)newValueLength & kValueLengthLowBitsMask) << kValueLengthShift)
            };
            // If there's still leftover filler after maxing out ValueLength, set it (this may itself trigger another split).
            if (newRemainingFiller > 0)
                newRDH.SetFiller(newRecordAddress, newRemainingFiller);

            *(RecordDataHeader*)(newRecordAddress + RecordInfo.Size) = newRDH;

            // TODO: REVIVIFICATION — if revivification is active, send this newly split-off record to TryTransferToFreeList so
            // the free-record pool can absorb it.

            // Step 3: Caller writes RecordSplitRetainFillerWords into this record's FillerWords (atomic update of the original RDH).
            return RecordSplitRetainFillerWords;
        }

        // ── Key and Value field info ───────────────────────────────────────────────

        /// <summary>Get the offset of the key data, relative to the RecordInfo start.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetOffsetToKeyStart() => Constants.FixedHeaderSize + ExtendedNamespaceLength;

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
            fillerLen = GetTotalFillerLength();

            valueAddress = recordBaseAddress + GetOffsetToKeyStart() + keyLength;
            return (keyLength, valueLength);
        }

        // ── Initialize ─────────────────────────────────────────────────────────────

        /// <summary>Initialize the DataHeader for a new or revivified record. Sets the field lengths, namespace, recordType,
        /// the indicator bits (KeyIsInline/Overflow and ValueIsInline/Overflow/Object based on <paramref name="sizeInfo"/>),
        /// and the FillerWords field — all in a SINGLE atomic 8-byte word write so a concurrent scanner observes either the
        /// pre-Initialize zero RDH or the fully-formed post-Initialize state, never a partial intermediate.
        /// <para>The inline/overflow/object decision flows directly from <paramref name="sizeInfo"/> — callers must NOT
        /// subsequently call <see cref="SetKeyIsInline"/>/<see cref="SetKeyIsOverflow"/>/<see cref="SetValueIsInline"/>/
        /// <see cref="SetValueIsOverflow"/>/<see cref="SetValueIsObject"/> on the RDH (each of those would be a separate
        /// word write, breaking atomicity).</para></summary>
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

            // Build indicator bits from sizeInfo so Initialize is the single source of truth for inline/overflow/object.
            ulong indicatorBits = 0;
            if (sizeInfo.KeyIsInline) indicatorBits |= kKeyIsInlineMask;
            if (sizeInfo.ValueIsInline)
                indicatorBits |= kValueIsInlineMask;
            else if (sizeInfo.ValueIsObject)
                indicatorBits |= kValueIsObjectMask;
            // (else: ValueIsOverflow, so both ValueIsInline and ValueIsObject are left clear)

            // Compute filler. We have all the values locally, so compute unalignedSum/alignedSum directly (without
            // calling helpers that depend on the RDH word being populated).
            var unalignedSum = Constants.FixedHeaderSize + extendedNamespaceSize + keyLength + valueLength + sizeInfo.ObjectLogPositionSize;
            var alignedSum = RoundUp(unalignedSum, Constants.kRecordAlignment);
            var totalFiller = sizeInfo.AllocatedInlineRecordSize - unalignedSum;
            var implicitFiller = alignedSum - unalignedSum;
            var explicitFiller = totalFiller > implicitFiller ? totalFiller - implicitFiller : 0;
            var fillerWords = explicitFiller >> Constants.kRecordAlignmentShift;
            if (fillerWords > MaxFillerWords)
                fillerWords = SplitOverflowingFiller(recordBaseAddress, alignedSum, explicitFiller);

            // Note: We do not set HasETag or HasExpiration here, as that may confuse ISessionFunctions into thinking those values have actually been set.
            // This is deferred to TrySetContentLengths, which should be first in the chain of calls that includes TrySetETag and/or TrySetExpiration.

            // SINGLE atomic 8-byte word write: indicator bits + FillerWords + KeyLength + ValueLength + Namespace + RecordType.
            // A concurrent scanner sees either the prior zero RDH (which routes through the GetRecordLength zero-RDH guard
            // to a 16-byte advance) or this fully-formed post-Initialize state.
            word = indicatorBits
                 | (((ulong)fillerWords & kFillerWordsValueMask) << kFillerWordsShift)
                 | (((ulong)keyLength & kKeyLengthLowBitsMask) << kKeyLengthShift)
                 | (((ulong)valueLength & kValueLengthLowBitsMask) << kValueLengthShift)
                 | ((ulong)namespaceByte << kNamespaceShift)
                 | ((ulong)recordType << kRecordTypeShift);

            namespaceAddress = recordBaseAddress + RecordInfo.Size + NamespaceOffsetInHeader;
            keyAddress = recordBaseAddress + Constants.FixedHeaderSize + extendedNamespaceSize;
            valueAddress = keyAddress + keyLength;

            return Size;
        }

        /// <summary>Prepare the header for revivification: clear filler, namespace, and recordType; preserve inline bits and lengths.
        /// This is called only when an existing allocation is being reused (revivification or retry on CAS failure), so preserves length info.
        /// <para>Atomicity: builds the cleaned RDH in a local then publishes via a single 8-byte word write (<c>word = local.word</c>).
        /// Concurrent scanners observe either the pre-revivification or post-revivification state, never an intermediate.</para></summary>
        internal void InitializeForRevivification(ref RecordSizeInfo sizeInfo, long recordBaseAddress)
        {
            Debug.Assert(KeyIsInline, "Expected Key to be inline in InitializeForRevivification");
            Debug.Assert(ValueIsInline, "Expected Value to be inline in InitializeForRevivification");
            Debug.Assert(!HasETag && !HasExpiration, "Expected no optionals in InitializeForRevivification");

            var recordLength = GetRecordLength();
            Debug.Assert(sizeInfo.AllocatedInlineRecordSize <= recordLength, "Cannot exceed previous Record size in InitializeForRevivification");

            // Build the cleaned RDH in a local: clear FillerWords + Namespace + RecordType bytes; preserve inline bits + lengths.
            var localDataHeader = this;
            localDataHeader.FillerWords = 0;
            localDataHeader.SetNamespaceByteRaw(0);
            localDataHeader.RecordType = 0;

            // Single atomic publish via word assignment through `ref this`.
            word = localDataHeader.word;

            // Ensure the AllocatedInlineRecordSize retains recordLength when LogRecord.InitializeRecord is called
            sizeInfo.AllocatedInlineRecordSize = recordLength;
            sizeInfo.SetIsRevivifiedRecord();
        }

        // ── ToString ───────────────────────────────────────────────────────────────

        /// <inheritdoc/>
        public override readonly string ToString() => ToString("na", "na");

        internal readonly string ToString(string keyString, string valueString)
        {
            if (word == 0)
                return "<empty>";
            static string bstr(bool value) => value ? "T" : "F";
            static string bstr01(bool value) => value ? "1" : "0";

            var keyLength = KeyLength;
            var valueLength = ValueLength;

            var recordLen = GetRecordLengths(out var unalignedSum, out var alignedSum, out var implicitFiller, out var explicitFiller);
            var recordLenStr = $"act: {alignedSum}, all: {recordLen}";
            var fillerLenStr = $"[i:{implicitFiller} + e:{explicitFiller}({FillerWords}w) = {implicitFiller + explicitFiller}]";

            var keyStr = KeyIsInline ? "inl" : "ovf";
            var valStr = ValueIsInline ? "inl" : (ValueIsObject ? "obj" : "ovf");

            return $"rec l:{recordLenStr}"
                 + $" | key {keyStr}/l:{keyLength} {keyString}"
                 + $" | val {valStr}/l:{valueLength}, {valueString}"
                 + $" | ETag {bstr(HasETag)}, Expir {bstr(HasExpiration)}"
                 + $" | fil {fillerLenStr} Ns:{(byte)((word >> kNamespaceShift) & ByteMask)}/x:{ExtendedNamespaceLength}, RT:{RecordType}"
                 + $" | Unused1 {bstr01(Unused1)}";
        }
    }
}