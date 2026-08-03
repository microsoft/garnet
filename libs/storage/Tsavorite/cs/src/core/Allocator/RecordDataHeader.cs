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
    /// <para>Disk-write paths (<see cref="LogRecord.SetObjectLogPositionAndLengthHints"/>) write the low <see cref="kKeyLengthBits"/> /
    /// <see cref="kValueLengthBits"/> bits of the on-disk overflow/object length into the RDH KeyLength/ValueLength field as a read-size
    /// hint (the authoritative length comes from the object-log stream framing); a length at or above the field maximum is capped at that
    /// maximum sentinel. The property getters still return <see cref="ObjectIdMap.ObjectIdSize"/> for non-inline keys/values regardless of
    /// the raw hint, so the runtime "non-inline → property returns ObjectIdSize" invariant holds. (Databases written before this format use
    /// the legacy split encoding, read via <see cref="LogRecord.GetObjectLogRecordStartPositionAndLengths_v21"/>: RDH low bits plus the
    /// next 32 bits in the objectId slot at keyAddress/valueAddress.)</para>
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

        /// <summary>Largest value that can be stored in <see cref="NamespaceByte"/>, larger values require extended namespace space.</summary>
        public const byte MaximumSingleByteNamespaceValue = (1 << ExtendedNamespaceIndicatorBit) - 1;

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

        /// <summary>True if the raw KeyLength field holds the sentinel (its maximum value): the actual overflow key length is at or above
        /// the field maximum, so it is carried out of band (a stream length prefix on the network, or a chunk header on disk).</summary>
        public readonly bool KeyLengthIsSentinel => GetKeyLengthRaw() == (int)kKeyLengthLowBitsMask;

        /// <summary>True if the raw ValueLength field holds the sentinel (its maximum value): the actual overflow/object value length is at
        /// or above the field maximum, so it is carried out of band (a stream length prefix on the network, or a chunk header on disk).</summary>
        public readonly bool ValueLengthIsSentinel => GetValueLengthRaw() == (int)kValueLengthLowBitsMask;

        /// <summary>The raw KeyLength field value (the read-size hint; the exact overflow key length when below the sentinel). The
        /// <see cref="KeyLength"/> property returns ObjectIdSize for an overflow key, so use this to read the encoded hint.</summary>
        public readonly int KeyLengthHint => GetKeyLengthRaw();

        /// <summary>The raw ValueLength field value (the read-size hint; the exact overflow/object value length when below the sentinel).
        /// The <see cref="ValueLength"/> property returns ObjectIdSize for a non-inline value, so use this to read the encoded hint.</summary>
        public readonly int ValueLengthHint => GetValueLengthRaw();

        /// <summary>Sets the KeyLength/ValueLength fields to the out-of-line read-size hints: the exact length when below the field sentinel,
        /// else the sentinel (the field maximum). Inline fields and absent components are left unchanged.</summary>
        /// <param name="keyActualLength">Actual overflow key length (applied only when the key is overflow).</param>
        /// <param name="valueActualLength">Actual overflow value or serialized object length (applied only when the value is out of line).</param>
        public void SetOverflowLengthHints(int keyActualLength, long valueActualLength)
        {
            if (KeyIsOverflow)
                KeyLength = keyActualLength >= (int)kKeyLengthLowBitsMask ? (int)kKeyLengthLowBitsMask : keyActualLength;
            if (ValueIsOverflow || ValueIsObject)
                ValueLength = valueActualLength >= (long)kValueLengthLowBitsMask ? (int)kValueLengthLowBitsMask : (int)valueActualLength;
        }

        // ── Flush (v2.2) non-inline ValueLength encoding ─────────────────────────────────────────────────────────
        // For a non-inline value, the FLUSH object-log format encodes the read extent into the 24-bit ValueLength field
        // (the ValueLength property still returns ObjectIdSize; read the encoding via GetValueLengthRaw()):
        //   Object value, bit 23 set   -> Chunked object: bits 0-11 = full-buffer count, bits 12-21 = final-buffer 4KB-page count
        //                                 (the read-ahead extent; the object stream is dense with no per-chunk framing and the
        //                                 deserializer self-terminates). Used when the serialized length is >= one buffer.
        //   Object value, bit 23 clear -> Headerless object: bits 0-22 = exact serialized length (< one buffer).
        //   Overflow value < sentinel  -> Exact byte length in the full 24-bit field.
        //   Overflow value == sentinel -> Length is at/above the field maximum; the full length precedes the bytes in a leading
        //                                 ChunkHeader (symmetric with a >= sentinel overflow KEY, whose KeyLength field is likewise the
        //                                 sentinel). The reader reads the header and extends the read-ahead (ReadOverflowHeaderLengthAndExtend).
        // Decode via DecodeFlushValueExtent (branches on ValueIsObject). Reader (ObjectLogReader) selects overflow-vs-object from the RDH.
        // Bit 22 (kFlushOverflowHeaderBit) and EncodeFlushOverflowHeader are RESERVED for a future precise first-read-hint for a headered
        // overflow value (currently the sentinel path reads one buffer up front, then extends); they are not used or read today.
        // The network (migration/replication) path uses SetOverflowLengthHints (sentinel-capped) instead; see
        // website/docs/dev/objectlog-serialization.md.
        internal const int kFlushChunkedObjectBit = 23;
        internal const uint kFlushChunkedObjectMask = 1u << kFlushChunkedObjectBit;
        internal const int kFlushOverflowHeaderBit = 22;
        internal const uint kFlushOverflowHeaderMask = 1u << kFlushOverflowHeaderBit;

        internal const int kFlushBufferCountBits = 12;                                   // bits 0-11
        internal const uint kFlushBufferCountMask = (1u << kFlushBufferCountBits) - 1;
        internal const int kFlushFinalPageShift = kFlushBufferCountBits;                 // bits 12-21
        internal const int kFlushFinalPageBits = 10;
        internal const uint kFlushFinalPageMask = (1u << kFlushFinalPageBits) - 1;
        internal const int kFlushReadHintBits = 22;                                      // bits 0-21
        internal const uint kFlushReadHintMask = (1u << kFlushReadHintBits) - 1;

        internal const int kFlushMaxBufferCount = (int)kFlushBufferCountMask;            // 4095 buffers (16 GB @ 4 MB)
        internal const int kFlushMaxFinalPages = (int)kFlushFinalPageMask;              // 1023 pages (4 MB @ 4 KB)
        internal const int kFlushMaxReadHint = (int)kFlushReadHintMask;                 // 4194303 bytes (< 4 MB)

        /// <summary>Encode a chunked (multi-buffer) object value's read extent into the 24-bit ValueLength field: full-buffer count +
        /// final-buffer 4 KB-page count.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint EncodeFlushChunkedObject(int bufferCount, int finalBufferPages)
        {
            Debug.Assert((uint)bufferCount <= kFlushBufferCountMask, $"bufferCount {bufferCount} exceeds {kFlushBufferCountBits}-bit max");
            Debug.Assert((uint)finalBufferPages <= kFlushFinalPageMask, $"finalBufferPages {finalBufferPages} exceeds {kFlushFinalPageBits}-bit max");
            return kFlushChunkedObjectMask | ((uint)finalBufferPages << kFlushFinalPageShift) | (uint)bufferCount;
        }

        /// <summary>Encode an overflow value that has a single leading header: bits 0-21 hold the first-buffer read hint (at most one buffer).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint EncodeFlushOverflowHeader(int firstReadHint)
        {
            Debug.Assert((uint)firstReadHint <= kFlushReadHintMask, $"firstReadHint {firstReadHint} exceeds {kFlushReadHintBits}-bit max");
            return kFlushOverflowHeaderMask | (uint)firstReadHint;
        }

        /// <summary>Encode a headerless small value: bits 0-21 hold the exact byte length (must be less than one buffer).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint EncodeFlushHeaderless(int exactLength)
        {
            Debug.Assert((uint)exactLength <= kFlushReadHintMask, $"exactLength {exactLength} exceeds {kFlushReadHintBits}-bit max");
            return (uint)exactLength;
        }

        /// <summary>True if the flush ValueLength encoding is a chunked (multi-buffer) object.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool FlushValueIsChunkedObject(uint encoded) => (encoded & kFlushChunkedObjectMask) != 0;
        /// <summary>Full-buffer count of a chunked object (valid only when <see cref="FlushValueIsChunkedObject"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int FlushChunkedBufferCount(uint encoded) => (int)(encoded & kFlushBufferCountMask);
        /// <summary>Final-buffer 4 KB-page count of a chunked object (valid only when <see cref="FlushValueIsChunkedObject"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int FlushChunkedFinalPages(uint encoded) => (int)((encoded >> kFlushFinalPageShift) & kFlushFinalPageMask);
        /// <summary>True if a non-chunked value has a single leading overflow header (valid only when not <see cref="FlushValueIsChunkedObject"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool FlushValueHasOverflowHeader(uint encoded) => (encoded & kFlushOverflowHeaderMask) != 0;
        /// <summary>The first-buffer read hint (overflow-with-header) or the exact length (headerless): bits 0-21.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int FlushValueReadHintOrExact(uint encoded) => (int)(encoded & kFlushReadHintMask);

        /// <summary>Size (bytes) of a chunked object's final-buffer page unit; the final-buffer page count in the chunked encoding is measured in these.</summary>
        internal const int kFlushFinalPageSize = 1 << 12;                                // 4 KB

        /// <summary>Encode a non-inline object value's serialized length into the 24-bit ValueLength field for the FLUSH format: the exact
        /// length when it fits headerless (&lt; one buffer), else the chunked (multi-buffer) full-buffer + final-4KB-page extent.</summary>
        internal static uint EncodeFlushObjectValue(long serializedLength)
        {
            if (serializedLength <= kFlushMaxReadHint)
                return EncodeFlushHeaderless((int)serializedLength);

            // Chunked: round the extent up to a 4 KB page and split into full-BufferSize buffers plus a final-buffer page count. The reader
            // sizes its read-ahead from this (>= serializedLength, over by < 4 KB); the deserializer self-terminates at the exact length.
            var extent = RoundUp(serializedLength, kFlushFinalPageSize);
            var fullBuffers = (int)(extent / IStreamBuffer.BufferSize);
            var finalPages = (int)((extent - (long)fullBuffers * IStreamBuffer.BufferSize) / kFlushFinalPageSize);
            if (fullBuffers > kFlushMaxBufferCount)
                throw new TsavoriteException($"Serialized object length {serializedLength} exceeds the {(long)kFlushMaxBufferCount * IStreamBuffer.BufferSize}-byte chunked-object encoding limit");
            return EncodeFlushChunkedObject(fullBuffers, finalPages);
        }

        /// <summary>Decode a FLUSH ValueLength encoding to the read-ahead byte extent: the chunked object full-buffer + final-page extent for
        /// a chunked object, else the raw field — the exact length for a headerless value, or the sentinel for an overflow value with a
        /// leading header (whose true length the reader learns from that header and then extends by).</summary>
        internal static ulong DecodeFlushValueExtent(uint encoded, bool valueIsObject)
        {
            if (valueIsObject && FlushValueIsChunkedObject(encoded))
                return (ulong)FlushChunkedBufferCount(encoded) * (ulong)IStreamBuffer.BufferSize
                     + (ulong)FlushChunkedFinalPages(encoded) * kFlushFinalPageSize;
            return encoded;
        }

        /// <summary>FLUSH-format variant of <see cref="SetOverflowLengthHints"/>: sets the KeyLength field to the sentinel-capped overflow
        /// key length, and the ValueLength field to the v2.2 12-bit out-of-line encoding (headerless exact size, or a leading-ChunkHeader +
        /// 4 KB-page-count/sentinel read hint) for an out-of-line value -- for BOTH overflow and object values.</summary>
        /// <param name="keyActualLength">Actual overflow key length (applied only when the key is overflow).</param>
        /// <param name="valueActualLength">Actual overflow value or serialized object DATA length (applied only when the value is out of line).</param>
        /// <param name="valueAlignmentPadding">Overflow O_DIRECT alignment padding (overflow value only); included in the overflow extent.</param>
        /// <param name="valueObjectExtent">The object value's total on-disk extent (prefix + 8-align padding + ChunkHeaders + data); required
        ///   for an object value (its page-count hint). For a headerless object (data length &lt;= cutoff) it equals the data length.</param>
        public void SetObjectLogLengthHints(int keyActualLength, long valueActualLength, int valueAlignmentPadding = 0, long valueObjectExtent = 0)
        {
            if (KeyIsOverflow)
                KeyLength = keyActualLength >= (int)kKeyLengthLowBitsMask ? (int)kKeyLengthLowBitsMask : keyActualLength;
            if (ValueIsObject)
            {
                // Object value uses the same v2.2 12-bit encoding as overflow: headerless exact size when the DATA length <= cutoff, else a
                // page-count (from the on-disk extent) with the has-header bit set. The writer supplies the extent (prefix + padding + per-chunk
                // ChunkHeaders + data). A headerless object's extent equals its data length.
                ValueLength = (int)EncodeFlushOutOfLineValue(valueActualLength, valueObjectExtent);
            }
            else if (ValueIsOverflow)
            {
                // Overflow value uses the v2.2 encoding: headerless exact size when <= 1023, else a leading ChunkHeader + a 4 KB-page-count
                // (or sentinel) read hint. The on-disk extent = data (headerless) else ChunkHeader.TotalSize + DMA alignment padding + data;
                // the padding is 0 on the buffered write path and the O_DIRECT padding the writer applied on the DMA path.
                var extent = valueActualLength <= kOutOfLineExactSizeCutoff
                    ? valueActualLength
                    : ChunkHeader.TotalSize + valueAlignmentPadding + valueActualLength;
                ValueLength = (int)EncodeFlushOutOfLineValue(valueActualLength, extent);
            }
        }

        // ── Flush (v2.2) out-of-line VALUE length encoding (low 12 bits of the ValueLength field) ─────────────────────
        // Supersedes the bit-23-chunked / bit-22-overflow-header / 24-bit-exact scheme above (which is being retired as
        // the writer/reader are rewired). Only the low 12 bits of the (physically 24-bit) ValueLength field are used for
        // an out-of-line (overflow or object) value; the ValueLength property still returns ObjectIdSize, so the encoding
        // is read via GetValueLengthRaw():
        //   bit 11 (isExactSize) set   -> bits 0-9 are the EXACT byte length (0..1023); NO ChunkHeader precedes the value.
        //   bit 11 clear               -> bits 0-9 are the count of 4 KB pages spanned by the value's total on-disk extent
        //                                 (leading ChunkHeader + any DMA alignment padding + data); a ChunkHeader precedes
        //                                 the value (bit 10 set). kOutOfLinePageSentinel (1023) is the sentinel: the extent
        //                                 is at/above 1023*4 KB (~4 MB), so the reader fetches in 4 MB blocks and learns the
        //                                 exact length(s) from the ChunkHeader(s) -- overflow: full length; object: per-chunk
        //                                 length + ContinuationFlag.
        //   bit 10 (hasHeader)         -> a leading ChunkHeader precedes the value bytes. Always set when isExactSize is clear.
        // A value <= kOutOfLineExactSizeCutoff (1023) bytes is encoded headerless (isExactSize); a longer value is encoded as
        // a page count (+ header). The cutoff is 1023 because that is the largest exact byte length the 10-bit payload holds;
        // above it only a page count fits, so the exact length must move into the ChunkHeader. This keeps small objects/overflow
        // values free of a header's space cost while giving a precise (no 4 MB over-read) initial read size for larger values.
        // KeyLength keeps its own 10-bit sentinel (KeyLengthIsSentinel); keys are never chunked and, being <= 1023 at the
        // sentinel (<< MaxCopySpanLen), always carry a ChunkHeader when DMA-padded, so no separate has-header bit is needed.
        internal const int kOutOfLinePayloadBits = 10;                                    // bits 0-9
        internal const uint kOutOfLinePayloadMask = (1u << kOutOfLinePayloadBits) - 1;    // 1023
        internal const int kOutOfLinePageSentinel = (int)kOutOfLinePayloadMask;           // 1023 pages -> read in 4 MB blocks
        internal const int kFlushValueHasHeaderBit = 10;
        internal const uint kFlushValueHasHeaderMask = 1u << kFlushValueHasHeaderBit;
        internal const int kFlushValueIsExactSizeBit = 11;
        internal const uint kFlushValueIsExactSizeMask = 1u << kFlushValueIsExactSizeBit;

        /// <summary>Largest out-of-line value length (bytes) encoded headerless (isExactSize); a longer value gets a ChunkHeader
        /// and a 4 KB-page-count encoding. 1023 = the largest value the 10-bit exact-size payload can hold.</summary>
        internal const int kOutOfLineExactSizeCutoff = (int)kOutOfLinePayloadMask;        // 1023

        /// <summary>Size (bytes) of the 4 KB page unit used by the page-count encoding.</summary>
        internal const int kFlushPageSize = 1 << 12;                                      // 4 KB

        /// <summary>Largest exactly-representable page count (one below the sentinel); its read-ahead extent is
        /// 1022*4 KB = 4 MB - 8 KB, just under one 4 MB read buffer -- which is why 1023 is reserved as the sentinel.</summary>
        internal const int kFlushMaxExactPageCount = kOutOfLinePageSentinel - 1;          // 1022

        /// <summary>Encode an out-of-line (overflow or object) value's on-disk extent into the low 12 bits of the ValueLength
        /// field. A value at/below <see cref="kOutOfLineExactSizeCutoff"/> is encoded as its exact byte length (headerless);
        /// a longer value is encoded as the count of 4 KB pages its total on-disk extent spans, with the has-header bit set
        /// (the exact length is carried in a leading <see cref="ChunkHeader"/>). A page count at/above the sentinel is clamped
        /// to the sentinel, telling the reader to fetch in 4 MB blocks.</summary>
        /// <param name="dataLength">The value's data length in bytes (overflow byte count or serialized object length).</param>
        /// <param name="totalOnDiskExtent">The value's total on-disk extent in bytes: leading ChunkHeader + any alignment
        ///   padding + data. Ignored for the headerless (exact) case.</param>
        internal static uint EncodeFlushOutOfLineValue(long dataLength, long totalOnDiskExtent)
        {
            Debug.Assert(dataLength >= 0, $"dataLength {dataLength} must be non-negative");
            if (dataLength <= kOutOfLineExactSizeCutoff)
                return kFlushValueIsExactSizeMask | (uint)dataLength;                     // headerless, exact byte size
            var pageCount = (int)((totalOnDiskExtent + kFlushPageSize - 1) / kFlushPageSize);
            if (pageCount >= kOutOfLinePageSentinel)
                pageCount = kOutOfLinePageSentinel;
            Debug.Assert(pageCount > 0, $"page count {pageCount} must be positive for a headered value (dataLength {dataLength}, extent {totalOnDiskExtent})");
            return kFlushValueHasHeaderMask | (uint)pageCount;                            // page count + header
        }

        /// <summary>True if the out-of-line value is encoded as an exact byte length (headerless).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool FlushValueIsExactSize(uint encoded) => (encoded & kFlushValueIsExactSizeMask) != 0;

        /// <summary>True if a leading <see cref="ChunkHeader"/> precedes the out-of-line value bytes.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool FlushValueHasHeader(uint encoded) => (encoded & kFlushValueHasHeaderMask) != 0;

        /// <summary>The exact byte length of a headerless value (valid only when <see cref="FlushValueIsExactSize"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int FlushValueExactByteSize(uint encoded) => (int)(encoded & kOutOfLinePayloadMask);

        /// <summary>The 4 KB-page count of a headered value (valid only when not <see cref="FlushValueIsExactSize"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int FlushValuePageCount(uint encoded) => (int)(encoded & kOutOfLinePayloadMask);

        /// <summary>True if a headered value's page count is the sentinel (extent at/above 1023*4 KB): the reader fetches in
        /// 4 MB blocks and learns the exact length(s) from the ChunkHeader(s).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool FlushValuePageCountIsSentinel(uint encoded) => (int)(encoded & kOutOfLinePayloadMask) == kOutOfLinePageSentinel;

        /// <summary>The initial read-ahead extent (bytes) for an out-of-line value: the exact byte size for a headerless value,
        /// the page count * 4 KB for a headered value below the sentinel, or one 4 MB read buffer for a sentinel page count.
        /// The header/padding are included in a headered value's page count, so this covers the whole framing for a
        /// below-sentinel value; a sentinel value's true length comes from its ChunkHeader(s) and the reader extends.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong DecodeFlushValueInitialReadExtent(uint encoded)
        {
            if (FlushValueIsExactSize(encoded))
                return (ulong)(uint)FlushValueExactByteSize(encoded);
            var pageCount = FlushValuePageCount(encoded);
            if (pageCount == kOutOfLinePageSentinel)
                return (ulong)IStreamBuffer.BufferSize;                                   // 4 MB block
            return (ulong)(uint)pageCount * kFlushPageSize;
        }

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

        /// <summary>Get the extended namespace length and extended namespace data address.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (int namespaceLength, long namespaceAddress) GetExtendedNamespaceInfo(long recordBaseAddress)
        => ((byte)((word >> kNamespaceShift) & ByteMask) & ~(1 << ExtendedNamespaceIndicatorBit), recordBaseAddress + Constants.FixedHeaderSize);

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

            // Namespace can be in two different places depending on if we're using the extended namespace space...
            if (extendedNamespaceSize == 0)
            {
                // In a fix position in DataHeader
                namespaceAddress = recordBaseAddress + RecordInfo.Size + NamespaceOffsetInHeader;
            }
            else
            {
                // Before the key
                namespaceAddress = recordBaseAddress + Constants.FixedHeaderSize;
            }

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