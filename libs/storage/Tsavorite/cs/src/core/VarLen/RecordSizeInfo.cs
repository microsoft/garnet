// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>
    /// Struct for information about the key and the fields and their sizes in a record.
    /// </summary>
    public struct RecordSizeInfo
    {
        // Bit layout for 'word':
        //   Bit 0:   KeyIsInline
        //   Bit 1:   ValueIsInline
        //   Bit 2:   IsRevivifiedRecord
        //   Bits 3-5: KeyLengthBytes (max value 4)
        //   Bits 6-8: RecordLengthBytes (max value 4)
        private const int KeyIsInlineBit = 1 << 0;
        private const int ValueIsInlineBit = 1 << 1;
        private const int IsRevivifiedRecordBit = 1 << 2;
        private const int KeyLengthBytesShift = 3;
        private const int RecordLengthBytesShift = 6;
        private const int LengthBytesMask = 0x7;

        /// <summary>Packed field containing KeyIsInline, ValueIsInline, IsRevivifiedRecord, KeyLengthBytes, and RecordLengthBytes.</summary>
        internal int word;

        /// <summary>The value length and whether optional fields are present.</summary>
        public RecordFieldInfo FieldInfo;

        /// <summary>Whether the key was within the inline max key length. Set automatically by Tsavorite based on <see cref="RecordFieldInfo.KeySize"/> key size.</summary>
        public readonly bool KeyIsInline
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (word & KeyIsInlineBit) != 0;
        }

        /// <summary>Whether the value was within the inline max value length.</summary>
        public readonly bool ValueIsInline
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (word & ValueIsInlineBit) != 0;
        }

        /// <summary>Sets <see cref="KeyIsInline"/> to true.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetKeyIsInline() => word |= KeyIsInlineBit;

        /// <summary>Sets <see cref="ValueIsInline"/> to true.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetValueIsInline() => word |= ValueIsInlineBit;

        /// <summary>Number of bytes in key length; see <see cref="RecordDataHeader"/>.</summary>
        public int KeyLengthBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => (word >> KeyLengthBytesShift) & LengthBytesMask;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Debug.Assert(value is <= sizeof(int) and > 0, $"KeyLengthBytes value {value} should be the number of bytes needed to store an int value from 1 to int.MaxValue");
                word = (word & ~(LengthBytesMask << KeyLengthBytesShift)) | (value << KeyLengthBytesShift);
            }
        }

        /// <summary>Number of bytes in entire record length; see <see cref="RecordDataHeader"/>.</summary>
        public int RecordLengthBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => (word >> RecordLengthBytesShift) & LengthBytesMask;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Debug.Assert(value is <= sizeof(int) and > 0, $"RecordLengthBytes value {value} should be the number of bytes needed to store an int value from 1 to int.MaxValue");
                word = (word & ~(LengthBytesMask << RecordLengthBytesShift)) | (value << RecordLengthBytesShift);
            }
        }

        /// <summary>Whether the record allocation returned a revivified record.</summary>
        internal readonly bool IsRevivifiedRecord
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (word & IsRevivifiedRecordBit) != 0;
        }

        /// <summary>Sets <see cref="IsRevivifiedRecord"/> to true.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetIsRevivifiedRecord() => word |= IsRevivifiedRecordBit;

        /// <summary>Whether the value was specified to be an object.</summary>
        public readonly bool ValueIsObject => FieldInfo.ValueIsObject;

        /// <summary>Whether the key is an overflow allocation.</summary>
        public readonly bool KeyIsOverflow => !KeyIsInline;

        /// <summary>Whether the value is an overflow allocation.</summary>
        public readonly bool ValueIsOverflow => !ValueIsInline && !FieldInfo.ValueIsObject;

        /// <summary>Returns the inline length of the key (the amount it will take in the record).</summary>
        public readonly int InlineKeySize => KeyIsInline ? FieldInfo.KeySize : ObjectIdMap.ObjectIdSize;

        /// <summary>Returns the inline length of the value (the amount it will take in the record).</summary>
        public readonly int InlineValueSize => ValueIsInline ? FieldInfo.ValueSize : ObjectIdMap.ObjectIdSize;

        /// <summary>Returns whether both the key and value are inline (no overflow or object).</summary>
        public readonly bool RecordIsInline => (word & (KeyIsInlineBit | ValueIsInlineBit)) == (KeyIsInlineBit | ValueIsInlineBit);

        /// <summary>The max inline value size if this is a record in the string log.</summary>
        public int MaxInlineValueSize { readonly get; internal set; }

        /// <summary>The inline size of the record (in the main log). If Key and/or Value are overflow (or value is Object),
        /// then their contribution to inline length is just <see cref="ObjectIdMap.ObjectIdSize"/>.</summary>
        public int ActualInlineRecordSize { readonly get; internal set; }

        /// <summary>The inline size of the record rounded up to <see cref="RecordInfo"/> alignment.</summary>
        public int AllocatedInlineRecordSize { readonly get; internal set; }

        /// <summary>Size to allocate for the 'long' offset into the Object log if this record will have objects or overflow, else 0.</summary>
        public readonly int ObjectLogPositionSize => RecordIsInline ? 0 : LogRecord.ObjectLogPositionSize;

        /// <summary>Size to allocate for all optional fields that will be included; possibly 0.</summary>
        public readonly int OptionalSize => FieldInfo.eTagSize + FieldInfo.expirationSize + ObjectLogPositionSize;

        /// <summary>Whether these values are set (default instances are used for Delete internally, for example).</summary>
        public readonly bool IsSet => AllocatedInlineRecordSize != 0;

        /// <summary>
        /// Calculate the Record sizes based on the given <paramref name="keySize"/> and <paramref name="valueSize"/> sizes, which are adjusted for inline vs. overflow/object.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CalculateSizes(int keySize, int valueSize)
        {
            if (FieldInfo.ExtendedNamespaceSize > sbyte.MaxValue)
                ThrowTsavoriteException($"FieldInfo.ExtendedNamespaceSize ({FieldInfo.ExtendedNamespaceSize}) exceeds max allowable ({sbyte.MaxValue})");

            // Calculate full used record size. Use the full possible RecordLengthBytes initially to reserve space in the record for it;
            // then replace it with the exact size needed and update ActualInlineRecordSize.
            KeyLengthBytes = RecordDataHeader.GetByteCount(keySize);
            const int initialRecordLengthBytes = sizeof(int);
            ActualInlineRecordSize = RecordInfo.Size + RecordDataHeader.NumIndicatorBytes + KeyLengthBytes + initialRecordLengthBytes
                            + FieldInfo.ExtendedNamespaceSize + keySize + valueSize + OptionalSize;

            // Adjust to the actual record length bytes needed (must include roundup).
            var allocatedSize = RoundUp(ActualInlineRecordSize, Constants.kRecordAlignment);
            RecordLengthBytes = RecordDataHeader.GetByteCount(allocatedSize);
            ActualInlineRecordSize -= initialRecordLengthBytes - RecordLengthBytes;

            // Finally, calculate allocated size (record-aligned). Round up again as our subtraction might have knocked us down
            // by one kRecordAlignment. This may leave us with one extra byte of RecordLengthBytes if for example ActualInlineRecordSize
            // went down from 257 to 255, so recalculate the size. This cannot reduce RecordLengthBytes by more than 1.
            AllocatedInlineRecordSize = RoundUp(ActualInlineRecordSize, Constants.kRecordAlignment);
            if (AllocatedInlineRecordSize != allocatedSize)
                RecordLengthBytes = RecordDataHeader.GetByteCount(AllocatedInlineRecordSize);
        }

        /// <summary>
        /// Called from Upsert or RMW methods for Span Values with the actual data size of the update value; ensures consistency between the Get*FieldInfo methods and the actual update methods.
        /// Usually called directly to save the cost of calculating actualDataSize twice (in Get*FieldInfo and the actual update methods).
        /// </summary>
        [Conditional("DEBUG")]
        public static void AssertValueDataLength(int dataSize, in RecordSizeInfo sizeInfo)
        {
            Debug.Assert(sizeInfo.FieldInfo.ValueSize == dataSize, $"Mismatch between expected value size {sizeInfo.FieldInfo.ValueSize} and actual value size {dataSize}");
        }

        /// <summary>Called from Upsert or RMW methods with the final record info; ensures consistency between the Get*FieldInfo methods and the actual update methods./// </summary>
        [Conditional("DEBUG")]
        public void AssertOptionalsIfSet(RecordInfo recordInfo, bool checkETag = true, bool checkExpiration = true)
        {
            if (!IsSet)
                return;
            if (checkETag)
                Debug.Assert(FieldInfo.HasETag == recordInfo.HasETag, $"Mismatch between expected HasETag {FieldInfo.HasETag} and actual ETag {recordInfo.HasETag}");
            if (checkExpiration)
                Debug.Assert(FieldInfo.HasExpiration == recordInfo.HasExpiration, $"Mismatch between expected HasExpiration {FieldInfo.HasExpiration} and actual HasExpiration {recordInfo.HasExpiration}");
        }

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            var keyString = KeyIsInline ? "inl" : "ovf";
            var valString = ValueIsInline ? "inl" : (ValueIsObject ? "obj" : "ovf");
            return $"[{FieldInfo}] | Key::{keyString}, Val::{valString}, ActRecSize {ActualInlineRecordSize}, AllocRecSize {AllocatedInlineRecordSize}, OptSize {OptionalSize}";
        }
    }
}