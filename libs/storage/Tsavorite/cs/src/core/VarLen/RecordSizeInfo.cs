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
        /// <summary>The value length and whether optional fields are present.</summary>
        public RecordFieldInfo FieldInfo;

        /// <summary>Whether the key was within the inline max key length. Set automatically by Tsavorite based on <see cref="RecordFieldInfo.KeySize"/> key size.</summary>
        public bool KeyIsInline;

        /// <summary>Whether the value was within the inline max value length.</summary>
        public bool ValueIsInline;

        /// <summary>Number of bytes in key length; see <see cref="RecordDataHeader"/>.</summary>
        public int KeyLengthBytes;

        /// <summary>Number of bytes in entire record length; see <see cref="RecordDataHeader"/>.</summary>
        public int RecordLengthBytes;

        /// <summary>Whether the record allocation returned a revivified record.</summary>
        internal bool IsRevivifiedRecord;

        /// <summary>Whether the value was specified to be an object.</summary>
        public readonly bool ValueIsObject => FieldInfo.ValueIsObject;

        /// <summary>Whether the key is an overflow allocation.</summary>
        public readonly bool KeyIsOverflow => !KeyIsInline;

        /// <summary>Whether the value is an overflow allocation.</summary>
        public readonly bool ValueIsOverflow => !ValueIsInline && !ValueIsObject;

        /// <summary>Returns the inline length of the key (the amount it will take in the record).</summary>
        public readonly int InlineKeySize => KeyIsInline ? FieldInfo.KeySize : ObjectIdMap.ObjectIdSize;

        /// <summary>Returns the inline length of the value (the amount it will take in the record).</summary>
        public readonly int InlineValueSize => ValueIsInline ? FieldInfo.ValueSize : ObjectIdMap.ObjectIdSize;

        /// <summary>Returns the inline length of the value (the amount it will take in the record).</summary>
        public readonly bool RecordIsInline => KeyIsInline && ValueIsInline;

        /// <summary>The max inline value size if this is a record in the string log.</summary>
        public int MaxInlineValueSize { readonly get; internal set; }

        /// <summary>The inline size of the record (in the main log). If Key and/or Value are overflow (or value is Object),
        /// then their contribution to inline length is just <see cref="ObjectIdMap.ObjectIdSize"/>.</summary>
        public int ActualInlineRecordSize { readonly get; internal set; }

        /// <summary>The inline size of the record rounded up to <see cref="RecordInfo"/> alignment.</summary>
        public int AllocatedInlineRecordSize { readonly get; internal set; }

        /// <summary>Size to allocate for Expiration if it will be included, else 0.</summary>
        public readonly int ObjectLogPositionSize => objectLogPositionSize;
        byte objectLogPositionSize;

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

            objectLogPositionSize = (byte)((KeyIsInline && ValueIsInline) ? 0 : LogRecord.ObjectLogPositionSize);

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