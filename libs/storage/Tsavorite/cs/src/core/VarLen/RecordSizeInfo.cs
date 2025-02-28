// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

namespace Tsavorite.core
{
    /// <summary>
    /// Struct for information about the key and the fields and their sizes in a record.
    /// </summary>
    public struct RecordSizeInfo
    {
        /// <summary>The value length and whether optional fields are present.</summary>
        public RecordFieldInfo FieldInfo;

        /// <summary>Whether the key was within the inline max key length. Set automatically by Tsavorite based on <see cref="RecordFieldInfo.KeyTotalSize"/> key size.</summary>
        public bool KeyIsInline;

        /// <summary>Whether the value was within the inline max value length.</summary>
        public bool ValueIsInline;

        /// <summary>Returns the inline length of the key (the amount it will take in the record).</summary>
        public readonly int InlineTotalKeySize => KeyIsInline ? FieldInfo.KeyTotalSize : SpanField.OverflowInlineSize;

        /// <summary>Returns the inline length of the value (the amount it will take in the record).</summary>
        public readonly int InlineTotalValueSize => ValueIsInline ? FieldInfo.ValueTotalSize : SpanField.OverflowInlineSize;

        /// <summary>Returns the data length of the key (the amount it will take in the record without length prefix).</summary>
        public readonly int KeyDataSize => KeyIsInline ? FieldInfo.KeyTotalSize - SpanField.FieldLengthPrefixSize : SpanField.OverflowInlineSize;

        /// <summary>Returns the data length of the value (the amount it will take in the record without length prefix).</summary>
        public readonly int ValueDataSize => ValueIsInline ? FieldInfo.ValueTotalSize - SpanField.FieldLengthPrefixSize : SpanField.OverflowInlineSize;

        /// <summary>The max inline value size if this is a record in the string log.</summary>
        public int MaxInlineValueSpanSize { readonly get; internal set; }

        /// <summary>The inline size of the record (in the main log). If Key and/or Value are overflow,
        /// then their contribution to inline length is just <see cref="SpanField.OverflowInlineSize"/> (a pointer with length prefix).</summary>
        public int ActualInlineRecordSize { readonly get; internal set; }

        /// <summary>The inline size of the record rounded up to <see cref="RecordInfo"/> alignment.</summary>
        public int AllocatedInlineRecordSize { readonly get; internal set; }

        /// <summary>Size to allocate for ETag if it will be included, else 0.</summary>
        public readonly int ETagSize => FieldInfo.HasETag ? LogRecord.ETagSize : 0;

        /// <summary>Size to allocate for Expiration if it will be included, else 0.</summary>
        public readonly int ExpirationSize => FieldInfo.HasExpiration ? LogRecord.ExpirationSize : 0;

        /// <summary>Size to allocate for all optional fields that will be included; possibly 0.</summary>
        public readonly int OptionalSize => ETagSize + ExpirationSize;

        /// <summary>Whether these values are set (default instances are used for Delete internally, for example).</summary>
        public readonly bool IsSet => AllocatedInlineRecordSize != 0;

        /// <summary>
        /// Called from Upsert or RMW methods for Span Values with the actual data size of the update value; ensures consistency between the Get*FieldInfo methods and the actual update methods.
        /// Usually called directly to save the cost of calculating actualDataSize twice (in Get*FieldInfo and the actual update methods).
        /// </summary>
        [Conditional("DEBUG")]
        public void AssertValueDataLength(int actualDataSize) => Debug.Assert(ValueDataSize == actualDataSize, $"Mismatch between expected value size {ValueDataSize} and actual value size {actualDataSize}");

        /// <summary>Called from Upsert or RMW methods with the final record info; ensures consistency between the Get*FieldInfo methods and the actual update methods./// </summary>
        [Conditional("DEBUG")]
        public void AssertOptionals(RecordInfo recordInfo)
        {
            Debug.Assert(FieldInfo.HasETag == recordInfo.HasETag, $"Mismatch between expected HasETag {FieldInfo.HasETag} and actual ETag {recordInfo.HasETag}");
            Debug.Assert(FieldInfo.HasExpiration == recordInfo.HasExpiration, $"Mismatch between expected HasExpiration {FieldInfo.HasExpiration} and actual HasExpiration {recordInfo.HasExpiration}");
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            return $"[{FieldInfo}] | KeyIsInln {bstr(KeyIsInline)}, ValIsInln {bstr(ValueIsInline)}, ActRecSize {ActualInlineRecordSize}, AllocRecSize {AllocatedInlineRecordSize}, OptSize {OptionalSize}";
        }
    }
}