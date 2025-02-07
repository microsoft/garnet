// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.IO;

namespace Tsavorite.core
{
    /// <summary>
    /// Struct for information about the key and the fields and their sizes in a record.
    /// </summary>
    public struct RecordSizeInfo
    {
        /// <summary>The value length and whether optional fields are present.</summary>
        public RecordFieldInfo FieldInfo;

        /// <summary>Whether the key was long enough to overflow the inline max length.</summary>
        public bool KeyIsOverflow;

        /// <summary>Whether the value was long enough to overflow the inline max length.</summary>
        public bool ValueIsOverflow;

        /// <summary>Returns the inline length of the key (the amount it will take in the record).</summary>
        public readonly int InlineKeySize => KeyIsOverflow ? SpanField.OverflowInlineSize : FieldInfo.KeySize;

        /// <summary>Returns the inline length of the value (the amount it will take in the record).</summary>
        public readonly int InlineValueSize => ValueIsOverflow ? SpanField.OverflowInlineSize : FieldInfo.ValueSize;

        /// <summary>Returns the data length of the key (the amount it will take in the record without length prefix).</summary>
        public readonly int KeyDataSize => FieldInfo.KeySize - SpanField.FieldLengthPrefixSize;

        /// <summary>Returns the data length of the value (the amount it will take in the record without length prefix).</summary>
        public readonly int ValueDataSize => FieldInfo.ValueSize - SpanField.FieldLengthPrefixSize;

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

        /// <summary>Shortcut to see if either key or value is overflow.</summary>
        public bool HasOverflow => KeyIsOverflow || ValueIsOverflow;

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
            return $"[{FieldInfo}] | KeyIsOF {bstr(KeyIsOverflow)}, ValIsOF {bstr(ValueIsOverflow)}, ActRecSize {ActualInlineRecordSize}, AllocRecSize {AllocatedInlineRecordSize}, OptSize {OptionalSize}, HasOF {bstr(HasOverflow)}";
        }
    }
}