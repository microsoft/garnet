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

        /// <summary>Whether the key was within the inline max key length. Set automatically by Tsavorite based on <see cref="RecordFieldInfo.KeyDataSize"/> key size.</summary>
        public bool KeyIsInline;

        /// <summary>Whether the value was within the inline max value length.</summary>
        public bool ValueIsInline;

        /// <summary>Whether the value was specified to be an object.</summary>
        public readonly bool ValueIsObject => FieldInfo.ValueIsObject;

        /// <summary>Whether the key is an overflow allocation.</summary>
        public readonly bool KeyIsOverflow => !KeyIsInline;

        /// <summary>Whether the value is an overflow allocation.</summary>
        public readonly bool ValueIsOverflow => !ValueIsInline && !ValueIsObject;

        /// <summary>Returns the inline length of the key (the amount it will take in the record).</summary>
        public readonly int InlineTotalKeySize => KeyIsInline ? FieldInfo.KeyDataSize : ObjectIdMap.ObjectIdSize;

        /// <summary>Returns the inline length of the value (the amount it will take in the record).</summary>
        public readonly int InlineTotalValueSize => ValueIsInline ? FieldInfo.ValueDataSize + LogField.InlineLengthPrefixSize : ObjectIdMap.ObjectIdSize;

        /// <summary>The max inline value size if this is a record in the string log.</summary>
        public int MaxInlineValueSpanSize { readonly get; internal set; }

        /// <summary>The inline size of the record (in the main log). If Key and/or Value are overflow (or value is Object),
        /// then their contribution to inline length is just <see cref="ObjectIdMap.ObjectIdSize"/>.</summary>
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
        public static void AssertValueDataLength(int dataSize, ref RecordSizeInfo sizeInfo)
        {
            Debug.Assert(sizeInfo.FieldInfo.ValueDataSize == dataSize, $"Mismatch between expected value size {sizeInfo.FieldInfo.ValueDataSize} and actual value size {dataSize}");
        }

        /// <summary>Called from Upsert or RMW methods with the final record info; ensures consistency between the Get*FieldInfo methods and the actual update methods./// </summary>
        [Conditional("DEBUG")]
        public void AssertOptionals(RecordInfo recordInfo)
        {
            Debug.Assert(FieldInfo.HasETag == recordInfo.HasETag, $"Mismatch between expected HasETag {FieldInfo.HasETag} and actual ETag {recordInfo.HasETag}");
            Debug.Assert(FieldInfo.HasExpiration == recordInfo.HasExpiration, $"Mismatch between expected HasExpiration {FieldInfo.HasExpiration} and actual HasExpiration {recordInfo.HasExpiration}");
        }

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            return $"[{FieldInfo}] | KeyIsInl {bstr(KeyIsInline)}, ValIsInl {bstr(ValueIsInline)}, ValIsObj {bstr(ValueIsObject)}, ActRecSize {ActualInlineRecordSize}, AllocRecSize {AllocatedInlineRecordSize}, OptSize {OptionalSize}";
        }
    }
}