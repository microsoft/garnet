// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Struct for information about the key and the fields and their sizes in a record.
    /// </summary>
    public struct RecordSizeInfo
    {
        /// <summary>The value length and whether optional fields are present.</summary>
        internal RecordFieldInfo FieldInfo;

        /// <summary>Whether the key was long enough to overflow the inline max length.</summary>
        internal bool KeyIsOverflow;

        /// <summary>Whether the value was long enough to overflow the inline max length.</summary>
        internal bool ValueIsOverflow;

        /// <summary>The inline size of the record (in the main log). If Key and/or Value are overflow,
        /// then their contribution to inline length is just <see cref="SpanField.OverflowInlineSize"/> (a pointer with length prefix).</summary>
        internal int ActualInlineRecordSize;

        /// <summary>The inline size of the record rounded up to <see cref="RecordInfo"/> alignment.</summary>
        internal int AllocatedInlineRecordSize;

        /// <summary>Size to allocate for ETag if it will be included, else 0.</summary>
        internal readonly int ETagSize => FieldInfo.HasETag ? LogRecord.ETagSize : 0;

        /// <summary>Size to allocate for Expiration if it will be included, else 0.</summary>
        internal readonly int ExpirationSize => FieldInfo.HasExpiration ? LogRecord.ExpirationSize : 0;

        /// <summary>Size to allocate for all optional fields that will be included; possibly 0.</summary>
        internal readonly int OptionalSize => ETagSize + ExpirationSize;

        /// <summary>Shortcut to see if either key or value is overflow.</summary>
        internal bool HasOverflow => KeyIsOverflow || ValueIsOverflow;
    }
}
