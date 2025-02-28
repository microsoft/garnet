// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Struct for information about fields (Value and optional fields) of a record, to determine required allocation size.
    /// </summary>
    public struct RecordFieldInfo
    {
        public const int ValueObjectIdSize = -1;
        public const int InlineObjectValueSizeMaxPageDivisor = 8;

        /// <summary>
        /// The length of the key for the new record, including the length prefix. May become overflow; see <see cref="RecordSizeInfo.InlineTotalKeySize"/>
        /// </summary>
        public int KeyTotalSize;

        /// <summary>
        /// The length of the value for the new record. Its behavior varies between the String and Object stores:
        /// <list type="bullet">
        ///     <item>String store: It is the length of the Span, including any length prefix, and may become overflow; see <see cref="RecordSizeInfo.InlineTotalValueSize"/></item>
        ///     <item>Object store: It should be either <see cref="ValueObjectIdSize"/> to hold an ObjectId, or for an optimized inline Span value it is the length.
        ///     This length will not be treated as an "overflow" as that is specific to the String store; if the length is larger than the size of
        ///     the object store page divided by <see cref="InlineObjectValueSizeMaxPageDivisor"/>, it will be silently converted to <see cref="ValueObjectIdSize"/>.</item>
        /// </list>
        /// </summary>
        public int ValueTotalSize;

        /// <summary>Whether the new record will have an ETag.</summary>
        public bool HasETag;

        /// <summary>Whether the new record will have an Expiration.</summary>
        public bool HasExpiration;

        /// <inheritdoc/>
        public override string ToString()
            => $"KeySize {KeyTotalSize}, ValSize {ValueTotalSize}, HasETag {HasETag}, HasExpir {HasExpiration}";
    }
}
