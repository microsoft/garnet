// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Struct for information about fields (Value and optional fields) of a record, to determine required allocation size.
    /// </summary>
    public struct RecordFieldInfo
    {
        /// <summary>
        /// The length of the key for the new record, including the length prefix. May become overflow; see <see cref="RecordSizeInfo.InlineTotalKeySize"/>
        /// </summary>
        public int KeyTotalSize;    // TODO: Replace with Length; app should not need to konw about prefix length ("total" size)

        /// <summary>
        /// The length of the value for the new record. Its behavior varies between the String and Object stores:
        /// <list type="bullet">
        ///     <item>String store: It is the length of the Span, including any length prefix, and may become overflow; see <see cref="RecordSizeInfo.InlineTotalValueSize"/></item>
        ///     <item>Object store: It is ignored if <see cref="ValueIsObject"/> is specified, else it is the length of the span, including any length prefix, and may become overflow; see <see cref="RecordSizeInfo.InlineTotalValueSize"/></item>
        /// </list>
        /// </summary>
        public int ValueTotalSize;  // TODO: Replace with Length; app should not need to konw about prefix length ("total" size)

        /// <summary>Whether the value was specified to be an object.</summary>
        public bool ValueIsObject;

        /// <summary>Whether the new record will have an ETag.</summary>
        public bool HasETag;

        /// <summary>Whether the new record will have an Expiration.</summary>
        public bool HasExpiration;

        /// <inheritdoc/>
        public override string ToString()
            => $"KeySize {KeyTotalSize}, ValSize {ValueTotalSize}, ValIsObj {ValueIsObject}, HasETag {HasETag}, HasExpir {HasExpiration}";
    }
}
