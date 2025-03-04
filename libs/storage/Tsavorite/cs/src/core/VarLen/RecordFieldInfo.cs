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
        /// The data length of the key for the new record without any length prefix and may become overflow; see <see cref="RecordSizeInfo.InlineTotalKeySize"/>
        /// </summary>
        public int KeyDataSize;

        /// <summary>
        /// The data length of the value for the new record. Its behavior varies between the String and Object stores:
        /// <list type="bullet">
        ///     <item>String store: It is the data length of the Span without any length prefix and may become overflow; see <see cref="RecordSizeInfo.InlineTotalValueSize"/></item>
        ///     <item>Object store: If <see cref="ValueIsObject"/> is specified it is ignored and should be set to <see cref="ObjectIdMap.ObjectIdSize"/>. 
        ///         Otherwise it is the same as <see cref="KeyDataSize"/>: the data length of the span without any length prefix and may become overflow;
        ///         see <see cref="RecordSizeInfo.InlineTotalValueSize"/></item>
        /// </list>
        /// </summary>
        public int ValueDataSize;

        /// <summary>Whether the value was specified to be an object.</summary>
        public bool ValueIsObject;

        /// <summary>Whether the new record will have an ETag.</summary>
        public bool HasETag;

        /// <summary>Whether the new record will have an Expiration.</summary>
        public bool HasExpiration;

        /// <inheritdoc/>
        public override string ToString()
            => $"KeySize {KeyDataSize}, ValSize {ValueDataSize}, ValIsObj {ValueIsObject}, HasETag {HasETag}, HasExpir {HasExpiration}";
    }
}
