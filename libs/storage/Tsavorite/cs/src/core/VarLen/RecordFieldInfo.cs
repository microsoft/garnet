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
        /// The data length of the key for the record. It is immutable unless the record is deleted and revivified. Its behavior varies between the String and Object stores:
        /// <list type="bullet">
        ///     <item>String store: It is the data length of the Span</item>
        ///     <item>Object store: It is the data length of the Span (which may or may not Overflow)</item>
        /// </list>
        /// </summary>
        public int KeySize;

        /// <summary>
        /// The data length of the value for the record. Its behavior varies between the String and Object stores:
        /// <list type="bullet">
        ///     <item>String store: It is the data length of the Span</item>
        ///     <item>Object store: It is either the data length of the Span (which may or may not Overflow) or <see cref="ObjectIdMap.ObjectIdSize"/> if the Value is an Object</item>
        /// </list>
        /// </summary>
        public int ValueSize;

        /// <summary>There is one byte reserved for the namespace in the <see cref="RecordDataHeader"/>, limited to integer values from 1-127. If more are desired, the entire length is here
        /// and the namespace is stored in the record immediately after the <see cref="RecordDataHeader"/>, just before the Key data bytes. This is immutable for the life of the key
        /// (unless the record is deleted and revivified).</summary>
        public int ExtendedNamespaceSize;

        /// <summary>Whether the value was specified to be an object.</summary>
        public bool ValueIsObject;

        /// <summary>Whether the new record will have an ETag.</summary>
        public bool HasETag;

        /// <summary>Whether the new record will have an Expiration.</summary>
        public bool HasExpiration;

        /// <inheritdoc/>
        public override string ToString()
            => $"KeySize {KeySize}, ValSize {ValueSize}, ValIsObj {ValueIsObject}, HasETag {HasETag}, HasExpir {HasExpiration}";
    }
}