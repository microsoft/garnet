// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Struct for information about fields (Value and optional fields) of a record, to determine required allocation size.
    /// </summary>
    public struct RecordFieldInfo
    {
        /// <summary>The length of the key for the new record.</summary>
        public int KeySize;

        /// <summary>The length of the value for the new record.</summary>
        public int ValueSize;

        /// <summary>Whether the new record will have an ETag.</summary>
        public bool HasETag;

        /// <summary>Whether the new record will have an Expiration.</summary>
        public bool HasExpiration;

        /// <inheritdoc/>
        public override string ToString()
            => $"KeySize {KeySize}, ValSize {ValueSize}, HasETag {HasETag}, HasExpir {HasExpiration}";
    }
}
