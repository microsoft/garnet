// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    using static LogAddress;

    /// <summary>
    /// A structure carrying metadata about a record in the log.
    /// </summary>
    public readonly struct RecordMetadata
    {
        /// <summary>
        /// The logical address of the record.
        /// </summary>
        public readonly long Address;

        /// <summary>
        /// The ETag of the record, if any; otherwise <see cref="LogRecord.NoETag"/>.
        /// </summary>
        /// <remarks>Included here to be available for multi-key operations.</remarks>
        public readonly long ETag;

        internal RecordMetadata(long address = kInvalidAddress, long eTag = LogRecord.NoETag)
        {
            Address = address;
            ETag = eTag;
        }

        /// <inheritdoc/>
        public override string ToString() => $"addr {AddressString(Address)}, eTag {ETag}";
    }
}