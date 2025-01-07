// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// A structure carrying metadata about a record in the log.
    /// </summary>
    public readonly struct RecordMetadata
    {
        /// <summary>
        /// The logical address of the record.
        /// </summary>
        public readonly long Address;

        internal RecordMetadata(long address = Constants.kInvalidAddress)
        {
            Address = address;
        }

        /// <inheritdoc/>
        public override string ToString() => $"addr {Address}";
    }
}