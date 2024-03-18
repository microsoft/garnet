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
        /// The <see cref="RecordInfo"/> header of the record.
        /// </summary>
        public readonly RecordInfo RecordInfo;

        /// <summary>
        /// The logical address of the record.
        /// </summary>
        public readonly long Address;

        internal RecordMetadata(RecordInfo recordInfo, long address = Constants.kInvalidAddress)
        {
            RecordInfo = recordInfo;
            Address = address;
        }

        /// <inheritdoc/>
        public override string ToString() => $"ri {RecordInfo}, addr {Address}";
    }
}