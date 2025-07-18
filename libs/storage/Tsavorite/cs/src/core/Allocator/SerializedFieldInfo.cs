// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    using static Utility;

    /// <summary>
    /// Returns field-length information about a serialized (text-format) record read from the disk.
    /// </summary>
    internal struct SerializedFieldInfo
    {
        /// <summary>
        /// A copy of the record header
        /// </summary>
        internal RecordInfo recordInfo;

        /// <summary>
        /// Offset to start of key (past RecordInfo and varbyte lengths)
        /// </summary>
        internal int offsetToKeyStart;

        /// <summary>
        /// Length of the key
        /// </summary>
        internal int keyLength;

        /// <summary>
        /// Length of the value
        /// </summary>
        internal long valueLength;

        /// <summary>
        /// Length of the optional fields (ETag and Expiration), if any
        /// </summary>
        internal int optionalLength;

        /// <summary>
        /// Whether the value is chunked (in which case we cannot know the actual length, so <see cref="isComplete"/> will be false).
        /// </summary>
        internal bool isChunkedValue;

        /// <summary>
        /// If true, then the record is completely contained in the number of available bytes when we were asked for these lengths.
        /// </summary>
        internal bool isComplete;

        /// <summary>
        /// Total serialized length of the record
        /// </summary>
        internal readonly long SerializedLength => RoundUp(offsetToKeyStart + keyLength + valueLength + optionalLength, Constants.kRecordAlignment);
    }
}
