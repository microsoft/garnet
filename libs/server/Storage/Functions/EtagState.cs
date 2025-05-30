// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Indirection wrapper to provide a way to set offsets related to Etags and use the getters opaquely from outside.
    /// </summary>
    public struct ETagState
    {
        public ETagState() { }

        /// <summary>
        /// Field provides access to getting an Etag from a record, hiding whether it is actually present or not.
        /// </summary>
        public long ETag { get; set; } = LogRecord.NoETag;

        /// <summary>
        /// Sets the values to indicate the presence of an Etag as a part of the payload value
        /// </summary>
        public static void SetValsForRecordWithEtag<TSourceLogRecord>(ref ETagState curr, in TSourceLogRecord srcLogRecord)
            where TSourceLogRecord : ISourceLogRecord
            => curr.ETag = srcLogRecord.ETag;

        public static void ResetState(ref ETagState curr)
            => curr.ETag = LogRecord.NoETag;
    }
}