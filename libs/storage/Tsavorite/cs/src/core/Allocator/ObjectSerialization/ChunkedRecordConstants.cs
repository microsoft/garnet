// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Framing constants shared by the chunked-record write/read paths (AOF/<see cref="TsavoriteLog"/> and the
    /// migration / replication <see cref="DiskLogRecord"/> path). A chunk's length prefix is an <see cref="int"/> whose
    /// high bit is the continuation flag, set on every chunk except the last of its component/record; the remaining bits
    /// are the chunk's data length.
    /// </summary>
    public static class ChunkedRecordConstants
    {
        /// <summary>High bit of a chunk-length prefix, set when more chunks of the same component/record follow.</summary>
        public const int ContinuationFlag = unchecked((int)0x80000000);
    }
}