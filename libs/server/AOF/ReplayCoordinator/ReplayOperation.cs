// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// A buffered replay operation: either a raw non-chunked AOF record (<see cref="Record"/>) or a completed chunked
    /// record's <see cref="ChunkedAccumulator"/> (<see cref="Chunk"/>). Lets a transaction group and the fuzzy-region buffer hold
    /// both kinds in a single ordered list.
    /// </summary>
    internal readonly struct ReplayOperation
    {
        /// <summary>Raw non-chunked record bytes; null when this is a chunked operation.</summary>
        public readonly byte[] Record;

        /// <summary>Completed chunked-record accumulator; null when this is a non-chunked operation.</summary>
        public readonly ChunkedAccumulator Chunk;

        /// <summary>Original log address or sharded sequence number used to order transaction operations.</summary>
        public readonly long SequenceNumber;

        /// <summary>Replay task that owns the operation's key, or -1 when supplied by the caller.</summary>
        public readonly int VirtualSublogIdx;

        /// <summary>Create a non-chunked (raw record) operation.</summary>
        public ReplayOperation(byte[] record, long sequenceNumber = 0, int virtualSublogIdx = -1)
        {
            Record = record;
            Chunk = null;
            SequenceNumber = sequenceNumber;
            VirtualSublogIdx = virtualSublogIdx;
        }

        /// <summary>Create a chunked operation from a completed accumulator.</summary>
        public ReplayOperation(ChunkedAccumulator chunk, long logAddressSequenceNumber = 0, int virtualSublogIdx = -1)
        {
            Record = null;
            Chunk = chunk;
            SequenceNumber = chunk.headerType == AofHeaderType.ShardedHeader ? chunk.sequenceNumber : logAddressSequenceNumber;
            VirtualSublogIdx = virtualSublogIdx;
        }

        /// <summary>Whether this operation is a completed chunked record.</summary>
        public bool IsChunked => Chunk is not null;
    }
}