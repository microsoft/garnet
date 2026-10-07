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

        /// <summary>Log address sequence number of the record, for operations that still need it when they are
        /// replayed later. Currently only the fuzzy-region TxnCommit marker, whose value drives the multi-log commit
        /// barrier in <see cref="AofProcessor.AofReplayCoordinator.ProcessTransactionGroup"/>.</summary>
        public readonly long SequenceNumber;

        /// <summary>Create a non-chunked (raw record) operation.</summary>
        public ReplayOperation(byte[] record, long sequenceNumber = 0)
        {
            Record = record;
            Chunk = null;
            SequenceNumber = sequenceNumber;
        }

        /// <summary>Create a chunked operation from a completed accumulator.</summary>
        public ReplayOperation(ChunkedAccumulator chunk)
        {
            Record = null;
            Chunk = chunk;
            SequenceNumber = 0;
        }

        /// <summary>Whether this operation is a completed chunked record.</summary>
        public bool IsChunked => Chunk is not null;
    }
}