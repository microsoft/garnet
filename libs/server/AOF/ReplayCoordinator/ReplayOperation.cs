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

        /// <summary>Create a non-chunked (raw record) operation.</summary>
        public ReplayOperation(byte[] record)
        {
            Record = record;
            Chunk = null;
        }

        /// <summary>Create a chunked operation from a completed accumulator.</summary>
        public ReplayOperation(ChunkedAccumulator chunk)
        {
            Record = null;
            Chunk = chunk;
        }

        /// <summary>Whether this operation is a completed chunked record.</summary>
        public bool IsChunked => Chunk is not null;
    }
}