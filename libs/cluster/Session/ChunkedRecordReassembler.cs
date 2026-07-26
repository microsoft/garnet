// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using Garnet.common;

namespace Garnet.cluster
{
    /// <summary>
    /// Reassembles a <see cref="Garnet.client.MigrationRecordSpanType.ChunkedLogRecord"/> from its chunk records. A record too
    /// large for one send buffer is sent as a sequence of chunks (each framed <c>[int chunkLength | continuation][chunk bytes]</c>);
    /// the receiver appends each chunk's payload here until the final chunk (its continuation flag clear), then deserializes the
    /// reassembled record. One instance is held per connection because a record's chunks may span multiple commands.
    /// </summary>
    /// <remarks>
    /// Chunks are kept as a list of buffers (not copied into one contiguous array) and exposed as a <see cref="ReadOnlySequence{T}"/>
    /// via the shared <see cref="Garnet.common.ReadOnlySequenceBuilder"/> (as the AOF reader's <c>ChunkedAccumulator.GetValueSequence</c>
    /// does): this lets an object value exceed 2 GB (the max length of a single <c>byte[]</c>) and be deserialized as a stream with
    /// no giant contiguous copy.
    /// </remarks>
    internal sealed class ChunkedRecordReassembler
    {
        readonly List<byte[]> chunks = [];
        long length;

        /// <summary>
        /// Append one chunk's payload (copied into an owned buffer). Returns true when the record is complete
        /// (<paramref name="moreChunksFollow"/> is false), after which <see cref="AsSequence"/> is the reassembled record and the
        /// caller must <see cref="Reset"/> before the next record.
        /// </summary>
        public bool Append(ReadOnlySpan<byte> chunk, bool moreChunksFollow)
        {
            chunks.Add(chunk.ToArray());
            length += chunk.Length;
            return !moreChunksFollow;
        }

        /// <summary>Total reassembled length (may exceed <see cref="int.MaxValue"/>).</summary>
        public long Length => length;

        /// <summary>The reassembled record bytes as a sequence (valid once <see cref="Append"/> returns true).</summary>
        public ReadOnlySequence<byte> AsSequence() => ReadOnlySequenceBuilder.FromChunks(chunks);

        /// <summary>Reset for the next record (keeps the buffer-list capacity for reuse).</summary>
        public void Reset()
        {
            chunks.Clear();
            length = 0;
        }
    }
}