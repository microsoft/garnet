// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;

namespace Garnet.common
{
    /// <summary>
    /// Builds a <see cref="ReadOnlySequence{T}"/> over a set of byte chunks without copying them into one contiguous
    /// array, so the combined data may exceed 2 GB (the max length of a single <c>byte[]</c>) and be consumed as a stream (see
    /// <see cref="ReadOnlySequenceStream"/>). Shared by the chunked-record readers — AOF <c>ChunkedAccumulator.GetValueSequence</c>
    /// and cluster migration/replication <c>ChunkedRecordReassembler.AsSequence</c>.
    /// </summary>
    public static class ReadOnlySequenceBuilder
    {
        /// <summary>
        /// Wrap <paramref name="chunks"/> as a single <see cref="ReadOnlySequence{T}"/> (no data copy). Returns
        /// <see cref="ReadOnlySequence{T}.Empty"/> when empty, and wraps a single chunk directly (no segment allocation);
        /// otherwise links the chunks as <see cref="ReadOnlySequenceSegment{T}"/> nodes.
        /// </summary>
        public static ReadOnlySequence<byte> FromChunks(PooledChunkList chunks)
        {
            if (chunks is null || chunks.Count == 0)
                return ReadOnlySequence<byte>.Empty;
            // Common case: a single chunk holds the whole payload — wrap it directly, with no ChunkSegment allocation.
            if (chunks.Count == 1)
                return new ReadOnlySequence<byte>(chunks.GetChunk(0));

            ChunkSegment first = null, last = null;
            for (var i = 0; i < chunks.Count; i++)
            {
                last = new ChunkSegment(chunks.GetChunk(i), last);
                first ??= last;
            }
            return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
        }

        sealed class ChunkSegment : ReadOnlySequenceSegment<byte>
        {
            public ChunkSegment(ReadOnlyMemory<byte> memory, ChunkSegment previous)
            {
                Memory = memory;
                if (previous is not null)
                {
                    previous.Next = this;
                    RunningIndex = previous.RunningIndex + previous.Memory.Length;
                }
            }
        }
    }
}