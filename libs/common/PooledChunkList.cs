// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;

namespace Garnet.common
{
    /// <summary>
    /// Accumulates a byte stream of unknown total length into pooled fixed-size buffers, exposed as a
    /// <see cref="ReadOnlySequence{T}"/> so the combined data may exceed 2 GB (the max length of a single <c>byte[]</c>).
    /// Used by the chunked-record paths that receive or build a serialized object value whose length is not known up front.
    /// </summary>
    /// <remarks>
    /// Incoming spans are <b>packed</b> into uniform buffers rather than each becoming its own array: the arriving segment
    /// sizes are dictated by transport framing, so per-segment arrays would be odd-sized (defeating pooling) and, at the
    /// sizes involved, would land on the large-object heap and be promoted while the whole payload is retained.
    /// <see cref="DefaultBufferSize"/> is deliberately below the 85,000-byte LOH threshold and within the size classes
    /// <see cref="ArrayPool{T}.Shared"/> pools, so accumulating an arbitrarily large value allocates nothing at steady state.
    /// <para>
    /// Buffers are returned by <see cref="Reset"/> / <see cref="Dispose"/>. A missed return is not a correctness problem
    /// (the array is simply garbage-collected instead of reused), but returning a buffer that is still referenced would be,
    /// so callers must reset only once the sequence is no longer in use.
    /// </para>
    /// </remarks>
    public sealed class PooledChunkList : IDisposable
    {
        /// <summary>Size of each pooled buffer: under the 85,000-byte large-object-heap threshold and within the
        /// <see cref="ArrayPool{T}.Shared"/> size classes.</summary>
        public const int DefaultBufferSize = 64 * 1024;

        readonly int bufferSize;
        readonly ArrayPool<byte> pool;
        readonly List<byte[]> buffers = [];

        /// <summary>Bytes filled in the last buffer; every earlier buffer is full to <see cref="bufferSize"/>.</summary>
        int lastFilled;
        long totalLength;

        /// <summary>Create a chunk list over <see cref="ArrayPool{T}.Shared"/>.</summary>
        /// <param name="bufferSize">Size of each pooled buffer; defaults to <see cref="DefaultBufferSize"/>.</param>
        /// <param name="pool">Pool to rent from; defaults to <see cref="ArrayPool{T}.Shared"/>.</param>
        public PooledChunkList(int bufferSize = DefaultBufferSize, ArrayPool<byte> pool = null)
        {
            if (bufferSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(bufferSize));
            this.bufferSize = bufferSize;
            this.pool = pool ?? ArrayPool<byte>.Shared;
        }

        /// <summary>Total bytes accumulated.</summary>
        public long TotalLength => totalLength;

        /// <summary>True when nothing has been accumulated.</summary>
        public bool IsEmpty => totalLength == 0;

        /// <summary>Number of chunks; see <see cref="GetChunk"/>.</summary>
        public int Count => buffers.Count;

        /// <summary>The chunk at <paramref name="index"/>, bounded to its valid bytes (only the last chunk is partial).</summary>
        public ReadOnlyMemory<byte> GetChunk(int index)
            => buffers[index].AsMemory(0, index == buffers.Count - 1 ? lastFilled : bufferSize);

        /// <summary>Append bytes, spilling into additional pooled buffers as needed.</summary>
        public void Append(ReadOnlySpan<byte> data)
        {
            while (!data.IsEmpty)
            {
                if (buffers.Count == 0 || lastFilled == bufferSize)
                {
                    buffers.Add(pool.Rent(bufferSize));
                    lastFilled = 0;
                }

                // Rent may return a larger array than requested; cap at bufferSize so chunk lengths stay uniform and
                // GetChunk can derive every non-final chunk's length without tracking it per buffer.
                var toCopy = Math.Min(data.Length, bufferSize - lastFilled);
                data.Slice(0, toCopy).CopyTo(buffers[^1].AsSpan(lastFilled));
                lastFilled += toCopy;
                totalLength += toCopy;
                data = data.Slice(toCopy);
            }
        }

        /// <summary>Wrap the accumulated bytes as one <see cref="ReadOnlySequence{T}"/> (no data copy). The sequence is
        /// valid until the next <see cref="Append"/>, <see cref="Reset"/>, or <see cref="Dispose"/>.</summary>
        public ReadOnlySequence<byte> AsSequence() => ReadOnlySequenceBuilder.FromChunks(this);

        /// <summary>Return every pooled buffer and clear. Call only once the sequence and chunks are no longer referenced.</summary>
        public void Reset()
        {
            foreach (var buffer in buffers)
                pool.Return(buffer);
            buffers.Clear();
            lastFilled = 0;
            totalLength = 0;
        }

        /// <inheritdoc/>
        public void Dispose() => Reset();
    }
}