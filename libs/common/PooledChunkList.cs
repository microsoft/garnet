// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.common
{
    /// <summary>
    /// Accumulates a byte stream of unknown total length into pooled buffers, exposed as a
    /// <see cref="ReadOnlySequence{T}"/> so the combined data may exceed 2 GB (the max length of a single <c>byte[]</c>).
    /// Used by the chunked-record paths that receive or build a serialized object value whose length is not known up front.
    /// </summary>
    /// <remarks>
    /// Incoming spans are <b>packed</b> into uniform buffers rather than each becoming its own array: the arriving segment
    /// sizes are dictated by transport framing, so per-segment arrays would be odd-sized, defeating pooling, and at the sizes
    /// involved would be allocated and promoted while the whole payload is retained.
    /// <para>
    /// Buffers come from a <see cref="SectorAlignedBufferPool"/>, whose blocks are pinned arrays that the pool reuses, so a
    /// large buffer size costs no repeated large-object-heap allocation. <see cref="DefaultBufferSize"/> is therefore chosen
    /// for segment count rather than to dodge an allocation threshold: it matches the size the object-log streaming path uses,
    /// and keeps the sequence to one segment per few megabytes instead of per few tens of kilobytes.
    /// </para>
    /// <para>
    /// Buffers are returned by <see cref="Reset"/> / <see cref="Dispose"/>, so every path that discards an accumulation —
    /// including error and teardown paths — must reach one of them. A pool buffer reserves its share of the pool's cacheable
    /// budget when it is allocated and releases it only when it is returned, so a rental dropped to the GC does not merely go
    /// unreused: it permanently consumes that budget, and enough of them leave the pool unable to cache at all. Returning a
    /// buffer that is still referenced would be worse still, so callers must reset only once the sequence is no longer in use.
    /// </para>
    /// </remarks>
    public sealed class PooledChunkList : IDisposable
    {
        /// <summary>Size of each pooled buffer. See the remarks on <see cref="PooledChunkList"/> for why this is large.</summary>
        public const int DefaultBufferSize = 4 * 1024 * 1024;

        /// <summary>Pool shared by every chunk accumulation, so AOF replay and cluster migration reuse the same blocks.
        /// These buffers never reach a device, so the sector size only has to be a legal alignment.</summary>
        static readonly SectorAlignedBufferPool SharedPool = new(1, 512);

        readonly int bufferSize;
        readonly SectorAlignedBufferPool pool;
        readonly List<SectorAlignedMemory> buffers = [];

        /// <summary>Bytes filled in the last buffer; every earlier buffer is full to <see cref="bufferSize"/>.</summary>
        int lastFilled;
        long totalLength;

        /// <summary>Create a chunk list.</summary>
        /// <param name="pool">Pool to rent buffers from; defaults to the shared chunk-accumulation pool.</param>
        /// <param name="bufferSize">Size of each pooled buffer; defaults to <see cref="DefaultBufferSize"/>.</param>
        public PooledChunkList(SectorAlignedBufferPool pool = null, int bufferSize = DefaultBufferSize)
        {
            if (bufferSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(bufferSize));
            this.pool = pool ?? SharedPool;
            this.bufferSize = bufferSize;
        }

        /// <summary>Total bytes accumulated.</summary>
        public long TotalLength => totalLength;

        /// <summary>True when nothing has been accumulated.</summary>
        public bool IsEmpty => totalLength == 0;

        /// <summary>Number of chunks; see <see cref="this[int]"/>.</summary>
        public int Count => buffers.Count;

        /// <summary>The chunk at <paramref name="index"/>, bounded to its valid bytes (only the last chunk is partial).</summary>
        public ReadOnlyMemory<byte> this[int index]
            => buffers[index].AsReadOnlyMemory(0, index == buffers.Count - 1 ? lastFilled : bufferSize);

        /// <summary>Append bytes, spilling into additional pooled buffers as needed.</summary>
        public unsafe void Append(ReadOnlySpan<byte> data)
        {
            while (!data.IsEmpty)
            {
                if (buffers.Count == 0 || lastFilled == bufferSize)
                {
                    // clearOnReturn is false because a chunk is always written before it is read (only the bytes appended
                    // since the last reset are ever exposed), so a previous rental's contents cannot be observed.
                    buffers.Add(pool.Get(bufferSize, clearOnReturn: false));
                    lastFilled = 0;
                }

                // The pool may return a larger block than requested; cap at bufferSize so chunk lengths stay uniform and
                // the indexer can derive every non-final chunk's length without tracking it per buffer.
                var toCopy = Math.Min(data.Length, bufferSize - lastFilled);
                data.Slice(0, toCopy).CopyTo(new Span<byte>(buffers[^1].aligned_pointer + lastFilled, toCopy));
                lastFilled += toCopy;
                totalLength += toCopy;
                data = data.Slice(toCopy);
            }
        }

        /// <summary>Wrap the accumulated bytes as one <see cref="ReadOnlySequence{T}"/> (no data copy), so the payload may
        /// exceed 2 GB and be consumed as a stream (see <see cref="ReadOnlySequenceStream"/>). The sequence is valid until
        /// the next <see cref="Append"/>, <see cref="Reset"/>, or <see cref="Dispose"/>.</summary>
        public ReadOnlySequence<byte> AsSequence()
        {
            if (buffers.Count == 0)
                return ReadOnlySequence<byte>.Empty;
            // Common case: a single chunk holds the whole payload — wrap it directly, with no ChunkSegment allocation.
            if (buffers.Count == 1)
                return new ReadOnlySequence<byte>(this[0]);

            ChunkSegment first = null, last = null;
            for (var i = 0; i < buffers.Count; i++)
            {
                last = new ChunkSegment(this[i], last);
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

        /// <summary>Return every pooled buffer and clear. Call only once the sequence and chunks are no longer referenced.
        /// Idempotent: resetting an already-reset list is a no-op, so the paths that discard an accumulation may overlap.</summary>
        public void Reset()
        {
            // Drop the reference to each buffer before handing it back, so no buffer can be returned twice even if this
            // list is reset again while the loop is in progress. A double return would push one block onto a pool free
            // list twice, and the lists are singly linked through the block itself, so the chain would be corrupted and
            // two renters would later be handed the same memory.
            for (var i = 0; i < buffers.Count; i++)
            {
                var buffer = buffers[i];
                buffers[i] = null;
                buffer?.Return();
            }
            buffers.Clear();
            lastFilled = 0;
            totalLength = 0;
        }

        /// <inheritdoc/>
        public void Dispose() => Reset();
    }
}