// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Heap container for <see cref="SpanByte"/> structs.
    /// </summary>
    /// <remarks>
    /// Supports optional per-session pooling: when constructed/initialized with a non-null
    /// <c>returnPool</c>, <see cref="Dispose"/> clears state and pushes <c>this</c> back onto
    /// that stack so the next pending operation in the session can reuse the wrapper instead
    /// of allocating a fresh one. Sessions are single-threaded so the stack does not need to
    /// be concurrent. This removes a ~40 B per-pending-op GC allocation from the disk-read
    /// hot path.
    /// </remarks>
    public sealed class SpanByteHeapContainer : IHeapContainer<PinnedSpanByte>
    {
        SectorAlignedMemory mem;
        PinnedSpanByte pinnedSpanByte;
        Stack<IHeapContainer<PinnedSpanByte>> returnPool;

        public SpanByteHeapContainer(ReadOnlySpan<byte> item, SectorAlignedBufferPool pool)
            => Initialize(item, pool, returnPool: null);

        internal SpanByteHeapContainer(ReadOnlySpan<byte> item, SectorAlignedBufferPool pool, Stack<IHeapContainer<PinnedSpanByte>> returnPool)
            => Initialize(item, pool, returnPool);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void Initialize(ReadOnlySpan<byte> item, SectorAlignedBufferPool pool, Stack<IHeapContainer<PinnedSpanByte>> returnPool)
        {
            this.returnPool = returnPool;
            if (item.Length == 0)
            {
                pinnedSpanByte = default;
                return;
            }

            var size = item.TotalSize();
            mem = pool.Get(size);
            item.SerializeTo(mem.GetValidPointer(), size);
            pinnedSpanByte = PinnedSpanByte.FromLengthPrefixedPinnedPointer(mem.GetValidPointer());
        }

        public ref PinnedSpanByte Get() => ref pinnedSpanByte;

        public void Dispose()
        {
            mem?.Return();
            mem = null;
            pinnedSpanByte = default;
            var pool = returnPool;
            returnPool = null;
            pool?.Push(this);
        }
    }
}