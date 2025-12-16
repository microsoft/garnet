// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Heap container for <see cref="SpanByte"/> structs
    /// </summary>
    public sealed class SpanByteHeapContainer : IHeapContainer<PinnedSpanByte>
    {
        readonly SectorAlignedMemory mem;
        PinnedSpanByte pinnedSpanByte;

        public unsafe SpanByteHeapContainer(ReadOnlySpan<byte> item, SectorAlignedBufferPool pool)
        {
            if (item.Length == 0)
            {
                pinnedSpanByte = default;
                return;
            }
            mem = pool.Get(item.TotalSize());
            item.SerializeTo(mem.GetValidPointer());
            pinnedSpanByte = PinnedSpanByte.FromLengthPrefixedPinnedPointer(mem.GetValidPointer());
        }

        public unsafe ref PinnedSpanByte Get() => ref pinnedSpanByte;

        public void Dispose() => mem?.Return();
    }
}