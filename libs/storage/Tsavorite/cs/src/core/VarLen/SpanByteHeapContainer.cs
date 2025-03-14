// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Heap container for <see cref="SpanByte"/> structs
    /// </summary>
    internal sealed class SpanByteHeapContainer : IHeapContainer<SpanByte>
    {
        readonly SectorAlignedMemory mem;
        SpanByte obj;

        public unsafe SpanByteHeapContainer(SpanByte obj, SectorAlignedBufferPool pool)
        {
            mem = pool.Get(obj.TotalSize);
            obj.CopyTo(mem.GetValidPointer());
            this.obj = SpanByte.FromLengthPrefixedPinnedPointer(mem.GetValidPointer());
        }

        public unsafe ref SpanByte Get() => ref obj;

        public void Dispose() => mem.Return();
    }
}