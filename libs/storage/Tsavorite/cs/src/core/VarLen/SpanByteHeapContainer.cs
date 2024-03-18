// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Heap container for SpanByte structs
    /// </summary>
    internal class SpanByteHeapContainer : IHeapContainer<SpanByte>
    {
        readonly SectorAlignedMemory mem;

        public unsafe SpanByteHeapContainer(ref SpanByte obj, SectorAlignedBufferPool pool)
        {
            mem = pool.Get(obj.TotalSize);
            obj.CopyTo(mem.GetValidPointer());
        }

        public unsafe ref SpanByte Get() => ref Unsafe.AsRef<SpanByte>(mem.GetValidPointer());

        public void Dispose() => mem.Return();
    }
}