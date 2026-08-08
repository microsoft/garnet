// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Backing allocator for the pooled, high-churn IO buffers of <see cref="SectorAlignedBufferPool"/>.
    /// The pool bypasses its own free-list recycling when a native allocator is set and lets the allocator
    /// own recycling (mimalloc's thread-local heaps + cross-thread free lists). Frees are by pointer only,
    /// which mimalloc supports directly (it tracks block size internally).
    /// </summary>
    internal unsafe interface INativePinnedAllocator
    {
        /// <summary>
        /// Allocate at least <paramref name="size"/> bytes aligned to <paramref name="alignment"/> (a nonzero
        /// power of two). When <paramref name="zeroed"/> is true the returned region is zero-initialized.
        /// Throws <see cref="OutOfMemoryException"/> on failure (never returns null).
        /// </summary>
        nint Allocate(nuint size, nuint alignment, bool zeroed);

        /// <summary>Free a pointer previously returned by <see cref="Allocate"/>. No-op for zero.</summary>
        void Free(nint ptr);
    }

    /// <summary>
    /// <see cref="INativePinnedAllocator"/> backed by mimalloc. Every allocation/free updates
    /// <see cref="NativeMemoryTracker"/> (telemetry only — see that type for why we do not use
    /// <see cref="System.GC.AddMemoryPressure(long)"/>).
    /// </summary>
    internal sealed unsafe class MimallocPooledAllocator : INativePinnedAllocator
    {
        public nint Allocate(nuint size, nuint alignment, bool zeroed)
        {
            var ptr = zeroed ? Mimalloc.ZallocAligned(size, alignment) : Mimalloc.MallocAligned(size, alignment);
            if (ptr == 0)
                throw new OutOfMemoryException($"mimalloc aligned allocation of {size} bytes (alignment {alignment}) failed");
            NativeMemoryTracker.Add((long)Mimalloc.UsableSize(ptr));
            return ptr;
        }

        public void Free(nint ptr)
        {
            if (ptr == 0)
                return;
            NativeMemoryTracker.Subtract((long)Mimalloc.UsableSize(ptr));
            Mimalloc.Free(ptr);
        }
    }
}
