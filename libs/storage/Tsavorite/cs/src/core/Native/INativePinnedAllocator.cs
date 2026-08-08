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

        /// <summary>
        /// Free a pointer previously returned by <see cref="Allocate"/>. <paramref name="size"/> is the size
        /// originally requested (the caller — the pool — already knows it), so the allocator need not query the
        /// native usable size on the hot path. No-op for zero.
        /// </summary>
        void Free(nint ptr, nuint size);
    }

    /// <summary>
    /// <see cref="INativePinnedAllocator"/> backed by mimalloc. Accounting uses the requested size (the pool
    /// knows it) rather than <c>mi_usable_size</c>, avoiding an extra native call per op, and updates the
    /// striped <see cref="NativeMemoryTracker"/> (telemetry only — see that type for why we do not use
    /// <see cref="System.GC.AddMemoryPressure(long)"/>).
    /// </summary>
    internal sealed unsafe class MimallocPooledAllocator : INativePinnedAllocator
    {
        public nint Allocate(nuint size, nuint alignment, bool zeroed)
        {
            var ptr = zeroed ? Mimalloc.ZallocAligned(size, alignment) : Mimalloc.MallocAligned(size, alignment);
            if (ptr == 0)
                throw new OutOfMemoryException($"mimalloc aligned allocation of {size} bytes (alignment {alignment}) failed");
            NativeMemoryTracker.Add((long)size);
            return ptr;
        }

        public void Free(nint ptr, nuint size)
        {
            if (ptr == 0)
                return;
            NativeMemoryTracker.Subtract((long)size);
            Mimalloc.Free(ptr);
        }
    }
}
