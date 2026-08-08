// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Per-surface capability set selecting which memory surfaces use a native (off-managed-heap)
    /// allocator instead of <see cref="System.GC.AllocateArray{T}(int, bool)"/>. Exposed to operators
    /// as ordered modes (<c>off</c> | <c>buffer-pool</c> | <c>full</c>) but represented internally as
    /// flags so per-surface control and future surfaces are available without a config break.
    /// </summary>
    [Flags]
    public enum NativeAllocatorSurfaces
    {
        /// <summary>All surfaces use the managed heap (behavior-identical default).</summary>
        None = 0,

        /// <summary>
        /// <see cref="SectorAlignedBufferPool"/> IO buffers use mimalloc (its thread-local heaps + cross-thread
        /// free lists replace the manual per-level free-list recycling). This is the primary win and the safe
        /// first increment: localized to the buffer pool with no allocator/epoch/recovery changes.
        /// </summary>
        BufferPool = 1,

        /// <summary>Main-log / object-log inline pages and <see cref="TsavoriteLog"/> pages use the direct-VM backend.</summary>
        LogPages = 2,

        /// <summary>
        /// The main hash-index table (<c>state[].tableAligned</c>) uses the direct-VM backend. The overflow-bucket
        /// pages (<c>MallocFixedPageSize&lt;HashBucket&gt;</c>) remain on the managed pinned heap in v1.
        /// </summary>
        HashIndex = 4,

        /// <summary>Recovery / flush frames (<c>BlittableFrame</c>) use the direct-VM backend.</summary>
        Frames = 8,

        /// <summary>Network send/receive buffers. Reserved for a future release (managed in v1).</summary>
        Network = 16,

        /// <summary>Everything except <see cref="Network"/> (which stays managed in v1).</summary>
        Full = BufferPool | LogPages | HashIndex | Frames,
    }
}
