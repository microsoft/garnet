// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Public startup entry point that resolves the requested <see cref="NativeAllocatorSurfaces"/> scope against
    /// backend availability and installs the native allocators. Must be called <b>once, before any store or
    /// buffer pool is constructed</b>. Keeps the concrete backends (<c>internal</c>) and the pool's static hook
    /// out of the host's view while exposing a single resolution API.
    /// <para>
    /// The resolution is deliberately made here rather than via a mutable process-global default consulted at
    /// use time: it is decided up front, never changed after stores exist, and there is no runtime fallback.
    /// </para>
    /// </summary>
    public static class NativeAllocatorInitializer
    {
        /// <summary>The surfaces actually enabled after availability resolution (may be narrower than requested).</summary>
        public static NativeAllocatorSurfaces EnabledSurfaces { get; private set; }

        /// <summary>
        /// Resolve and install native allocators for the requested <paramref name="requested"/> scope. When a
        /// requested surface's backend is unavailable (e.g. no mimalloc prebuilt for the platform/RID), that
        /// surface falls back to the managed allocator and a warning is logged; other surfaces proceed.
        /// </summary>
        /// <param name="requested">Requested surfaces (from the <c>--native-allocator</c> mode).</param>
        /// <param name="logger">Optional logger for availability/fallback diagnostics.</param>
        /// <returns>The surfaces actually enabled.</returns>
        public static NativeAllocatorSurfaces Initialize(NativeAllocatorSurfaces requested, ILogger logger = null)
        {
            var enabled = NativeAllocatorSurfaces.None;

            if ((requested & NativeAllocatorSurfaces.BufferPool) != 0)
            {
                if (Mimalloc.TryInitialize(logger))
                {
                    SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator();
                    enabled |= NativeAllocatorSurfaces.BufferPool;
                    logger?.LogInformation("Native allocator enabled for SectorAlignedBufferPool (mimalloc).");
                }
                else
                {
                    logger?.LogWarning("Native allocator 'buffer-pool' requested but mimalloc is unavailable; falling back to the managed buffer pool.");
                }
            }

            // Direct-VM singletons (always available; no shipped binary needed). Wired incrementally.
            if ((requested & NativeAllocatorSurfaces.HashIndex) != 0)
            {
                enabled |= NativeAllocatorSurfaces.HashIndex;
                logger?.LogInformation("Native allocator enabled for the hash index (direct virtual memory).");
            }

            if ((requested & NativeAllocatorSurfaces.LogPages) != 0)
            {
                enabled |= NativeAllocatorSurfaces.LogPages;
                logger?.LogInformation("Native allocator enabled for log pages (direct virtual memory).");
            }

            if ((requested & NativeAllocatorSurfaces.Frames) != 0)
            {
                enabled |= NativeAllocatorSurfaces.Frames;
                logger?.LogInformation("Native allocator enabled for recovery/scan frames (direct virtual memory).");
            }

            EnabledSurfaces = enabled;

            if (enabled != NativeAllocatorSurfaces.None)
                logger?.LogInformation("Native allocator active ({enabled}). This memory is OUTSIDE the managed GC heap: " +
                    "size GCHeapHardLimit/GCHeapHardLimitPercent to leave headroom for it; set DOTNET_GCDynamicAdaptationMode=0 " +
                    "(DATAS is blind to native memory and may grow the heap into container OOM); monitor 'native_allocator_bytes' in INFO memory.",
                    enabled);

            return enabled;
        }

        /// <summary>Return unused native memory to the OS (best-effort). Intended for shutdown.</summary>
        public static void Collect() => Mimalloc.Collect(force: true);
    }
}
