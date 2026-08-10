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
        /// <summary>The surfaces currently installed process-wide.</summary>
        public static NativeAllocatorSurfaces EnabledSurfaces { get; private set; }

        /// <summary>The surfaces requested at process start via the <c>GARNET_NATIVE_ALLOCATOR</c> environment
        /// variable (or <see cref="NativeAllocatorSurfaces.None"/> if unset). Test harnesses that temporarily change
        /// <see cref="EnabledSurfaces"/> restore it to this baseline.</summary>
        public static NativeAllocatorSurfaces EnvBaselineSurfaces { get; internal set; }

        /// <summary>
        /// Install native allocators for the requested <paramref name="requested"/> scope. Must be called once,
        /// before any store or buffer pool is constructed.
        /// <para>
        /// The mimalloc-backed <see cref="NativeAllocatorSurfaces.BufferPool"/> surface requires the mimalloc native
        /// library: if it cannot be loaded for this platform/RID, initialization throws rather than running on the
        /// managed pool. The direct-VM surfaces (<see cref="NativeAllocatorSurfaces.LogPages"/>,
        /// <see cref="NativeAllocatorSurfaces.HashIndex"/>, <see cref="NativeAllocatorSurfaces.Frames"/>) use
        /// <c>mmap</c>/<c>VirtualAlloc</c> and are always available.
        /// </para>
        /// </summary>
        /// <param name="requested">Requested surfaces (from the <c>--native-allocator</c> mode).</param>
        /// <param name="logger">Optional logger for diagnostics.</param>
        /// <returns>The installed surfaces.</returns>
        /// <exception cref="TsavoriteException">
        /// The <see cref="NativeAllocatorSurfaces.BufferPool"/> surface was requested but mimalloc is unavailable.
        /// </exception>
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
                    // BufferPool mode requires the mimalloc native library; abort rather than run on the managed pool.
                    SectorAlignedBufferPool.NativeAllocator = null;
                    EnabledSurfaces = NativeAllocatorSurfaces.None;
                    throw new TsavoriteException(
                        $"Native allocator mode requires mimalloc, but it could not be loaded for RID '{Mimalloc.GetRuntimeIdentifier()}'. " +
                        "Ship the mimalloc native library for this platform (Native/runtimes/<rid>/native/), or set --native-allocator off.");
                }
            }
            else
            {
                // Put the buffer pool on the managed path (idempotent — uninstalls any installed native pool).
                SectorAlignedBufferPool.NativeAllocator = null;
            }

            // Direct-VM surfaces (always available; no native library needed).
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