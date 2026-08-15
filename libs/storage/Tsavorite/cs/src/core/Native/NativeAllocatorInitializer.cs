// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Public startup entry point that installs the direct-VM native allocators for the log pages, hash index,
    /// and recovery/scan frames. Must be called <b>once, before any store is constructed</b>. Keeps the concrete
    /// backends (<c>internal</c>) out of the host's view while exposing a single resolution API.
    /// <para>
    /// The decision is deliberately made here rather than via a mutable process-global default consulted at
    /// use time: it is decided up front, never changed after stores exist, and there is no runtime fallback.
    /// </para>
    /// </summary>
    public static class NativeAllocatorInitializer
    {
        /// <summary>Whether the native allocator is currently installed process-wide.</summary>
        public static bool Enabled { get; private set; }

        /// <summary>
        /// Install the native allocator when <paramref name="enable"/> is true. Must be called once, before any
        /// store is constructed. The direct-VM backend (<c>mmap</c>/<c>VirtualAlloc</c>) is always available;
        /// no native library is required.
        /// </summary>
        /// <param name="enable">Whether to enable the native allocator (from the <c>--use-native-allocator</c> switch).</param>
        /// <param name="logger">Optional logger for diagnostics.</param>
        /// <returns>Whether the native allocator is enabled.</returns>
        public static bool Initialize(bool enable, ILogger logger = null)
        {
            Enabled = enable;

            if (enable)
                logger?.LogInformation("Native allocator active (log pages, hash index, recovery/scan frames via direct " +
                    "virtual memory). This memory is OUTSIDE the managed GC heap: size GCHeapHardLimit/GCHeapHardLimitPercent " +
                    "to leave headroom for it; set DOTNET_GCDynamicAdaptationMode=0 (DATAS is blind to native memory and may " +
                    "grow the heap into container OOM); monitor 'native_allocator_bytes' in INFO memory.");

            return enable;
        }
    }
}