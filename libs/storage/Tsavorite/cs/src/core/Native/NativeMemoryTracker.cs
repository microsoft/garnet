// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Process-wide accounting of memory allocated through the native (mimalloc / direct-VM) allocators.
    /// This is <b>telemetry only</b>: it lets the host surface native usage (e.g. via <c>INFO memory</c>) and
    /// size the managed GC heap limit against the container limit. It is deliberately <b>not</b> wired to
    /// <see cref="System.GC.AddMemoryPressure(long)"/> — for large, long-lived, deterministically-freed
    /// allocations that would only bias the GC toward unproductive Gen2 collections without being able to
    /// reclaim the native memory. Heap-sizing / OOM avoidance is handled via <c>GCHeapHardLimit</c> +
    /// <c>GC.RefreshMemoryLimit()</c> at the host, informed by this counter.
    /// </summary>
    public static class NativeMemoryTracker
    {
        static long bytes;

        /// <summary>Current outstanding native bytes (best-effort; usable-size granularity for mimalloc).</summary>
        public static long Bytes => Interlocked.Read(ref bytes);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Add(long delta) => Interlocked.Add(ref bytes, delta);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Subtract(long delta) => Interlocked.Add(ref bytes, -delta);
    }
}
