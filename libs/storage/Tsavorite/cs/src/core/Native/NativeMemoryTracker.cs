// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;
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
    /// <para>
    /// The counter is <b>striped per-CPU</b> and cache-line separated. A single global <see cref="Interlocked"/>
    /// counter is a shared-cache-line bottleneck on the buffer-pool hot path (millions of rent/return ops/sec):
    /// profiling showed it capping mimalloc throughput at ~9 Mops/s vs ~385 Mops/s untracked. Striping keeps
    /// per-op accounting effectively contention-free (each core updates its own line) while <see cref="Bytes"/>
    /// sums on demand.
    /// </para>
    /// </summary>
    public static class NativeMemoryTracker
    {
        // Longs per stripe (128-byte stride) so adjacent stripes never share a cache line regardless of the
        // managed array's base alignment.
        const int Stride = 16;

        static readonly long[] slots;
        static readonly int mask;

        static NativeMemoryTracker()
        {
            var stripes = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(1, Environment.ProcessorCount));
            slots = new long[stripes * Stride];
            mask = stripes - 1;
        }

        /// <summary>Current outstanding native bytes (best-effort snapshot; sums the per-CPU stripes).</summary>
        public static long Bytes
        {
            get
            {
                long total = 0;
                for (var i = 0; i < slots.Length; i += Stride)
                    total += Interlocked.Read(ref slots[i]);
                return total;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Add(long delta)
            => Interlocked.Add(ref slots[(Thread.GetCurrentProcessorId() & mask) * Stride], delta);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Subtract(long delta)
            => Interlocked.Add(ref slots[(Thread.GetCurrentProcessorId() & mask) * Stride], -delta);
    }
}
