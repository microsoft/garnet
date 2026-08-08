// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Process-wide accounting of memory allocated through the native allocators, surfaced for telemetry
    /// (e.g. <c>INFO memory</c>) and for sizing the managed GC heap limit against the container limit. It is
    /// deliberately <b>not</b> wired to <see cref="System.GC.AddMemoryPressure(long)"/> — for large, long-lived,
    /// deterministically-freed allocations that would only bias the GC toward unproductive Gen2 collections
    /// without being able to reclaim the native memory.
    /// <para>
    /// mimalloc-backed usage (the buffer pool) is read <b>on demand</b> from mimalloc's own stats
    /// (<see cref="Mimalloc.CommittedBytes"/>) so the hot rent/return path does <b>zero</b> per-op accounting —
    /// profiling showed per-op counting cost ~15ns/op and (before striping) also capped throughput. Direct-VM
    /// singletons (full mode) are allocated outside mimalloc and are counted via the striped per-CPU counter
    /// below (cheap because those allocations are rare).
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

        /// <summary>
        /// Current outstanding native bytes: mimalloc committed (on-demand) plus direct-VM tracked bytes.
        /// Best-effort snapshot.
        /// </summary>
        public static long Bytes
        {
            get
            {
                long total = Mimalloc.CommittedBytes();
                for (var i = 0; i < slots.Length; i += Stride)
                    total += Interlocked.Read(ref slots[i]);
                return total;
            }
        }

        /// <summary>Direct-VM only: record a native allocation (mmap/VirtualAlloc). Not called on the pool hot path.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Add(long delta)
            => Interlocked.Add(ref slots[(Thread.GetCurrentProcessorId() & mask) * Stride], delta);

        /// <summary>Direct-VM only: record a native free (munmap/VirtualFree). Not called on the pool hot path.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Subtract(long delta)
            => Interlocked.Add(ref slots[(Thread.GetCurrentProcessorId() & mask) * Stride], -delta);
    }
}