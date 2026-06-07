// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Thread-local LIFO cache for object references, intended as the fast tier of a
    /// two-tier pool (TLS-first → caller-provided global queue fallback). Single-thread
    /// access to <see cref="ThreadStaticAttribute"/>-backed storage means no locks, no
    /// atomics, ~2-5 ns per <see cref="TryRent"/> / <see cref="TryReturn"/>.
    /// <para>
    /// Used by two-tier object pools that need to absorb back-to-back Rent/Return cycles
    /// from a single worker thread without paying the CAS cost of the shared global
    /// queue on the hot path. Callers handle the global fallback themselves so each
    /// consumer keeps its own bounding / drop policy.
    /// </para>
    /// <para>
    /// The per-thread array starts at <see cref="InitialCapacity"/> and grows geometrically
    /// (up to the caller-supplied <c>maxCapacity</c> on <see cref="TryReturn"/>) so a worker
    /// whose in-flight depth exceeds the initial size keeps a same-thread Rent/Return cycle
    /// entirely in TLS instead of spilling to the shared global tier — the spill is the
    /// scaling bottleneck under many threads. One <see cref="ThreadStaticAttribute"/> slot is
    /// allocated per <em>closed</em> <typeparamref name="T"/>, so different pooled types never
    /// share a slot; footprint is the grown array (≤ maxCapacity × 8 B) plus the cached
    /// instances (bounded by actual peak demand).
    /// </para>
    /// </summary>
    /// <typeparam name="T">The pooled object type. Must be a reference type since we
    /// store <c>null</c> in vacated slots to allow GC of abandoned wrappers.</typeparam>
    public static class ThreadLocalCache<T> where T : class
    {
        /// <summary>
        /// Initial per-thread array size. The array grows geometrically from here up to the
        /// <c>maxCapacity</c> passed to <see cref="TryReturn"/> when a worker's in-flight depth
        /// exceeds it, so low-demand threads keep a ≈ 512 B footprint while a high-in-flight
        /// worker (e.g. a deep read-pending batch) auto-sizes to absorb its full burst.
        /// </summary>
        public const int InitialCapacity = 64;

        [ThreadStatic]
        private static T[] cache;

        [ThreadStatic]
        private static int count;

        /// <summary>
        /// Try to pop the most-recently-Returned <typeparamref name="T"/> from this
        /// thread's cache. Returns <c>false</c> if the thread's cache is empty (or
        /// uninitialized) — the caller should then fall back to its global queue.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRent(out T item)
        {
            var c = cache;
            var idx = count - 1;
            if (c != null && idx >= 0)
            {
                item = c[idx];
                // Clear the slot so a rented-but-abandoned wrapper can be GC'd.
                c[idx] = null;
                count = idx;
                return true;
            }
            item = null;
            return false;
        }

        /// <summary>
        /// Try to push <paramref name="item"/> onto this thread's cache, growing the backing
        /// array geometrically (up to <paramref name="maxCapacity"/>) so a worker whose
        /// in-flight depth exceeds <see cref="InitialCapacity"/> keeps returning into TLS
        /// instead of the shared tier. Returns <c>false</c> only once the cache has reached
        /// <paramref name="maxCapacity"/> — the caller should then enqueue <paramref name="item"/>
        /// on its global queue (or drop it per the caller's own retention policy).
        /// </summary>
        /// <param name="item">The instance to cache.</param>
        /// <param name="maxCapacity">Growth ceiling for this caller's per-thread cache. Bounds
        /// the worst-case footprint; pass a value covering the caller's peak in-flight depth.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReturn(T item, int maxCapacity = InitialCapacity)
        {
            var c = cache;
            var n = count;
            if (c == null)
            {
                c = cache = new T[InitialCapacity < maxCapacity ? InitialCapacity : maxCapacity];
            }
            else if (n == c.Length)
            {
                if (n >= maxCapacity)
                    return false;
                var grown = c.Length * 2;
                var nc = new T[grown < maxCapacity ? grown : maxCapacity];
                Array.Copy(c, nc, n);
                c = cache = nc;
            }
            c[n] = item;
            count = n + 1;
            return true;
        }
    }
}