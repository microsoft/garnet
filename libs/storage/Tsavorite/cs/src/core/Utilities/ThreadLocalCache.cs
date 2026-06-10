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
    /// One <see cref="ThreadStaticAttribute"/> slot is allocated per <em>closed</em>
    /// <typeparamref name="T"/>, so different pooled types never share a TLS slot.
    /// Per-thread footprint is <c>Capacity × 8 B</c> of references (≈ 512 B per cached
    /// type) plus the cached instances themselves (bounded by actual peak demand, not
    /// by <see cref="Capacity"/>).
    /// </para>
    /// </summary>
    /// <typeparam name="T">The pooled object type. Must be a reference type since we
    /// store <c>null</c> in vacated slots to allow GC of abandoned wrappers.</typeparam>
    public static class ThreadLocalCache<T> where T : class
    {
        /// <summary>
        /// Per-thread cache slot count. Sized to comfortably cover the typical short-burst
        /// Rent/Return pattern of one worker thread (a few dozen in flight) while keeping
        /// per-thread footprint to ≈ 512 B of references. Empirically (measured on
        /// KV.bench and resp.bench disk-bound recipes) raising this to 1024 to fully
        /// absorb a 1024-batch burst gives no throughput change — the global-queue
        /// fallback is not the bottleneck at the rates this is used.
        /// </summary>
        public const int Capacity = 64;

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
        /// Try to push <paramref name="item"/> onto this thread's cache. Returns
        /// <c>false</c> if the cache is full — the caller should then enqueue
        /// <paramref name="item"/> on its global queue (or drop it per the caller's
        /// own retention policy).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReturn(T item)
        {
            var c = cache;
            if (c == null)
            {
                c = new T[Capacity];
                cache = c;
            }
            var n = count;
            if ((uint)n < (uint)Capacity)
            {
                c[n] = item;
                count = n + 1;
                return true;
            }
            return false;
        }
    }
}