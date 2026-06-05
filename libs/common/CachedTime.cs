// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Process-wide cached UTC time, refreshed by a background <see cref="Timer"/>.
    /// Reading the cached value is a pure memory load and avoids the per-call
    /// <see cref="DateTime.UtcNow"/> syscall on hot paths where the
    /// <see cref="RefreshPeriodMs"/> ms granularity is acceptable.
    /// </summary>
    /// <remarks>
    /// Use only when the callsite is on a hot path AND the bounded staleness is
    /// acceptable; for anything requiring sub-<see cref="RefreshPeriodMs"/> ms
    /// precision call <see cref="DateTime.UtcNow"/> directly.
    /// </remarks>
    public static class CachedTime
    {
        /// <summary>
        /// Refresh period in milliseconds. The cached value can lag the real
        /// wall-clock by up to this many milliseconds.
        /// </summary>
        public const int RefreshPeriodMs = 100;

        private static long utcTicks = DateTime.UtcNow.Ticks;

        // Rooted in a static field so the Timer survives for process lifetime. Callbacks
        // can in principle overlap if the ThreadPool is starved, but the race is benign:
        // both writers race a fresh DateTime.UtcNow.Ticks into the same long, and a
        // briefly-stale (or briefly-backwards) cached value is exactly the bounded
        // staleness this class already promises.
        private static readonly Timer timer = new Timer(static _ => UpdateCachedUtc(), state: null, dueTime: RefreshPeriodMs, period: RefreshPeriodMs);

        private static void UpdateCachedUtc() => Volatile.Write(ref utcTicks, DateTime.UtcNow.Ticks);

        /// <summary>
        /// Most recently cached <see cref="DateTime.UtcNow"/> Ticks. May lag the
        /// real wall-clock by up to <see cref="RefreshPeriodMs"/> ms.
        /// </summary>
        public static long UtcNowTicks => Volatile.Read(ref utcTicks);

        /// <summary>
        /// Most recently cached <see cref="DateTime.UtcNow"/>. May lag the real
        /// wall-clock by up to <see cref="RefreshPeriodMs"/> ms.
        /// </summary>
        public static DateTime UtcNow => new DateTime(Volatile.Read(ref utcTicks), DateTimeKind.Utc);
    }
}
