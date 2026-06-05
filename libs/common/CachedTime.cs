// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.common;

/// <summary>
/// Process-wide cached UTC time, refreshed by a background <see cref="Timer"/>.
/// Reading the cached value is a pure memory load and avoids the per-call
/// <see cref="DateTime.UtcNow"/> syscall (clock_gettime via vDSO on Linux,
/// QueryPerformanceCounter on Windows) on hot paths where the
/// <see cref="RefreshPeriodMs"/> ms granularity is acceptable.
/// </summary>
/// <remarks>
/// Modeled after Redis's <c>server.mstime</c> cache. Use only when the
/// callsite is on a hot path AND the bounded staleness is acceptable;
/// for anything requiring sub-20ms precision call <see cref="DateTime.UtcNow"/>
/// directly.
/// </remarks>
public static class CachedTime
{
    /// <summary>
    /// Refresh period in milliseconds. The cached value can lag the real
    /// wall-clock by up to this many milliseconds. Matches Redis's default
    /// <c>hz=10</c> cadence.
    /// </summary>
    public const int RefreshPeriodMs = 100;

    private static long _utcTicks = DateTime.UtcNow.Ticks;

    // Rooted in a static field so the Timer survives for process lifetime.
    private static readonly Timer _timer = new Timer(
        static _ => Volatile.Write(ref _utcTicks, DateTime.UtcNow.Ticks),
        state: null, dueTime: RefreshPeriodMs, period: RefreshPeriodMs);

    /// <summary>
    /// Most recently cached <see cref="DateTime.UtcNow"/> Ticks. May lag the
    /// real wall-clock by up to <see cref="RefreshPeriodMs"/> ms.
    /// </summary>
    public static long UtcNowTicks => Volatile.Read(ref _utcTicks);

    /// <summary>
    /// Most recently cached <see cref="DateTime.UtcNow"/>. May lag the real
    /// wall-clock by up to <see cref="RefreshPeriodMs"/> ms.
    /// </summary>
    public static DateTime UtcNow => new DateTime(Volatile.Read(ref _utcTicks), DateTimeKind.Utc);
}
