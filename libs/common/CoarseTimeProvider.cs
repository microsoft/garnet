// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// A <see cref="TimeProvider"/> whose <see cref="GetUtcNow"/> returns a coarse,
    /// cached value (refreshed every <see cref="RefreshPeriod"/>) so hot paths can
    /// read the wall clock as a pure memory load. All other <see cref="TimeProvider"/>
    /// members delegate to <see cref="TimeProvider.System"/>. The cache and its refresh
    /// timer are static (process-wide), so this type is not customizable — its sole
    /// purpose is to act as an injection point for code that wants to depend on the
    /// <see cref="TimeProvider"/> abstraction while still benefiting from the cached
    /// hot-path read.
    /// </summary>
    public sealed class CoarseTimeProvider : TimeProvider
    {
        /// <summary>
        /// Refresh cadence. The cached value can lag the wall clock by approximately
        /// this duration (best-effort: actual lag depends on Timer cadence, which can
        /// slip under GC pauses or ThreadPool starvation).
        /// </summary>
        public static readonly TimeSpan RefreshPeriod = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Shared instance for callers that want to avoid an allocation. All
        /// <see cref="CoarseTimeProvider"/> instances are functionally identical
        /// — they all read the same static cache — so this is purely a convenience.
        /// </summary>
        public static readonly CoarseTimeProvider Instance = new();

        // MUST stay declared before `timer`: C# initializes static fields in textual
        // order, and timer's callback reads utcTicks via a static reference.
        //
        // Cached as `long ticks` (not `DateTimeOffset`) for atomic read/write: `long`
        // is 8 bytes and naturally aligned, so reads/writes are atomic on every
        // platform we target. Volatile.Read/Write give us release/acquire ordering
        // so updates propagate promptly across cores. We pay the cost of
        // reconstructing a DateTimeOffset on every read (~1 ns range-check) in
        // exchange for tearing-free, ordering-correct access.
        private static long utcTicks = TimeProvider.System.GetUtcNow().UtcTicks;

        // Process-wide background refresh. Held in a static field to keep it rooted
        // for process lifetime — the cache is shared by every CoarseTimeProvider
        // instance, so there is exactly one refresh Timer regardless of how many
        // instances are constructed.
        private static readonly ITimer timer = TimeProvider.System.CreateTimer(
            static _ => Volatile.Write(ref utcTicks, TimeProvider.System.GetUtcNow().UtcTicks),
            null,
            RefreshPeriod,
            RefreshPeriod);

        /// <summary>
        /// Constructs a <see cref="CoarseTimeProvider"/>. Instances hold no state —
        /// all reads route through the shared static cache. Use this constructor (or
        /// the <see cref="Instance"/> singleton) when you need a <see cref="TimeProvider"/>
        /// to inject and want production code to benefit from the cached hot-path read.
        /// </summary>
        public CoarseTimeProvider() { }

        /// <summary>
        /// Coarse, cached UTC time. May lag the wall clock by ~<see cref="RefreshPeriod"/>.
        /// </summary>
        public override DateTimeOffset GetUtcNow() => new(Volatile.Read(ref utcTicks), TimeSpan.Zero);

        /// <summary>
        /// Coarse, cached UTC time as a <see cref="DateTime"/>. Convenience wrapper
        /// around <see cref="GetUtcNow"/> for callers that want a <see cref="DateTime"/>
        /// directly.
        /// </summary>
        public DateTime UtcNow => new(Volatile.Read(ref utcTicks), DateTimeKind.Utc);

        /// <summary>
        /// Ticks of the coarse, cached UTC time. Reading this is a single memory load
        /// — preferred on hot paths over <see cref="UtcNow"/> / <see cref="GetUtcNow"/>
        /// because it skips <see cref="DateTime"/>'s Kind-bit masking on comparison and
        /// avoids the <see cref="DateTimeOffset"/> struct construction.
        /// </summary>
        public long UtcNowTicks => Volatile.Read(ref utcTicks);

        // The remaining TimeProvider surface delegates to TimeProvider.System — only
        // GetUtcNow is coarse. We intentionally do NOT cache monotonic time or wrap
        // timers; callers that need either get the accurate underlying behaviour.
        /// <inheritdoc/>
        public override long GetTimestamp() => TimeProvider.System.GetTimestamp();
        /// <inheritdoc/>
        public override long TimestampFrequency => TimeProvider.System.TimestampFrequency;
        /// <inheritdoc/>
        public override TimeZoneInfo LocalTimeZone => TimeProvider.System.LocalTimeZone;
        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object state, TimeSpan dueTime, TimeSpan period)
            => TimeProvider.System.CreateTimer(callback, state, dueTime, period);
    }
}