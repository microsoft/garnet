// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// A <see cref="TimeProvider"/> whose <see cref="GetUtcNow"/> returns a coarse,
    /// cached value (refreshed every <see cref="RefreshPeriodMs"/> ms) so hot paths
    /// can read the wall clock as a pure memory load. All other <see cref="TimeProvider"/>
    /// members delegate to the wrapped provider — only <see cref="GetUtcNow"/> is coarse.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The cache refresh is scheduled via <see cref="TimeProvider.CreateTimer"/> on the
    /// wrapped provider, so a <c>FakeTimeProvider</c> used in tests will drive refreshes
    /// when its time is advanced — production and test code therefore exercise the same
    /// hot path.
    /// </para>
    /// <para>
    /// Every instance owns a refresh <see cref="ITimer"/>. To keep production at exactly
    /// one process-wide Timer, callers MUST go through <see cref="Create"/> (which
    /// returns the <see cref="System"/> singleton when handed <see cref="TimeProvider.System"/>)
    /// instead of invoking the public constructor directly. The constructor is public
    /// only so tests can wrap a custom <c>FakeTimeProvider</c> without ceremony.
    /// </para>
    /// </remarks>
    public sealed class CoarseTimeProvider : TimeProvider, IDisposable
    {
        /// <summary>
        /// Refresh period in milliseconds. The cached value can lag the underlying
        /// provider by approximately this many milliseconds (best-effort: actual lag
        /// depends on Timer cadence, which can slip under GC pauses or ThreadPool
        /// starvation).
        /// </summary>
        public const int RefreshPeriodMs = 100;

        /// <summary>
        /// Process-wide shared instance backed by <see cref="TimeProvider.System"/>.
        /// Use this — or <see cref="Create"/> when the provider is parameterized — in
        /// production code; it amortizes the single background Timer across all callers.
        /// </summary>
        public static new readonly CoarseTimeProvider System = new(TimeProvider.System);

        private readonly TimeProvider timeProvider;
        private readonly ITimer timer;
        private long utcTicks;
        private bool disposed;

        /// <summary>
        /// Returns a <see cref="CoarseTimeProvider"/> for <paramref name="timeProvider"/>:
        /// the <see cref="System"/> singleton when <paramref name="timeProvider"/> is
        /// <c>null</c> or <see cref="TimeProvider.System"/>, otherwise a newly-constructed
        /// instance the caller is responsible for disposing. This is the recommended way
        /// to obtain a <see cref="CoarseTimeProvider"/> — it prevents callers from
        /// accidentally spawning a redundant background Timer when they intended to use
        /// the process-wide cache.
        /// </summary>
        public static CoarseTimeProvider Create(TimeProvider timeProvider)
        {
            if (timeProvider is null || ReferenceEquals(timeProvider, TimeProvider.System))
            {
                return System;
            }
            return new CoarseTimeProvider(timeProvider);
        }

        /// <summary>
        /// Constructs a new coarse time cache backed by <paramref name="timeProvider"/>
        /// — always spawns a background refresh Timer. Prefer <see cref="Create"/>
        /// for any non-test caller so passing <see cref="TimeProvider.System"/> reuses
        /// the singleton instead of leaking a Timer.
        /// </summary>
        public CoarseTimeProvider(TimeProvider timeProvider)
        {
            ArgumentNullException.ThrowIfNull(timeProvider);
            this.timeProvider = timeProvider;
            utcTicks = timeProvider.GetUtcNow().UtcTicks;
            timer = timeProvider.CreateTimer(
                static state => ((CoarseTimeProvider)state).Refresh(),
                this,
                TimeSpan.FromMilliseconds(RefreshPeriodMs),
                TimeSpan.FromMilliseconds(RefreshPeriodMs));
        }

        private void Refresh() => Volatile.Write(ref utcTicks, timeProvider.GetUtcNow().UtcTicks);

        /// <summary>
        /// Coarse, cached UTC time. May lag the underlying provider by ~<see cref="RefreshPeriodMs"/> ms.
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

        // The remaining TimeProvider surface delegates to the wrapped provider — only
        // GetUtcNow is coarse. We intentionally do NOT cache monotonic time or wrap
        // timers; callers that need either get the accurate underlying behaviour.
        /// <inheritdoc/>
        public override long GetTimestamp() => timeProvider.GetTimestamp();
        /// <inheritdoc/>
        public override long TimestampFrequency => timeProvider.TimestampFrequency;
        /// <inheritdoc/>
        public override TimeZoneInfo LocalTimeZone => timeProvider.LocalTimeZone;
        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object state, TimeSpan dueTime, TimeSpan period)
            => timeProvider.CreateTimer(callback, state, dueTime, period);

        /// <summary>
        /// Disposes the refresh timer. No-op on the <see cref="System"/> singleton
        /// (which must live for process lifetime) and safe to call multiple times,
        /// so callers can dispose unconditionally regardless of whether they obtained
        /// the singleton or a fresh instance from <see cref="Create"/>.
        /// </summary>
        public void Dispose()
        {
            if (ReferenceEquals(this, System)) return;
            if (disposed) return;
            disposed = true;
            timer.Dispose();
        }
    }
}

