// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Coarse-grained UTC time cache backed by an arbitrary <see cref="TimeProvider"/>.
    /// Reading the cached value is a pure memory load and avoids per-call clock reads
    /// on hot paths where <see cref="RefreshPeriodMs"/> ms staleness is acceptable.
    /// </summary>
    /// <remarks>
    /// The cache refresh is scheduled via <see cref="TimeProvider.CreateTimer"/>, so a
    /// <c>FakeTimeProvider</c> used in tests will drive refreshes when its time is
    /// advanced — production and test code therefore exercise the same hot path.
    /// </remarks>
    public sealed class CoarseTimeProvider : IDisposable
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
        /// Use this in production code — it amortizes the single background Timer
        /// across all callers.
        /// </summary>
        public static readonly CoarseTimeProvider System = new(TimeProvider.System);

        private readonly TimeProvider _timeProvider;
        private readonly ITimer _timer;
        private long _utcTicks;
        private bool _disposed;

        /// <summary>
        /// Create a coarse time cache backed by <paramref name="timeProvider"/>.
        /// </summary>
        public CoarseTimeProvider(TimeProvider timeProvider)
        {
            ArgumentNullException.ThrowIfNull(timeProvider);
            _timeProvider = timeProvider;
            _utcTicks = timeProvider.GetUtcNow().UtcTicks;
            _timer = timeProvider.CreateTimer(
                static state => ((CoarseTimeProvider)state).Refresh(),
                this,
                TimeSpan.FromMilliseconds(RefreshPeriodMs),
                TimeSpan.FromMilliseconds(RefreshPeriodMs));
        }

        private void Refresh() => Volatile.Write(ref _utcTicks, _timeProvider.GetUtcNow().UtcTicks);

        /// <summary>
        /// Most recently cached UTC time. May lag the underlying provider by ~<see cref="RefreshPeriodMs"/> ms.
        /// </summary>
        public DateTime UtcNow => new(Volatile.Read(ref _utcTicks), DateTimeKind.Utc);

        /// <summary>
        /// Ticks of the most recently cached UTC time. Reading this is a single
        /// memory load — preferred on hot paths over <see cref="UtcNow"/> because it
        /// avoids <see cref="DateTime"/>'s Kind-bit masking on comparison.
        /// </summary>
        public long UtcNowTicks => Volatile.Read(ref _utcTicks);

        /// <summary>
        /// Disposes the refresh timer. Safe to call multiple times. Do NOT dispose
        /// <see cref="System"/> — it's a process-wide singleton.
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _timer.Dispose();
        }
    }
}
