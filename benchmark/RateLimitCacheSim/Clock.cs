// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

namespace RateLimitCacheSim
{
    /// <summary>
    /// Timing helpers shared by the runners.
    /// </summary>
    public static class Clock
    {
        /// <summary>Stopwatch ticks in one millisecond.</summary>
        public static readonly long TicksPerMs = Stopwatch.Frequency / 1000;

        /// <summary>Current timestamp.</summary>
        /// <returns>Stopwatch ticks.</returns>
        public static long Now() => Stopwatch.GetTimestamp();

        /// <summary>
        /// Waits until the given timestamp, sleeping for the bulk of the interval and spinning for the
        /// last millisecond so that sub-millisecond schedules are not quantised by the timer.
        /// </summary>
        /// <param name="dueTicks">Target timestamp.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task that completes at or just after the target.</returns>
        public static async Task WaitUntilAsync(long dueTicks, CancellationToken token)
        {
            var remaining = dueTicks - Now();
            if (remaining <= 0) return;

            var sleepMs = (int)((remaining - TicksPerMs) / TicksPerMs);
            if (sleepMs > 0)
            {
                try
                {
                    await Task.Delay(sleepMs, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }

            while (Now() < dueTicks && !token.IsCancellationRequested)
                Thread.SpinWait(64);
        }

        /// <summary>
        /// Blocking equivalent of <see cref="WaitUntilAsync"/>.
        /// </summary>
        /// <param name="dueTicks">Target timestamp.</param>
        /// <param name="token">Cancellation token.</param>
        public static void WaitUntil(long dueTicks, CancellationToken token)
        {
            var remaining = dueTicks - Now();
            if (remaining <= 0) return;

            var sleepMs = (int)((remaining - TicksPerMs) / TicksPerMs);
            if (sleepMs > 0)
            {
                if (token.WaitHandle.WaitOne(sleepMs)) return;
            }

            while (Now() < dueTicks && !token.IsCancellationRequested)
                Thread.SpinWait(64);
        }
    }
}