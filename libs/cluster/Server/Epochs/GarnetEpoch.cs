// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.cluster
{
    /// <summary>
    /// Tracks cluster configuration epochs shared across sessions and lets a bump wait until every
    /// active session has observed the transition. Both wait strategies share the same
    /// <see cref="IEpochObserver"/> quiescence predicate and differ only in how they wait:
    /// <list type="bullet">
    /// <item><see cref="BumpAndWaitForEpochTransitionAsync"/> — the production path. A short adaptive
    /// spin covers the common case where sessions drain immediately; a bump that must wait longer falls
    /// back to a jittered, capped exponential-backoff <em>self-poll</em>. There is no waker: releasing
    /// sessions do nothing, so the session hot path is untouched, concurrent bumps need no coordination
    /// (each independently observes quiescence), and a stuck bump idles at the capped poll rate instead
    /// of pinning a core.</item>
    /// <item><see cref="BumpAndSpinWaitForEpochTransitionAsync"/> — the legacy cooperative busy-spin,
    /// retained only as an A/B baseline for the cluster benchmarks. It burns CPU for the whole wait and
    /// is exactly what the self-poll path replaces.</item>
    /// </list>
    /// </summary>
    /// <typeparam name="TEpochObserver">
    /// Concrete observer source scanned to determine epoch quiescence. Constrained to
    /// <c>struct</c> so the JIT specializes this generic per source type and the quiescence
    /// check devirtualizes and inlines instead of dispatching through an interface.
    /// </typeparam>
    internal sealed class GarnetEpoch<TEpochObserver>
        where TEpochObserver : struct, IEpochObserver
    {
        /// <summary>Default baseline park slice; the first self-poll waits roughly this long.</summary>
        static readonly TimeSpan DefaultBaseParkDelay = TimeSpan.FromMilliseconds(1);

        /// <summary>Default cap on the self-poll slice; a genuinely stuck bump re-scans no slower than this.</summary>
        static readonly TimeSpan DefaultMaxParkDelay = TimeSpan.FromMilliseconds(50);

        /// <summary>Adaptive spin iterations on the fast path before a bump falls back to self-polling.</summary>
        const int SpinIterations = 40;

        readonly TEpochObserver epochObserver;
        readonly TimeSpan baseParkDelay;
        readonly TimeSpan maxParkDelay;
        long currentEpoch = 1;

        /// <summary>
        /// Creates a new Garnet epoch tracker that determines quiescence through <paramref name="observerSource"/>.
        /// </summary>
        /// <param name="observerSource">Source scanned to determine epoch quiescence.</param>
        /// <param name="baseParkDelay">
        /// Baseline slice for the first self-poll on the slow path. Each waiter derives its own jittered,
        /// exponentially growing slice from this baseline, so concurrent bumps re-scan out of lockstep.
        /// Defaults to <see cref="DefaultBaseParkDelay"/>.
        /// </param>
        /// <param name="maxParkDelay">Upper bound on the self-poll slice. Defaults to <see cref="DefaultMaxParkDelay"/>.</param>
        internal GarnetEpoch(TEpochObserver observerSource, TimeSpan? baseParkDelay = null, TimeSpan? maxParkDelay = null)
        {
            this.epochObserver = observerSource;
            this.baseParkDelay = baseParkDelay ?? DefaultBaseParkDelay;
            this.maxParkDelay = maxParkDelay ?? DefaultMaxParkDelay;
        }

        /// <summary>
        /// Gets the current epoch.
        /// </summary>
        internal long GetCurrentEpoch() => Volatile.Read(ref currentEpoch);

        /// <summary>
        /// Bumps the current epoch and waits for active cluster sessions to observe the transition using
        /// a cooperative busy-spin. Retained only as an A/B baseline for the cluster benchmarks; the
        /// production path is <see cref="BumpAndWaitForEpochTransitionAsync"/>. Shares the
        /// <see cref="IEpochObserver"/> predicate with the self-poll variant, so the two differ only in
        /// how they wait, not in what they wait for.
        /// </summary>
        /// <returns>True when all active cluster sessions have transitioned.</returns>
        internal async Task<bool> BumpAndSpinWaitForEpochTransitionAsync()
        {
            var target = Interlocked.Increment(ref currentEpoch);
            while (!epochObserver.AllSessionsQuiesced(target))
                await Task.Yield();
            return true;
        }

        /// <summary>
        /// Bumps the current epoch and waits for active cluster sessions to observe the transition. A
        /// short adaptive spin handles the common case where sessions drain immediately; otherwise the
        /// bump self-polls the quiescence predicate on a jittered, capped exponential backoff.
        ///
        /// There is no waker. Each parked bump re-scans on its own schedule, so a releasing session pays
        /// nothing, concurrent bumps need no coordination (each independently observes quiescence), and a
        /// long-stuck bump idles at the capped poll rate instead of burning a core. The bounded slice also
        /// serves as the liveness backstop: a session that quiesces is always observed by the next re-scan.
        /// </summary>
        /// <param name="token">Cancellation token. When canceled, returns false without further waiting.</param>
        /// <returns>True when all active cluster sessions have transitioned; false if canceled.</returns>
        internal async Task<bool> BumpAndWaitForEpochTransitionAsync(CancellationToken token = default)
        {
            var target = Interlocked.Increment(ref currentEpoch);

            // Fast path: brief adaptive spin for the common case where sessions drain immediately.
            var spinner = new SpinWait();
            for (var i = 0; i < SpinIterations; i++)
            {
                if (epochObserver.AllSessionsQuiesced(target))
                    return true;
                if (token.IsCancellationRequested)
                    return false;
                spinner.SpinOnce();
            }

            // Slow path: jittered, capped exponential-backoff self-poll. No locking or allocation on the
            // session side; the only cost is this bump's own periodic re-scan.
            var backoff = new ExponentialBackoff(baseParkDelay, maxParkDelay);
            while (!epochObserver.AllSessionsQuiesced(target))
            {
                if (token.IsCancellationRequested)
                    return false;
                try
                {
                    await Task.Delay(backoff.RecordFailure(), token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return false;
                }
            }
            return true;
        }
    }
}