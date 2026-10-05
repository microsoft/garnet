// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.cluster
{
    /// <summary>
    /// Tracks cluster configuration epochs shared across sessions.
    /// </summary>
    /// <typeparam name="TEpochObserver">
    /// Concrete observer source scanned to determine epoch quiescence. Constrained to
    /// <c>struct</c> so the JIT specializes this generic per source type and the quiescence
    /// check devirtualizes and inlines instead of dispatching through an interface.
    /// </typeparam>
    internal sealed class GarnetEpoch<TEpochObserver>
        where TEpochObserver : struct, IEpochObserver
    {
        /// <summary>Adaptive spin iterations on the fast path before a waiter parks.</summary>
        const int SpinIterations = 40;

        /// <summary>
        /// Upper bound on each parked wait. A theoretically missed wake degrades to a re-scan after
        /// this interval instead of hanging; in steady state the release signal delivers the wake first.
        /// Fixed for now; a future stage should replace the constant slice with exponential backoff
        /// (short initial slices growing to a cap) so a genuinely stuck bump re-scans cheaply at first
        /// yet idles quietly if the wait is long.
        /// </summary>
        static readonly TimeSpan DefaultWaitSleep = TimeSpan.FromMilliseconds(100);

        readonly TEpochObserver epochObserver;
        long currentEpoch = 1;

        // Wake-barrier state. The signal is set only when a bump-waiter is parked, so releasing
        // sessions pay nothing in the common case.
        readonly AsyncManualResetSignal epochReleased = new();
        int parkedWaiters;

        /// <summary>
        /// Creates a new Garnet epoch tracker that determines quiescence through <paramref name="observerSource"/>.
        /// </summary>
        /// <param name="observerSource">Source scanned to determine epoch quiescence.</param>
        internal GarnetEpoch(TEpochObserver observerSource)
        {
            this.epochObserver = observerSource;
        }

        /// <summary>
        /// Gets the current epoch.
        /// </summary>
        internal long GetCurrentEpoch() => Volatile.Read(ref currentEpoch);

        /// <summary>
        /// Bumps the current epoch and waits for active cluster sessions to observe the transition,
        /// busy-spinning (cooperatively yielding) over the quiescence scan until all have transitioned.
        /// Shares the <see cref="IEpochObserver"/> predicate with the wake-based variant, so the two
        /// differ only in how they wait, not in what they wait for.
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
        /// Bumps the current epoch and waits for active cluster sessions to observe the transition,
        /// using a short adaptive spin followed by a true async park. Releasing sessions signal the
        /// waiter via <see cref="NotifyEpochReleased"/>, so no CPU is burned while the bump is pending.
        ///
        /// Lost-wake-safe protocol: the waiter publishes that it is parked, re-arms the signal, then
        /// re-checks quiescence before awaiting; a release that set the session epoch before reading
        /// <see cref="parkedWaiters"/> is therefore always observed by the re-check, and the bounded
        /// park guarantees liveness even if a signal is theoretically missed.
        /// </summary>
        /// <remarks>
        /// Concurrency assumption: epoch bumps are rare and effectively serialized in practice.
        /// Migration bumps run strictly one after another as key batches are read; failover bumps
        /// apply to replicas while migration applies to primaries, so the two do not overlap. The
        /// only realistic concurrency is a parallel failover producing at most two in-flight bumps.
        /// The shared <see cref="AsyncManualResetSignal"/> handles this by coalescing a single
        /// <c>Set</c> across every parked waiter, so concurrent bumps are correct today.
        ///
        /// If frequent parallel bumps ever become a reality, this waiter-side wake needs revisiting:
        /// one <c>Set</c> wakes all parked waiters and each re-runs the full quiescence scan (a bounded
        /// thundering herd that grows with the number of simultaneous bumps). A redesign would then
        /// serialize bump-waiters (e.g. a single-slot async gate) and could switch to a zero-allocation
        /// single-waiter park primitive.
        /// </remarks>
        /// <param name="waitSleep">
        /// Optional override for the bounded park interval between re-scans while waiting. Tests and benchmarks use this to tighten or
        /// loosen the liveness backstop without affecting production callers.
        /// </param>
        /// <param name="token">Cancellation token. When canceled, returns false without further waiting.</param>
        /// <returns>True when all active cluster sessions have transitioned; false if canceled.</returns>
        internal async Task<bool> BumpAndWaitForEpochTransitionAsync(TimeSpan waitSleep = default, CancellationToken token = default)
        {
            var target = Interlocked.Increment(ref currentEpoch);
            var slice = waitSleep == default ? DefaultWaitSleep : waitSleep;

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

            // Slow path: park until every session has observed the new epoch.
            Interlocked.Increment(ref parkedWaiters);
            try
            {
                while (!epochObserver.AllSessionsQuiesced(target))
                {
                    if (token.IsCancellationRequested)
                        return false;

                    epochReleased.Reset();

                    // Re-check after reset so a signal that raced the reset is not lost.
                    if (epochObserver.AllSessionsQuiesced(target))
                        return true;

                    await epochReleased.WaitAsync(slice, token).ConfigureAwait(false);
                }

                return true;
            }
            finally
            {
                Interlocked.Decrement(ref parkedWaiters);
            }
        }

        /// <summary>
        /// Waker hook invoked when a session releases its observed epoch. O(1) and allocation-free:
        /// it signals the shared wake only when a bump-waiter is actually parked, so sessions that
        /// are not racing an in-flight bump pay a single volatile read.
        /// </summary>
        internal void NotifyEpochReleased()
        {
            if (Volatile.Read(ref parkedWaiters) > 0)
                epochReleased.Set();
        }
    }
}