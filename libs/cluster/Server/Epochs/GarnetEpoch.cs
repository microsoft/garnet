// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;

namespace Garnet.cluster
{
    /// <summary>
    /// Tracks cluster configuration epochs shared across sessions.
    /// </summary>
    /// <typeparam name="TObserverSource">
    /// Concrete observer source scanned to determine epoch quiescence. Constrained to
    /// <c>struct</c> so the JIT specializes this generic per source type and the quiescence
    /// check devirtualizes and inlines instead of dispatching through an interface.
    /// </typeparam>
    internal sealed class GarnetEpoch<TObserverSource>
        where TObserverSource : struct, IEpochObserverSource
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
        static readonly TimeSpan ParkSlice = TimeSpan.FromMilliseconds(100);

        readonly StoreWrapper storeWrapper;
        readonly TObserverSource observerSource;
        long CurrentEpoch = 1;

        // Wake-barrier state. The signal is set only when a bump-waiter is parked, so releasing
        // sessions pay nothing in the common case.
        readonly AsyncEpochSignal epochReleased = new();
        int parkedWaiters;

        /// <summary>
        /// Creates a new Garnet epoch tracker over the servers owned by <paramref name="storeWrapper"/>.
        /// </summary>
        /// <param name="storeWrapper">Store wrapper containing the active servers (may be null in tests that only use the wake path).</param>
        /// <param name="observerSource">Source scanned to determine epoch quiescence.</param>
        internal GarnetEpoch(StoreWrapper storeWrapper, TObserverSource observerSource)
        {
            this.storeWrapper = storeWrapper;
            this.observerSource = observerSource;
        }

        /// <summary>
        /// Gets the current epoch.
        /// </summary>
        internal long GetCurrentEpoch() => Volatile.Read(ref CurrentEpoch);

        /// <summary>
        /// Bumps the current epoch and waits for active cluster sessions to observe the transition.
        /// </summary>
        /// <returns>True when all active cluster sessions have transitioned.</returns>
        internal async Task<bool> BumpAndWaitForEpochTransitionAsync()
        {
            var currentEpoch = Interlocked.Increment(ref CurrentEpoch);
            foreach (var server in storeWrapper.Servers)
            {
                while (true)
                {
                retry:
                    await Task.Yield();
                    var sessions = ((GarnetServerTcp)server).ActiveClusterSessions();
                    foreach (var session in sessions)
                    {
                        var entryEpoch = session.LocalCurrentEpoch;
                        if (entryEpoch != 0 && entryEpoch < currentEpoch)
                            goto retry;
                    }
                    break;
                }
            }
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
        /// The shared <see cref="AsyncEpochSignal"/> handles this by coalescing a single
        /// <c>Set</c> across every parked waiter, so concurrent bumps are correct today.
        ///
        /// If frequent parallel bumps ever become a reality, this waiter-side wake needs revisiting:
        /// one <c>Set</c> wakes all parked waiters and each re-runs the full quiescence scan (a bounded
        /// thundering herd that grows with the number of simultaneous bumps). A redesign would then
        /// serialize bump-waiters (e.g. a single-slot async gate) and could switch to a zero-allocation
        /// single-waiter park primitive.
        /// </remarks>
        /// <param name="token">Cancellation token. When canceled, returns false without further waiting.</param>
        /// <returns>True when all active cluster sessions have transitioned; false if canceled.</returns>
        internal async Task<bool> BumpAndWaitForEpochTransitionWakeAsync(CancellationToken token = default)
        {
            var target = Interlocked.Increment(ref CurrentEpoch);

            // Fast path: brief adaptive spin for the common case where sessions drain immediately.
            var spinner = new SpinWait();
            for (var i = 0; i < SpinIterations; i++)
            {
                if (observerSource.AllObserversQuiesced(target))
                    return true;
                if (token.IsCancellationRequested)
                    return false;
                spinner.SpinOnce();
            }

            // Slow path: park until every session has observed the new epoch.
            Interlocked.Increment(ref parkedWaiters);
            try
            {
                while (!observerSource.AllObserversQuiesced(target))
                {
                    if (token.IsCancellationRequested)
                        return false;

                    epochReleased.Reset();

                    // Re-check after reset so a signal that raced the reset is not lost.
                    if (observerSource.AllObserversQuiesced(target))
                        return true;

                    await epochReleased.WaitAsync(ParkSlice, token).ConfigureAwait(false);
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