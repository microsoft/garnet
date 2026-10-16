// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace Garnet.server
{
    /// <summary>
    /// Await source for a session waiting to be woken by another session, which is the shape every real
    /// blocking command takes: a reader parks on a key and a writer on a different connection releases it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Unlike <see cref="SessionDelay"/>, whose wakeup time is fixed when the wait starts, the wakeup here is
    /// produced by another session's command and so has to be armed before the signal becomes reachable.
    /// </para>
    /// <para>
    /// The signal is reachable from a server-wide list for as long as it waits, and a session that is torn down
    /// while it waits is never resumed, so the signal takes itself off the list when it is cancelled.
    /// </para>
    /// </remarks>
    internal sealed class SessionSignal : SessionWaitSource
    {
        SessionSignalRegistry.Waiters listedOn;

        /// <summary>
        /// True once the session has been torn down, after which the signal must not stay listed.
        /// </summary>
        internal bool TornDown => Cancelled;

        /// <summary>
        /// Records the list the signal has joined, so that a teardown from here on takes it off. The exchange is
        /// a full fence that pairs with the one in <see cref="OnCancelling"/>, so the teardown check that
        /// <see cref="SessionSignalRegistry.Register"/> makes next and the list lookup in <see cref="OnCancelling"/>
        /// cannot both miss the other side.
        /// </summary>
        /// <param name="waiters">The list the signal has joined.</param>
        internal void ListedOn(SessionSignalRegistry.Waiters waiters) => _ = Interlocked.Exchange(ref listedOn, waiters);

        /// <inheritdoc/>
        protected override void OnCancelling() => Interlocked.Exchange(ref listedOn, null)?.Remove(this);

        /// <summary>
        /// Arms the source so that a signal arriving from this point wakes the session. Called by
        /// <see cref="SessionSignalRegistry.Register"/> before the signal is published to signallers; a
        /// signal published before it is armed is lost and the session parks forever.
        /// </summary>
        internal new void Arm() => base.Arm();

        /// <summary>
        /// Waits for an armed signal. The returned task completes when <see cref="Signal"/> is called, and
        /// faults if the session is torn down first.
        /// </summary>
        /// <returns>A task representing the wait.</returns>
        internal ValueTask WaitAsync()
        {
            // Teardown may have run between arming and here, in which case nothing else will complete it.
            if (Cancelled)
                _ = Complete(canceled: true);

            return Pending;
        }

        /// <summary>
        /// Wakes a waiting session. Does nothing if it is not waiting.
        /// </summary>
        /// <returns>True if a waiter was woken.</returns>
        internal bool Signal() => Complete(canceled: false);
    }
}