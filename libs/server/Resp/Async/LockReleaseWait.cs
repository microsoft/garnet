// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Await source for a session parked until another session lets go of a key's latch that the command it was running
    /// found held (see <see cref="RespServerSession.OnLockContended"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// A transaction unlocking its keys wakes the wait through <see cref="LockReleaseSignal"/>. A latch held for a
    /// single operation is released without announcement, so the wait also probes the latch on a timer, backing off
    /// from <see cref="InitialPollMs"/> to <see cref="MaxPollMs"/>. A wake does not mean the latch is free: the command
    /// is run again and parks again if it meets the latch still held.
    /// </para>
    /// <para>
    /// Whoever takes the waiter off the signal's list completes the wait, so there is exactly one completion. The lock
    /// order is <see cref="gate"/>, then the signal's; the signal wakes the wait under its own and never takes this one.
    /// </para>
    /// </remarks>
    internal sealed class LockReleaseWait : SessionWaitSource
    {
        /// <summary>First interval at which the wait probes the latch.</summary>
        internal const int InitialPollMs = 1;

        /// <summary>Longest interval at which the wait probes the latch.</summary>
        internal const int MaxPollMs = 64;

        readonly object gate = new();

        /// <summary>
        /// Index of this waiter in the signal's list, or -1 when it is not listed. Guarded by the signal's lock.
        /// </summary>
        internal int listIndex = -1;

        // Guarded by the gate
        LockContention contention;
        LockReleaseSignal signal;
        Timer timer;
        bool polling;
        int pollMs;

        /// <summary>
        /// Records the latch the next wait is for. Called from the session's own thread, before the wait is armed.
        /// </summary>
        /// <param name="contention">The contended latch.</param>
        internal void Prepare(in LockContention contention)
        {
            lock (gate)
                this.contention = contention;
        }

        /// <summary>
        /// Arms the source for one wait. Called before the waiter is listed: a wake that arrives before it is armed is lost.
        /// </summary>
        internal new void Arm() => base.Arm();

        /// <summary>
        /// Whether releasing the latch of a key with hash <paramref name="keyHash"/> may end the wait. Called by the signal,
        /// under its lock, for a listed waiter, whose contention is not rewritten while it is listed.
        /// </summary>
        /// <param name="keyHash">Hash of a key just unlocked.</param>
        internal bool SharesBucketWith(long keyHash) => contention.SharesBucketWith(keyHash);

        /// <summary>
        /// Lists the waiter with <paramref name="signal"/> and starts polling the latch, then wakes the wait at once if
        /// the latch was released before the waiter was listed.
        /// </summary>
        /// <param name="signal">The signal of the store the latch is in.</param>
        internal void Register(LockReleaseSignal signal)
        {
            LockContention probe;
            lock (gate)
            {
                this.signal = signal;
                signal.Add(this);
                pollMs = InitialPollMs;
                polling = true;
                timer ??= new Timer(static self => ((LockReleaseWait)self).OnTimer(), this, Timeout.Infinite, Timeout.Infinite);
                _ = timer.Change(pollMs, Timeout.Infinite);
                probe = contention;
            }

            if (!probe.IsContended)
                TryWake();
        }

        /// <summary>
        /// Waits for a registered waiter. The returned task completes when the waiter has been woken, and faults if the
        /// session is torn down first.
        /// </summary>
        /// <returns>A task representing the wait.</returns>
        internal ValueTask WaitAsync()
        {
            // Teardown may have run between registering and here, in which case it found nothing listed and nothing else
            // will take the waiter off the list
            if (Cancelled)
            {
                _ = signal?.TryRemove(this);
                StopPolling();
                _ = Complete(canceled: true);
            }

            return Pending;
        }

        /// <summary>
        /// Ends a wait that has completed: stops polling the latch.
        /// </summary>
        internal void Finish() => StopPolling();

        /// <summary>
        /// Completes the wait of a waiter the signal has just taken off its list.
        /// </summary>
        internal void Wake() => _ = Complete(canceled: false);

        /// <inheritdoc/>
        protected override void OnCancelling()
        {
            _ = signal?.TryRemove(this);
            StopPolling();
        }

        void TryWake()
        {
            if (signal.TryRemove(this))
                _ = Complete(canceled: false);
        }

        void StopPolling()
        {
            lock (gate)
            {
                polling = false;
                _ = timer?.Change(Timeout.Infinite, Timeout.Infinite);
            }
        }

        /// <summary>
        /// Probes the latch, and wakes the wait if it has been released or polls again later if not. Run by the timer.
        /// </summary>
        void OnTimer()
        {
            LockContention probe;
            lock (gate)
            {
                // The timer of an earlier wait can fire after it has finished
                if (!polling)
                    return;
                probe = contention;
            }

            if (!probe.IsContended)
            {
                TryWake();
                return;
            }

            lock (gate)
            {
                if (!polling)
                    return;
                pollMs = Math.Min(pollMs * 2, MaxPollMs);
                _ = timer.Change(pollMs, Timeout.Infinite);
            }
        }
    }
}