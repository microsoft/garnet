// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace Garnet.server
{
    /// <summary>
    /// Await source for a session parked until the DiskANN index behind a Vector Set has been dropped, so that the command
    /// it was running can recreate it (see <see cref="RespServerSession.ParkUntilIndexDropped"/>), or for a Vector Set
    /// replication replay task that waits for it to apply an add (see <see cref="VectorManager.WaitForIndexDropAsync"/>).
    /// </summary>
    /// <remarks>
    /// Whoever takes the waiter off the signal's list completes the wait, so there is exactly one completion.
    /// </remarks>
    internal sealed class IndexDropWait : SessionWaitSource
    {
        /// <summary>
        /// Index of this waiter in the signal's list, or -1 when it is not listed. Guarded by the signal's lock.
        /// </summary>
        internal int listIndex = -1;

        /// <summary>
        /// Hash of the key whose index the wait is for. Not rewritten while the waiter is listed.
        /// </summary>
        internal long KeyHash { get; private set; }

        IndexDropSignal signal;

        /// <summary>
        /// Arms the source for one wait and lists the waiter with <paramref name="signal"/>. Called from the session's own
        /// thread, before it checks whether the drop is still pending.
        /// </summary>
        /// <param name="signal">The signal of the Vector Set manager whose drop task drops the index.</param>
        /// <param name="keyHash">Hash of the key whose index is waited on.</param>
        internal void Register(IndexDropSignal signal, long keyHash)
        {
            Arm();
            KeyHash = keyHash;
            Volatile.Write(ref this.signal, signal);
            signal.Add(this);
        }

        /// <summary>
        /// Takes back a registration whose drop was found done before the session parked, and completes the wait.
        /// </summary>
        internal void Withdraw()
        {
            _ = signal.TryRemove(this);
            _ = Complete(canceled: false);
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
                _ = signal.TryRemove(this);
                _ = Complete(canceled: true);
            }

            return Pending;
        }

        /// <summary>
        /// Completes the wait of a waiter the signal has just taken off its list.
        /// </summary>
        internal void Wake() => _ = Complete(canceled: false);

        /// <inheritdoc/>
        protected override void OnCancelling() => _ = Volatile.Read(ref signal)?.TryRemove(this);
    }
}