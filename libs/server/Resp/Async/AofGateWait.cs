// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace Garnet.server
{
    /// <summary>
    /// Await source for a session parked because the AOF is further ahead of its replicas than the lag budget
    /// allows, so a command that would append to it has to wait for them to catch up.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The gate calls <see cref="OnGateOpen"/> from its poller thread. Completing the wait only queues the
    /// session's continuation to the thread pool, so the poller is not held up by the sessions it wakes.
    /// </para>
    /// <para>
    /// The gate stays closed for as long as a replica lags, and a session that is torn down while it waits is never
    /// resumed, so the wait takes itself off the gate when it is cancelled.
    /// </para>
    /// </remarks>
    internal sealed class AofGateWait : SessionWaitSource, IAofGateWaiter
    {
        AofBackpressure listedOn;

        /// <summary>
        /// Registers the wait with <paramref name="gate"/>, so that a teardown from here on takes it off.
        /// </summary>
        /// <param name="gate">The gate to wait on.</param>
        /// <returns>False if admission is open, or the gate has shut down, in which case nothing was registered.</returns>
        /// <remarks>
        /// The wait records the gate after joining it, then checks for a teardown. A teardown that ran earlier found
        /// no gate to take the wait off and is seen by the check; one that runs later finds the gate, and the wait is
        /// already on it. The exchange is a full fence that pairs with the one in <see cref="OnCancelling"/>, so the
        /// two cannot both miss the other side.
        /// </remarks>
        internal bool TryRegister(AofBackpressure gate)
        {
            if (!gate.TryRegisterWaiter(this))
                return false;

            _ = Interlocked.Exchange(ref listedOn, gate);
            if (Cancelled)
                gate.RemoveWaiter(this);
            return true;
        }

        /// <inheritdoc/>
        protected override void OnCancelling() => Interlocked.Exchange(ref listedOn, null)?.RemoveWaiter(this);
        /// <summary>
        /// Arms the source for one wait. Called before the source is registered with the gate: a notification that
        /// arrives before it is armed is lost.
        /// </summary>
        internal new void Arm() => base.Arm();

        /// <summary>
        /// Waits for an armed source. The returned task completes when the gate admits appends again, and faults
        /// if the session is torn down first.
        /// </summary>
        /// <returns>A task representing the wait.</returns>
        internal ValueTask WaitAsync()
        {
            // Teardown may have run between arming and here, in which case nothing else will complete it.
            if (Cancelled)
                _ = Complete(canceled: true);

            return Pending;
        }

        /// <inheritdoc/>
        public void OnGateOpen() => _ = Complete(canceled: false);
    }
}