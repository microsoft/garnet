// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;

namespace Garnet.server
{
    /// <summary>
    /// Await source for a session parked because the AOF is further ahead of its replicas than the lag budget
    /// allows, so a command that would append to it has to wait for them to catch up.
    /// </summary>
    /// <remarks>
    /// The gate calls <see cref="OnGateOpen"/> from its poller thread. Completing the wait only queues the
    /// session's continuation to the thread pool, so the poller is not held up by the sessions it wakes.
    /// </remarks>
    internal sealed class AofGateWait : SessionWaitSource, IAofGateWaiter
    {
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