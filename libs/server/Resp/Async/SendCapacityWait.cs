// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using Garnet.networking;

namespace Garnet.server
{
    /// <summary>
    /// Await source for a session parked because the socket has not yet taken the replies the session already
    /// handed it, so producing more would only grow what a slow reader leaves queued in memory.
    /// </summary>
    /// <remarks>
    /// The sender calls <see cref="OnSendCapacity"/> from whichever thread completes the send that frees
    /// capacity, which can be a network IO thread. Completing the wait only queues the session's continuation to
    /// the thread pool, so that thread is not held up.
    /// </remarks>
    internal sealed class SendCapacityWait : SessionWaitSource, ISendCapacityWaiter
    {
        /// <summary>
        /// Arms the source for one wait. Called before the source is registered with the sender: a completion
        /// that arrives before it is armed is lost.
        /// </summary>
        internal new void Arm() => base.Arm();

        /// <summary>
        /// Waits for an armed source. The returned task completes when the sender signals capacity, and faults if
        /// the session is torn down first.
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
        public void OnSendCapacity() => _ = Complete(canceled: false);
    }
}
