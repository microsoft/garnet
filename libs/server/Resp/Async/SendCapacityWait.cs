// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;
using Garnet.networking;

namespace Garnet.server
{
    /// <summary>
    /// Await source for a session parked because the socket has not yet taken the replies the session already
    /// handed it, so producing more would only grow what a slow reader leaves queued in memory.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The sender calls <see cref="OnSendCapacity"/> from whichever thread completes the send that frees
    /// capacity, which can be a network IO thread. Completing the wait only queues the session's continuation to
    /// the thread pool, so that thread is not held up.
    /// </para>
    /// <para>
    /// The pub/sub broker's consumer waits on one too, for a subscriber's connection, and is its only waiter. Its
    /// "teardown" is the broker's disposal, which calls <see cref="SessionWaitSource.Cancel"/>.
    /// </para>
    /// </remarks>
    internal sealed class SendCapacityWait : SessionWaitSource, ISendCapacityWaiter
    {
        /// <summary>
        /// The sender of another session that this wait was last registered with. A registration with the
        /// session's own sender goes when the session does, but one with another session's sender stays in that
        /// sender's list for as long as its connection lives, so <see cref="OnCancelling"/> takes it back. Once a
        /// registration has been called or taken back, taking it back again does nothing.
        /// </summary>
        /// <remarks>
        /// A parked body is never resumed after the session is torn down, so what it registered cannot be
        /// taken back from the body itself: it has to be taken back from here.
        /// </remarks>
        INetworkSender peerSender;

        /// <summary>
        /// Arms the source for one wait. Called before the source is registered with the sender: a completion
        /// that arrives before it is armed is lost.
        /// </summary>
        internal new void Arm() => base.Arm();

        /// <summary>
        /// Arms the source for one wait on another session's sender and registers it there, as a publisher waits
        /// for the connection of a subscriber.
        /// </summary>
        /// <param name="sender">The other session's sender.</param>
        /// <returns>
        /// False if the sender has room now, in which case nothing waits. Otherwise the registration holds until
        /// the sender signals capacity or this session is torn down.
        /// </returns>
        internal bool TryRegisterWithPeer(INetworkSender sender)
        {
            Arm();

            // Recorded before the registration, so that a teardown that finds the registration finds the sender
            _ = Interlocked.Exchange(ref peerSender, sender);
            if (!sender.TryRegisterSendCapacityWaiter(this))
                return false;

            // The teardown sets its flag and then looks for the sender, and this registers and then looks for
            // the flag, so one of the two sees the other. A teardown that missed the registration is
            // found out here, and takes it back through the local sender, which the teardown may have cleared.
            Interlocked.MemoryBarrier();
            if (Cancelled)
                _ = sender.WithdrawSendCapacityWaiter(this);

            return true;
        }

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

        /// <inheritdoc/>
        protected override void OnCancelling()
            => _ = Interlocked.Exchange(ref peerSender, null)?.WithdrawSendCapacityWaiter(this);
    }
}