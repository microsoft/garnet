// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.networking
{
    /// <summary>
    /// Session-side half of the session parking contract described on <see cref="ISessionParkHost"/>.
    /// Implemented by message consumers that can suspend themselves on a blocking command instead of
    /// occupying the receive thread for the duration of the wait.
    /// </summary>
    public interface IParkableMessageConsumer : IMessageConsumer
    {
        /// <summary>
        /// Supplies the handler the consumer parks against. Called once, when the handler binds the consumer.
        /// </summary>
        /// <param name="parkHost">Handler owning this consumer's receive loop.</param>
        void SetParkHost(ISessionParkHost parkHost);

        /// <summary>
        /// Resumes a parked consumer. Emits the reply for the command that parked, then consumes whatever
        /// pipelined bytes were already buffered when it parked. Returns the number of bytes consumed from
        /// <paramref name="reqBuffer"/>, with the same meaning as
        /// <see cref="IMessageConsumer.TryConsumeMessages"/>.
        /// </summary>
        /// <remarks>
        /// Unlike <see cref="IMessageConsumer.TryConsumeMessages"/> this must be called even when
        /// <paramref name="bytesAvailable"/> is zero, which is the common case: the parked command still owes
        /// the client a reply.
        /// </remarks>
        /// <param name="reqBuffer">Buffer holding bytes not yet consumed when the consumer parked.</param>
        /// <param name="bytesAvailable">Number of such bytes.</param>
        unsafe int ResumeParkedMessages(byte* reqBuffer, int bytesAvailable);

        /// <summary>
        /// Abandons the parked operation because the connection is being torn down. Should result in a call
        /// to <see cref="ISessionParkHost.UnparkSession"/> so the handler's resume path can run and release
        /// its receive state; the resume path then observes the teardown and discards the reply.
        /// </summary>
        /// <remarks>
        /// Deliberately returns nothing. There is no answer this call could give that a caller could rely on:
        /// the operation may have been claimed by a completion that has not released yet, or consumed by a
        /// disposal that will never release at all, and neither is distinguishable from the outside at the
        /// moment of asking. Callers that need the park woken must stand in for the release themselves.
        /// </remarks>
        void AbortParkedOperation();

        /// <summary>
        /// Wakes any wait the consumer is performing *in place* on a thread, rather than by parking, so that
        /// connection teardown is not left waiting behind one.
        /// </summary>
        /// <remarks>
        /// Reclamation of connection-owned state is deferred while a resume is in flight, and a resume that
        /// drains a pipelined command which still waits in place is in flight for as long as that wait runs.
        /// Those waits normally end when the session is disposed -- which is part of the very reclamation
        /// being deferred -- so without this the two wait on each other. Delivering the cancellation up front
        /// breaks that cycle without handing the session's resources to a thread still using them. Must be
        /// idempotent: the ordinary disposal path delivers the same cancellations again.
        /// <para>
        /// Only waits that teardown is what ends belong here. A wait that completes on its own -- one driven
        /// by a commit, a checkpoint or a peer -- also holds the resume in flight and so delays reclamation,
        /// but it cannot deadlock against it, and cancelling it here would change observable behaviour on a
        /// connection that is merely closing.
        /// </para>
        /// </remarks>
        void CancelInPlaceWaits();

        /// <summary>
        /// Releases the parked operation's state once the resume observes that the connection is gone and
        /// the reply is moot. Must be safe to call when the consumer has already released it, which is the
        /// usual case: the consumer is normally disposed before the resume runs.
        /// </summary>
        void DiscardParkedOperation();
    }
}