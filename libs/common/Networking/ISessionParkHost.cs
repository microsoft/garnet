// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.networking
{
    /// <summary>
    /// Network-side half of the session parking contract. A message consumer that must wait on a server-side
    /// operation parks itself instead of blocking the thread it is running on: the network handler stops
    /// reading from the socket, the receive call stack unwinds, and the thread goes back to serving other
    /// connections. When the operation completes the handler resumes the same session on a pool thread,
    /// lets it emit the reply for the command that blocked, and re-arms the socket receive.
    /// </summary>
    /// <remarks>
    /// Parking is the only safe way to wait inside command processing. Garnet runs command processing
    /// synchronously on the IO completion thread, so waiting in place consumes a pool thread for the whole
    /// wait; past some number of concurrently waiting connections the pool is exhausted and the server stops
    /// making progress on unrelated connections. Parking bounds the thread cost of a waiting connection to
    /// zero.
    /// <para>
    /// Because no receive is outstanding while parked, bytes the client sends in the meantime stay in the
    /// kernel socket buffer. That is deliberate: it preserves strict FIFO ordering within the connection and
    /// applies TCP backpressure to a client that pipelines past a blocking command.
    /// </para>
    /// <para>
    /// The contract is callback-driven rather than task-driven so that parking costs no allocation in the
    /// network layer: there is no task to await, no state machine to box, and no continuation delegate.
    /// </para>
    /// </remarks>
    public interface ISessionParkHost
    {
        /// <summary>
        /// Whether this handler can park its session. Callers must have a fallback for the transports that
        /// cannot.
        /// </summary>
        bool CanParkSession { get; }

        /// <summary>
        /// Parks the session. Called by the message consumer from inside
        /// <see cref="IMessageConsumer.TryConsumeMessages"/>; the consumer must then stop parsing and return
        /// normally so the receive stack can unwind.
        /// </summary>
        /// <remarks>
        /// Call this before starting the operation. The handler will not resume before the receive stack has
        /// unwound and handed over its receive state, so an operation that completes instantly is handled
        /// correctly rather than racing the park.
        /// </remarks>
        void ParkSession();

        /// <summary>
        /// Releases a parked session. Callable from any thread, exactly once per
        /// <see cref="ParkSession"/>, as soon as the blocking operation has finished.
        /// </summary>
        /// <remarks>
        /// The resume always runs on a thread pool thread, never inline on the caller: the caller is
        /// typically an unrelated session's thread, a timer thread, or the thread tearing the connection
        /// down, and none of them may be taken over by this connection's work.
        /// </remarks>
        void UnparkSession();

        /// <summary>
        /// Latches a terminal close for this connection and abandons any parked operation. Callable from any
        /// thread and idempotent.
        /// </summary>
        /// <remarks>
        /// Closing the socket is not by itself enough to release a parked session. A close driven from
        /// outside the receive path -- <c>CLIENT KILL</c>, or a send that failed while flushing replies --
        /// disposes the network sender without entering the handler's own teardown, so a session that is
        /// parked, or is in the middle of parking, would otherwise keep waiting with no receive outstanding
        /// to notice. For an operation with no deadline that means forever. Latching the request separately
        /// from handler teardown lets the park path reconcile it after it publishes its state, closing the
        /// window where a kill lands while the park is still being established.
        /// </remarks>
        void AbortPark();

        /// <summary>
        /// Latches a terminal close for this connection and contributes the release the abandoned operation
        /// will never make. Callable from any thread and idempotent.
        /// </summary>
        /// <remarks>
        /// <see cref="AbortPark"/> alone is not enough when the park never starts. An operation abandoned
        /// before it starts is consumed without ever releasing its session, so the handoff between the
        /// session parking and the receive state arriving at the handler has one contributor that will
        /// never arrive, and the receive state -- the accept socket and the pinned receive buffer -- is
        /// held until the handler is collected rather than disposed when the connection closes. Standing
        /// in for the missing release is what lets the handler reclaim it deterministically.
        /// <para>
        /// On TLS this is the only thing that reclaims it. The reader loop catches its own exceptions and
        /// disposes the handler without the receive state in hand, so the receive path's own failure
        /// handling -- which would otherwise dispose exactly these args -- is never reached.
        /// </para>
        /// </remarks>
        void ClosePark();
    }
}