// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Session parking: how a RESP command waits on a server-side operation without holding the thread it was
    /// dispatched on.
    /// </summary>
    /// <remarks>
    /// Garnet processes commands synchronously on the IO completion thread, which is what keeps the common
    /// path free of async machinery. A command that waits in place therefore consumes a pool thread for the
    /// whole wait, and enough concurrently waiting connections exhaust the pool and stall every other
    /// connection in the server.
    /// <para>
    /// Instead, a blocking command captures its state into a <see cref="BlockingCommandContext"/>, parks, and
    /// returns without writing a reply. The parse loop stops, pending replies for commands earlier in the
    /// batch are flushed, the session releases everything it holds, and the receive callback returns. The
    /// connection then occupies no thread. When the operation completes the session is resumed on a pool
    /// thread: it writes the parked command's reply, consumes whatever the client had already pipelined
    /// behind it, and re-arms the socket receive.
    /// </para>
    /// <para>
    /// No receive is outstanding while parked, so bytes the client sends in the meantime stay in the kernel
    /// socket buffer. That is what makes the pattern safe: commands behind the blocking one are neither
    /// parsed nor executed until it completes, so ordering within a connection stays strictly FIFO and a
    /// client that pipelines past a blocking command gets TCP backpressure rather than reordering.
    /// </para>
    /// </remarks>
    internal sealed unsafe partial class RespServerSession : IParkableMessageConsumer
    {
        /// <summary>
        /// Handler owning this session's receive loop, or null for transports that do not park.
        /// </summary>
        ISessionParkHost parkHost;

        /// <summary>
        /// Command this session is parked on, and which owes the client a reply. Non-null from the moment the
        /// command parks until the reply is written on resume.
        /// </summary>
        BlockingCommandContext parkedCommand;

        /// <summary>
        /// Set when the session has been disposed, so a command parking concurrently can tell that its
        /// context will not be claimed by anyone else.
        /// </summary>
        int sessionDisposed;

        /// <summary>
        /// Set by <see cref="CancelInPlaceWaits"/> before it delivers its notifications, and read by
        /// <see cref="BlockingWaitInPlace{T}"/> after a wait has registered.
        /// </summary>
        /// <remarks>
        /// The two form a Dekker pair, and the pair is what makes the cancellation total rather than
        /// point-in-time. Teardown notifies once; a resume draining a pipeline can register a further wait
        /// after that notification has already been delivered, and nothing would ever end it. Publishing the
        /// latch first means any registration that teardown's sweep misses must have happened after the
        /// latch was written, so the waiter's own read sees it and re-delivers the cancellation to itself.
        /// <para>
        /// Both sides fence store-before-load explicitly. Release/acquire is not enough for a Dekker pair:
        /// it orders each thread's own accesses but permits the store and the following load to be seen out
        /// of order, which is the one reordering that loses here. The locks inside the registration
        /// dictionary happen to supply the barrier today, and relying on that would make correctness a
        /// property of a collection's internals. Neither side is hot -- teardown runs once per connection,
        /// and the waiter is about to block for as long as the client asked -- so the barrier is free.
        /// </para>
        /// </remarks>
        int inPlaceWaitsCancelled;

        /// <summary>
        /// Logger for the parked-command machinery, which runs outside the session's own call stack.
        /// </summary>
        internal ILogger Logger => logger;

        /// <inheritdoc />
        public void SetParkHost(ISessionParkHost parkHost) => this.parkHost = parkHost;

        /// <summary>
        /// Whether the command currently being dispatched may park.
        /// </summary>
        /// <remarks>
        /// Parking is only valid for a command the network layer dispatched directly, on a transport that can
        /// suspend its receive loop, outside any transaction, with no async <c>GET</c> processing in flight,
        /// and on a connection that is not carrying a replication stream. Blocking commands must fall back
        /// to non-blocking behavior when this is false -- they must not wait in place instead.
        /// <para>
        /// The async clause is what makes the park's exclusivity claim true. A parked session parses no
        /// further commands, so a blocking operation may drive its <see cref="StorageSession"/> directly
        /// from the thread it completes on, which is what lets a blocking command read or mutate the store
        /// on completion. The async <c>GET</c> processor is the one other driver of that same storage
        /// session: it drains pending reads on its own thread, on the contexts this session owns, at times
        /// unrelated to the parse loop. Parking alongside it would put two threads on a Tsavorite session
        /// that permits one. Refusing is cheap -- async <c>GET</c> is opt-in per session -- and it keeps the
        /// exclusivity rule stated in <see cref="BlockingCommandContext.OnStart"/> unconditional rather than
        /// qualified by a mode the operation cannot see.
        /// </para>
        /// <para>
        /// The replication clause closes a strand rather than expressing a preference. A session that has run
        /// <c>CLUSTER APPENDLOG</c> hands its raw <see cref="INetworkSender"/> to the replay driver, which
        /// disposes that alias directly when replication is reset. That closes the socket without going
        /// through handler disposal, so it never reaches the park abort and a parked session on such a
        /// connection is left parked on a socket that is already gone. Making driver teardown notify the park
        /// host would couple replication cleanup to session lifetime for a connection that has no business
        /// running a blocking command in the first place.
        /// </para>
        /// </remarks>
        internal bool CanParkSession
            => parkedCommand == null &&
               txnManager.state == TxnState.None &&
               asyncStarted == 0 &&
               clusterSession?.IsReplicating != true &&
               parkHost != null &&
               parkHost.CanParkSession;

        /// <summary>
        /// Starts <paramref name="command"/> and parks the session on it.
        /// </summary>
        /// <remarks>
        /// On success the caller must return to the parse loop immediately without writing a reply: the reply
        /// belongs to <see cref="BlockingCommandContext.WriteResponse"/> on the resume path. On failure the
        /// session cannot park and the caller must complete the command some other way.
        /// </remarks>
        /// <param name="command">Operation to wait on.</param>
        /// <returns>True if the session parked.</returns>
        internal bool TryParkSession(BlockingCommandContext command)
        {
            if (!CanParkSession)
                return false;

            // Ends the parse loop without costing it a check of its own: the loop runs while unparsed bytes
            // remain, and this leaves none. readHead has already been advanced past the blocking command, so
            // the bytes behind it stay in the receive buffer for the resume.
            bytesRead = 0;

            // Parked before the operation starts, so an operation that completes instantly rendezvous with
            // the park rather than racing it. This also resets the rendezvous, so it must precede anything
            // that could release the session.
            parkHost.ParkSession();

            // Bound before it is published, so every thread that can reach this context through the session
            // already sees an owner -- which is how the context tells "never parked" from "mid-start".
            command.Attach(this);

            // Published before the operation starts, so a teardown racing the park always finds a context to
            // abort. Reaching a context that has not finished starting is safe: both abort and dispose defer
            // until the operation is coherent rather than tearing down a half-built one.
            // Released, not plain: a thread that reaches this context through the field must also see the
            // start flag Attach raised, and a plain store carries no such guarantee on weakly ordered
            // hardware. Park paths only, so the command hot path is unaffected.
            Volatile.Write(ref parkedCommand, command);

            // Counted while the context is live, between the publish above and whichever of
            // CompleteParkedCommand or DisposeParkedCommand wins the exchange that clears it. Both are
            // interlocked, so exactly one decrement follows each increment.
            storeWrapper.IncrementParkedSessions();
#if DEBUG
            // Snapshot of the reply stream as the parked command left it, for the loop-tail contract check.
            parkResponseCursor = dcurr;
#endif
            command.Start();

            // A dispose that ran between the check above and the publish would have found nothing to claim,
            // leaving this context to leak. Re-check now that it is published; the handler makes the matching
            // check for connection-level teardown when it arms the park.
            if (Volatile.Read(ref sessionDisposed) != 0)
                AbortParkedOperation();

            return true;
        }

#if DEBUG
        byte* parkResponseCursor;
#endif

        /// <summary>
        /// Trips if a handler kept working after parking. <see cref="TryParkSession"/> has a calling
        /// convention -- it must be the last thing a handler does -- and breaking it corrupts the reply
        /// stream rather than failing outright, so it is worth a check that runs over every command.
        /// </summary>
        /// <remarks>
        /// Two distinct mistakes, because leaving no unparsed bytes only covers one of them. Continuing to
        /// *parse* would run commands the client pipelined behind the blocking one, out of order. Continuing
        /// to *write* is subtler and invisible to a byte count: the response cursor moves but
        /// <c>bytesRead</c> does not, and the stray reply is emitted ahead of the parked command's, which
        /// then arrives in the wrong position when the session resumes.
        /// </remarks>
        [Conditional("DEBUG")]
        void AssertParkContract()
        {
#if DEBUG
            if (parkedCommand == null)
                return;

            if (bytesRead != 0)
                BlockingCommandContext.ReportContractViolation(
                    "A parked session must leave no unparsed bytes; TryParkSession must be the last thing a handler does.");
            else if (dcurr != parkResponseCursor)
                BlockingCommandContext.ReportContractViolation(
                    "A parked session must not write to the reply stream; TryParkSession must be the last thing a handler does.");
#endif
        }

        /// <summary>
        /// Releases this session from the operation it parked on. Called by the parked command's context,
        /// from whatever thread completed the operation.
        /// </summary>
        internal void UnparkSession() => parkHost.UnparkSession();

        /// <inheritdoc />
        public int ResumeParkedMessages(byte* reqBuffer, int bytesAvailable)
        {
            // The reply is written and flushed on its own rather than folded into the batch below, so that
            // TryConsumeMessages -- which every command in the server goes through -- carries no check for a
            // parked reply. Anything pipelined behind the blocking command is rare, so this is usually the
            // same single send a non-blocking reply would have cost.
            CompleteParkedCommand();

            return bytesAvailable > 0 ? TryConsumeMessages(reqBuffer, bytesAvailable) : 0;
        }

        /// <inheritdoc />
        public void AbortParkedOperation()
        {
            // Runs on the thread tearing the connection down, not on the session's own thread. Only the
            // context is touched, and only to make its completion fire so the handler's resume path can run
            // and release the receive state it is holding.
            var command = Volatile.Read(ref parkedCommand);
            if (command == null)
                return;

            try
            {
                command.Abort();
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Error aborting parked command for session Id={id}", Id);
            }
        }

        /// <inheritdoc />
        public void DiscardParkedOperation() => DisposeParkedCommand();

        /// <inheritdoc />
        public void CancelInPlaceWaits()
        {
            // The latch goes up before the notification goes out. A resume draining a pipeline can register
            // a further wait at any point, including after the sweep below has run, so a sweep on its own
            // only cancels what happens to exist at this instant. BlockingWaitInPlace re-reads the latch
            // once its wait is registered, which leaves no window a registration can fall into.
            // Interlocked rather than Volatile: this store must not be reordered past the sweep's reads.
            Interlocked.Exchange(ref inPlaceWaitsCancelled, 1);

            // Only the collection broker families (BLPOP and friends) wait in place for something that
            // teardown itself is what ends. The other in-place waits on the command path -- AOF commit,
            // cluster PUBLISH, SAVE, ASYNC BARRIER -- complete on their own, so they delay reclamation
            // rather than deadlocking it, and cancelling them would change what a merely-closing
            // connection observes.
            //
            // This is exactly the notification ordinary disposal delivers, and it is idempotent, so
            // delivering it early only moves the wake-up ahead of the reclamation waiting on it.
            storeWrapper.itemBroker?.HandleSessionDisposed(this);
        }

        /// <summary>
        /// Waits in place for an already-registered blocking operation, reconciling against a cancellation
        /// that teardown may have delivered before this wait existed.
        /// </summary>
        /// <typeparam name="T">Result type of the operation.</typeparam>
        /// <param name="registered">
        /// Task for an operation that has already published itself to whatever teardown cancels. The broker
        /// families satisfy this: they add the observer synchronously, before the task they return can
        /// suspend.
        /// </param>
        /// <returns>The operation's result.</returns>
        /// <remarks>
        /// Waits that only teardown ends are a deadlock risk when reached from a resume, because the resume
        /// defers the very reclamation that would end them. <see cref="CancelInPlaceWaits"/> is how teardown
        /// breaks that cycle, but it fires once -- so this re-reads its latch after the operation has
        /// registered and re-delivers the cancellation if it arrived too early to see it.
        /// </remarks>
        internal T BlockingWaitInPlace<T>(Task<T> registered)
        {
            // Pairs with the store in CancelInPlaceWaits. The barrier is the half of the handshake that
            // keeps this read from being satisfied before the registration above it is visible to the sweep.
            Interlocked.MemoryBarrier();

            if (Volatile.Read(ref inPlaceWaitsCancelled) != 0)
                CancelInPlaceWaits();

            return AsyncUtils.BlockingWait(registered);
        }

        /// <summary>
        /// Reconciles a park that was established during a batch which then failed. Disposing the network
        /// sender closes the socket but leaves the park standing, and a parked connection has no outstanding
        /// receive to notice, so without this the session would stay parked -- holding its
        /// <see cref="System.Net.Sockets.SocketAsyncEventArgs"/> -- until the operation's own deadline, or
        /// forever if it has none.
        /// </summary>
        /// <remarks>
        /// Cold path only: called from the exception handlers that tear the session down, never from
        /// command processing.
        /// </remarks>
        void AbortParkOnSessionFailure()
        {
            if (Volatile.Read(ref parkedCommand) == null)
                return;

            AbortParkedOperation();
            parkHost?.AbortPark();
        }

        /// <summary>
        /// Writes and flushes the reply owed by the command this session parked on, ahead of anything the
        /// client pipelined behind it. Does nothing if a concurrent teardown already claimed the command, in
        /// which case the connection is gone and the reply is moot.
        /// </summary>
        void CompleteParkedCommand()
        {
            var command = Interlocked.Exchange(ref parkedCommand, null);
            if (command == null)
                return;

            storeWrapper.DecrementParkedSessions();

            // The claim above is exclusive, so from here on nobody else will release this context.
            try
            {
                try
                {
                    // Inside the finally's own try, because the acquisition takes the sender lock before it
                    // can fail -- the response-object stack throws when disposed, which §5.7's close makes
                    // routine, and allocating a replacement buffer can throw too. The lock is always held by
                    // the time either happens, so the unlock below is both reachable and correct; acquiring
                    // outside this try would leak it and wedge every other writer this session has, since
                    // the async GET processor and pub/sub delivery both take the same lock.
                    networkSender.EnterAndGetResponseObject(out dcurr, out dend);

                    // A start that never got off the ground owes a reply the command itself was never told
                    // about, so the base class owns it rather than trusting WriteResponse to describe a
                    // failure it cannot see.
                    if (command.StartFailed)
                        WriteError(CmdStrings.RESP_ERR_BLOCKING_FAILED);
                    else
                        command.WriteResponse(this);

                    if (dcurr > networkSender.GetResponseObjectHead())
                        Send(networkSender.GetResponseObjectHead());
                }
                finally
                {
                    // Cleared before the lock is released, not after. These fields belong to the session,
                    // not to this call: the next thread to take the lock sets them for itself, and a write
                    // landing after the release would null the pointers out from under it mid-reply.
                    dcurr = dend = null;
                    networkSender.ExitAndReturnResponseObject();
                    scratchBufferBuilder.Reset();
                    scratchBufferAllocator.Reset();
                }
            }
            finally
            {
                command.Dispose();
            }
        }

        /// <summary>
        /// Records that the session has been disposed, with a barrier, so that a command parking at the same
        /// moment observes it after publishing its own context.
        /// </summary>
        void MarkDisposedForParking() => Interlocked.Exchange(ref sessionDisposed, 1);

        /// <summary>
        /// Releases a command still parked when the session is torn down. Exactly one caller wins, so a
        /// teardown racing the park cannot dispose the context twice or leave it undisposed.
        /// </summary>
        void DisposeParkedCommand()
        {
            var command = Interlocked.Exchange(ref parkedCommand, null);
            if (command == null)
                return;

            storeWrapper.DecrementParkedSessions();
            command.Dispose();
        }
    }
}