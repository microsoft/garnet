// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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

#if DEBUG
        /// <summary>
        /// Set while this session is inside a batch, so that a parked operation starting from the batch's
        /// cleanup can be told from one started inside the batch.
        /// </summary>
        bool insideBatch;
#endif

        /// <summary>
        /// Command that parked during the batch currently unwinding, and whose operation has not been
        /// started yet. Written and read only by the session's own thread, between
        /// <see cref="TryParkSession"/> and <see cref="StartParkedCommand"/> in the same call stack.
        /// </summary>
        BlockingCommandContext pendingParkStart;

        /// <summary>A parked operation is driving this session's storage.</summary>
        const int StorageScopeHeld = 1;

        /// <summary>
        /// Teardown has handed the session's storage to whoever is holding it, and no further entry is
        /// permitted. Never cleared: once teardown has arrived the storage is going away.
        /// </summary>
        const int StorageTeardownDeferred = 2;

        /// <summary>
        /// Lease on the storage a parked operation drives -- the database sessions, the consistent-read
        /// state and the cluster session -- deciding which thread disposes them.
        /// </summary>
        /// <remarks>
        /// A context that has claimed its outcome is deliberately not waited for by teardown (see
        /// <see cref="BlockingCommandContext.Abort"/>), so a parked operation can still be reading through
        /// the database sessions when teardown reaches them. Teardown therefore does not free them itself
        /// if the operation is inside: it hands them over, and the operation frees them on its way out.
        /// <para>
        /// A handover rather than a wait, because waiting would make teardown depend on the liveness of
        /// arbitrary storage work. A read can block on a disk device, on a consistent-read catch-up whose
        /// configured timeout may legitimately be infinite, or on a lock held by a transaction on another
        /// connection that is itself waiting to be told it is going away. Each of those turns a
        /// disconnecting client into a teardown that never finishes, and burns the very thread this whole
        /// pattern exists to give back. Handing the storage over costs one interlocked operation and cannot
        /// deadlock, because no thread ever waits for another.
        /// </para>
        /// <para>
        /// Every access is an interlocked read-modify-write on this one word, so the two sides are ordered
        /// against each other without any further fencing, and exactly one of them observes itself as last.
        /// </para>
        /// </remarks>
        int parkStorageLease;

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
        /// <para>
        /// The park-host clause also covers scripts, which is why there is no separate nesting rule. Lua
        /// dispatches <c>redis.call</c> through a <see cref="RespServerSession"/> of its own, built by
        /// <see cref="SessionScriptCache"/> over a scratch-buffer sender; that session is never attached to
        /// a network handler, so it has no park host and cannot park. A blocking command reached from a
        /// script therefore falls back to non-blocking behavior, which is what Redis does for them anyway.
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
            // Published, but deliberately not started. Starting here would put the operation on a timer or
            // pool thread while this call stack still owns the session -- it has a response object checked
            // out, holds the cluster epoch, and has scratch buffers it is about to reset and may trim. The
            // parse loop's own cleanup is the handoff point, so the start waits for it.
            pendingParkStart = command;

            return true;
        }

        /// <summary>
        /// Returns everything a batch borrowed from the session. Runs at every batch boundary, so it is on
        /// the hot path for ordinary non-blocking traffic and is deliberately free of exception handling.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void EndBatch()
        {
            networkSender.ExitAndReturnResponseObject();
            clusterSession?.ReleaseCurrentEpoch();
            scratchBufferBuilder.Reset();
            scratchBufferAllocator.Reset();

            // Batch boundary: no argument pointers outlive it, so over-sized per-session buffers grown for
            // one unusually wide command can be released here. Counting down an integer keeps this off the
            // parse state itself, which measurably degrades code generation for the parse loop when read on
            // every batch.
            if (--sessionTrimCountdown <= 0)
                TrimSessionBuffers();

            ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Session_Fail_Batch_Cleanup);

#if DEBUG
            insideBatch = false;
#endif
        }

        /// <summary>
        /// Ends a batch during which a command parked, and then starts the operation it parked on.
        /// </summary>
        /// <remarks>
        /// The start is deliberately last, after every reset in <see cref="EndBatch"/>. A command that parked
        /// published its context where it parked but left the operation unstarted, because starting it there
        /// would let it run -- on a timer or pool thread, against this session's storage and scratch buffers
        /// -- while the parse loop still owed the session that cleanup.
        /// <para>
        /// The cleanup is wrapped here rather than in the batch's <c>finally</c> so that the common path
        /// carries no exception-handling region: a failed cleanup must still hand the context off, since one
        /// already marked as starting leaves the connection parked on an operation nothing can finish or
        /// reclaim, but that obligation exists only when something parked.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        void EndBatchHoldingAParkedCommand()
        {
            try
            {
                EndBatch();
            }
            catch (Exception cleanupFailure)
            {
#if DEBUG
                insideBatch = false;
#endif
                AbandonParkedCommandStart(cleanupFailure);
                throw;
            }

            StartParkedCommand();
        }

        /// <summary>
        /// Starts the operation a command parked on, once the parse loop has finished with the session.
        /// </summary>
        /// <remarks>
        /// Runs from the batch's <c>finally</c>, so it is reached however the batch ended -- including the
        /// failure paths, which abort the context before this runs and are applied by
        /// <see cref="BlockingCommandContext.Start"/> rather than racing it. The park host arms the resume
        /// only after the whole receive call stack has returned, which is strictly later than this, so an
        /// operation that completes the instant it is started still rendezvous with the park rather than
        /// resuming the session underneath the thread that parked it.
        /// </remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        void StartParkedCommand()
        {
            var command = pendingParkStart;
            pendingParkStart = null;
            command.Start();

            // A dispose that ran between the publish in TryParkSession and this point would have found a
            // context that was still starting and deferred to it; one that ran before the publish would
            // have found nothing to claim. Re-checking here, after the start has settled, covers both.
            if (Volatile.Read(ref sessionDisposed) != 0)
                AbortParkedOperation();
        }

        /// <summary>
        /// Takes this session's storage for a parked operation, unless teardown has already claimed it.
        /// </summary>
        /// <returns>
        /// True if the storage may be used, and the caller must call <see cref="ExitParkedStorage"/> when
        /// it is done with it. False if the session is going away and its storage must not be touched.
        /// </returns>
        internal bool TryEnterParkedStorage()
        {
            // Counted on the store before the session is claimed, never after. The session's own teardown
            // hands the storage over rather than waiting, so this count is the only thing standing between
            // a parked operation and a store that is being torn down -- and a claim taken first would leave
            // a window in which teardown has already handed the storage over while the store still sees no
            // user of it. Admission that is then refused gives the count straight back.
            storeWrapper.EnterParkedStorageScope();

            if (Interlocked.CompareExchange(ref parkStorageLease, StorageScopeHeld, 0) == 0)
                return true;

            storeWrapper.ExitParkedStorageScope();
            return false;
        }

        /// <summary>
        /// Gives this session's storage back, disposing it if teardown handed it over while it was in use.
        /// </summary>
        internal void ExitParkedStorage()
        {
            if (Interlocked.CompareExchange(ref parkStorageLease, 0, StorageScopeHeld) == StorageScopeHeld)
            {
                storeWrapper.ExitParkedStorageScope();
                return;
            }

            // Teardown arrived while this operation was inside, and left the storage to it. Drop only the
            // scope bit: the teardown bit stays set so nothing enters storage that is about to go.
            const int HeldAndDeferred = StorageScopeHeld | StorageTeardownDeferred;
            if (Interlocked.CompareExchange(ref parkStorageLease, StorageTeardownDeferred, HeldAndDeferred) != HeldAndDeferred)
            {
                // Neither CAS saw a scope, so this exit has no entry behind it. Releasing the storage or
                // the store's count here would free a session still in use and drive the store's drain
                // below zero, where it never terminates.
                BlockingCommandContext.ReportContractViolation(
                    "A blocking operation left a storage scope it never entered.");
                return;
            }

            try
            {
                DisposeSessionStorage();
                _ = Interlocked.Increment(ref parkStorageHandoversCompleted);
            }
            finally
            {
                // After the release above, so that the store's drain covers it.
                storeWrapper.ExitParkedStorageScope();
            }
        }

        /// <summary>
        /// Closes this session's storage to parked operations and disposes it, unless one is inside it --
        /// in which case that operation disposes it when it leaves.
        /// </summary>
        void ReleaseSessionStorage()
        {
            if ((Interlocked.Or(ref parkStorageLease, StorageTeardownDeferred) & StorageScopeHeld) == 0)
            {
                DisposeSessionStorage();
                return;
            }

            _ = Interlocked.Increment(ref parkStorageHandovers);
        }

        static int parkStorageHandovers;
        static int parkStorageHandoversCompleted;

        /// <summary>
        /// Number of times connection teardown found a parked operation inside its session's storage and
        /// left the release of that storage to it.
        /// </summary>
        internal static int ParkStorageHandovers => Volatile.Read(ref parkStorageHandovers);

        /// <summary>
        /// Number of handed-over storages a parked operation has gone on to release. Lags
        /// <see cref="ParkStorageHandovers"/> only while an operation is still inside one.
        /// </summary>
        internal static int ParkStorageHandoversCompleted => Volatile.Read(ref parkStorageHandoversCompleted);

        /// <summary>
        /// Releases the storage a parked operation is allowed to drive. Runs exactly once, on whichever of
        /// teardown or a parked operation is last to let go of it.
        /// </summary>
        void DisposeSessionStorage()
        {
            readSessionState?.Dispose();
            consistentReadDBSession?.Dispose();

            foreach (var dbSession in databaseSessions.Map)
                dbSession?.Dispose();

            clusterSession?.Dispose();
        }

        /// <summary>
        /// Ends every wait a parked operation could be inside, so that the storage it holds comes back
        /// without teardown having to wait for it.
        /// </summary>
        /// <remarks>
        /// Separated from <see cref="DisposeSessionStorage"/> because the object that carries a wait is
        /// usually the same object that carries its cancellation, and disposing it is what delivers the
        /// cancellation today. Handing the storage to a parked operation defers that disposal behind the
        /// very wait it would end, so the signal has to be delivered up front and the release left to
        /// whoever is last.
        /// </remarks>
        void CancelSessionStorageWaits() => readSessionState?.Cancel();

#if DEBUG
        byte* parkResponseCursor;
#endif

        /// <summary>
        /// Trips if a blocking operation was started while its session was still inside a batch.
        /// </summary>
        /// <remarks>
        /// Covers every blocking command rather than the one a test was written against, which matters
        /// because the damage is a race -- an operation started early may touch the session's storage,
        /// cluster epoch or scratch buffers while the parse loop is still giving them back -- and a race
        /// that a command-specific test has to win is a race it will usually lose.
        /// </remarks>
        [Conditional("DEBUG")]
        internal void AssertStartedAfterBatch()
        {
#if DEBUG
            if (insideBatch)
            {
                BlockingCommandContext.ReportContractViolation(
                    "A blocking operation must start from the parse loop's cleanup, not from inside the batch.");
            }
#endif
        }

        /// <summary>
        /// Hands a command that parked during this batch to a start that will never run, because the batch
        /// failed while giving the session back.
        /// </summary>
        /// <remarks>
        /// The handoff cannot simply be skipped. <see cref="BlockingCommandContext.Attach"/> has already
        /// marked the context as starting, so teardown defers its disposal to a start that is no longer
        /// coming, and the session stays parked on an operation nothing can finish -- with no receive
        /// outstanding to ever notice. Nor can the operation be started: the cleanup that failed is what
        /// gives back the response object, the cluster epoch and the scratch buffers it would run against.
        /// Reporting the failure to the command is the only remaining option, and it resumes the session.
        /// </remarks>
        /// <param name="cause">Failure that kept the batch from completing its cleanup.</param>
        [MethodImpl(MethodImplOptions.NoInlining)]
        void AbandonParkedCommandStart(Exception cause)
        {
            var command = pendingParkStart;
            if (command == null)
                return;

            pendingParkStart = null;
            command.AbandonStart(cause);

            // The operation was never started, so nothing will ever release this park -- an abort would only
            // be recorded against a context still logically starting, and no start is coming to apply it.
            // Stand in for the release, which latches the close first so the resume it may schedule discards
            // the session instead of re-parsing the batch whose cleanup just failed.
            parkHost?.ClosePark();

            if (Volatile.Read(ref sessionDisposed) != 0)
                AbortParkedOperation();
        }

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

            // Delivered here rather than only from Dispose, because a resume draining a pipelined read can
            // be inside a consistent-read catch-up while holding the resume lease -- and that lease is what
            // makes the caller defer session disposal. The cancellation would then sit behind the wait it
            // exists to end. Idempotent, so Dispose still delivers it on the paths that skip this hook.
            CancelSessionStorageWaits();

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
        /// Unwinds the work a failed batch left behind, and reports whether the session can carry on.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Two things are owed. A park established during the failed batch has to be abandoned: disposing
        /// the network sender closes the socket but leaves the park standing, and a parked connection has
        /// no outstanding receive to notice, so the session would stay parked -- holding its
        /// <see cref="System.Net.Sockets.SocketAsyncEventArgs"/> -- until the operation's own deadline, or
        /// forever if it has none. And a transaction that was running has to be unwound, because its locks
        /// outlive the session otherwise: they are held by the transactional contexts rather than by the
        /// connection, so every other session blocks on them indefinitely, including a parked operation
        /// whose storage the store's own teardown then waits for.
        /// </para>
        /// <para>
        /// The two are mutually exclusive, which is why their order here does not matter. A session cannot
        /// park inside a transaction -- <see cref="CanParkSession"/> requires
        /// <see cref="TxnState.None"/> -- and a parked session parses no further commands, so it cannot
        /// reach <c>MULTI</c> while parked. Whichever of the two did happen, the other is a no-op.
        /// </para>
        /// <para>
        /// A transaction still <see cref="TxnState.Running"/> when the batch failed died inside <c>EXEC</c>'s
        /// replay pass. <see cref="TransactionManager.Abandon"/> is what ends it, releasing the locks,
        /// terminating the AOF group, and deregistering the transaction from the checkpoint state machine.
        /// It deliberately leaves the watch container alone: every path that reaches here destroys the
        /// connection, and watches are session-local state that dies with it.
        /// </para>
        /// <para>
        /// That session cannot carry on, which is what the return value says. <c>EXEC</c> has already written
        /// the array header announcing one reply per queued command and the replay stopped partway through
        /// it, so the client is owed elements that will never be written and would read the next command's
        /// reply as one of them. The parse cursor is equally unrecoverable: it is somewhere inside the region
        /// <c>EXEC</c> rewound it to, so the next bytes to arrive resume parsing in the middle of the queued
        /// commands and run them a second time. Neither can be repaired from here, so the connection closes.
        /// </para>
        /// <para>
        /// Nothing adjusts the cursor on the way out. Outside a transaction the parser has already consumed
        /// the command that threw before dispatching it, so the cursor is at a command boundary and the
        /// pipeline behind it is intact; forcing it to the end of the receive instead would discard whatever
        /// else arrived in the same read, and a receive boundary is not a command boundary -- a bulk payload
        /// split across two reads would leave the second read's payload bytes to be parsed as commands.
        /// </para>
        /// <para>
        /// Cold path only: called from the exception handlers that tear the session down, never from
        /// command processing.
        /// </para>
        /// </remarks>
        /// <returns>True if the session's protocol state is unrecoverable and the connection must close.</returns>
        bool AbandonSessionWorkOnFailure()
        {
            var diedInsideTransaction = txnManager is { state: TxnState.Running };

            // No-op unless a transaction was actually running, and self-clearing, so the three failure
            // paths that reach this can overlap without unlocking twice.
            try
            {
                txnManager?.Abandon();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Failed to unwind a transaction left running by a failed batch");
            }

            if (Volatile.Read(ref parkedCommand) == null)
                return diedInsideTransaction;

            AbortParkedOperation();
            parkHost?.AbortPark();

            return diedInsideTransaction;
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