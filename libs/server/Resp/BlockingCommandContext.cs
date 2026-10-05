// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// A command that suspended its session rather than waiting on the thread it was dispatched on. The
    /// session hands one of these to <see cref="RespServerSession.TryParkSession"/>; the network handler
    /// unwinds the receive call stack, and once the operation signals completion the session is resumed on a
    /// pool thread and <see cref="WriteResponse"/> emits the reply the command still owes its client.
    /// </summary>
    /// <remarks>
    /// Implementations must capture everything they need into their own state before parking. Nothing owned
    /// by the session survives the park: the response buffer and its network-sender lock are released, the
    /// scratch buffers are reset, the cluster epoch is dropped, and the receive buffer may be moved or
    /// resized. In particular a parked command must not retain a <c>PinnedSpanByte</c> into the receive
    /// buffer or into either scratch buffer, and <see cref="WriteResponse"/> must not touch the store: it
    /// runs without the cluster epoch held.
    /// <para>
    /// Completion is signalled by calling <see cref="Complete"/> rather than by returning a task, so that
    /// parking allocates nothing in the session or network layers. Whatever an implementation needs to
    /// observe its own operation is its own cost.
    /// </para>
    /// <para>
    /// Exactly one of <see cref="OnStart"/>'s completion path and <see cref="OnAbort"/> wins, and only the
    /// winner may publish an outcome. <see cref="TryClaimOutcome"/> is how an implementation finds out
    /// whether it is the winner; a loser must leave the published outcome alone, because the winner's reply
    /// may already have been written.
    /// </para>
    /// </remarks>
    internal abstract class BlockingCommandContext
    {
        /// <summary>Operation is being started and has not yet been published as running.</summary>
        const int StateStarting = 0;

        /// <summary>Operation is running and may be completed or aborted.</summary>
        const int StateRunning = 1;

        /// <summary>An abort arrived while the operation was still starting, and must be applied once it is.</summary>
        const int StateAbortPending = 2;

        /// <summary>
        /// The outcome has been claimed and is being published. Transient, and the only state in which the
        /// context has an owner that is neither constructing it nor finished with it.
        /// </summary>
        const int StatePublishing = 3;

        /// <summary>Terminal. The outcome is published and the session has been, or is being, released.</summary>
        const int StateCompleted = 4;

        RespServerSession owner;

        /// <summary>
        /// Session this operation is parked on, for implementations that need its storage.
        /// </summary>
        /// <remarks>
        /// Valid from <see cref="Attach"/> until the session is released. While the session is parked it
        /// parses no further commands, so its <see cref="StorageSession"/> and the Tsavorite contexts
        /// reached through it have no other user and may be operated on directly from the thread the
        /// operation completes on -- inside <see cref="TryEnterStorageScope"/>, and subject to the rules in
        /// the remarks on <see cref="OnStart"/>.
        /// </remarks>
        protected RespServerSession Owner => owner;

        /// <summary>
        /// Takes the parked session's storage for this operation.
        /// </summary>
        /// <returns>
        /// True if the storage may be used, in which case the caller must call
        /// <see cref="ExitStorageScope"/> when it is done with it -- from a <c>finally</c>, because the
        /// session's storage is not released until it does. False if the session is being torn down, in
        /// which case the storage must not be touched and the operation has nothing to publish.
        /// </returns>
        /// <remarks>
        /// A parked session has no other user of its storage, but it can still be *disposed* while an
        /// operation is using it: connection teardown deliberately does not wait for an operation that has
        /// already claimed its outcome, because waiting would stall reclamation behind the very thing it is
        /// cancelling. The scope resolves that by moving the release rather than delaying it -- teardown
        /// hands the storage to an operation that is already inside, and refuses entry to any that is not.
        /// </remarks>
        protected bool TryEnterStorageScope()
        {
            AssertClaimedBeforeStorage();
            return owner.TryEnterParkedStorage();
        }

        /// <summary>
        /// Gives the parked session's storage back. Releases it if teardown arrived while it was in use.
        /// </summary>
        protected void ExitStorageScope() => owner.ExitParkedStorage();

        /// <summary>
        /// Trips if an operation reached for its session's storage without having claimed its outcome
        /// first.
        /// </summary>
        /// <remarks>
        /// The claim is what makes the storage the operation's to use: it is the point at which no other
        /// thread can still complete this command, so the session is not about to resume and start parsing
        /// again underneath it. Entering without one leaves two threads on a Tsavorite session that permits
        /// one, which corrupts quietly rather than failing. A second entry is refused by the lease; an exit
        /// with no entry is reported where it happens, since the lease can tell it apart from a real one but
        /// cannot make it harmless on its own.
        /// <para>
        /// A convention, not a cage: an operation that caches a context or an API alias obtained before it
        /// parked can still reach the storage without passing through here. See the authoring rules in
        /// <see cref="OnStart"/>.
        /// </para>
        /// </remarks>
        [Conditional("DEBUG")]
        void AssertClaimedBeforeStorage()
        {
            if (Volatile.Read(ref state) != StatePublishing)
                ReportContractViolation("A blocking operation used its session's storage without claiming its outcome.");
        }

        int state;
        int starting;
        int disposeRequested;
        int disposed;
        int disposeComplete;
        int pendingCallbacks;
        bool startFailed;

        /// <summary>
        /// Registers a callback that can still reach this context after the operation ends -- a queued work
        /// item, an armed timer. Must be paired with <see cref="CallbackCompleted"/> on every path.
        /// </summary>
        /// <remarks>
        /// Reuse is what makes this necessary. A context is recycled only once nothing can still call into
        /// it, because a stale callback from the previous park would otherwise win
        /// <see cref="TryClaimOutcome"/> against the next one and complete a command that has not finished.
        /// Counting registrations is what lets <see cref="TryBeginReuse"/> tell the two apart; a callback
        /// that is cancelled rather than run simply never decrements, and the context is not reused.
        /// </remarks>
        protected void AddCallback() => Interlocked.Increment(ref pendingCallbacks);

        /// <summary>
        /// Records that a callback registered by <see cref="AddCallback"/> can no longer reach this context.
        /// Call from a <c>finally</c>, after the last use of the context on that path.
        /// </summary>
        protected void CallbackCompleted() => Interlocked.Decrement(ref pendingCallbacks);

        /// <summary>
        /// Returns this context to its pristine state so the session can park on it again, or refuses if
        /// anything can still reach it.
        /// </summary>
        /// <returns>
        /// True if the context was reset and may be parked on again. False if it must not be reused, in
        /// which case the caller allocates a fresh one.
        /// </returns>
        /// <remarks>
        /// Refusing is always safe, which is what makes the conservative test the right one: a context is
        /// reused only once the previous operation's disposal has run to completion -- so it is fully
        /// published, released and torn down -- and no registered callback is outstanding. Anything else
        /// allocates, including the ordinary case of a timer cancelled before it fired.
        /// <para>
        /// The writes below are plain because every path to this context is published afterwards by a
        /// release barrier: <see cref="Attach"/> ends with a volatile write, and the session publishes the
        /// context itself with another. A thread that can see the context therefore sees this reset.
        /// </para>
        /// </remarks>
        internal bool TryBeginReuse()
        {
            if (Volatile.Read(ref disposeComplete) != 1 || Volatile.Read(ref pendingCallbacks) != 0)
                return false;

            owner = null;
            startFailed = false;
            state = StateStarting;
            starting = 0;
            disposeRequested = 0;
            disposed = 0;
            disposeComplete = 0;
#if DEBUG
            // Per-park, like everything else reset here: carrying it over would make the next park's single
            // release look like a second one.
            releaseCount = 0;
#endif

            OnReset();
            CountReused();
            return true;
        }

        /// <summary>
        /// Clears command-specific state so the context can be parked on again. Called only from
        /// <see cref="TryBeginReuse"/>, when nothing else can reach the context.
        /// </summary>
        protected virtual void OnReset() { }

#if DEBUG
        int releaseCount;
#endif

        static int contractViolations;

        /// <summary>
        /// Number of times a blocking command has been observed breaking one of this class's contracts.
        /// Always zero outside Debug builds, where the checks that feed it compile away.
        /// </summary>
        /// <remarks>
        /// A counter rather than a bare <see cref="Debug.Assert(bool)"/>, for two reasons. An assertion is
        /// not a reliable tripwire: whether it fails the process or merely logs depends on the trace
        /// listeners the host has installed, so a test could watch the contract break and still pass. And
        /// the violations reported here include ones raised from unwind paths, where throwing would turn an
        /// authoring mistake into a second exception on top of whatever is already in flight. The violations
        /// these guard against are all invisible from a client -- a leaked registration, a connection torn
        /// down either way -- so a test needs something to read.
        /// </remarks>
        internal static int ContractViolations => Volatile.Read(ref contractViolations);

        /// <summary>
        /// Clears <see cref="ContractViolations"/>, so a test can attribute what it counts to itself.
        /// </summary>
        internal static void ResetContractViolations() => Volatile.Write(ref contractViolations, 0);

        [Conditional("DEBUG")]
        internal static void ReportContractViolation(string message)
        {
            _ = Interlocked.Increment(ref contractViolations);
            Debug.WriteLine(message);
        }

        /// <summary>
        /// Whether the operation never got off the ground. The reply is then owned by the base class rather
        /// than by <see cref="WriteResponse"/>, which cannot be relied on to describe a failure it was never
        /// told about.
        /// </summary>
        internal bool StartFailed => startFailed;

        /// <summary>
        /// Counts the context against <see cref="AllocatedContexts"/>.
        /// </summary>
        protected BlockingCommandContext() => CountAllocated();

        /// <summary>
        /// Binds the context to its session. Called by <see cref="RespServerSession.TryParkSession"/>
        /// *before* the context is published, so that any thread which can reach the context through the
        /// session already sees a bound owner.
        /// </summary>
        /// <param name="session">Session being parked.</param>
        internal void Attach(RespServerSession session)
        {
            owner = session;
            CountLive(1);

            // Released before the caller publishes the context, so any thread that can reach it already
            // sees a start in progress and defers disposal to Start rather than racing OnStart.
            Volatile.Write(ref starting, 1);
        }

        /// <summary>
        /// Starts the operation. Called after the context has been published and the session has parked, so
        /// that an operation completing instantly -- or failing outright -- is handled by the park rendezvous
        /// rather than racing it.
        /// </summary>
        /// <remarks>
        /// Also called after the parse loop has finished with the session, which is a separate guarantee and
        /// the stronger one: the operation may run on another thread the instant it is started, and until
        /// that cleanup is done the session still has a response object checked out, holds the cluster
        /// epoch, and has scratch buffers it is about to reset.
        /// </remarks>
        internal void Start()
        {
            owner.AssertStartedAfterBatch();

            try
            {
                OnStart();
            }
            catch (Exception ex)
            {
                FailStart(ex);
                return;
            }

            // Publish as running. If an abort arrived while starting, it deferred to here rather than racing
            // a half-built operation, so apply it now. Claim the outcome first: the deferred abort has not
            // won anything yet, and leaving the state at AbortPending would let the operation's own
            // completion claim it afterwards and release the session a second time.
            if (Interlocked.CompareExchange(ref state, StateRunning, StateStarting) == StateAbortPending &&
                TryClaimOutcome())
            {
                ApplyAbort();
            }

            FinishStarting();
        }

        /// <summary>
        /// Trips if teardown released the operation's resources while a thread was still building them or
        /// still publishing an outcome into them. Debug-only and therefore free in release builds, but it
        /// covers every blocking command rather than just the one a test was written against -- which
        /// matters because the windows it guards are narrow enough that a command-specific test would have
        /// to work hard to hit them.
        /// </summary>
        /// <remarks>
        /// The start flag is read rather than the state because the two answer different questions. A
        /// racing completion can drive the state out of <c>Starting</c> while <see cref="OnStart"/> is
        /// still on the stack -- an operation that finishes the instant it is armed does exactly that -- so
        /// the state says nothing about whether construction has finished.
        /// </remarks>
        [Conditional("DEBUG")]
        void AssertNotInUse()
        {
            if (Volatile.Read(ref starting) != 0)
                ReportContractViolation("Blocking command was disposed while it was still starting");

            if (Volatile.Read(ref state) == StatePublishing)
                ReportContractViolation("Blocking command was disposed while it was publishing its outcome");
        }

        /// <summary>
        /// Trips if an operation released its session twice.
        /// </summary>
        [Conditional("DEBUG")]
        void AssertReleasedOnce()
        {
#if DEBUG
            if (Interlocked.Increment(ref releaseCount) != 1)
                ReportContractViolation("Blocking command released its session more than once");
#endif
        }

        /// <summary>
        /// Completes a context that was attached and published but whose operation will never be started,
        /// because the batch that parked it failed before it could hand the session back.
        /// </summary>
        /// <remarks>
        /// Indistinguishable to the command from a start that threw, and handled the same way: the session
        /// resumes and the command reports an error. The distinction matters only to the session, which
        /// must not start storage work against a session whose cleanup did not finish.
        /// </remarks>
        /// <param name="cause">Failure that kept the operation from being started.</param>
        internal void AbandonStart(Exception cause)
        {
            owner.Logger?.LogError(cause, "Blocking command was abandoned before it could be started");

            // Deliberately not FailStart, which releases the session. The batch that parked this context
            // threw while giving the session back, so its return value never reached the transport and the
            // bytes it consumed were never accounted for; a session resumed on that footing would re-parse
            // the batch and run every command ahead of the blocking one a second time. The connection is
            // going away regardless -- the throw is already unwinding towards the handler that tears it
            // down, and every path there disposes the receive args this park is holding -- so the only thing
            // owed here is to stop deferring disposal to a start that is not coming.
            startFailed = true;
            FinishStarting();
        }

        /// <summary>
        /// Turns a start that did not happen into a normal completion.
        /// </summary>
        /// <remarks>
        /// Startup is a cold path, but it is not guaranteed not to fail: registering with a broker can
        /// allocate, and a timer can fail to arm. Swallowing the failure would leave the session parked on
        /// an operation that will never complete, with no receive outstanding to ever notice -- the one
        /// failure this design cannot recover from.
        /// </remarks>
        /// <param name="ex">Failure to report.</param>
        void FailStart(Exception ex)
        {
            owner.Logger?.LogError(ex, "Blocking command failed to start");

            if (TryClaimOutcome())
            {
                // Recorded before the hook runs, so the reply is owed even if the hook itself fails.
                startFailed = true;

                try
                {
                    OnStartFailed(ex);
                }
                catch (Exception cleanupFailure)
                {
                    owner.Logger?.LogError(cleanupFailure, "Blocking command failed to clean up a failed start");
                }

                ReleaseSession();
            }

            FinishStarting();
        }

        /// <summary>
        /// Marks construction complete and runs a disposal that arrived while it was in progress.
        /// </summary>
        void FinishStarting()
        {
            _ = Interlocked.Exchange(ref starting, 0);
            RunDeferredDispose();
        }

        /// <summary>
        /// Gives up the claim won by <see cref="TryClaimOutcome"/>, and runs a disposal that arrived while
        /// it was held.
        /// </summary>
        void FinishPublishing()
        {
            _ = Interlocked.Exchange(ref state, StateCompleted);
            RunDeferredDispose();
        }

        /// <summary>
        /// Performs a disposal that was deferred while this thread held the context.
        /// </summary>
        /// <remarks>
        /// Both callers clear their claim on the context with an interlocked write before this reads
        /// <c>disposeRequested</c>, and <see cref="Dispose"/> publishes <c>disposeRequested</c> with an
        /// interlocked write before reading those claims. Two stores, each fenced and each followed by a
        /// read of the other, cannot both read stale: at least one thread sees the other, so the disposal
        /// never falls between them.
        /// </remarks>
        void RunDeferredDispose()
        {
            if (Volatile.Read(ref disposeRequested) != 0)
                DisposeOnce();
        }

        /// <summary>
        /// Claims the right to publish this operation's outcome. Returns true to exactly one caller, which
        /// may then write the result fields the reply will be built from. Every other caller must not touch
        /// them.
        /// </summary>
        /// <remarks>
        /// Claiming only decides the winner; it does not release the session. A winner must go on to call
        /// <see cref="ReleaseSession"/>, or the session stays parked on an operation that has already
        /// finished.
        /// <para>
        /// The claim also holds off disposal: between winning here and returning from
        /// <see cref="ReleaseSession"/> the context is terminal to every other claimant but still in use by
        /// its owner, so teardown defers rather than reclaiming a result that has not been handed back yet.
        /// A winner must therefore reach <see cref="ReleaseSession"/> on every path — wrap the work in
        /// between in a <c>try/finally</c>. A claim that is won and then abandoned leaves the context held
        /// forever, and disposal defers to a callback that never comes.
        /// </para>
        /// </remarks>
        protected bool TryClaimOutcome()
        {
            while (true)
            {
                var observed = Volatile.Read(ref state);

                // Publishing is as terminal as Completed to anyone asking for the claim: it is held by a
                // winner that has not finished with it yet.
                if (observed is StatePublishing or StateCompleted)
                    return false;

                // Winning moves the state to Publishing rather than straight to Completed, so that the
                // window between claiming the outcome and publishing it is visible to teardown. A disposal
                // landing there would otherwise reclaim a context whose result does not exist yet, and the
                // result that arrives a moment later would never be handed back.
                if (Interlocked.CompareExchange(ref state, StatePublishing, observed) == observed)
                    return true;
            }
        }

        /// <summary>
        /// Resumes the session. Must be called exactly once, by whichever caller won
        /// <see cref="TryClaimOutcome"/>.
        /// </summary>
        protected void ReleaseSession()
        {
            if (owner == null)
            {
                ReportContractViolation("Blocking command released a session it was never attached to");
                FinishPublishing();
                return;
            }

            // A second release double-counts the park rendezvous. That is masked while this park is live,
            // but if the session parks again first, the stale count resumes the next park early -- writing a
            // reply for an operation still in flight. Record it loudly here instead.
            AssertReleasedOnce();

            try
            {
                owner.UnparkSession();
            }
            finally
            {
                FinishPublishing();
            }
        }

        /// <summary>
        /// Claims the outcome and releases the session in one step, for an operation whose completion has no
        /// result to publish. Safe to call from any thread, and at most one call takes effect, so an
        /// operation finishing at the same moment as <see cref="Abort"/> resumes the session once.
        /// </summary>
        protected void Complete()
        {
            if (TryClaimOutcome())
                ReleaseSession();
        }

        /// <summary>
        /// Starts the blocking work. Must arrange for <see cref="Complete"/> to be called when it finishes,
        /// however it finishes -- success, failure, timeout or cancellation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Should not throw; a throw is handled as a failed start, reported through
        /// <see cref="OnStartFailed"/>, but an implementation that can fail predictably should record the
        /// failure and call <see cref="Complete"/> itself.
        /// </para>
        /// <para>
        /// The operation may use the parked session's storage. A parked session parses no further commands,
        /// so its <see cref="StorageSession"/>, the Tsavorite contexts reached through it, its transaction
        /// manager and its scratch buffers have no other user for as long as the park lasts, and the thread
        /// the operation completes on may drive them exactly as the inline session would. This is what makes
        /// a blocking command that has to *read* or *mutate* the store on completion expressible --
        /// <c>BLPOP</c> popping the element it waited for, say -- rather than only commands whose reply is
        /// known before they block.
        /// </para>
        /// <para>
        /// Three rules bound that. Storage work must happen inside <see cref="TryEnterStorageScope"/>, which
        /// is what keeps it from running against a session being torn down. It must be finished before
        /// <see cref="ReleaseSession"/> is called, because the resume that triggers hands the same storage
        /// back to the parse loop for whatever the client pipelined behind the blocking command. And it must
        /// happen here rather than in <see cref="WriteResponse"/>, which runs holding the session's sender
        /// lock, where a read that goes to disk would stall every other writer the session has. The natural
        /// shape is therefore to claim the outcome, enter the storage scope, do the work, publish its
        /// result, leave the scope, and only then release.
        /// </para>
        /// <para>
        /// The session's own storage API must be captured before the park, not reconstructed after it. The
        /// dispatched <c>TGarnetApi</c> is what selects between the basic, transactional and consistent-read
        /// contexts, and on a replica it is the consistent-read one; an operation that reaches past it to a
        /// context of its own choosing silently reads through the wrong one. It is captured by value because
        /// it arrives by <c>ref</c> and the park outlives the frame that holds it.
        /// </para>
        /// <para>
        /// The cluster epoch is *not* held. The parse loop releases it before this runs, so a cluster-aware
        /// operation must acquire its own; the Tsavorite epoch that <c>BasicContext</c> takes internally is
        /// a different epoch and is not a substitute.
        /// </para>
        /// <para>
        /// Exclusivity is a property the park establishes, not one the context can assume unconditionally:
        /// <see cref="RespServerSession.CanParkSession"/> refuses to park a session whose storage already
        /// has another driver, which is why a session with async <c>GET</c> processing in flight does not
        /// park, and why a command dispatched from a Lua script does not park either.
        /// </para>
        /// </remarks>
        protected abstract void OnStart();

        /// <summary>
        /// Records that the operation could not be started. The outcome has already been claimed, so an
        /// implementation can write its result fields directly. The default reports a generic error.
        /// </summary>
        /// <param name="exception">Failure that prevented the operation from starting.</param>
        protected virtual void OnStartFailed(Exception exception) { }

        /// <summary>
        /// Writes the command's RESP reply. Runs once, on the resuming thread, with the session's response
        /// buffer acquired, in the same position in the reply stream the command occupied in the request
        /// stream. Must serialize the already-published outcome rather than compute it: the session's sender
        /// lock is held here, so a store access that went to disk would stall pub/sub delivery and async
        /// <c>GET</c> replies behind it. Storage work belongs in the operation itself -- see
        /// <see cref="OnStart"/>.
        /// </summary>
        /// <param name="session">Session being resumed.</param>
        internal abstract void WriteResponse(RespServerSession session);

        /// <summary>
        /// Abandons the wait because the connection is being torn down. Callable from any thread and at any
        /// point in the operation's life, including before it has finished starting and after it has already
        /// completed; at most one abort is delivered, and never concurrently with a successful completion.
        /// </summary>
        internal void Abort()
        {
            while (true)
            {
                var observed = Volatile.Read(ref state);

                // Already finished, already being finished by a winner, or already carrying an abort
                // pending against the start. Publishing counts as finished here, and must: spinning for
                // the winner to leave it would make teardown wait on an operation it is trying to cancel.
                if (observed is StatePublishing or StateCompleted or StateAbortPending)
                    return;

                if (observed == StateStarting)
                {
                    // Defer: the operation is mid-construction and OnAbort would race a half-built state.
                    // Start applies it as soon as the operation is coherent.
                    if (Interlocked.CompareExchange(ref state, StateAbortPending, StateStarting) == StateStarting)
                        return;
                    continue;
                }

                if (Interlocked.CompareExchange(ref state, StatePublishing, StateRunning) == StateRunning)
                {
                    // Publishing, not Completed: an abort is an outcome like any other, and OnAbort runs
                    // with the same protection from disposal that a natural completion gets. Landing on
                    // Completed here would let teardown reclaim the context out from under OnAbort.
                    ApplyAbort();
                    return;
                }
            }
        }

        /// <summary>
        /// Runs the implementation's cancellation and releases the session. The caller has already won the
        /// outcome, so this cannot race a natural completion.
        /// </summary>
        void ApplyAbort()
        {
            try
            {
                OnAbort();
            }
            catch (Exception ex)
            {
                owner.Logger?.LogError(ex, "Blocking command failed to abort");
            }

            // Unconditional: the caller already claimed the outcome, so Complete would suppress this and
            // the session would stay parked forever on a connection that is already gone.
            ReleaseSession();
        }

        /// <summary>
        /// Cancels the operation in progress. Called at most once, never concurrently with a successful
        /// completion, and never before <see cref="OnStart"/> has returned. The reply is discarded rather
        /// than written, so this only needs to release whatever the operation registered.
        /// </summary>
        protected abstract void OnAbort();

        /// <summary>
        /// Releases operation state. Runs exactly once, and never while <see cref="OnStart"/> is still on
        /// the stack.
        /// </summary>
        /// <remarks>
        /// The deferral matters more than it looks. Teardown can reach a published context while its
        /// <see cref="OnStart"/> is still running, and disposing there would release resources the operation
        /// has not created yet -- a broker registration made after its only unregister would leak for the
        /// lifetime of the server, holding a reference to a session that is already gone. Deferring to
        /// <see cref="Start"/> makes disposal strictly follow construction.
        /// </remarks>
        internal void Dispose()
        {
            // Published before DisposeOnce reads either claim. Each claim is given up with an interlocked
            // write whose holder then re-runs DisposeOnce, so no pair of threads can both decide the other
            // will do the work.
            _ = Interlocked.Exchange(ref disposeRequested, 1);
            DisposeOnce();
        }

        /// <summary>
        /// Disposes the context if nothing is using it, and otherwise leaves the disposal to whichever
        /// thread is. Every path that gives up a claim calls this again, so the last one out runs it.
        /// </summary>
        void DisposeOnce()
        {
            // Construction in progress. Disposing here would release resources OnStart has not created
            // yet -- a broker registration made after its only unregister leaks for the lifetime of the
            // server, holding a reference to a session that is already gone.
            if (Volatile.Read(ref starting) != 0)
                return;

            // Consume the claim, so that a completion which has not yet reached TryClaimOutcome cannot win
            // one against a context that is being released: it finds the outcome already taken, publishes
            // nothing and releases nothing, which is correct because the connection this context belonged
            // to is already gone. A winner that got there first holds Publishing instead and owns the
            // disposal, because its result does not exist yet.
            while (true)
            {
                var observed = Volatile.Read(ref state);
                if (observed == StatePublishing)
                    return;

                if (observed == StateCompleted ||
                    Interlocked.CompareExchange(ref state, StateCompleted, observed) == observed)
                {
                    break;
                }
            }

            if (Interlocked.Exchange(ref disposed, 1) != 0)
                return;

            AssertNotInUse();
            CountAttachedAsDisposed();
            OnDispose();

            // After OnDispose, not before: disposed is claimed up front to make disposal one-shot, so it is
            // set while the command's own release is still running. Reuse keys off this instead, because
            // resetting the context's fields underneath OnDispose would strand whatever it was releasing.
            // Left clear if OnDispose throws, which only costs an allocation at the next park.
            Volatile.Write(ref disposeComplete, 1);
        }

        static int liveContexts;

        /// <summary>
        /// Contexts bound to a session and not yet disposed, across the process. Always zero in release
        /// builds, where the count is not maintained.
        /// </summary>
        /// <remarks>
        /// Disposal is owed from <see cref="Attach"/> onwards, and is deferred by every path that could be
        /// using the context, so a path that fails to re-run the deferred disposal strands it: the context
        /// and the session it points at are retained for the life of the process, with any registration
        /// <see cref="OnStart"/> made never undone. That is invisible from the wire -- the connection is
        /// gone either way -- and is not specific to any one command, so it is counted here rather than
        /// being left to each command to notice. Debug-only, and off the hot path regardless, since a park
        /// is already the expensive case.
        /// </remarks>
        internal static int LiveContexts => Volatile.Read(ref liveContexts);

        [Conditional("DEBUG")]
        static void CountLive(int delta) => Interlocked.Add(ref liveContexts, delta);

        static int reusedContexts;

        /// <summary>
        /// Number of times a context was parked on again instead of a fresh one being allocated. Always
        /// zero in release builds, where the count is not maintained.
        /// </summary>
        /// <remarks>
        /// A park is not required to allocate: a session parses no further commands while parked, so it has
        /// at most one operation outstanding and can hold a single context for the life of the connection.
        /// This counts the reuse actually achieved, so a test can assert that a burst of blocking commands
        /// allocates per connection rather than per call.
        /// </remarks>
        internal static int ReusedContexts => Volatile.Read(ref reusedContexts);

        /// <summary>
        /// Clears <see cref="ReusedContexts"/>, so a test can attribute what it counts to itself.
        /// </summary>
        internal static void ResetReusedContexts() => Volatile.Write(ref reusedContexts, 0);

        [Conditional("DEBUG")]
        static void CountReused() => Interlocked.Increment(ref reusedContexts);

        static int allocatedContexts;

        /// <summary>
        /// Contexts constructed across the process. Always zero in release builds, where the count is not
        /// maintained.
        /// </summary>
        /// <remarks>
        /// The two properties a test needs about context construction are both exact counts rather than
        /// magnitudes, so they are read from here instead of being inferred from allocation measurements: a
        /// command that cannot park must construct none at all, and a connection parking repeatedly must
        /// construct one rather than one per call. Measured bytes can establish neither, because reuse
        /// leaves both paths too close to zero to tell apart.
        /// </remarks>
        internal static int AllocatedContexts => Volatile.Read(ref allocatedContexts);

        /// <summary>
        /// Clears <see cref="AllocatedContexts"/>, so a test can attribute what it counts to itself.
        /// </summary>
        internal static void ResetAllocatedContexts() => Volatile.Write(ref allocatedContexts, 0);

        [Conditional("DEBUG")]
        static void CountAllocated() => Interlocked.Increment(ref allocatedContexts);

        /// <summary>
        /// Balances <see cref="CountLive"/> for a context that was attached. A park that is refused
        /// disposes a context that was never attached and so was never counted.
        /// </summary>
        [Conditional("DEBUG")]
        void CountAttachedAsDisposed()
        {
            if (owner != null)
                CountLive(-1);
        }

        /// <summary>
        /// Releases operation state. Called after <see cref="WriteResponse"/>, or when the session is
        /// disposed while still parked. Runs at most once.
        /// </summary>
        protected virtual void OnDispose() { }
    }
}