// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Suspension and resumption of a RESP command that cannot complete on the network thread.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A command body suspends inside <see cref="TryConsumeMessagesAsync"/>. Because the stack has not
    /// unwound past <see cref="TryConsumeMessagesAsync"/>, the network handler has not shifted or resized the
    /// receive buffer and has not issued the next receive. The suspended command therefore keeps its
    /// arguments valid, and commands pipelined behind it stay unparsed until it finishes -- the session is
    /// parked, exactly as a synchronous blocking command would leave it, but without owning a thread.
    /// </para>
    /// <para>
    /// What happens to the session's per-batch scope -- the response object, the scratch buffers, and the
    /// cluster epoch -- is the suspending command's choice, made through
    /// <see cref="BeginAsyncCommand(bool)"/>. A command waiting on an operation already in flight keeps the
    /// scope, so it resumes on the buffers and at the epoch it suspended on and differs from a synchronous
    /// completion only by the thread switch. A command waiting while idle, on an event with no bound on when
    /// it arrives, releases the scope and retakes it on resume, and so may carry no pointer into it across
    /// the wait.
    /// </para>
    /// <para>
    /// Resumption runs through <see cref="ResumeAsyncCommand"/>, which re-enters the resource scope -- or
    /// finds it still held -- before driving the body's state machine, then drains the rest of the batch.
    /// </para>
    /// <para>
    /// What no suspension releases is the transaction. Key locks taken by an enclosing <c>MULTI</c>/
    /// <c>EXEC</c> are held for as long as the body waits, so a command that parks inside a transaction
    /// stalls every other session contending for those keys. That is why the per-batch scope can be released
    /// by an idle suspension but the transaction cannot: the scope lasts one batch, whereas the locks are what
    /// makes the transaction atomic and cannot be dropped mid-transaction. A command whose wait has no bound
    /// therefore declines to wait inside a transaction: see <see cref="DenyBlocking"/>.
    /// </para>
    /// <para>
    /// That reasoning is about a wait with no bound on it, and it does not extend to a pending device read.
    /// Such a read is bounded, and its locks are held for the device latency whether the session parks or
    /// the thread blocks, so parking neither lengthens nor shortens the hold -- it only returns the thread.
    /// Nothing thread-affine spans a transaction: <c>AcquireTransactional</c> records session state, the
    /// locks live in the store's lock table, and each operation takes and releases its own epoch. Nor is a
    /// transaction replayed on a stack of its own that a suspension would have to unwind:
    /// <see cref="NetworkEXEC"/> rewinds the read head and returns, and the queued commands are re-parsed by
    /// the ordinary parse loop, which stops at a suspension and picks up from the same read head on resume.
    /// A pending read inside <c>MULTI</c>/<c>EXEC</c> therefore parks exactly as it does outside one.
    /// </para>
    /// <para>
    /// What does decide between parking and waiting is the transport: see <see cref="CanSuspend"/>.
    /// </para>
    /// </remarks>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Session whose command dispatch is on this thread, read by <see cref="RespAsyncMethodBuilder"/>.
        /// Published only around an async command body, so synchronous dispatch never touches it.
        /// </summary>
        [ThreadStatic]
        internal static RespServerSession CurrentAsyncCommandScope;

        /// <summary>Decides which thread drives a resume; see <see cref="SuspensionHandoff"/>.</summary>
        SuspensionHandoff handoff;

        /// <summary>
        /// Set while a command body is suspended, read by the parse loop to stop consuming the batch.
        /// Written and read only on the thread that holds the resource scope.
        /// </summary>
        bool asyncSuspended;

        /// <summary>
        /// Depth of consume passes with no receive loop to unwind to: those of the embedded and in-process
        /// transports, and the nested pass a script runs its own commands through.
        /// </summary>
        int unparkableConsumeDepth;

        /// <summary>
        /// Whether a command running now may park the session rather than wait on this thread.
        /// </summary>
        /// <remarks>
        /// Parking is a property of the transport driving the current consume, not of what the command is
        /// doing or of what else the session holds. It needs a receive loop to unwind to that will leave the
        /// receive buffer alone and issue nothing further until the resume, and only
        /// <see cref="TryConsumeMessagesAsync"/> has one. Where there is none, a command whose wait is
        /// bounded -- a pending device read -- completes it on this thread instead, spending the device
        /// latency to keep the reply on the stack that asked for it.
        /// </remarks>
        internal bool CanSuspend => unparkableConsumeDepth == 0;

        /// <summary>
        /// Whether a blocking command must answer at once rather than wait: inside <c>MULTI</c>/<c>EXEC</c>
        /// and inside a script. There, as in Redis, it behaves as though its timeout had already expired.
        /// </summary>
        /// <remarks>
        /// Unlike <see cref="CanSuspend"/>, this is about what the session holds rather than the transport. A
        /// transaction keeps its key locks until it ends, and those include the keys the command would wait
        /// on, so the client that could satisfy the wait is held off by the waiter. A script runs to completion
        /// on the thread that called it, with no receive loop to unwind to. The script session is the one a
        /// <see cref="LuaRunner"/> was created over, which is what sets <see cref="noScriptBitmap"/>.
        /// </remarks>
        internal bool DenyBlocking => txnManager.state == TxnState.Running || noScriptBitmap != null;

        /// <summary>
        /// Set while the suspended command keeps the per-batch scope -- response object, scratch buffers,
        /// and the cluster epoch -- across its wait. See the remarks on <see cref="BeginAsyncCommand(bool)"/>.
        /// </summary>
        /// <remarks>
        /// Read by <see cref="ConsumeCore"/> on both edges of a suspension: it skips the release on the way
        /// out and the matching acquire on resume, so the body re-enters on the buffers and at the epoch it
        /// suspended on.
        /// </remarks>
        bool asyncCommandRetainsBatchScope;

        /// <summary>Box of the currently suspended body.</summary>
        RespAsyncBox resumeBox;

        /// <summary>Single-box cache. A session suspends at most one command at a time.</summary>
        RespAsyncBox cachedAsyncBox;

        /// <summary>The suspended body's task, observed once the body completes so faults reach the client.</summary>
        ValueTask pendingAsyncBody;

        byte* suspendedRecvBufferPtr;
        int suspendedBytesRead;

        RespConsumeCompletionSource consumeSource;

        /// <summary>
        /// Reusable timed wait for suspending command bodies, released when the session is torn down so a
        /// parked command stops waiting instead of holding its timer until it expires.
        /// </summary>
        /// <remarks>
        /// Created on first use, so a session that never parks does not allocate one. Deliberately not
        /// disposed here: a suspended body may still be completing against it after <see cref="Dispose"/> has
        /// run, and <see cref="SessionWaitSource.Cancel"/> has already stopped its timer.
        /// </remarks>
        internal SessionDelay SessionDelay
        {
            get
            {
                var delay = sessionDelay;
                if (delay is null)
                {
                    delay = new SessionDelay();
                    var existing = Interlocked.CompareExchange(ref sessionDelay, delay, null);
                    if (existing is not null)
                        delay = existing;
                    else if (sessionTornDown)
                        delay.Cancel();
                }
                return delay;
            }
        }

        SessionDelay sessionDelay;

        /// <summary>
        /// Reusable wait for a command body parked until another session releases it, released when this
        /// session is torn down.
        /// </summary>
        /// <remarks>
        /// Created on first use, and deliberately not disposed, for the same reasons as
        /// <see cref="SessionDelay"/>.
        /// </remarks>
        internal SessionSignal SessionSignal
        {
            get
            {
                var signal = sessionSignal;
                if (signal is null)
                {
                    signal = new SessionSignal();
                    var existing = Interlocked.CompareExchange(ref sessionSignal, signal, null);
                    if (existing is not null)
                        signal = existing;
                    else if (sessionTornDown)
                        signal.Cancel();
                }
                return signal;
            }
        }

        SessionSignal sessionSignal;

        volatile bool sessionTornDown;

        /// <summary>
        /// Set from <see cref="EnterBatchScope"/> to <see cref="ExitBatchScope"/>, including across a
        /// suspension that keeps the scope. Teardown reads it to decide whether the storage sessions are left
        /// to the batch to release; see <see cref="LeaveStorageToRunningBatch"/>.
        /// </summary>
        /// <remarks>
        /// Ordered against <see cref="sessionTornDown"/> by the response object's lock: it is written before
        /// the lock is taken and cleared before it is released, and both are full fences.
        /// </remarks>
        bool batchInScope;

        /// <summary>
        /// One while teardown holds a storage hold on behalf of a batch that may still be in scope. Taken
        /// back to zero by whichever of teardown and the batch's scope exit releases that hold.
        /// </summary>
        int batchStorageHoldPending;

        /// <summary>
        /// Publishes this session for the duration of an async command body's synchronous start, so the
        /// body's builder can find it. Restores the previous value, which makes nesting safe.
        /// </summary>
        /// <param name="retainBatchScope">
        /// Whether a suspension of this body keeps the per-batch scope. Pass <c>true</c> for a body that
        /// suspends with an operation in flight, and <c>false</c> for one that suspends while idle.
        /// </param>
        /// <remarks>
        /// <para>
        /// A batch runs inside a scope that <see cref="ConsumeCore"/> takes on entry and drops on exit: the
        /// response object the replies are written into, the two scratch buffers command arguments and
        /// outputs are laid out in, and, in cluster mode, the cluster epoch. The epoch is what makes a
        /// session's "this slot is mine" decision and its action on that decision atomic with respect to
        /// configuration changes: a migration flips the slot state and then waits for every session to
        /// leave the older epoch before it moves any data, so a session holding the epoch cannot land a
        /// write on a key that has already been copied away.
        /// </para>
        /// <para>
        /// A suspension therefore has to choose. A body that suspends with an operation in flight -- a
        /// storage read that went to disk, say -- must pass <c>true</c>, and the scope is held across the
        /// wait exactly as it is when the same operation completes synchronously: the key it is operating
        /// on stays put, the output it has already written stays addressable, and the pointers it parked
        /// on stay valid. Such a suspension differs from a synchronous completion only by the thread switch
        /// on resume. The wait must be bounded by the operation, because nothing else bounds how long the
        /// scope is held.
        /// </para>
        /// <para>
        /// A body that suspends while idle, waiting on a client-visible event with no bound on when it
        /// arrives, must pass <c>false</c>. The scope is dropped on the way out and retaken on resume, so
        /// the body may carry no pointer into it and no configuration-dependent decision across the wait:
        /// it re-enters at the current epoch, on fresh buffers, and has to re-verify anything it concluded
        /// before suspending.
        /// </para>
        /// <para>
        /// Keeping the response object necessarily keeps its lock -- <c>EnterAndGetResponseObject</c>
        /// asserts the session is not already holding a buffer, so the two cannot be separated. A push to
        /// this session from another thread cannot be written until the suspension ends, and does not wait
        /// for it on a thread: the pusher parks, or leaves the message for the session to write out when it
        /// lets go of the response object (see <see cref="heldPushes"/>).
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal AsyncCommandScope BeginAsyncCommand(bool retainBatchScope = false)
        {
            asyncCommandRetainsBatchScope = retainBatchScope;
            return new(this);
        }

        /// <summary>
        /// True while a suspended command is holding the per-batch scope across its wait, which is when the
        /// batch's resources must not be released and its replies must not be flushed.
        /// </summary>
        bool SuspendedHoldingBatchScope => asyncSuspended && asyncCommandRetainsBatchScope;

        /// <summary>
        /// Takes the per-batch scope: the response object replies are written into, and, in cluster mode, the
        /// cluster epoch. See the remarks on <see cref="BeginAsyncCommand(bool)"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void EnterBatchScope()
        {
            // Before the lock below, whose acquire is a full fence: teardown either sees the batch in scope or
            // the batch sees the teardown (see LeaveStorageToRunningBatch)
            batchInScope = true;
            if (clusterSession is not null)
                clusterSession.AcquireCurrentEpoch();
            networkSender.EnterAndGetResponseObject(out dcurr, out dend);
        }

        /// <summary>
        /// Gives the per-batch scope back, including the scratch buffers whose contents it kept addressable, and
        /// serves what was left for the response object while the batch held it.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ExitBatchScope()
        {
            asyncCommandRetainsBatchScope = false;

            if (clusterSession is not null)
                clusterSession.ReleaseCurrentEpoch();

            scratchBufferBuilder.Reset();
            scratchBufferAllocator.Reset();

            // Batch boundary: no argument pointers outlive it, so over-sized per-session buffers grown for one
            // unusually wide command can be released here. Counting down an integer keeps this off the parse
            // state itself, which measurably degrades code generation for the batch loop when read on every
            // batch. Inside the scope, because some of the buffers go with the storage sessions after teardown.
            if (--sessionTrimCountdown <= 0 && !sessionTornDown)
                TrimSessionBuffers();

            // Cleared before the lock is released, which is a full fence. From here teardown may dispose the
            // storage sessions, so nothing below touches them.
            batchInScope = false;
            networkSender.ExitAndReturnResponseObject();
            ServeSenderWaiters();

            if (sessionTornDown)
                ReleaseBatchStorageHold();
        }

        /// <summary>
        /// Called from <see cref="Dispose"/>: keeps the storage sessions alive for a batch that is still in
        /// scope, which then releases them on its way out.
        /// </summary>
        /// <remarks>
        /// Teardown runs on the session's own thread after its batch, except on server shutdown, which disposes
        /// every session from the shutting-down thread while their batches may still be running. A batch's scope
        /// exit clears <see cref="batchInScope"/> and then reads <see cref="sessionTornDown"/>, while teardown
        /// sets <see cref="sessionTornDown"/>, publishes the hold, and then reads <see cref="batchInScope"/>, each
        /// with a full fence in between. So either teardown sees the batch gone and releases the hold itself, or
        /// the exit sees the teardown and releases it; the exchange on <see cref="batchStorageHoldPending"/>
        /// makes sure only one does. A batch that enters after teardown sees <see cref="sessionTornDown"/> and
        /// returns before running anything.
        /// </remarks>
        void LeaveStorageToRunningBatch()
        {
            _ = Interlocked.Increment(ref storageHolds);
            _ = Interlocked.Exchange(ref batchStorageHoldPending, 1);

            if (!Volatile.Read(ref batchInScope))
            {
                ReleaseBatchStorageHold();
                return;
            }

            // Before the connection's hold is released, so that whichever thread disposes the storage sessions
            // sees it
            CountStorageTeardown();
        }

        /// <summary>
        /// Releases the hold <see cref="LeaveStorageToRunningBatch"/> took, unless the other side already has.
        /// </summary>
        void ReleaseBatchStorageHold()
        {
            if (Interlocked.Exchange(ref batchStorageHoldPending, 0) == 1)
                ReleaseStorageHold();
        }


        /// <summary>
        /// Drops a per-batch scope that was being held across a suspension. Called when the suspension ends
        /// without resuming, so a torn-down session neither leaks its response object nor stalls a cluster
        /// transition indefinitely.
        /// </summary>
        /// <remarks>
        /// Runs on whichever thread observed the teardown, which is not the thread that took the scope. The
        /// response object's lock is constructed without thread-owner tracking for this reason.
        /// </remarks>
        void ReleaseRetainedBatchScope()
        {
            if (!asyncCommandRetainsBatchScope)
                return;

            DropHeldBackPushes();
            ExitBatchScope();

            // A publisher parked on the hold delivers to the closed connection, which drops the message
            Interlocked.MemoryBarrier();
            networkSender.NotifySendCapacityWaiters();
        }

        /// <summary>
        /// Scope that publishes <see cref="CurrentAsyncCommandScope"/> for an async command body.
        /// </summary>
        internal readonly ref struct AsyncCommandScope
        {
            readonly RespServerSession previous;

            internal AsyncCommandScope(RespServerSession session)
            {
                previous = CurrentAsyncCommandScope;
                CurrentAsyncCommandScope = session;
            }

            /// <summary>
            /// Restores the previously published session.
            /// </summary>
            public void Dispose() => CurrentAsyncCommandScope = previous;
        }

        /// <summary>
        /// Finishes dispatching an async command body. Always returns true: either the body ran to completion
        /// inline and has already written its reply, or it suspended and will write it on resume.
        /// </summary>
        /// <param name="body">The task returned by the command body.</param>
        internal bool CompleteAsyncCommand(ValueTask body)
        {
            if (body.IsCompletedSuccessfully)
                return true;

            if (!asyncSuspended)
            {
                // Completed inline and faulted, or completed inline after an awaiter that never suspended.
                // Rethrow here so the session's own handlers turn it into a RESP error.
                body.GetAwaiter().GetResultGuarded();
                return true;
            }

            pendingAsyncBody = body;
            return true;
        }

        /// <summary>
        /// Called by <see cref="RespAsyncMethodBuilder"/> immediately before a body hands its resume delegate
        /// to an awaiter.
        /// </summary>
        /// <param name="box">Box holding the suspending body.</param>
        internal void BeginSuspension(RespAsyncBox box)
        {
            Debug.Assert(!asyncSuspended, "A session may suspend only one command at a time");
            Debug.Assert(recvBufferPtr != null, "A command may only suspend while the session holds its receive buffer");

            asyncSuspended = true;
            resumeBox = box;
            suspendedRecvBufferPtr = recvBufferPtr;
            suspendedBytesRead = bytesRead;

            // Published last: from here a completion on another thread may observe the suspension.
            handoff.Arm();
        }

        /// <summary>
        /// Completion callback for a suspended body. Resumes it inside the session scope, unless the
        /// suspending thread is still inside that scope, in which case it resumes on its way out.
        /// </summary>
        /// <param name="box">Box holding the suspended body.</param>
        internal void ResumeAsyncCommand(RespAsyncBox box)
        {
            Debug.Assert(ReferenceEquals(box, resumeBox));

            if (handoff.ClaimResume())
                DrainResumes();
        }

        /// <summary>
        /// Hands the suspension off to whichever thread resumes it, once the resource scope has been left.
        /// Returns the task the network handler waits on for the rest of the batch.
        /// </summary>
#pragma warning disable VSTHRD200 // Not an async method: it publishes a suspension and returns its completion
        ValueTask<int> PublishSuspension()
        {
            var source = consumeSource ??= new RespConsumeCompletionSource();
            source.Reset();
            var pending = new ValueTask<int>(source, source.Version);

            if (ParkSuspension())
                DrainResumes();

            return pending;
        }
#pragma warning restore VSTHRD200

        /// <summary>
        /// Re-enters the session scope and drives the suspended body, repeating while the body suspends again
        /// and its completion has already arrived. Looping rather than recursing bounds the stack when a body
        /// awaits operations that keep completing immediately.
        /// </summary>
        void DrainResumes()
        {
            // An executor thread hands the receive loop back to the thread pool, rather than run it, and every
            // batch it receives, itself
            var leaveThread = OffloadExecutor.OnExecutorThread;

            while (true)
            {
                if (!TryEnterResume())
                {
                    AbandonSuspension();
                    consumeSource.SetException(new SessionTornDownException(nameof(RespServerSession)), leaveThread);
                    return;
                }

                int consumed;
                try
                {
                    asyncSuspended = false;
                    consumed = ConsumeCore(suspendedRecvBufferPtr, suspendedBytesRead, resumeBox);
                }
                catch (Exception ex)
                {
                    // ConsumeCore turns command faults into RESP errors, so reaching here means the session
                    // itself is unusable. Surface it to the receive loop, which tears the connection down.
                    ExitResume();
                    AbandonSuspension();
                    consumeSource.SetException(ex, leaveThread);
                    return;
                }

                ExitResume();

                if (!asyncSuspended)
                {
                    handoff.Complete();
                    consumeSource.SetResult(consumed, leaveThread);
                    return;
                }

                if (!ParkSuspension())
                    return;
            }
        }

        /// <summary>
        /// Runs the suspended body's continuation inside the resource scope, and observes its task once it
        /// completes so a fault is reported to the client. Called from <see cref="ConsumeCore"/>.
        /// </summary>
        void ResumeBody(RespAsyncBox box)
        {
            box.MoveNext();

            if (asyncSuspended)
                return;

            resumeBox = null;
            var body = pendingAsyncBody;
            pendingAsyncBody = default;

            try
            {
                body.GetAwaiter().GetResultGuarded();
            }
            finally
            {
                ReturnAsyncBox(box);

                // The command has now run for its whole duration, measured from the start of the batch that
                // carried it, so the entry it was owed can be written.
                if (slowLogSuspendedCmd != RespCommand.INVALID)
                {
                    var suspendedCmd = slowLogSuspendedCmd;
                    slowLogSuspendedCmd = RespCommand.INVALID;
                    if (slowLogThreshold > 0)
                        HandleSlowLog(suspendedCmd);
                }
            }
        }

        /// <summary>
        /// Rents a box for a state machine type, preferring the session's own slot.
        /// </summary>
        /// <typeparam name="TStateMachine">The body's state machine type.</typeparam>
        internal RespAsyncBox<TStateMachine> RentAsyncBox<TStateMachine>() where TStateMachine : IAsyncStateMachine
        {
            RespAsyncBox<TStateMachine> box;
            if (cachedAsyncBox is RespAsyncBox<TStateMachine> mine)
            {
                cachedAsyncBox = null;
                box = mine;
            }
            else if (RespAsyncBoxCache<TStateMachine>.Cached is { } shared)
            {
                RespAsyncBoxCache<TStateMachine>.Cached = null;
                box = shared;
            }
            else
            {
                box = new RespAsyncBox<TStateMachine>();
            }

            box.Rearm(this);
            return box;
        }

        void ReturnAsyncBox(RespAsyncBox box)
        {
            box.Clear();
            if (cachedAsyncBox is null)
                cachedAsyncBox = box;
            else
                box.ReturnToThreadCache();
        }

        /// <summary>
        /// Claims the right to run inside the session scope on behalf of a resume. Fails once the session has
        /// been torn down, at which point the receive buffer and response object are no longer the session's
        /// to touch.
        /// </summary>
        bool TryEnterResume() => resumeGate.TryEnter();

        void ExitResume() => resumeGate.Exit();

        ResumeGate resumeGate;

        /// <summary>
        /// Drops a suspension whose session has gone away. The body's state machine is left parked: its
        /// continuation has already fired, nothing will drive it again, and the garbage collector reclaims it
        /// along with the box. The box is not pooled, because an awaiter may still hold its resume delegate.
        /// </summary>
        /// <remarks>
        /// Everything the body would still have run is dropped with it, including the <c>finally</c> of a
        /// <c>try</c> that spans its await. A suspendable body may therefore only put work the garbage
        /// collector can finish -- returning a pooled buffer, disposing a memory owner, session-local
        /// bookkeeping -- in such a <c>finally</c>. Anything a torn-down session must release to the rest of
        /// the server, such as a lock, an epoch, or an entry in a server-wide structure, belongs in
        /// <see cref="CancelAsyncCommands"/>, which runs on the teardown path itself.
        /// </remarks>
        void AbandonSuspension()
        {
            resumeBox = null;
            pendingAsyncBody = default;
            asyncSuspended = false;
            ReleaseRetainedBatchScope();
            handoff.Complete();
        }

        /// <summary>
        /// Stops a suspended command and blocks any further resume from entering the session scope. Called
        /// from <see cref="Dispose"/>, before the network handler reclaims the receive buffer.
        /// </summary>
        internal void CancelAsyncCommands()
        {
            sessionTornDown = true;

            try
            {
                sessionDelay?.Cancel();
                sessionSignal?.Cancel();
                sendCapacityWait?.Cancel();
                migrationWait?.Cancel();
                aofGateWait?.Cancel();
                freshnessWait?.Cancel();
                lockReleaseWait?.Cancel();
            }
            catch (Exception ex)
            {
                // Releasing a wait threw. Swallow it: teardown must continue, or the session leaks.
                logger?.LogDebug(ex, "Cancelling suspended commands threw");
            }

            // A test pins the outcome in which the resume the cancellations queued is admitted: a resume that has
            // started has cleared the suspension, and the gate stays open until it has
            if (ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.Session_Teardown_Admits_Queued_Resume))
                SpinWait.SpinUntil(() => !Volatile.Read(ref asyncSuspended), TimeSpan.FromSeconds(30));

            resumeGate.Close();
        }
    }

    /// <summary>
    /// Completion source for a receive whose batch could not be consumed synchronously. One per session, reset
    /// per suspension, so parking allocates nothing.
    /// </summary>
    internal sealed class RespConsumeCompletionSource : IValueTaskSource<int>
    {
        ManualResetValueTaskSourceCore<int> core;

        internal short Version => core.Version;

        internal void Reset() => core.Reset();

        /// <summary>
        /// Completes the receive with the bytes its batch consumed.
        /// </summary>
        /// <param name="consumed">Bytes consumed.</param>
        /// <param name="runContinuationAsynchronously">
        /// Whether the receive loop waiting on this must continue on the thread pool rather than on the calling
        /// thread, which is the case when the caller is an <see cref="OffloadExecutor"/> thread.
        /// </param>
        internal void SetResult(int consumed, bool runContinuationAsynchronously)
        {
            core.RunContinuationsAsynchronously = runContinuationAsynchronously;
            core.SetResult(consumed);
        }

        /// <summary>
        /// Fails the receive, which ends the connection.
        /// </summary>
        /// <param name="exception">Why the batch could not be finished.</param>
        /// <param name="runContinuationAsynchronously">As for <see cref="SetResult"/>.</param>
        internal void SetException(Exception exception, bool runContinuationAsynchronously)
        {
            core.RunContinuationsAsynchronously = runContinuationAsynchronously;
            core.SetException(exception);
        }

        /// <inheritdoc />
        public int GetResult(short token) => core.GetResult(token);

        /// <inheritdoc />
        public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);

        /// <inheritdoc />
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
            => core.OnCompleted(continuation, state, token, flags);
    }
}