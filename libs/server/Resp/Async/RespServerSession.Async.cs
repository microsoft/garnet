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
    /// stalls every other session contending for those keys. That matches what the existing blocking list
    /// commands already do -- they join the running transaction rather than opting out of it -- and it is
    /// why the per-batch scope can be released by an idle suspension but the transaction cannot: the scope
    /// lasts one batch, whereas the locks are what makes the transaction atomic and cannot be dropped
    /// mid-transaction. A blocking command whose wait is unbounded should therefore bound it, or decline to
    /// wait when <c>txnManager.state == TxnState.Running</c>.
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
        /// this session, from <see cref="Publish"/> on a publisher's thread, therefore waits for the
        /// suspension to end. That is the same exposure a read completed in place already has, since it
        /// holds the same lock for the same device round trip, and it is the other reason a retaining wait
        /// must be bounded by the operation.
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
        void EnterBatchScope()
        {
            if (clusterSession is not null)
                clusterSession.AcquireCurrentEpoch();
            networkSender.EnterAndGetResponseObject(out dcurr, out dend);
        }

        /// <summary>
        /// Gives the per-batch scope back, including the scratch buffers whose contents it kept addressable.
        /// </summary>
        void ExitBatchScope()
        {
            asyncCommandRetainsBatchScope = false;
            networkSender.ExitAndReturnResponseObject();

            if (clusterSession is not null)
                clusterSession.ReleaseCurrentEpoch();

            scratchBufferBuilder.Reset();
            scratchBufferAllocator.Reset();
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
            if (asyncCommandRetainsBatchScope)
                ExitBatchScope();
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

            if (handoff.Park())
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
            while (true)
            {
                if (!TryEnterResume())
                {
                    AbandonSuspension();
                    consumeSource.SetException(new ObjectDisposedException(nameof(RespServerSession),
                        "The session was torn down while a command was suspended"));
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
                    consumeSource.SetException(ex);
                    return;
                }

                ExitResume();

                if (!asyncSuspended)
                {
                    handoff.Complete();
                    consumeSource.SetResult(consumed);
                    return;
                }

                if (!handoff.Park())
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
            }
            catch (Exception ex)
            {
                // Releasing a wait threw. Swallow it: teardown must continue, or the session leaks.
                logger?.LogDebug(ex, "Cancelling suspended commands threw");
            }

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

        internal void SetResult(int consumed) => core.SetResult(consumed);

        internal void SetException(Exception exception) => core.SetException(exception);

        /// <inheritdoc />
        public int GetResult(short token) => core.GetResult(token);

        /// <inheritdoc />
        public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);

        /// <inheritdoc />
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
            => core.OnCompleted(continuation, state, token, flags);
    }
}