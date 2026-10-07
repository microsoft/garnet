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
    /// A command body suspends inside <see cref="TryConsumeMessagesAsync"/> but after the session's resource
    /// scope has been released, so no response-object lock, cluster epoch, or scratch buffer is held across
    /// the wait. Because the stack has not unwound past <see cref="TryConsumeMessagesAsync"/>, the network
    /// handler has not shifted or resized the receive buffer and has not issued the next receive. The
    /// suspended command therefore keeps its arguments valid, and commands pipelined behind it stay unparsed
    /// until it finishes -- the session is parked, exactly as a synchronous blocking command would leave it,
    /// but without owning a thread.
    /// </para>
    /// <para>
    /// Resumption runs through <see cref="ResumeAsyncCommand"/>, which re-enters the resource scope before
    /// driving the body's state machine, then drains the rest of the batch.
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
        /// run, and <see cref="SessionDelay.Cancel"/> has already stopped its timer.
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal AsyncCommandScope BeginAsyncCommand() => new(this);

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