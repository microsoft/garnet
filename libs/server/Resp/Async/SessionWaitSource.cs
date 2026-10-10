// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Garnet.server
{
    /// <summary>
    /// Base for the reusable await sources a suspending command body waits on, and the session-scoped
    /// cancellation that releases one when the connection goes away.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the shape a blocking command's await source should take. The framework equivalents --
    /// <see cref="Task.Delay(TimeSpan)"/>, a <see cref="TaskCompletionSource"/> per wait -- allocate a promise
    /// every time, which on a command that parks once per request puts an allocation on the hot path for no
    /// reason. Here one <see cref="ManualResetValueTaskSourceCore{T}"/> is reset between waits, so a
    /// steady-state park allocates nothing.
    /// </para>
    /// <para>
    /// A session parks at most one command at a time, so one instance per session suffices and the states
    /// need only distinguish "a wait is outstanding" from "it is not". Teardown always completes a wait by
    /// faulting it, so a body unwinds without writing a reply into a response object that is being reclaimed.
    /// </para>
    /// </remarks>
    internal abstract class SessionWaitSource : IValueTaskSource
    {
        /// <summary>No wait is outstanding.</summary>
        const int Idle = 0;

        /// <summary>A wait is outstanding and has not yet been completed.</summary>
        const int Waiting = 1;

        ManualResetValueTaskSourceCore<bool> core;
        int state;
        bool cancelled;

        protected SessionWaitSource()
        {
            // Completions arrive on threads that must not run a session's batch: the shared timer thread, or
            // another session that is holding its own response object and epoch. The hand-off costs a
            // thread-pool queue, which allocates nothing.
            core.RunContinuationsAsynchronously = true;
        }

        /// <summary>True once the session has been torn down, after which no wait is served.</summary>
        protected bool Cancelled => Volatile.Read(ref cancelled);

        /// <summary>
        /// The task for the wait most recently armed. Valid only until that wait's result is read.
        /// </summary>
        protected ValueTask Pending => new(this, core.Version);

        /// <summary>
        /// Readies the source for one wait. Called only from the session's own thread, so two waits are never
        /// armed concurrently; the interlocked work in <see cref="Complete"/> guards against the completing
        /// threads, which are other threads.
        /// </summary>
        protected void Arm()
        {
            core.Reset();
            Volatile.Write(ref state, Waiting);
        }

        /// <summary>
        /// Completes an outstanding wait, if there is one that nothing else has completed yet.
        /// </summary>
        /// <param name="canceled">Whether the wait is being released by teardown rather than satisfied.</param>
        /// <returns>True if this call completed the wait.</returns>
        protected bool Complete(bool canceled)
        {
            // Exactly one completing thread gets to complete a given wait.
            if (Interlocked.CompareExchange(ref state, Idle, Waiting) != Waiting)
                return false;

            if (canceled)
                core.SetException(new OperationCanceledException());
            else
                core.SetResult(true);

            return true;
        }

        /// <summary>
        /// Releases an outstanding wait and refuses later ones. Called when the session is torn down, so a
        /// parked command stops waiting instead of holding whatever it was waiting on.
        /// </summary>
        internal void Cancel()
        {
            Volatile.Write(ref cancelled, true);
            OnCancelling();
            _ = Complete(canceled: true);
        }

        /// <summary>
        /// Releases whatever the wait holds besides the source itself, before the wait is faulted.
        /// </summary>
        protected virtual void OnCancelling() { }

        /// <inheritdoc/>
        public void GetResult(short token) => core.GetResult(token);

        /// <inheritdoc/>
        public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);

        /// <inheritdoc/>
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
            => core.OnCompleted(continuation, state, token, flags);
    }
}