// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Garnet.server
{
    /// <summary>
    /// A reusable timed wait for a suspending command body, and the session-scoped cancellation that releases
    /// it when the connection goes away.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the shape a blocking command's await source should take. <see cref="Task.Delay(TimeSpan)"/>
    /// allocates a timer and a promise on every call, which on a command that parks once per request puts an
    /// allocation on the hot path for no reason. Here a single <see cref="Timer"/> is created per session that
    /// ever waits and rearmed thereafter, and completion runs through one
    /// <see cref="ManualResetValueTaskSourceCore{T}"/> that is reset between waits, so a steady-state park
    /// allocates nothing.
    /// </para>
    /// <para>
    /// A session parks at most one command at a time, so one instance per session suffices and the arm and
    /// completion states need only distinguish "a wait is outstanding" from "it is not".
    /// </para>
    /// </remarks>
    internal sealed class SessionDelay : IValueTaskSource
    {
        /// <summary>No wait is outstanding.</summary>
        const int Idle = 0;

        /// <summary>A wait is outstanding and has not yet been completed by the timer or by cancellation.</summary>
        const int Waiting = 1;

        ManualResetValueTaskSourceCore<bool> core;
        Timer timer;
        int state;
        bool cancelled;

        internal SessionDelay()
        {
            // Timer callbacks for every due timer in the process run one after another on a single thread, so
            // resuming a session inline from one would stall every other timer behind it. The hand-off costs
            // a thread-pool queue, which allocates nothing.
            core.RunContinuationsAsynchronously = true;
        }

        /// <summary>
        /// Starts a wait of the given length.
        /// </summary>
        /// <param name="delay">How long to wait. Must be positive.</param>
        /// <returns>A task that completes when the delay elapses, or faults if the session is torn down first.</returns>
        /// <remarks>
        /// Called only from a command body on the session's own thread, so there is never a second wait being
        /// armed concurrently; the interlocked work below guards against the timer and
        /// <see cref="Cancel"/>, which do run on other threads.
        /// </remarks>
        internal ValueTask WaitAsync(TimeSpan delay)
        {
            if (Volatile.Read(ref cancelled))
                return ValueTask.FromException(new OperationCanceledException());

            core.Reset();
            Volatile.Write(ref state, Waiting);

            // Created on first use so a session that never parks does not pay for a timer, and reused
            // afterwards because rearming an existing timer does not allocate.
            timer ??= new Timer(static self => ((SessionDelay)self).OnTimer(), this, Timeout.Infinite, Timeout.Infinite);
            timer.Change(delay, Timeout.InfiniteTimeSpan);

            // Cancel may have run between the check above and the arm, in which case nothing else will
            // complete this wait.
            if (Volatile.Read(ref cancelled))
                Complete(canceled: true);

            return new ValueTask(this, core.Version);
        }

        /// <summary>
        /// Releases an outstanding wait and refuses later ones. Called when the session is torn down, so a
        /// parked command stops waiting instead of holding its timer until it expires.
        /// </summary>
        internal void Cancel()
        {
            Volatile.Write(ref cancelled, true);
            timer?.Change(Timeout.Infinite, Timeout.Infinite);
            Complete(canceled: true);
        }

        void OnTimer() => Complete(canceled: false);

        void Complete(bool canceled)
        {
            // Exactly one of the timer and Cancel gets to complete a given wait.
            if (Interlocked.CompareExchange(ref state, Idle, Waiting) != Waiting)
                return;

            if (canceled)
                core.SetException(new OperationCanceledException());
            else
                core.SetResult(true);
        }

        /// <inheritdoc/>
        public void GetResult(short token) => core.GetResult(token);

        /// <inheritdoc/>
        public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);

        /// <inheritdoc/>
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
            => core.OnCompleted(continuation, state, token, flags);
    }
}