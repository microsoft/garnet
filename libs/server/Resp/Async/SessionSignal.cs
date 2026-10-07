// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Garnet.server
{
    /// <summary>
    /// Await source for a session waiting to be woken by another session, which is the shape every real
    /// blocking command takes: a reader parks on a key and a writer on a different connection releases it.
    /// </summary>
    /// <remarks>
    /// Continuations are queued rather than run inline. A session signals from inside its own batch, which
    /// means it holds its response object and, in cluster mode, its epoch; resuming the parked session inline
    /// would run that session's batch nested inside those, re-entering the epoch on one thread and making the
    /// stack depth of a wakeup chain unbounded. Queuing costs one thread-pool dispatch on the wakeup path and
    /// keeps the resume on a thread that owns nothing.
    ///
    /// One instance per session, reused across waits, so parking allocates nothing.
    /// </remarks>
    internal sealed class SessionSignal : IValueTaskSource
    {
        ManualResetValueTaskSourceCore<bool> core;

        const int Idle = 0;
        const int Waiting = 1;

        int state;
        bool cancelled;

        internal SessionSignal() => core.RunContinuationsAsynchronously = true;

        /// <summary>
        /// Arms the source so that a signal arriving from this point wakes the session. Called by
        /// <see cref="SessionSignalRegistry.Register"/> before the signal is published to signallers; a
        /// signal published before it is armed is lost and the session parks forever.
        /// </summary>
        internal void Arm()
        {
            core.Reset();
            Volatile.Write(ref state, Waiting);
        }

        /// <summary>
        /// Waits for an armed signal. The returned task completes when <see cref="Signal"/> or
        /// <see cref="Cancel"/> is called, and is already completed if the session has been torn down.
        /// </summary>
        /// <returns>A task representing the wait.</returns>
        internal ValueTask WaitAsync()
        {
            // Teardown may have run between arming and here, in which case nothing else will complete it.
            if (Volatile.Read(ref cancelled))
                _ = Complete();

            return new ValueTask(this, core.Version);
        }

        /// <summary>
        /// Wakes a waiting session. Does nothing if it is not waiting.
        /// </summary>
        /// <returns>True if a waiter was woken.</returns>
        internal bool Signal() => Complete();

        /// <summary>
        /// Permanently releases this source, waking any waiter. Called when the session is torn down.
        /// </summary>
        internal void Cancel()
        {
            Volatile.Write(ref cancelled, true);
            _ = Complete();
        }

        bool Complete()
        {
            // Exactly one of the signalling thread and a cancelling teardown gets to complete a given wait.
            if (Interlocked.CompareExchange(ref state, Idle, Waiting) != Waiting)
                return false;

            core.SetResult(true);
            return true;
        }

        /// <inheritdoc />
        public void GetResult(short token) => core.GetResult(token);

        /// <inheritdoc />
        public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);

        /// <inheritdoc />
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
            => core.OnCompleted(continuation, state, token, flags);
    }
}