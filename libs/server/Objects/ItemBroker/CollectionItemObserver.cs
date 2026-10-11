// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// This class defines an observer for a specific blocking command
    /// </summary>
    /// <remarks>
    /// The observer is its own completion source. Exactly one session awaits it and exactly one caller
    /// completes it - the winner of the <see cref="Status"/> transition out of
    /// <see cref="ObserverStatus.WaitingForResult"/>, taken under <see cref="ObserverStatusLock"/> - so the
    /// wait is served by a single <see cref="IValueTaskSource{TResult}"/> rather than by a semaphore, a
    /// cancellation token source, and the waiter, timer and registration objects those imply.
    /// </remarks>
    internal sealed class CollectionItemObserver : IValueTaskSource<CollectionItemResult>
    {
        /// <summary>
        /// The session in which the blocking command was called
        /// </summary>
        internal RespServerSession Session { get; }

        /// <summary>
        /// The command type for the blocking command
        /// </summary>
        internal RespCommand Command { get; }

        /// <summary>
        /// Additional arguments for the command
        /// </summary>
        internal CollectionItemArgs CommandArgs { get; }

        /// <summary>
        /// Status of the observer
        /// </summary>
        internal ObserverStatus Status { get; private set; } = ObserverStatus.WaitingForResult;

        /// <summary>
        /// Lock for the status of the observer
        /// </summary>
        internal SingleWriterMultiReaderLock ObserverStatusLock;

        /// <summary>
        /// Continuations are scheduled, never inlined: the completer is usually the broker's main loop, and
        /// resuming the session inline there would run that session's command and response write on the loop
        /// every other blocking command depends on.
        /// </summary>
        ManualResetValueTaskSourceCore<CollectionItemResult> resultSource = new() { RunContinuationsAsynchronously = true };

        /// <summary>
        /// Fires once when a finite timeout expires. Null for an indefinite wait.
        /// </summary>
        Timer timeoutTimer;

        internal CollectionItemObserver(RespServerSession session, RespCommand command, CollectionItemArgs commandArgs = default)
        {
            Session = session;
            Command = command;
            CommandArgs = commandArgs;
        }

        /// <summary>
        /// Waits for this observer to be given a result, to be unblocked, or for its timeout to expire.
        /// </summary>
        /// <param name="timeoutInSeconds">Timeout of operation (in seconds, 0 for waiting indefinitely)</param>
        /// <returns>The result the observer was completed with</returns>
        internal ValueTask<CollectionItemResult> WaitAsync(double timeoutInSeconds)
        {
            if (timeoutInSeconds != 0)
            {
                // Armed only after the field is assigned, so a timeout that fires immediately cannot race the
                // awaiter's disposal of the timer.
                timeoutTimer = new Timer(static state => ((CollectionItemObserver)state).HandleTimeout(), this,
                    Timeout.Infinite, Timeout.Infinite);
                _ = timeoutTimer.Change(TimeSpan.FromSeconds(timeoutInSeconds), Timeout.InfiniteTimeSpan);
            }

            return new ValueTask<CollectionItemResult>(this, resultSource.Version);
        }

        /// <summary>
        /// Safely set the result for the observer
        /// </summary>
        /// <param name="result">Result to complete the observer with</param>
        /// <param name="isWriteLocked">True if the ObserverStatusLock was write locked by the caller</param>
        internal void HandleSetResult(CollectionItemResult result, bool isWriteLocked = false)
            => TryComplete(result, ObserverStatus.ResultSet, isWriteLocked);

        /// <summary>
        /// Unblocks the observer with an empty result, or with an error if <paramref name="throwError"/> is set.
        /// </summary>
        /// <param name="throwError">True to complete the waiting session with an error rather than an empty result</param>
        /// <returns>True if this call unblocked the observer</returns>
        internal bool TryForceUnblock(bool throwError = false)
            => TryComplete(throwError ? CollectionItemResult.ForceUnblocked : CollectionItemResult.Empty,
                ObserverStatus.ResultSet, isWriteLocked: false);

        /// <summary>
        /// Safely set the status of the observer to reflect that its calling session was disposed
        /// </summary>
        internal void HandleSessionDisposed()
            => TryComplete(CollectionItemResult.Empty, ObserverStatus.SessionDisposed, isWriteLocked: false);

        void HandleTimeout()
            => TryComplete(CollectionItemResult.Empty, ObserverStatus.ResultSet, isWriteLocked: false);

        /// <summary>
        /// Completes the wait, if this observer is still waiting for one.
        /// </summary>
        /// <returns>True if this call is the one that completed the observer</returns>
        bool TryComplete(CollectionItemResult result, ObserverStatus status, bool isWriteLocked)
        {
            // The result is already set, or the observer's session is disposed
            if (Status != ObserverStatus.WaitingForResult)
                return false;

            if (!isWriteLocked)
                ObserverStatusLock.WriteLock();
            try
            {
                if (Status != ObserverStatus.WaitingForResult)
                    return false;

                Status = status;

                // Safe under the lock: continuations are scheduled rather than run here.
                resultSource.SetResult(result);
                return true;
            }
            finally
            {
                if (!isWriteLocked)
                    ObserverStatusLock.WriteUnlock();
            }
        }

        public CollectionItemResult GetResult(short token)
        {
            try
            {
                return resultSource.GetResult(token);
            }
            finally
            {
                timeoutTimer?.Dispose();
            }
        }

        public ValueTaskSourceStatus GetStatus(short token) => resultSource.GetStatus(token);

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
            => resultSource.OnCompleted(continuation, state, token, flags);
    }

    internal enum ObserverStatus
    {
        // Observer is ready and waiting for result
        WaitingForResult,
        // Observer's result is set
        ResultSet,
        // Observer's calling RESP server session is disposed
        SessionDisposed,
    }
}