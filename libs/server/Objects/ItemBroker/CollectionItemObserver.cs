// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
    /// <para>
    /// The observer is its own completion source. Exactly one session awaits it and exactly one caller
    /// completes it - the winner of the <see cref="Status"/> transition out of
    /// <see cref="ObserverStatus.WaitingForResult"/>, taken under <see cref="ObserverStatusLock"/> - so the
    /// wait is served by a single <see cref="IValueTaskSource{TResult}"/> rather than by a semaphore, a
    /// cancellation token source, and the waiter, timer and registration objects those imply.
    /// </para>
    /// <para>
    /// The broker's main loop claims the observer (<see cref="TryClaim"/>) for the length of an attempt to serve
    /// it, which may wait on the device, and releases it with the attempt's result
    /// (<see cref="ReleaseClaim"/>). The lock is held for each transition and around a pop, never across a device wait. A
    /// timeout, CLIENT UNBLOCK or the session's disposal that arrives while the observer is claimed is deferred
    /// to the release, and the attempt then takes nothing for the observer.
    /// </para>
    /// <para>
    /// Whether the attempt takes items is decided where it takes them: inside the store's update of the key,
    /// under that key's lock, the pop first checks for a deferred completion (<see cref="TryBeginTake"/>). If none
    /// has arrived, the observer is committed to whatever the pop takes (<see cref="EndTake"/>), and a completion
    /// arriving later fails, as it would against an observer already served. So a client that CLIENT UNBLOCK
    /// reports as unblocked receives no item, and one that receives an item was not reported unblocked; and no
    /// item is ever popped for an observer that is not then given it.
    /// </para>
    /// </remarks>
    internal sealed class CollectionItemObserver : IValueTaskSource<CollectionItemResult>
    {
        /// <summary>
        /// The session in which the blocking command was called
        /// </summary>
        internal RespServerSession Session { get; }

        /// <summary>
        /// The database the blocking command was called against, captured here because the broker's main
        /// loop reads it after the session has parked and may have moved on.
        /// </summary>
        internal int DatabaseId { get; }

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

        /// <summary>
        /// The status a completion that arrived while the observer was claimed leaves it in, or
        /// <see cref="ObserverStatus.WaitingForResult"/> if none arrived.
        /// </summary>
        ObserverStatus deferredStatus = ObserverStatus.WaitingForResult;

        /// <summary>
        /// The result of the completion recorded in <see cref="deferredStatus"/>.
        /// </summary>
        CollectionItemResult deferredResult;

        internal CollectionItemObserver(RespServerSession session, RespCommand command, CollectionItemArgs commandArgs = default)
        {
            Session = session;
            DatabaseId = session.activeDbId;
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
        /// Whether the observer has been completed, and so will never be served.
        /// </summary>
        internal bool IsCompleted => Status is ObserverStatus.ResultSet or ObserverStatus.SessionDisposed;

        /// <summary>
        /// Claims the observer for an attempt to serve it, if it is still waiting.
        /// </summary>
        /// <returns>True if the observer is now claimed and must be released with <see cref="ReleaseClaim"/></returns>
        internal bool TryClaim()
        {
            if (Status != ObserverStatus.WaitingForResult)
                return false;

            ObserverStatusLock.WriteLock();
            try
            {
                if (Status != ObserverStatus.WaitingForResult)
                    return false;

                Status = ObserverStatus.Claimed;
                return true;
            }
            finally
            {
                ObserverStatusLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Releases the claim taken by <see cref="TryClaim"/>, completing the observer with the attempt's result if it
        /// took items for the observer, otherwise with any completion deferred while the observer was claimed, and
        /// otherwise with a type mismatch the attempt found.
        /// </summary>
        /// <param name="result">The attempt's result: an item or a type mismatch, or the default result if the attempt took nothing</param>
        /// <returns>True if the observer is still waiting</returns>
        internal bool ReleaseClaim(in CollectionItemResult result)
        {
            ObserverStatusLock.WriteLock();
            try
            {
                if (Status == ObserverStatus.Committed)
                {
                    Debug.Assert(result.Found, "An attempt committed to serving an observer did not deliver an item");
                    Status = ObserverStatus.ResultSet;
                    resultSource.SetResult(result);
                    return false;
                }

                Debug.Assert(Status == ObserverStatus.Claimed, "Released an observer that was not claimed");
                Debug.Assert(!result.Found, "An attempt delivered an item it did not commit to the observer");

                if (deferredStatus != ObserverStatus.WaitingForResult)
                {
                    Status = deferredStatus;
                    resultSource.SetResult(deferredResult);
                    return false;
                }

                if (result.IsTypeMismatch)
                {
                    Status = ObserverStatus.ResultSet;
                    resultSource.SetResult(result);
                    return false;
                }

                Status = ObserverStatus.WaitingForResult;
                return true;
            }
            finally
            {
                ObserverStatusLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Begins taking items for the claimed observer, unless a completion deferred while it was claimed has
        /// already decided its outcome. Called from inside the store's update of the key, just before the pop.
        /// </summary>
        /// <remarks>
        /// On success the lock stays held until <see cref="EndTake"/>, so a completion arriving meanwhile waits
        /// for the take's outcome rather than deciding against it: a pop that takes an item commits the observer,
        /// and the completion then fails. The wait is for one in-memory pop, which does no IO, so it uses the
        /// lock's own yielding wait. Deciding before the pop would not do, since a sorted set drops members as
        /// they expire, so a pop can find nothing where a check just before it found a member.
        /// </remarks>
        /// <returns>True if the caller must pop and then call <see cref="EndTake"/>; false if it must take nothing</returns>
        internal bool TryBeginTake()
        {
            ObserverStatusLock.WriteLock();
            Debug.Assert(Status == ObserverStatus.Claimed, "Took items for an observer that was not claimed");

            if (deferredStatus == ObserverStatus.WaitingForResult)
                return true;

            ObserverStatusLock.WriteUnlock();
            return false;
        }

        /// <summary>
        /// Ends the take begun by <see cref="TryBeginTake"/>, committing the observer to the items popped, if any.
        /// </summary>
        /// <param name="took">True if the pop took at least one item</param>
        internal void EndTake(bool took)
        {
            if (took)
                Status = ObserverStatus.Committed;
            ObserverStatusLock.WriteUnlock();
        }

        /// <summary>
        /// Commits the claimed observer to an item the attempt delivers without popping it, unless a completion
        /// deferred while the observer was claimed has already decided its outcome.
        /// </summary>
        /// <returns>True if the attempt must deliver the item; false if it must deliver nothing</returns>
        internal bool TryCommit()
        {
            if (!TryBeginTake())
                return false;

            EndTake(took: true);
            return true;
        }

        /// <summary>
        /// Unblocks the observer with an empty result, or with an error if <paramref name="throwError"/> is set.
        /// </summary>
        /// <remarks>
        /// While the observer is claimed but not committed, the unblock is deferred to the end of the attempt, which
        /// then takes nothing for the observer. Once committed, the observer is being served and is not unblocked.
        /// </remarks>
        /// <param name="throwError">True to complete the waiting session with an error rather than an empty result</param>
        /// <returns>True if this call unblocked the observer</returns>
        internal bool TryForceUnblock(bool throwError = false)
            => TryComplete(throwError ? CollectionItemResult.ForceUnblocked : CollectionItemResult.Empty,
                ObserverStatus.ResultSet);

        /// <summary>
        /// Safely set the status of the observer to reflect that its calling session was disposed
        /// </summary>
        internal void HandleSessionDisposed()
            => TryComplete(CollectionItemResult.Empty, ObserverStatus.SessionDisposed);

        void HandleTimeout()
            => TryComplete(CollectionItemResult.Empty, ObserverStatus.ResultSet);

        /// <summary>
        /// Completes the wait if this observer is still waiting for one, or defers the completion to the release if
        /// the observer is claimed. Fails once the observer is committed to an attempt's items.
        /// </summary>
        /// <returns>True if this call completed the observer or deferred its completion</returns>
        bool TryComplete(CollectionItemResult result, ObserverStatus status)
        {
            if (IsCompleted)
                return false;

            ObserverStatusLock.WriteLock();
            try
            {
                if (Status == ObserverStatus.WaitingForResult)
                {
                    Status = status;

                    // Safe under the lock: continuations are scheduled rather than run here.
                    resultSource.SetResult(result);
                    return true;
                }

                if (Status == ObserverStatus.Claimed && deferredStatus == ObserverStatus.WaitingForResult)
                {
                    deferredStatus = status;
                    deferredResult = result;
                    return true;
                }

                return false;
            }
            finally
            {
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
        // The broker's main loop is making an attempt to serve the observer
        Claimed,
        // The claiming attempt has taken items for the observer, which it must deliver
        Committed,
        // Observer's result is set
        ResultSet,
        // Observer's calling RESP server session is disposed
        SessionDisposed,
    }
}