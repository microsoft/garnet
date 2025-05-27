// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// This class defines an observer for a specific blocking command
    /// </summary>
    internal class CollectionItemObserver
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
        internal PinnedSpanByte[] CommandArgs { get; }

        /// <summary>
        /// Status of the observer
        /// </summary>
        internal ObserverStatus Status { get; set; } = ObserverStatus.WaitingForResult;

        /// <summary>
        /// Result of the observer
        /// </summary>
        internal CollectionItemResult Result { get; private set; }

        /// <summary>
        /// Lock for the status of the observer
        /// </summary>
        internal SingleWriterMultiReaderLock ObserverStatusLock;

        /// <summary>
        /// Semaphore to notify the ResultSet status
        /// </summary>
        internal SemaphoreSlim ResultFoundSemaphore { get; } = new(0, 1);

        /// <summary>
        /// Cancellation token to signal the semaphore awaiter to stop
        /// </summary>
        internal CancellationTokenSource CancellationTokenSource { get; } = new();

        internal CollectionItemObserver(RespServerSession session, RespCommand command, PinnedSpanByte[] commandArgs = null)
        {
            Session = session;
            Command = command;
            CommandArgs = commandArgs;
            Result = CollectionItemResult.Empty;
        }

        /// <summary>
        /// Safely set the result for the observer
        /// </summary>
        /// <param name="result"></param>
        /// <param name="isWriteLocked">True if the ObserverStatusLock was write locked by the caller</param>
        internal void HandleSetResult(CollectionItemResult result, bool isWriteLocked = false)
        {
            // If the result is already set or the observer session is disposed
            // There is no need to set the result
            if (Status != ObserverStatus.WaitingForResult)
                return;

            if (!isWriteLocked)
                ObserverStatusLock.WriteLock();
            try
            {
                if (Status != ObserverStatus.WaitingForResult)
                    return;

                // Set the result, update the status and release the semaphore
                Result = result;
                Status = ObserverStatus.ResultSet;
                ResultFoundSemaphore.Release();
            }
            finally
            {
                if (!isWriteLocked)
                    ObserverStatusLock.WriteUnlock();
            }
        }

        internal bool TryForceUnblock(bool throwError = false)
        {
            // If the result is already set or the observer session is disposed
            // There is no need to set the result
            if (Status != ObserverStatus.WaitingForResult)
                return false;

            ObserverStatusLock.WriteLock();
            try
            {
                if (Status != ObserverStatus.WaitingForResult)
                    return false;

                // Set the result, update the status and release the semaphore
                Result = throwError ? CollectionItemResult.ForceUnblocked : CollectionItemResult.Empty;
                Status = ObserverStatus.ResultSet;
                ResultFoundSemaphore.Release();
                return true;
            }
            finally
            {
                ObserverStatusLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Safely set the status of the observer to reflect that its calling session was disposed
        /// </summary>
        internal void HandleSessionDisposed()
        {
            ObserverStatusLock.WriteLock();
            try
            {
                Status = ObserverStatus.SessionDisposed;
                CancellationTokenSource.Cancel();
            }
            finally
            {
                ObserverStatusLock.WriteUnlock();
            }
        }
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