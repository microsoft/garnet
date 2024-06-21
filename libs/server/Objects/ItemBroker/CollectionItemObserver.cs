// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;

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
        internal ArgSlice[] CommandArgs { get; }

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
        internal ReaderWriterLockSlim ObserverStatusLock { get; } = new();

        /// <summary>
        /// Semaphore to notify the ResultSet status
        /// </summary>
        internal SemaphoreSlim ResultFoundSemaphore { get; } = new(0, 1);

        /// <summary>
        /// Cancellation token to signal the semaphore awaiter to stop
        /// </summary>
        internal CancellationTokenSource CancellationTokenSource { get; } = new();

        internal CollectionItemObserver(RespServerSession session, RespCommand command, ArgSlice[] commandArgs = null)
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
        internal void HandleSetResult(CollectionItemResult result)
        {
            // If the result is already set or the observer session is disposed
            // There is no need to set the result
            if (Status != ObserverStatus.WaitingForResult)
                return;

            ObserverStatusLock.EnterWriteLock();
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
                ObserverStatusLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Safely set the status of the observer to reflect that its calling session was disposed
        /// </summary>
        internal void HandleSessionDisposed()
        {
            ObserverStatusLock.EnterWriteLock();
            try
            {
                Status = ObserverStatus.SessionDisposed;
                CancellationTokenSource.Cancel();
            }
            finally
            {
                ObserverStatusLock.ExitWriteLock();
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