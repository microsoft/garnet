// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.devices
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    // For indicating and initiating termination, and for tracing errors and warnings relating to a partition.
    // Is is basically a wrapper around CancellationTokenSource with features for diagnostics.
    class StorageErrorHandler : IStorageErrorHandler
    {
        readonly CancellationTokenSource cts = new CancellationTokenSource();
        readonly ILogger logger;
        readonly LogLevel logLevelLimit;
        readonly string account;
        readonly string taskHub;
        readonly TaskCompletionSource<object> shutdownComplete;

        public event Action OnShutdown;

        public CancellationToken Token
        {
            get
            {
                try
                {
                    return cts.Token;
                }
                catch (ObjectDisposedException)
                {
                    return new CancellationToken(true);
                }
            }
        }

        public bool IsTerminated => terminationStatus != NotTerminated;

        public bool NormalTermination => terminationStatus == TerminatedNormally;

        volatile int terminationStatus = NotTerminated;
        const int NotTerminated = 0;
        const int TerminatedWithError = 1;
        const int TerminatedNormally = 2;

        public StorageErrorHandler(ILogger logger, LogLevel logLevelLimit, string storageAccountName, string taskHubName)
        {
            cts = new CancellationTokenSource();
            this.logger = logger;
            this.logLevelLimit = logLevelLimit;
            account = storageAccountName;
            taskHub = taskHubName;
            shutdownComplete = new TaskCompletionSource<object>();
        }

        public void HandleError(string context, string message, Exception exception, bool terminatePartition, bool isWarning)
        {
            TraceError(isWarning, context, message, exception, terminatePartition);

            // terminate this partition in response to the error

            if (terminatePartition && terminationStatus == NotTerminated)
            {
                if (Interlocked.CompareExchange(ref terminationStatus, TerminatedWithError, NotTerminated) == NotTerminated)
                {
                    Terminate();
                }
            }
        }

        public void TerminateNormally()
        {
            if (Interlocked.CompareExchange(ref terminationStatus, TerminatedNormally, NotTerminated) == NotTerminated)
            {
                Terminate();
            }
        }

        void TraceError(bool isWarning, string context, string message, Exception exception, bool terminatePartition)
        {
            var logLevel = isWarning ? LogLevel.Warning : LogLevel.Error;
            if (logLevelLimit <= logLevel)
            {
                // for warnings, do not print the entire exception message
                string details = exception == null ? string.Empty : (isWarning ? $"{exception.GetType().FullName}: {exception.Message}" : exception.ToString());

                logger?.Log(logLevel, "!!! {message} in {context}: {details} terminatePartition={terminatePartition}", message, context, details, terminatePartition);
            }
        }

        void Terminate()
        {
            try
            {
                logger?.LogDebug("Started PartitionCancellation");
                cts.Cancel();
                logger?.LogDebug("Completed PartitionCancellation");
            }
            catch (AggregateException aggregate)
            {
                foreach (var e in aggregate.InnerExceptions)
                {
                    HandleError("PartitionErrorHandler.Terminate", "Exception in PartitionCancellation", e, false, true);
                }
            }
            catch (Exception e)
            {
                HandleError("PartitionErrorHandler.Terminate", "Exception in PartitionCancellation", e, false, true);
            }

            // we use a dedicated shutdown thread to help debugging and to contain damage if there are hangs
            Thread shutdownThread = TrackedThreads.MakeTrackedThread(Shutdown, "PartitionShutdown");
            shutdownThread.Start();

            void Shutdown()
            {
                try
                {
                    logger?.LogDebug("Started PartitionShutdown");

                    OnShutdown?.Invoke();

                    cts.Dispose();

                    logger?.LogDebug("Completed PartitionShutdown");
                }
                catch (AggregateException aggregate)
                {
                    foreach (var e in aggregate.InnerExceptions)
                    {
                        HandleError("PartitionErrorHandler.Shutdown", "Exception in PartitionShutdown", e, false, true);
                    }
                }
                catch (Exception e)
                {
                    HandleError("PartitionErrorHandler.Shutdown", "Exception in PartitionShutdown", e, false, true);
                }

                shutdownComplete.TrySetResult(null);
            }
        }

        public async Task<bool> WaitForTermination(TimeSpan timeout)
        {
            Task timeoutTask = Task.Delay(timeout);
            var first = await Task.WhenAny(timeoutTask, shutdownComplete.Task);
            return first == shutdownComplete.Task;
        }
    }
}