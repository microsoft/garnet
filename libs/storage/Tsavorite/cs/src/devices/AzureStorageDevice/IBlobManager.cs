// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.devices
{
    /// <summary>
    /// Blob manager interface
    /// </summary>
    public interface IBlobManager
    {
        /// <summary>
        /// Stop tasks associated with blob manager
        /// </summary>
        Task StopAsync();

        /// <summary>
        /// Storage error handler
        /// </summary>
        IStorageErrorHandler StorageErrorHandler { get; }

        /// <summary>
        /// Storage tracer
        /// </summary>
        TsavoriteTraceHelper StorageTracer { get; }

        /// <summary>
        /// Handle storage error
        /// </summary>
        void HandleStorageError(string where, string message, string blobName, Exception e, bool isFatal, bool isWarning);

        /// <summary>
        /// Read concurrency
        /// </summary>
        SemaphoreSlim AsynchronousStorageReadMaxConcurrency { get; }

        /// <summary>
        /// Write concurrency
        /// </summary>
        SemaphoreSlim AsynchronousStorageWriteMaxConcurrency { get; }

        /// <summary>
        /// Perform operation with retries
        /// </summary>
        Task PerformWithRetriesAsync(
            SemaphoreSlim semaphore,
            bool requireLease,
            string name,
            string intent,
            string data,
            string target,
            int expectedLatencyBound,
            bool isCritical,
            Func<int, Task<long>> operationAsync,
            Func<Task> readETagAsync = null);

        /// <summary>
        /// Confirm lease is good for a while
        /// </summary>
        ValueTask ConfirmLeaseIsGoodForAWhileAsync();
    }
}