// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>Tracks and controls size of log</summary>
    /// <typeparam name="TStoreFunctions"></typeparam>
    /// <typeparam name="TAllocator"></typeparam>
    public class LogSizeTracker<TStoreFunctions, TAllocator> : IObserver<ITsavoriteScanIterator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// The number of seconds to timeout the wait on <see cref="resizeTaskEvent"/>. Useful for ensuring that we don't 
        /// miss a check due to non-atomicity of updating size, determining it is beyond budget, and signaling the event.
        /// </summary>
        public static readonly int ResizeTaskDelaySeconds = 10;

        /// <summary>
        /// The event to be signaled when an <see cref="UpdateSize{TSourceLogRecord}(in TSourceLogRecord, bool)"/> call detects we're over budget.
        /// </summary>
        private CompletionEvent resizeTaskEvent;

        /// <summary>The current size of the log (inline + heap)</summary>
        private ConcurrentCounter logSize;

        private readonly ILogger logger;

        /// <summary>Memory usage at which to trigger trimming</summary>
        private long highTargetSize;
        /// <summary>Memory usage at which to stop trimming once started</summary>
        private long lowTargetSize;

        internal LogAccessor<TStoreFunctions, TAllocator> logAccessor;

        /// <summary>Indicates whether resizer task has been stopped</summary>
        public volatile bool Stopped;

        /// <summary>
        /// Callback for when we have trimmed memory, such as by shifting headAddress to close records and/or evicting pages.
        /// Passes the current number of allocated log pages and the headAddress.
        /// </summary>
        /// <remarks>Currently used for tests.</remarks>
        internal Action<int, long> PostMemoryTrim { get; set; } = (allocatedPageCount, headAddress) => { };

        /// <summary>Total size occupied by log, including heap</summary>
        public long TotalSizeBytes => logAccessor.MemorySizeBytes + logSize.Total;

        /// <summary>Size of log heap memory</summary>
        public long LogHeapSizeBytes => logSize.Total;

        /// <summary>Target size for the hybrid log memory utilization</summary>
        public long TargetSize { get; set; }

        /// <summary>Creates a new log size tracker</summary>
        /// <param name="logAccessor">Hybrid log accessor</param>
        /// <param name="targetSize">Target size for the hybrid log memory utilization</param>
        /// <param name="highDelta">Delta above the target size at which to trigger hybrid log memory usage trimming</param>
        /// <param name="lowDelta">Delta below the target size at which to stop trimming hybrid log memory usage once started</param>
        /// <param name="logger"></param>
        public LogSizeTracker(LogAccessor<TStoreFunctions, TAllocator> logAccessor, long targetSize, long highDelta, long lowDelta, ILogger logger)
        {
            Debug.Assert(logAccessor != null);

            this.logAccessor = logAccessor;
            logSize = new ConcurrentCounter();
            UpdateTargetSize(targetSize, highDelta, lowDelta);
            this.logger = logger;
            Stopped = false;
        }

        /// <summary>
        /// Starts the log size tracker
        /// NOTE: Not thread safe to start multiple times
        /// </summary>
        /// <param name="cancellationToken"></param>
        public void Start(CancellationToken cancellationToken)
        {
            Debug.Assert(Stopped == false);
            _ = Task.Run(() => ResizerTask(cancellationToken), cancellationToken);
        }

        /// <summary>
        /// Update target size for the hybrid log memory utilization
        /// </summary>
        /// <param name="targetSize">The target size</param>
        /// <param name="highDelta">Delta above the target size at which to trigger trimming</param>
        /// <param name="lowDelta">Delta below the target size at which to stop trimming once started</param>
        public void UpdateTargetSize(long targetSize, long highDelta, long lowDelta)
        {
            Debug.Assert(highDelta >= 0);
            Debug.Assert(lowDelta >= 0);
            Debug.Assert(targetSize > highDelta);
            Debug.Assert(targetSize > lowDelta);

            TargetSize = targetSize;
            highTargetSize = targetSize + highDelta;
            lowTargetSize = targetSize - lowDelta;
            logger?.LogInformation("Target size updated to {targetSize} with highDelta {highDelta}, lowDelta {lowDelta}", targetSize, highDelta, lowDelta);

            resizeTaskEvent = new();
        }

        /// <summary>Return true if the total size is outside the target plus delta</summary>
        public bool IsSizeBeyondLimit => TotalSizeBytes > highTargetSize;

        /// <summary>Callback on allocator completion</summary>
        public void OnCompleted() { }

        /// <summary>Callback on allocator error</summary>
        public void OnError(Exception error) { }

        /// <summary>Callback on allocator evicting a page to disk</summary>
        public void OnNext(ITsavoriteScanIterator recordIter)
        {
            long size = 0;
            while (recordIter.GetNext())
                size += MemoryUtils.CalculateHeapMemorySize(in recordIter);

            if (size != 0)
                logSize.Increment(-size); // Reduce size as records are being evicted
        }

        /// <summary>Adds size to the tracked total count</summary>
        public void IncrementSize(long size) => logSize.Increment(size);

        /// <summary>Adds the <see cref="LogRecord"/> size to the tracked total count.</summary>
        public void UpdateSize<TSourceLogRecord>(in TSourceLogRecord logRecord, bool add)
            where TSourceLogRecord : ISourceLogRecord
        {
            var size = MemoryUtils.CalculateHeapMemorySize(in logRecord);
            if (size != 0)
            {
                if (add)
                {
                    logSize.Increment(size);
                    if (IsSizeBeyondLimit)
                        resizeTaskEvent.Set();
                }
                else
                {
                    // Nothing needed if we are decreasing.
                    logSize.Increment(-size);
                }
            }
        }

        /// <summary>
        /// Performs resizing by waiting for an event that is signaled whenever memory utilization changes.
        /// This is invoked on the threadpool to avoid blocking calling threads during the resize operation.
        /// </summary>
        async Task ResizerTask(CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    await resizeTaskEvent.WaitAsync(TimeSpan.FromSeconds(ResizeTaskDelaySeconds), cancellationToken);
                    _ = ResizeIfNeeded(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    logger?.LogTrace("Log resize task has been cancelled.");
                    Stopped = true;
                    return;
                }
                catch (Exception e)
                {
                    logger?.LogWarning(e, "Exception when attempting to perform memory resizing.");
                }
            }
        }

        private bool DetermineEvictionRange(long fromAddress, long untilAddress, long currentSize, long requiredSize, CancellationToken cancellationToken,
            out int allocatedPageCount, out int evictPageCount, out long trimmedSize, out bool isPartialLastPage)
        {
            var overSize = currentSize - lowTargetSize + requiredSize;
            trimmedSize = 0L;

            var allocator = logAccessor.allocatorBase;
            var fromPage = allocator.GetPage(fromAddress);

            var iterator = logAccessor.Scan(fromAddress, untilAddress);
            allocatedPageCount = allocator.AllocatedPageCount;
            evictPageCount = 0;
            long lastPageTrimmedSize = 0;
            while (trimmedSize < overSize && iterator.GetNext())
            {
                cancellationToken.ThrowIfCancellationRequested();
                trimmedSize += iterator.CalculateHeapMemorySize();

                // If we've crossed a page boundary, we can subtract the pagesize as well.
                var currentPage = allocator.GetPage(iterator.CurrentAddress);
                if (currentPage > fromPage)
                {
                    trimmedSize += allocator.PageSize;
                    fromPage = currentPage;
                    ++evictPageCount;
                    lastPageTrimmedSize = trimmedSize;
                }
            }

            isPartialLastPage = trimmedSize > lastPageTrimmedSize;

            // Return whether we could satisfy the resize request; for Recovery, we may need to wait on flush.
            return trimmedSize >= overSize;
        }

        /// <summary>
        /// Adjusts the log size to maintain its size within the range of highTargetSize and lowTargetSize.
        /// </summary>
        /// <returns>True if resize not needed or was complete, else false (need to wait for evictions, possible with flushes before that)</returns>
        private bool ResizeIfNeeded(CancellationToken cancellationToken)
        {
            // Monitor the total size
            var currentSize = logSize.Total;
            if (currentSize <= highTargetSize)
                return true;

            logger?.LogDebug("Heap size {totalLogSize} > target {highTargetSize}. Alloc: {AllocatedPageCount} EPC: {EmptyPageCount}", logSize.Total, highTargetSize, logAccessor.AllocatedPageCount, logAccessor.BufferSize - logAccessor.AllocatedPageCount);

            var fromAddress = logAccessor.HeadAddress;
            var untilAddress = logAccessor.ReadOnlyAddress;

            var isComplete = DetermineEvictionRange(fromAddress, untilAddress, currentSize, requiredSize: 0, cancellationToken,
                out var allocatedPageCount, out var evictPageCount, out var trimmedSize, out _ /*isPartialLastPage*/);

            // We don't wait for the HeadAddress movement to finish closing; if we stopped while still overbudget due to hitting ReadOnlyAddress,
            // then TryAllocate's NeedToWait() check will force a retry while we wait for flushes and evictions to complete.
            // If readOnlyAddress hasn't changed, the ShiftReadOnlyAddress in logAccessor.ShiftHeadAddress will not do anything. 
            var readOnlyAddress = logAccessor.allocatorBase.CalculateReadOnlyAddress(untilAddress, fromAddress);
            logAccessor.ShiftAddresses(readOnlyAddress, fromAddress, wait: false);

            // Now subtract what we were able to trim from logSize.
            logSize.Increment(-trimmedSize);

            // Calculate the number of trimmed pages and report the new expected AllocatedPageCount here, since we don't wait for the actual eviction.
            PostMemoryTrim(allocatedPageCount - evictPageCount, fromAddress);
            logger?.LogDebug("Decreased Allocated page count to {allocatedPageCount} and HeadAddress to {headAddress}", allocatedPageCount - evictPageCount, fromAddress);
            return isComplete;
        }

        /// <summary>
        /// Adjusts the log size to maintain its size within the range of highTargetSize and lowTargetSize.
        /// </summary>
        internal bool ResizeForRecoveryIfNeeded(long fromAddress, long untilAddress, long requiredSize, CancellationToken cancellationToken, out int evictPageCount)
        {
            // Monitor the total size. requiredSize indicates what we know we are about to use, such as loading pages.
            var currentSize = logSize.Total;
            if (currentSize + requiredSize <= highTargetSize)
            {
                evictPageCount = 0;
                return true;
            }

            var isComplete = DetermineEvictionRange(fromAddress, untilAddress, currentSize, requiredSize, cancellationToken,
                out _ /*allocatedPageCount*/, out evictPageCount, out var trimmedSize, out var isPartialLastPage);

            // For recovery we want to evict the page if any part of it went oversize.
            if (isPartialLastPage)
                evictPageCount++;
 
            // Now subtract what we were able to trim from logSize.
            logSize.Increment(-trimmedSize);
            return isComplete;
        }
    }
}