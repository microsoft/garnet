// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Type-free base class for hybrid log memory allocator. Contains utility methods that do not need type args and are not performance-critical
    /// so can be virtual.
    /// </summary>
    public class LogSizeTracker
    {
        /// <summary>
        /// The number of seconds to timeout the wait on <see cref="LogSizeTracker{TStoreFunctions, TAllocator}.resizeTaskEvent"/>. Useful for ensuring that we don't 
        /// miss a check due to non-atomicity of updating size, determining it is beyond budget, and signaling the event.
        /// </summary>
        public static readonly int ResizeTaskDelaySeconds = 10;

        /// <summary>Target size must be at least this many pages; this gives us (at least a little) room for heap allocations in a minimum of
        /// <see cref="MinResizeTargetPageCount"/> pages.</summary>
        public const int MinTargetPageCount = MinResizeTargetPageCount * 2;

        /// <summary>When resizing we must preserve at least this many pages</summary>
        public const int MinResizeTargetPageCount = 2;
    }

    /// <summary>Tracks and controls size of log</summary>
    /// <typeparam name="TStoreFunctions"></typeparam>
    /// <typeparam name="TAllocator"></typeparam>
    public sealed class LogSizeTracker<TStoreFunctions, TAllocator> : LogSizeTracker, IObserver<ITsavoriteScanIterator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// The event to be signaled when an <see cref="UpdateSize{TSourceLogRecord}(in TSourceLogRecord, bool)"/> call detects we're over budget.
        /// </summary>
        private CompletionEvent resizeTaskEvent;

        /// <summary>The current heap size of the log</summary>
        private ConcurrentCounter heapSize;

        private readonly ILogger logger;

        /// <summary>Memory usage at which to trigger trimming</summary>
        private long highTargetSize;
        /// <summary>Memory usage at which to stop trimming once started</summary>
        private long lowTargetSize;

        internal LogAccessor<TStoreFunctions, TAllocator> logAccessor;

        /// <summary>Indicates whether resizer task has been stopped</summary>
        enum RunState : int { NotStarted, Running, StopRequested, Stopped };
        /// <summary>The integer value of the current <see cref="RunState"/>, for Interlocked operations. Indicates whether resizer task has been stopped</summary>
        volatile int runState;

        /// <summary>Indicates whether resizer task has been stopped</summary>
        public bool IsStopped => runState == (int)RunState.Stopped;

        /// <summary>
        /// Callback for when we have trimmed memory, such as by shifting headAddress to close records and/or evicting pages.
        /// Passes the current number of allocated log pages and the headAddress.
        /// </summary>
        /// <remarks>Currently used for tests.</remarks>
        internal Action<int, long> PostMemoryTrim { get; set; } = (allocatedPageCount, headAddress) => { };

        /// <summary>Total size occupied by log, including heap</summary>
        public long TotalSize => logAccessor.MemorySizeBytes + heapSize.Total;

        /// <summary>Size of log heap memory only</summary>
        public long LogHeapSizeBytes => heapSize.Total;

        /// <summary>Target size for the hybrid log memory utilization</summary>
        public long TargetSize { get; private set; }

        /// <summary>High and low deltas for <see cref="TargetSize"/></summary>
        public (long high, long low) TargetDeltaRange => (highTargetSize, lowTargetSize);

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"{runState}; TargetSize: [{TargetSize}, hi: {highTargetSize}, lo: {lowTargetSize}]; TotalSize: [{TotalSize}, Heap: {heapSize.Total}];"
                 + $" isOver: [{IsBeyondSizeLimit}, canEvict {IsBeyondSizeLimitAndCanEvict}]; AllocPgCt: {logAccessor.AllocatedPageCount}; PgSize {logAccessor.allocatorBase.PageSize}";
        }

        /// <summary>Return true if the total size is outside the target plus delta</summary>
        public bool IsBeyondSizeLimit => TotalSize > highTargetSize;

        /// <summary>Return true if the total size is outside the target plus delta *and* we have pages we can (partially or completely) evict</summary>
        /// <param name="addingPage">If true, we are allocating a new page. Otherwise, we are called when adding or growing a new <see cref="IHeapObject"/></param>
        /// <remarks>This should be used only for non-Recovery, because Recovery does not set up HeadAddress and TailAddress before this is called.</remarks>
        public bool IsBeyondSizeLimitAndCanEvict(bool addingPage = false)
        {
            var headPage = logAccessor.allocatorBase.GetPage(logAccessor.allocatorBase.HeadAddress);
            var tailPage = logAccessor.allocatorBase.GetPage(logAccessor.allocatorBase.UnstableGetTailAddress());

            // The number of pages we have is untilPage - headPage + 1. If we're called here when allocating a new page, see if the new page
            // would put us over the maximum count.
            var numPages = (int)(tailPage - headPage + 1);
            if (addingPage && numPages == logAccessor.allocatorBase.MaxAllocatedPageCount)
                return true;

            // Otherwise, we need at least MinResizeTargetPageCount to be able to evict anything.
            return (TotalSize > highTargetSize) && numPages > MinResizeTargetPageCount;
        }

        /// <summary>Return true if the total size plus the size needed for the requested number of pages to read is outside the target plus delta *and*
        /// we have pages we can (partially or completely) evict</summary>
        /// <remarks>This is called by Recovery.</remarks>
        public bool IsBeyondSizeLimitToReadPages(int numPagesToRead) => TotalSize + (numPagesToRead * logAccessor.allocatorBase.PageSize) > highTargetSize;

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
            heapSize = new ConcurrentCounter();
            resizeTaskEvent = new();
            this.logger = logger;
            runState = (int)RunState.NotStarted;
            UpdateTargetSize(targetSize, highDelta, lowDelta);
        }

        /// <summary>Starts the log size tracker</summary>
        /// <remarks>NOTE: Not thread safe to start multiple times</remarks>
        /// <param name="cancellationToken"></param>
        public void Start(CancellationToken cancellationToken)
        {
            Debug.Assert(runState == (int)RunState.NotStarted, "Cannot restart LogSizeTracker");
            resizeTaskEvent.Initialize();
            runState = (int)RunState.Running;
            _ = Task.Run(() => ResizerTask(cancellationToken), cancellationToken);
        }

        /// <summary>Stop the resizer task</summary>
        public void Stop(bool wait = false)
        {
            var prevState = Interlocked.CompareExchange(ref runState, (int)RunState.StopRequested, (int)RunState.Running);
            if (prevState == (int)RunState.Running)
            {
                // This Set() will wake up the task and it will detect StopRequested and call OnStopped().
                resizeTaskEvent.Set();
                while (wait && !IsStopped)
                    _ = Thread.Yield();
            }
        }

        void OnStopped()
        {
            _ = Interlocked.Exchange(ref runState, (int)RunState.Stopped);
            resizeTaskEvent.Dispose();
            resizeTaskEvent = default;
        }

        /// <summary>
        /// Update target size for the hybrid log memory utilization
        /// </summary>
        /// <param name="newTargetSize">The target size</param>
        /// <param name="highDelta">Delta above the target size at which to trigger trimming</param>
        /// <param name="lowDelta">Delta below the target size at which to stop trimming once started</param>
        public void UpdateTargetSize(long newTargetSize, long highDelta, long lowDelta)
        {
            Debug.Assert(highDelta >= 0);
            Debug.Assert(lowDelta >= 0);
            Debug.Assert(newTargetSize > highDelta);
            Debug.Assert(newTargetSize > lowDelta);

            if (newTargetSize < logAccessor.allocatorBase.PageSize * MinTargetPageCount)
                throw new TsavoriteException($"Target size must be at least {MinTargetPageCount} pages");

            var shrink = newTargetSize < TargetSize;
            TargetSize = newTargetSize;
            highTargetSize = newTargetSize + highDelta;
            lowTargetSize = newTargetSize - lowDelta;
            logger?.LogInformation("Target size updated to {targetSize} with highDelta {highDelta}, lowDelta {lowDelta}", newTargetSize, highDelta, lowDelta);
            
            // Only signal if we are shrinking; growth is handled normally as we add pages and records.
            if (shrink)
                resizeTaskEvent.Set();
        }

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
                heapSize.Increment(-size); // Reduce size as records are being evicted
        }

        /// <summary>Adds size to the tracked total count</summary>
        public void IncrementSize(long size)
        {
            if (size != 0)
            {
                heapSize.Increment(size);
                if (size > 0 && IsBeyondSizeLimitAndCanEvict())
                    resizeTaskEvent.Set();
                Debug.Assert(size > 0 || heapSize.Total >= 0, $"HeapSize.Total should be >= 0 but is {heapSize.Total} in Resize");
            }
        }

        /// <summary>Adds the <see cref="LogRecord"/> size to the tracked total count.</summary>
        public void UpdateSize<TSourceLogRecord>(in TSourceLogRecord logRecord, bool add)
            where TSourceLogRecord : ISourceLogRecord
        {
            var size = MemoryUtils.CalculateHeapMemorySize(in logRecord);
            if (size != 0)
            {
                if (add)
                {
                    heapSize.Increment(size);
                    if (IsBeyondSizeLimitAndCanEvict())
                        resizeTaskEvent.Set();
                }
                else
                {
                    // Nothing needed if we are decreasing.
                    heapSize.Increment(-size);
                    Debug.Assert(heapSize.Total >= 0, $"HeapSize.Total should be >= 0 but is {heapSize.Total} in UpdateSize");
                }
            }
        }

        /// <summary>Called when the caller has determined we are over budget, to signal the event.</summary>
        public void Signal() => resizeTaskEvent.Set();

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
                    // Note: CompletionEvent functions as an AutoResetEvent, so any signals that arrive between 
                    // these calls to WaitAsync will be lost. ResizeIfNeeded retries as long as we are over budget, 
                    // but there is still a chance we'll miss a growth+signal between that check and the next WaitAsync.
                    // The timeout mitigates this but it would be better to find an awaitable ManualResetEvent.
                    await resizeTaskEvent.WaitAsync(TimeSpan.FromSeconds(ResizeTaskDelaySeconds), cancellationToken);
                    if (runState == (int)RunState.Running)
                        ResizeIfNeeded(cancellationToken);
                    if (runState != (int)RunState.Running)
                    {
                        OnStopped();
                        return;
                    }
                }
                catch (OperationCanceledException)
                {
                    logger?.LogTrace("Log resize task has been cancelled.");
                    OnStopped();
                    return;
                }
                catch (Exception e)
                {
                    logger?.LogWarning(e, "Exception when attempting to perform memory resizing.");
                }
            }
        }

        private bool DetermineEvictionRange(long currentSize, CancellationToken cancellationToken, out long headAddress,
            ref int allocatedPageCount, out long heapTrimmedSize)
        {
            // We know we are oversize so we calculate how much we need to trim to get to lowTargetSize.
            var overSize = currentSize - lowTargetSize;
            heapTrimmedSize = 0L;

            var allocator = logAccessor.allocatorBase;
            headAddress = allocator.HeadAddress;
            var headPage = allocator.GetPage(headAddress);
            var untilAddress = allocator.UnstableGetTailAddress();
            var untilPage = allocator.GetPage(untilAddress);

            // The number of pages we have is untilPage - headPage + 1.
            if (untilPage - headPage + 1 <= MinResizeTargetPageCount)
                return false;
            untilAddress = allocator.GetLogicalAddressOfStartOfPage(untilPage - MinResizeTargetPageCount + 1);

            // If there is nothing to trim from the heap, we can just do math to advance HA.
            if (heapSize.Total == 0)
            {
                var evictableSize = untilAddress - headAddress;
                var isComplete = overSize <= evictableSize;
                if (!isComplete)
                    overSize = evictableSize;
                headAddress += overSize;
                allocatedPageCount -= (int)(allocator.GetPage(headAddress) - headPage);
                return isComplete;
            }

            // This will iterate until iterator.CurrentAddress == untilAddress
            using var iterator = logAccessor.Scan(headAddress, untilAddress);
            allocatedPageCount = allocator.AllocatedPageCount;
            var pageTrimmedSize = 0L;
            while (heapTrimmedSize + pageTrimmedSize < overSize && iterator.GetNext() && !IsStopped)
            {
                cancellationToken.ThrowIfCancellationRequested();
                heapTrimmedSize += iterator.CalculateHeapMemorySize();

                // If we've crossed a page boundary, we can subtract the pagesize as well.
                var currentPage = allocator.GetPage(iterator.CurrentAddress);
                if (currentPage > headPage)
                {
                    headPage = currentPage;
                    --allocatedPageCount;
                    pageTrimmedSize += allocator.PageSize;
                }
            }

            // iterator.NextAddress is the end of the last-processed record; if we did not advance far enough to clear all the oversize space
            // it is the start of the next record we would have processed (and probably equal to untilAddress). In both cases it is how far we
            // can evict to, and because it is the next address we've not yet evaluated whether it's crossed the page boundary; do that here.
            headAddress = iterator.NextAddress;

            // Return whether we could satisfy the resize request; for Recovery, we may need to wait on flush.
            return heapTrimmedSize + pageTrimmedSize >= overSize;
        }

        /// <summary>
        /// Adjusts the log size to maintain its size within the range of highTargetSize and lowTargetSize.
        /// </summary>
        /// <returns>True if resize not needed or was complete, else false (need to wait for evictions, possible with flushes before that)</returns>
        private void ResizeIfNeeded(CancellationToken cancellationToken)
        {
            // Loop to decrease size. These variables retain the values they acquired during the last loop iteration.
            var currentSize = TotalSize;
            if (currentSize <= highTargetSize)
                return;

            long headAddress, heapTrimmedSize, readOnlyAddress;
            var isComplete = false;
            var allocatedPageCount = logAccessor.AllocatedPageCount;
            logger?.LogDebug("Heap size {totalLogSize} > target {highTargetSize}. Alloc: {AllocatedPageCount} BufferSize: {BufferSize}", heapSize.Total, highTargetSize, allocatedPageCount, logAccessor.BufferSize);

            // Acquire the epoch long enough to calculate eviction ranges.
            logAccessor.allocatorBase.epoch.Resume();
            try
            {
                // See how much we can evict between ReadOnlyAddress and HeadAddress. Ignore the return value that indicates whether this is complete;
                // we calculate the new ROA up to MinTargetPageCount pages before TailAddress, and that's as far as we can go.
                isComplete = DetermineEvictionRange(currentSize, cancellationToken, out headAddress, ref allocatedPageCount, out heapTrimmedSize);
                if (runState != (int)RunState.Running)
                    return;

                // Calculate new ReadOnlyAddress; if it hasn't changed then the ShiftReadOnlyAddress in logAccessor.ShiftHeadAddress will do nothing. 
                readOnlyAddress = logAccessor.allocatorBase.CalculateReadOnlyAddress(logAccessor.TailAddress, headAddress);
            }
            finally
            {
                logAccessor.allocatorBase.epoch.Suspend();
            }

            // Release the epoch because ShiftAddresses will wait for the ROA shift to complete so we don't evict pages before they're flushed.
            // Wait until the SHA has completed eviction to avoid going further over budget.
            logAccessor.ShiftAddresses(readOnlyAddress, headAddress, waitForEviction: true);

            // Now subtract what we were able to trim from heapSize. Inline page total size is tracked separately in logAccessor.MemorySizeBytes.
            heapSize.Increment(-heapTrimmedSize);
            Debug.Assert(heapSize.Total >= 0, $"HeapSize.Total should be >= 0 but is {heapSize.Total} in Resize");

            // Calculate the number of trimmed pages and report the new expected AllocatedPageCount here, since our last iteration (which may have been the only one)
            // would have returned isComplete and thus we didn't wait for the actual eviction.
            PostMemoryTrim(allocatedPageCount, headAddress);
            logger?.LogDebug("Decreased Allocated page count to {allocatedPageCount} and HeadAddress to {headAddress}; isComplete {isComplete}", allocatedPageCount, headAddress, isComplete);
        }
    }
}