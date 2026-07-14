// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using static Tsavorite.core.Utility;

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
        /// <see cref="LogSettings.kMinPageCount"/> pages.</summary>
        public const int MinTargetPageCount = LogSettings.kMinPageCount * 2;

        /// <summary>
        /// When evicting, do not allow HeadAddress to advance to within this many bytes of TailAddress. This usually allows more than one usable record in
        /// the database. If there are records with objects in that range that exceed the memory budget, then the memory budget should be adjusted to allow for it.
        /// </summary>
        public const int MinEvictionHeadAddressLag = 4096;
    }

    /// <summary>Tracks and controls size of log</summary>
    /// <typeparam name="TStoreFunctions"></typeparam>
    /// <typeparam name="TAllocator"></typeparam>
    public sealed class LogSizeTracker<TStoreFunctions, TAllocator> : LogSizeTracker
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// The event to be signaled when an <see cref="UpdateSize{TSourceLogRecord}(in TSourceLogRecord, bool)"/> call detects we're over budget.
        /// </summary>
        private CompletionEvent resizeTaskEvent;

        /// <summary>The running resizer task, retained so <see cref="Stop"/> can observe its completion.</summary>
        private volatile Task resizerTask;

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
        /// Indicates whether the background resizer task is currently running (started and not yet stop-requested/stopped).
        /// Callers on the allocation path use this to decide whether they must evict synchronously themselves (when the
        /// resizer is not running, e.g. during recovery/AOF replay) instead of deferring eviction to the resizer.
        /// </summary>
        public bool IsRunning => runState == (int)RunState.Running;

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
                 + $" isOver: [{IsOverBudget}, canEvict {IsBeyondSizeLimitAndCanEvict}]; AllocPgCt: {logAccessor.AllocatedPageCount}; PgSize {logAccessor.allocatorBase.PageSize}";
        }

        /// <summary>Returns the memory budget we have remaining</summary>
        /// <remarks>May return a negative value if already over budget.</remarks>
        public long RemainingBudget => highTargetSize - TotalSize;

        /// <summary>Return true if the total size is outside the target plus delta</summary>
        public bool IsOverBudget => TotalSize > highTargetSize;

        /// <summary>Return true if the total size is outside the target plus delta *and* we have pages we can (partially or completely) evict</summary>
        /// <param name="addingPage">If true, we are allocating a new page. Otherwise, we are called when adding or growing a new <see cref="IHeapObject"/></param>
        /// <remarks>This should be used only for non-Recovery, because Recovery does not set up HeadAddress and TailAddress before this is called.</remarks>
        public bool IsBeyondSizeLimitAndCanEvict(bool addingPage = false)
        {
            var headPage = logAccessor.allocatorBase.GetPage(logAccessor.allocatorBase.HeadAddress);
            var tailPage = logAccessor.allocatorBase.GetPage(logAccessor.allocatorBase.UnstableGetTailAddress(out _));

            // The number of pages we have is untilPage - headPage + 1. If we're called here when allocating a new page, see if the new page
            // would put us over the maximum count.
            var numPages = (int)(tailPage - headPage + 1);
            if (addingPage && numPages == logAccessor.allocatorBase.MaxAllocatedPageCount)
                return true;

            // Otherwise, we need at least MinEvictionHeadAddressLag to be able to evict anything. Use UnstableGetTailAddress (as above): this is
            // reached from HandlePageOverflow on the thread that owns tail-address stabilization, and the stable GetTailAddress() would spin-wait
            // forever for a TailPageOffset that only this same thread can reset (after NeedToWaitForClose returns).
            return (TotalSize > highTargetSize) && logAccessor.allocatorBase.UnstableGetTailAddress(out _) - logAccessor.allocatorBase.HeadAddress >= MinEvictionHeadAddressLag;
        }

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
            // Do NOT pass cancellationToken as Task.Run's second argument. If the token is already canceled when the queued
            // task is dequeued (e.g. ctsCommit was cancelled in StoreWrapper.Dispose before the resizer got a thread), Task.Run
            // transitions the task to Canceled WITHOUT ever running its body, so OnStopped() would never run and a Stop(wait: true)
            // caller would spin forever. The body observes cancellation itself via the awaited WaitAsync and always reaches OnStopped().
            resizerTask = Task.Run(() => ResizerTask(cancellationToken));
        }

        /// <summary>Stop the resizer task</summary>
        public void Stop(bool wait = false)
        {
            var prevState = Interlocked.CompareExchange(ref runState, (int)RunState.StopRequested, (int)RunState.Running);
            if (prevState == (int)RunState.Running)
            {
                // This Set() will wake up the task and it will detect StopRequested and call OnStopped().
                resizeTaskEvent.Set();
            }

            if (wait && (prevState == (int)RunState.Running || prevState == (int)RunState.StopRequested))
            {
                // Wait until the resizer has stopped. Also break if the task itself has completed for any reason: defense in
                // depth so a caller passing wait: true can never spin forever even if OnStopped() somehow did not run.
                while (!IsStopped && !(resizerTask is { IsCompleted: true }))
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
                    await resizeTaskEvent.WaitAsync(TimeSpan.FromSeconds(ResizeTaskDelaySeconds), cancellationToken).ConfigureAwait(false);
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
            ref int allocatedPageCount, out long estimatedHeapTrimmedSize)
        {
            // We know we are oversize so we calculate how much we need to trim to get to lowTargetSize.
            var overBudgetAmount = currentSize - lowTargetSize;
            estimatedHeapTrimmedSize = 0L;

            var allocator = logAccessor.allocatorBase;
            headAddress = allocator.HeadAddress;
            var startingHeadPage = allocator.GetPage(headAddress);
            var maxEvictUntilAddress = allocator.UnstableGetTailAddress(out _) - MinEvictionHeadAddressLag;
            var maxEvictUntilPage = allocator.GetPage(maxEvictUntilAddress);

            // If there is nothing to trim from the heap, we just do math to trim as many pages as we need to (up to the limit).
            if (heapSize.Total == 0)
            {
                // We are evicting in units of pages, so we set this to the start of the maxEvictUntilPage.
                maxEvictUntilAddress = allocator.GetLogicalAddressOfStartOfPage(maxEvictUntilPage);
                var evictableSize = maxEvictUntilAddress - headAddress;

                // evictableSize is the resident span [headAddress, tail-aligned). When heapSize is 0, TotalSize == AllocatedPageCount * PageSize, so being
                // over budget here means AllocatedPageCount * PageSize > budget; recovery keeps AllocatedPageCount within MaxAllocatedPageCount (the read
                // batch is capped at the budget and a final trim evicts any object-free overage), so AllocatedPageCount ~= the resident page count and that
                // resident span must itself exceed the budget => evictableSize > 0. A negative value would mean AllocatedPageCount exceeds the resident set
                // (stale pages left allocated below headAddress), which we must not reach.
                Debug.Assert(evictableSize >= 0, $"evictableSize ({evictableSize}) must be non-negative; AllocatedPageCount exceeds the resident set below headAddress.");

                var margin = evictableSize - overBudgetAmount;
                var isComplete = margin > 0;
                if (isComplete)
                {
                    // We can completely satisfy the over-budget amount, so we can add some pages back to keep more below maxEvictUntilPage.
                    var additionalPagesToKeep = margin / allocator.PageSize;
                    maxEvictUntilPage -= additionalPagesToKeep;
                }

                // We'll evict the maxEvictUntilPage so start at the first valid logical address on the next page.
                headAddress = allocator.GetFirstValidLogicalAddressOnPage(maxEvictUntilPage);

                allocatedPageCount -= (int)(maxEvictUntilPage - startingHeadPage);
                return isComplete;
            }

            // We have heap objects we can potentially evict. This will iterate until iterator.CurrentAddress == untilAddress.
            // To optimize performance, iterate pages and skip the whole page if objectIdMap.IsEmpty, else enumerate records on the page.
            var pageTrimmedSize = 0L;
            var lastEvictPage = allocator.GetPage(maxEvictUntilAddress);
            for (var currentPage = startingHeadPage; currentPage <= lastEvictPage && estimatedHeapTrimmedSize + pageTrimmedSize < overBudgetAmount && !IsStopped; currentPage++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (currentPage != startingHeadPage)
                    headAddress = allocator.GetFirstValidLogicalAddressOnPage(currentPage);

                // If there are no objects on this page and it's below maxEvictUntilPage (which may not be able to be evicted fully),
                // we can skip the whole page and just subtract the pagesize from the amount we need to trim.
                if (currentPage < maxEvictUntilPage)
                {
                    var oidMap = allocator._wrapper.GetPageObjectIdMap(currentPage);
                    if (oidMap is null || oidMap.Count == 0)
                    {
                        pageTrimmedSize += allocator.PageSize;
                        if (estimatedHeapTrimmedSize + pageTrimmedSize >= overBudgetAmount)
                        {
                            // Set headAddress to the start of the next page and we're done.
                            headAddress = allocator.GetFirstValidLogicalAddressOnPage(currentPage + 1);
                            break;
                        }
                        continue;
                    }
                }

                // We have objects, so iterate records to see where the new headAddress must be. Don't go past maxEvictUntilAddress.
                var endAddress = allocator.GetLogicalAddressOfStartOfPage(currentPage + 1);
                if (endAddress > maxEvictUntilAddress)
                    endAddress = maxEvictUntilAddress;
                while (headAddress < endAddress)
                {
                    var logRecord = allocator._wrapper.CreateLogRecord(headAddress);
                    var allocatedSize = logRecord.AllocatedSize;
                    if (allocatedSize <= 0)
                        ThrowTsavoriteException($"LogRecord size should be > 0; encountered {allocatedSize}");

                    headAddress += allocatedSize;
                    if (!logRecord.Info.Valid)
                        continue;

                    estimatedHeapTrimmedSize += logRecord.CalculateHeapMemorySize();
                    if (estimatedHeapTrimmedSize + pageTrimmedSize >= overBudgetAmount)
                        break;
                }

                // If we have finished a page, add its size to our eviction total and set headAddress to the start of the next page.
                if (headAddress >= endAddress)
                {
                    pageTrimmedSize += allocator.PageSize;
                    headAddress = allocator.GetFirstValidLogicalAddressOnPage(currentPage + 1);
                }

                if (estimatedHeapTrimmedSize + pageTrimmedSize >= overBudgetAmount)
                    break;
            }

            // headAddress is now properly set. Return whether we could satisfy the resize request; for Recovery, we may need to wait on flush.
            return estimatedHeapTrimmedSize + pageTrimmedSize >= overBudgetAmount;
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

            long headAddress, estimatedHeapTrimmedSize, readOnlyAddress;
            var isComplete = false;
            int allocatedPageCount;

            // Acquire the epoch long enough to calculate eviction ranges.
            logAccessor.allocatorBase.epoch.Resume();
            try
            {
                // AllocatedPageCount is set here, after we've resumed the epoch (which may have done eviction).
                allocatedPageCount = logAccessor.AllocatedPageCount;
                logger?.LogDebug("Heap size {totalLogSize} > target {highTargetSize}. Alloc: {AllocatedPageCount} BufferSize: {BufferSize}", heapSize.Total, highTargetSize, allocatedPageCount, logAccessor.BufferSize);

                // See how much we can evict from HeadAddress onwards. Ignore the return value that indicates whether this is complete;
                // we calculate the new ROA up to MinTargetPageCount pages before TailAddress, and that's as far as we can go.
                isComplete = DetermineEvictionRange(currentSize, cancellationToken, out headAddress, ref allocatedPageCount, out estimatedHeapTrimmedSize);
                if (runState != (int)RunState.Running)
                    return;

                // Calculate new ReadOnlyAddress; if it hasn't changed then the ShiftReadOnlyAddress in logAccessor.ShiftHeadAddress will do nothing. 
                readOnlyAddress = logAccessor.allocatorBase.CalculateReadOnlyAddress(logAccessor.TailAddress, headAddress);
            }
            finally
            {
                logAccessor.allocatorBase.epoch.Suspend();
            }

            // Release the epoch before calling this because ShiftAddresses will wait for the ROA flush to complete. We need that wait because
            // ShiftHeadAddress caps the new HeadAddress at FlushedUntilAddress. Wait until the SHA eviction is complete to avoid going further over budget.
            logAccessor.ShiftAddresses(readOnlyAddress, headAddress, waitForEviction: true);

            // Heap size subtraction is handled by the OnNext eviction callback (called during ShiftAddresses),
            // which subtracts each record's CURRENT HeapMemorySize at eviction time.
            Debug.Assert(heapSize.Total >= 0, $"HeapSize.Total should be >= 0 but is {heapSize.Total} in Resize");

            // Calculate the number of trimmed pages and report the new expected AllocatedPageCount here, since our last iteration (which may have been the only one)
            // would have returned isComplete and thus we didn't wait for the actual eviction.
            PostMemoryTrim(allocatedPageCount, headAddress);
            logger?.LogDebug("Decreased Allocated page count to {allocatedPageCount} and HeadAddress to {headAddress}; isComplete {isComplete}", allocatedPageCount, headAddress, isComplete);
        }
    }
}