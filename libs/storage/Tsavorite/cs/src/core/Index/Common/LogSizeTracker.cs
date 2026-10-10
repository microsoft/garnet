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
    /// <remarks>
    /// The budget covers the hybrid log's circular-buffer pages and any heap objects referenced from records on those
    /// pages, and is independent of how the page memory itself was obtained: a page counts the same whether it is a
    /// pinned managed array or a direct-VM (mmap/VirtualAlloc) block. Eviction therefore behaves identically on both
    /// backends — the tracker sizes the log in pages and objects, not in GC-heap bytes.
    /// </remarks>
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

        /// <summary>
        /// Nonzero while a wakeup of <see cref="ResizerTask"/> is outstanding, so that the over-budget record
        /// path can coalesce redundant signals. See <see cref="SignalResizer"/>.
        /// </summary>
        private int resizePending;

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
        /// Set by <see cref="ResizerTask"/> once its body is actually executing. <see cref="Start"/> publishes
        /// <see cref="RunState.Running"/> and then queues the body with <c>Task.Run</c>, so between those two points the
        /// resizer is nominally running but cannot act on anything; under thread-pool starvation that window is unbounded.
        /// </summary>
        /// <remarks>
        /// <see cref="RunState.Running"/> must keep being published synchronously in <see cref="Start"/> rather than moved
        /// here: <see cref="Stop"/> hands off by CAS-ing Running to StopRequested, so if the state were still NotStarted at
        /// that point the CAS would fail, <see cref="Stop"/> would return without signalling or waiting, and the body would
        /// then start against an allocator that has already torn down its epoch and buffer pool.
        /// </remarks>
        volatile bool resizerDispatched;

        /// <summary>
        /// Indicates whether the background resizer task is currently running: started, actually dispatched, and not yet
        /// stop-requested/stopped. Callers on the allocation path use this to decide whether they must evict synchronously
        /// themselves (when the resizer is not running, such as before it is started or after it has been stopped for
        /// shutdown) instead of deferring eviction to the resizer, which would never act on the request.
        /// </summary>
        /// <remarks>
        /// This requires <see cref="resizerDispatched"/> rather than <see cref="RunState.Running"/> alone. Deferring to a
        /// resizer that the thread pool has not dispatched yet stalls the allocation retry loop for as long as dispatch
        /// takes, which a starved pool makes unbounded; treating that window as "not running" costs nothing when the pool
        /// is healthy and falls back to the same synchronous page-cap eviction used when there is no size tracker at all.
        /// </remarks>
        public bool IsRunning => runState == (int)RunState.Running && resizerDispatched;

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

        /// <summary>
        /// Supplies the size of memory that lives outside the log but is charged against the log's budget, so that the
        /// log yields pages as that memory grows instead of the two sets of allocations summing past the machine's RAM.
        /// </summary>
        /// <remarks>
        /// Garnet uses this for hash-index memory beyond the configured index budget. The index holds one entry per
        /// distinct key, including keys whose records live only on disk, so it grows with record count and no log
        /// setting bounds it.
        /// <para>Sampled once per resizer iteration, not on every budget comparison, so a provider may cost a few field
        /// reads.</para>
        /// </remarks>
        public Func<long> ExternalMemorySizeProvider { get; set; }

        /// <summary>The most recent sample taken from <see cref="ExternalMemorySizeProvider"/>; zero if there is none.</summary>
        public long ExternalMemorySize => Volatile.Read(ref externalMemorySize);

        private long externalMemorySize;

        /// <summary>Value of <see cref="externalMemorySize"/> at the last growth report, which keeps reporting to one line per doubling.</summary>
        private long reportedExternalMemorySize;

        /// <summary>Growth below this size is not reported; the overflow bucket allocator reserves one chunk at
        /// construction, which an idle store charges but which is not growth.</summary>
        private const long MinExternalMemoryReportBytes = 1L << 20;

        /// <summary>
        /// The smallest budget the log may be reduced to, which is the same floor <see cref="UpdateTargetSize"/> enforces
        /// on an explicitly configured target.
        /// </summary>
        private long BudgetFloor => logAccessor.allocatorBase.PageSize * (long)MinTargetPageCount;

        /// <summary>
        /// <see cref="highTargetSize"/> less <see cref="ExternalMemorySize"/>: the trimming trigger actually in force.
        /// </summary>
        /// <remarks>
        /// External memory is subtracted from the budget rather than added to <see cref="TotalSize"/> so that being over
        /// budget continues to imply the <em>log</em> exceeds its budget, which <see cref="DetermineEvictionRange"/>
        /// relies on to bound the trim by the resident span.
        /// </remarks>
        private long EffectiveHighTargetSize => Math.Max(highTargetSize - ExternalMemorySize, BudgetFloor);

        /// <summary><see cref="lowTargetSize"/> less <see cref="ExternalMemorySize"/>: the level trimming runs down to.</summary>
        private long EffectiveLowTargetSize => Math.Max(lowTargetSize - ExternalMemorySize, BudgetFloor);

        /// <summary>Target size for the hybrid log memory utilization</summary>
        public long TargetSize { get; private set; }

        /// <summary>High and low deltas for <see cref="TargetSize"/></summary>
        public (long high, long low) TargetDeltaRange => (highTargetSize, lowTargetSize);

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"{runState}; TargetSize: [{TargetSize}, hi: {highTargetSize}, lo: {lowTargetSize}, effHi: {EffectiveHighTargetSize}]; TotalSize: [{TotalSize}, Heap: {heapSize.Total}, External: {ExternalMemorySize}];"
                 + $" isOver: [{IsOverBudget}, canEvict {IsBeyondSizeLimitAndCanEvict()}]; AllocPgCt: {logAccessor.AllocatedPageCount}; PgSize {logAccessor.allocatorBase.PageSize}";
        }

        /// <summary>Returns the memory budget we have remaining</summary>
        /// <remarks>May return a negative value if already over budget.</remarks>
        public long RemainingBudget => EffectiveHighTargetSize - TotalSize;

        /// <summary>Return true if the total size is outside the target plus delta</summary>
        public bool IsOverBudget => TotalSize > EffectiveHighTargetSize;

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
            return (TotalSize > EffectiveHighTargetSize) && logAccessor.allocatorBase.UnstableGetTailAddress(out _) - logAccessor.allocatorBase.HeadAddress >= MinEvictionHeadAddressLag;
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

            // The field is deliberately left in place: a size update that observed Running can still call
            // SignalResizer after this point, and Set on a disposed event is a safe no-op.
            resizeTaskEvent.Dispose();
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
                SignalResizer();
        }

        /// <summary>
        /// Signal the resizer, coalescing redundant signals. While a wakeup is already outstanding this costs a
        /// single read of a line that is written only on the signal edge and on resizer wakeups, so the
        /// over-budget record path performs no allocation and no atomic read-modify-write.
        /// </summary>
        /// <remarks>
        /// No size update can be missed, given <see cref="ResizerTask"/>'s capture-then-clear-then-sample
        /// ordering. A caller that reads <see cref="resizePending"/> as 1, and therefore skips the signal, is
        /// ordered before the next clear, which precedes the next sample; its size update precedes that read
        /// because <see cref="ConcurrentCounter.Increment"/> is interlocked and therefore a full fence, so the
        /// next sample observes it. A caller whose exchange returns 0 is ordered after the last clear, hence
        /// after the capture that preceded it, so its <c>Set</c> retires the captured generation and the
        /// resizer's wait returns at once rather than sleeping out the timeout. Callers must accordingly call
        /// this only after publishing the size update.
        /// </remarks>
        /// <summary>
        /// Number of signals actually raised on <see cref="resizeTaskEvent"/>, as opposed to the (much larger)
        /// number of times a caller asked for one. Incremented only on the rare path that genuinely signals, so
        /// it costs the over-budget record path nothing; exposed so tests can assert that coalescing holds under
        /// sustained pressure rather than inferring it from allocation counts.
        /// </summary>
        internal long ResizerSignalCount => Interlocked.Read(ref resizerSignalCount);

        private long resizerSignalCount;

        private void SignalResizer()
        {
            if (Volatile.Read(ref resizePending) == 0 && Interlocked.Exchange(ref resizePending, 1) == 0)
            {
                _ = Interlocked.Increment(ref resizerSignalCount);
                resizeTaskEvent.Set();
            }
        }

        /// <summary>Adds size to the tracked total count</summary>
        public void IncrementSize(long size)
        {
            if (size != 0)
            {
                heapSize.Increment(size);
                if (size > 0 && IsBeyondSizeLimitAndCanEvict())
                    SignalResizer();
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
                        SignalResizer();
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
        public void Signal() => SignalResizer();

        /// <summary>
        /// Takes a fresh sample from <see cref="ExternalMemorySizeProvider"/>, returning true if the external memory
        /// grew. Callers that want the resizer woken on growth follow a true return with <see cref="Signal"/>; the
        /// resizer loop and recovery do not, as both act on the new value themselves.
        /// </summary>
        public bool RefreshExternalMemorySize()
        {
            var provider = ExternalMemorySizeProvider;
            if (provider is null)
                return false;

            var newSize = provider();
            if (newSize < 0)
                newSize = 0;

            var previousSize = Interlocked.Exchange(ref externalMemorySize, newSize);
            if (newSize <= previousSize)
                return false;

            // Report on each doubling, so sustained growth is visible without a line per sample.
            var lastReported = Volatile.Read(ref reportedExternalMemorySize);
            if (newSize >= Math.Max(lastReported, MinExternalMemoryReportBytes) * 2
                && Interlocked.CompareExchange(ref reportedExternalMemorySize, newSize, lastReported) == lastReported)
            {
                logger?.LogWarning("Memory outside the log has grown to {externalMemorySize} bytes and is charged against the log budget of {targetSize} bytes,"
                    + " reducing the log to {effectiveTargetSize} bytes. In Garnet this is hash-index memory beyond the configured index size, which grows with"
                    + " record count: raise the index size to match the number of keys, or lower the log memory size.", newSize, TargetSize, EffectiveHighTargetSize);
            }
            return true;
        }

        /// <summary>
        /// Performs resizing by waiting for an event that is signaled whenever memory utilization changes.
        /// This is invoked on the threadpool to avoid blocking calling threads during the resize operation.
        /// </summary>
        async Task ResizerTask(CancellationToken cancellationToken)
        {
            // Publish that the body is executing, so IsRunning stops reporting a resizer the thread pool has not dispatched
            // yet. Until this point the allocation path evicts synchronously rather than waiting on a signal we cannot act on.
            resizerDispatched = true;

            while (true)
            {
                try
                {
                    // Capture the event generation BEFORE resizing, and wait on the capture. Set() retires the current
                    // generation and installs a fresh, unsignaled one, so a signal raised while we are resizing (rather
                    // than parked in WaitAsync) releases the generation captured here and the wait below returns at once.
                    // Re-reading the field after resizing would instead pick up the fresh generation and sleep out the
                    // full timeout, having missed the signal. This is the same capture-before-check discipline every
                    // flushEvent caller uses, and the reason CompletionEvent is a struct.
                    // The capture must be taken afresh each iteration: Set() releases int.MaxValue permits on the
                    // generation it retires, so a capture that has already been consumed never blocks again.
                    var localResizeTaskEvent = resizeTaskEvent;

                    // Consume any outstanding wakeup AFTER capturing and BEFORE ResizeIfNeeded samples sizes below.
                    // Both halves of that ordering are load-bearing; see SignalResizer.
                    //
                    // Clearing before capturing would lose wakeups: a signaller slipping into that window sets
                    // resizePending and calls Set(), the capture then picks up the generation Set() just published,
                    // and the wait below parks on it -- having consumed the signal without acting on it -- while
                    // resizePending stays latched at 1 so every later signaller coalesces itself away. The resizer
                    // would then sleep out the full ResizeTaskDelaySeconds with work outstanding, stalling the
                    // allocation retry loop that NeedToWaitForClose drives through Signal().
                    _ = Interlocked.Exchange(ref resizePending, 0);

                    if (runState == (int)RunState.Running)
                    {
                        // Contain resize failures so they cannot skip the wait below; otherwise a persistently failing
                        // resize would spin this loop with no delay between attempts.
                        try
                        {
                            _ = RefreshExternalMemorySize();
                            ResizeIfNeeded(cancellationToken);
                        }
                        catch (OperationCanceledException)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            logger?.LogWarning(e, "Exception when attempting to perform memory resizing.");
                        }
                    }

                    if (runState != (int)RunState.Running)
                    {
                        OnStopped();
                        return;
                    }

                    await localResizeTaskEvent.WaitAsync(TimeSpan.FromSeconds(ResizeTaskDelaySeconds), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    logger?.LogTrace("Log resize task has been cancelled.");
                    OnStopped();
                    return;
                }
                catch (Exception e)
                {
                    logger?.LogWarning(e, "Exception while waiting to perform memory resizing.");
                }
            }
        }

        private bool DetermineEvictionRange(long currentSize, CancellationToken cancellationToken, out long headAddress,
            ref int allocatedPageCount, out long estimatedHeapTrimmedSize)
        {
            // We know we are oversize so we calculate how much we need to trim to get to the effective lowTargetSize.
            var overBudgetAmount = currentSize - EffectiveLowTargetSize;
            estimatedHeapTrimmedSize = 0L;

            var allocator = logAccessor.allocatorBase;
            headAddress = allocator.HeadAddress;
            var startingHeadAddress = headAddress;
            var startingHeadPage = allocator.GetPage(headAddress);
            var tailAddress = allocator.UnstableGetTailAddress(out _);
            var maxEvictUntilAddress = tailAddress - MinEvictionHeadAddressLag;
            var maxEvictUntilPage = allocator.GetPage(maxEvictUntilAddress);

            // If there is nothing to trim from the heap, we just do math to trim as many pages as we need to (up to the limit).
            if (heapSize.Total == 0)
            {
                // We are evicting in units of pages, so we set this to the start of the maxEvictUntilPage.
                maxEvictUntilAddress = allocator.GetLogicalAddressOfStartOfPage(maxEvictUntilPage);

                // Snapping down to the page start can land at or below headAddress despite the caller's gate on
                // tailAddress - headAddress >= MinEvictionHeadAddressLag, because headAddress need not be page-aligned
                // and the lag can be smaller than a page. No whole page is evictable, so retry as the tail advances.
                if (maxEvictUntilPage <= startingHeadPage)
                    return false;

                var evictableSize = maxEvictUntilAddress - headAddress;

                // evictableSize is the resident span [headAddress, tail-aligned). When heapSize is 0, TotalSize == AllocatedPageCount * PageSize, so being
                // over budget here means AllocatedPageCount * PageSize > budget; recovery keeps AllocatedPageCount within MaxAllocatedPageCount (the read
                // batch is capped at the budget and a final trim evicts any object-free overage), so AllocatedPageCount ~= the resident page count and that
                // resident span must itself exceed the budget => evictableSize > 0. This holds when ExternalMemorySize has reduced the budget, because the
                // reduced budget is floored at BudgetFloor (MinTargetPageCount pages). A negative value would mean AllocatedPageCount exceeds the resident
                // set, i.e. stale pages left allocated below headAddress.
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

                // If we have finished a page, add its size to our eviction total and set headAddress to the start of the next page,
                // but only while that start is still behind the tail. The scan stops at maxEvictUntilAddress, which falls inside the
                // tail page when PageSize exceeds MinEvictionHeadAddressLag, so completing the tail page would put headAddress past
                // TailAddress, which ShiftHeadAddress caps at FlushedUntilAddress and the caller then waits on forever.
                if (headAddress >= endAddress)
                {
                    var nextPageAddress = allocator.GetFirstValidLogicalAddressOnPage(currentPage + 1);
                    if (nextPageAddress <= tailAddress)
                    {
                        pageTrimmedSize += allocator.PageSize;
                        headAddress = nextPageAddress;
                    }
                }

                if (estimatedHeapTrimmedSize + pageTrimmedSize >= overBudgetAmount)
                    break;
            }

            Debug.Assert(headAddress <= Math.Max(startingHeadAddress, tailAddress),
                $"headAddress ({headAddress}) must not pass TailAddress ({tailAddress}); it would be waited on forever.");

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
            if (currentSize <= EffectiveHighTargetSize)
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
                logger?.LogDebug("Heap size {totalLogSize} > target {highTargetSize}. Alloc: {AllocatedPageCount} BufferSize: {BufferSize}", heapSize.Total, EffectiveHighTargetSize, allocatedPageCount, logAccessor.BufferSize);

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