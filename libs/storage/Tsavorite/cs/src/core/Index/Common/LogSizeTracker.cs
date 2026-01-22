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
    public class LogSizeTracker<TStoreFunctions, TAllocator> : ITsavoriteRecordObserver<ITsavoriteScanIterator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        public static readonly int ResizeTaskDelaySeconds = 10;

        private ConcurrentCounter logSize;
        private long lowTargetSize;
        private long highTargetSize;
        private readonly ILogger logger;

        internal LogAccessor<TStoreFunctions, TAllocator> logAccessor;

        /// <summary>Indicates whether resizer task has been stopped</summary>
        public volatile bool Stopped;

        internal Action<int> PostEmptyPageCountIncrease { get; set; } = (int count) => { };

        internal Action<int> PostEmptyPageCountDecrease { get; set; } = (int count) => { };

        /// <summary>Total size occupied by log, including heap</summary>
        public long TotalSizeBytes => logAccessor.MemorySizeBytes + logSize.Total;

        /// <summary>Size of log heap memory</summary>
        public long LogHeapSizeBytes => logSize.Total;

        /// <summary>Target size for the hybrid log memory utilization</summary>
        public long TargetSize => (highTargetSize + lowTargetSize) / 2;

        /// <summary>Creates a new log size tracker</summary>
        /// <param name="logAccessor">Hybrid log accessor</param>
        /// <param name="targetSize">Target size for the hybrid log memory utilization</param>
        /// <param name="delta">Delta from target size to maintain memory utilization</param>
        /// <param name="logger"></param>
        public LogSizeTracker(LogAccessor<TStoreFunctions, TAllocator> logAccessor, long targetSize, long delta, ILogger logger)
        {
            Debug.Assert(logAccessor != null);
            Debug.Assert(delta >= 0);
            Debug.Assert(targetSize > delta);

            this.logAccessor = logAccessor;
            logSize = new ConcurrentCounter();
            UpdateTargetSize(targetSize, delta);
            this.logger = logger;
            Stopped = false;
        }

        /// <summary>
        /// Starts the log size tracker
        /// NOTE: Not thread safe to start multiple times
        /// </summary>
        /// <param name="token"></param>
        public void Start(CancellationToken token)
        {
            Debug.Assert(Stopped == false);
            _ = Task.Run(() => ResizerTask(token));
        }

        /// <summary>
        /// Update target size for the hybrid log memory utilization
        /// </summary>
        /// <param name="targetSize">The target size</param>
        /// <param name="delta">Delta from the target size</param>
        public void UpdateTargetSize(long targetSize, long delta)
        {
            Debug.Assert(delta >= 0);
            Debug.Assert(targetSize > delta);
            lowTargetSize = targetSize - delta;
            highTargetSize = targetSize + delta;
            logger?.LogInformation("Target size updated to {targetSize} with delta {delta}", targetSize, delta);
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

        public void OnRecord<TSourceLogRecord>(in TSourceLogRecord logRecord)
            where TSourceLogRecord : ISourceLogRecord
            => UpdateSize(in logRecord, add: false);   // Reduce size as records are being evicted

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
                    logSize.Increment(size);
                else
                    logSize.Increment(-size);
            }
        }

        /// <summary>
        /// Performs resizing by waiting for an event that is signaled whenever memory utilization changes.
        /// This is invoked on the threadpool to avoid blocking calling threads during the resize operation.
        /// </summary>
        async Task ResizerTask(CancellationToken token)
        {
            while (true)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(ResizeTaskDelaySeconds), token);
                    ResizeIfNeeded(token);
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

        /// <summary>
        /// Adjusts the log size to maintain its size within the range of +/- delta of the target size.
        /// It does so by adjusting the number of empty pages in the underlying log. Also, it does this by
        /// incrementing/decrementing the empty page count by 1 at a time to avoid large jumps in memory utilization.
        /// </summary>
        private void ResizeIfNeeded(CancellationToken token)
        {
            // Monitor the heap size
            if (logSize.Total > highTargetSize)
            {
                logger?.LogDebug("Heap size {totalLogSize} > target {highTargetSize}. Alloc: {AllocatedPageCount} EPC: {EmptyPageCount}", logSize.Total, highTargetSize, logAccessor.AllocatedPageCount, logAccessor.EmptyPageCount);
                while (logSize.Total > highTargetSize &&
                    logAccessor.EmptyPageCount < logAccessor.MaxEmptyPageCount)
                {
                    token.ThrowIfCancellationRequested();

                    if (logAccessor.AllocatedPageCount > logAccessor.BufferSize - logAccessor.EmptyPageCount + 1)
                        return; // wait for allocation to stabilize

                    logAccessor.EmptyPageCount++;
                    PostEmptyPageCountIncrease(logAccessor.EmptyPageCount);
                    logger?.LogDebug("Increasing empty page count to {EmptyPageCount}", logAccessor.EmptyPageCount);
                }
            }
            else if (logSize.Total < lowTargetSize)
            {
                logger?.LogDebug("Heap size {totalLogSize} < target {lowTargetSize}. Alloc: {AllocatedPageCount} EPC: {EmptyPageCount}", logSize.Total, lowTargetSize, logAccessor.AllocatedPageCount, logAccessor.EmptyPageCount);
                while (logSize.Total < lowTargetSize &&
                    logAccessor.EmptyPageCount > logAccessor.MinEmptyPageCount)
                {
                    token.ThrowIfCancellationRequested();

                    if (logAccessor.AllocatedPageCount < logAccessor.BufferSize - logAccessor.EmptyPageCount - 1)
                        return; // wait for allocation to stabilize

                    logAccessor.EmptyPageCount--;
                    PostEmptyPageCountDecrease(logAccessor.EmptyPageCount);
                    logger?.LogDebug("Decreasing empty page count to {EmptyPageCount}", logAccessor.EmptyPageCount);
                }
            }
        }
    }
}