// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>Interface for calculating the size of the log</summary>
    /// <typeparam name="Key">Type of key</typeparam>
    /// <typeparam name="Value">Type of value</typeparam>
    public interface ILogSizeCalculator<Key, Value>
    {
        /// <summary>Calculates the size of a log record</summary>
        /// <param name="recordInfo">Information about the record</param>
        /// <param name="key">The key</param>
        /// <param name="value">The value</param>
        /// <returns>The size of the record</returns>
        long CalculateRecordSize(RecordInfo recordInfo, Key key, Value value);
    }

    /// <summary>Tracks and controls size of log</summary>
    /// <typeparam name="Key">Type of key</typeparam>
    /// <typeparam name="Value">Type of value</typeparam>
    /// <typeparam name="TLogSizeCalculator">Type of the log size calculator</typeparam>
    public class LogSizeTracker<Key, Value, TLogSizeCalculator> : IObserver<ITsavoriteScanIterator<Key, Value>>
        where TLogSizeCalculator : ILogSizeCalculator<Key, Value>
    {
        private ConcurrentCounter logSize;
        private long lowTargetSize;
        private long highTargetSize;
        private TLogSizeCalculator logSizeCalculator;
        private readonly ILogger logger;
        internal const int resizeTaskDelaySeconds = 10;

        internal LogAccessor<Key, Value> logAccessor;

        internal Action<int> PostEmptyPageCountIncrease { get; set; } = (int count) => { };

        internal Action<int> PostEmptyPageCountDecrease { get; set; } = (int count) => { };

        /// <summary>Total size occupied by log, including heap</summary>
        public long TotalSizeBytes => logAccessor.MemorySizeBytes + logSize.Total;

        /// <summary>Size of log heap memory</summary>
        public long LogHeapSizeBytes => logSize.Total;

        /// <summary>Creates a new log size tracker</summary>
        /// <param name="logAccessor">Hybrid log accessor</param>
        /// <param name="logSizeCalculator">Size calculator</param>
        /// <param name="targetSize">Target size for the hybrid log memory utilization</param>
        /// <param name="delta">Delta from target size to maintain memory utilization</param>
        /// <param name="logger"></param>
        public LogSizeTracker(LogAccessor<Key, Value> logAccessor, TLogSizeCalculator logSizeCalculator, long targetSize, long delta, ILogger logger)
        {
            Debug.Assert(logAccessor != null);
            Debug.Assert(logSizeCalculator != null);
            Debug.Assert(delta >= 0);
            Debug.Assert(targetSize > delta);

            this.logAccessor = logAccessor;
            logSize = new ConcurrentCounter();
            lowTargetSize = targetSize - delta;
            highTargetSize = targetSize + delta;
            this.logSizeCalculator = logSizeCalculator;
            this.logger = logger;
            Task.Run(ResizerTask);
        }

        /// <summary>Callback on allocator completion</summary>
        public void OnCompleted() { }

        /// <summary>Callback on allocator error</summary>
        public void OnError(Exception error) { }

        /// <summary>Callback on allocator evicting a page to disk</summary>
        public void OnNext(ITsavoriteScanIterator<Key, Value> records)
        {
            long size = 0;
            while (records.GetNext(out RecordInfo info, out Key key, out Value value))
            {
                Debug.Assert(key != null);
                Debug.Assert(value != null);

                size += logSizeCalculator.CalculateRecordSize(info, key, value);
            }

            if (size == 0) return;

            IncrementSize(-size); // Reduce size as records are being evicted
        }

        /// <summary>Adds size to the tracked total count</summary>
        /// <param name="size">Size to add</param>
        public void IncrementSize(long size)
        {
            logSize.Increment(size);
        }

        /// <summary>
        /// Performs resizing by waiting for an event that is signaled whenever memory utilization changes.
        /// This is invoked on the threadpool to avoid blocking calling threads during the resize operation.
        /// </summary>
        async Task ResizerTask()
        {
            while (true)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(resizeTaskDelaySeconds));
                    ResizeIfNeeded();
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
        private void ResizeIfNeeded()
        {
            // Include memory size from the log (logAccessor.MemorySizeBytes) + heap size (logSize.Total) to check utilization
            if (logSize.Total + logAccessor.MemorySizeBytes > highTargetSize)
            {
                logger?.LogDebug($"Heap size {logSize.Total} + log {logAccessor.MemorySizeBytes} > target {highTargetSize}. Alloc: {logAccessor.AllocatedPageCount} EPC: {logAccessor.EmptyPageCount}");
                while (logSize.Total + logAccessor.MemorySizeBytes > highTargetSize && logAccessor.EmptyPageCount < logAccessor.MaxEmptyPageCount)
                {
                    if (logAccessor.AllocatedPageCount > logAccessor.BufferSize - logAccessor.EmptyPageCount + 1)
                    {
                        return; // wait for allocation to stabilize
                    }

                    logAccessor.EmptyPageCount++;
                    PostEmptyPageCountIncrease(logAccessor.EmptyPageCount);
                    logger?.LogDebug($"Increasing empty page count to {logAccessor.EmptyPageCount}");
                }
            }
            else if (logSize.Total + logAccessor.MemorySizeBytes < lowTargetSize)
            {
                logger?.LogDebug($"Heap size {logSize.Total} + log {logAccessor.MemorySizeBytes} < target {lowTargetSize}. Alloc: {logAccessor.AllocatedPageCount} EPC: {logAccessor.EmptyPageCount}");
                while (logSize.Total + logAccessor.MemorySizeBytes < lowTargetSize && logAccessor.EmptyPageCount > logAccessor.MinEmptyPageCount)
                {
                    if (logAccessor.AllocatedPageCount < logAccessor.BufferSize - logAccessor.EmptyPageCount - 1)
                    {
                        return; // wait for allocation to stabilize
                    }

                    logAccessor.EmptyPageCount--;
                    PostEmptyPageCountDecrease(logAccessor.EmptyPageCount);
                    logger?.LogDebug($"Decreasing empty page count to {logAccessor.EmptyPageCount}");
                }
            }
        }
    }
}