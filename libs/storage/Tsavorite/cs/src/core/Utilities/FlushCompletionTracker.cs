// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Tracks the completion of page flush operations during snapshot checkpoints.
    /// Signals a <see cref="TaskCompletionSource{TResult}"/> when all pages have been flushed,
    /// or faults it if an exception occurs. Optionally supports per-page throttle waiting.
    /// </summary>
    internal sealed class FlushCompletionTracker
    {
        /// <summary>
        /// Task completion source to signal when all page flushes are done, or to fault on error.
        /// </summary>
        readonly TaskCompletionSource<bool> completionTcs;

        /// <summary>
        /// Semaphore for per-page flush completion, used only when throttling is enabled.
        /// </summary>
        readonly SemaphoreSlim flushSemaphore;

        /// <summary>
        /// Number of pages being flushed
        /// </summary>
        int count;

        public override string ToString()
        {
            var flushSemCount = flushSemaphore?.CurrentCount.ToString() ?? "null";
            return $"count {count}, flushSemCount {flushSemCount}";
        }

        /// <summary>
        /// Create a flush completion tracker
        /// </summary>
        /// <param name="completionTcs">TaskCompletionSource to signal when all flushes complete or to fault on error</param>
        /// <param name="enableThrottling">If true, creates a semaphore for per-page throttle waiting</param>
        /// <param name="count">Number of pages to flush</param>
        public FlushCompletionTracker(TaskCompletionSource<bool> completionTcs, bool enableThrottling, int count)
        {
            this.completionTcs = completionTcs;
            this.flushSemaphore = enableThrottling ? new SemaphoreSlim(0) : null;
            this.count = count;

            if (count == 0)
                _ = completionTcs.TrySetResult(true);
        }

        /// <summary>
        /// Complete flush of one page
        /// </summary>
        public void CompleteFlush()
        {
            _ = (flushSemaphore?.Release());
            if (Interlocked.Decrement(ref count) == 0)
                _ = completionTcs.TrySetResult(true);
        }

        /// <summary>
        /// Signal that the flush failed with an exception.
        /// </summary>
        public void SetException(Exception ex)
            => _ = completionTcs.TrySetException(ex);

        /// <summary>
        /// Wait for one page flush to complete. Only valid when throttling is enabled.
        /// </summary>
        public void WaitOneFlush() => flushSemaphore?.Wait();
    }
}