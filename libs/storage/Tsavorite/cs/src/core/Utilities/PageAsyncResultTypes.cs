// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define CALLOC

using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Result of async page read
    /// </summary>
    /// <typeparam name="TContext"></typeparam>
    public sealed class PageAsyncReadResult<TContext>
    {
        internal long page;
        internal long offset;
        internal TContext context;
        internal CountdownEvent handle;

        /// <summary>
        /// This is the record buffer, passed through the IO process to retain a reference to it so it will not be GC'd before the Flush write completes.
        /// </summary>
        internal SectorAlignedMemory freeBuffer1;

        internal DeviceIOCompletionCallback callback;
        internal object frame;
        internal CancellationTokenSource cts;
        internal uint numBytesRead;

        /* Used for iteration */
        internal long resumePtr;    // TODO unused
        internal long untilPtr;     // TODO unused
        internal long maxPtr;       // TODO set but not used

        /// <summary>
        /// Free
        /// </summary>
        public void Free()
        {
            if (freeBuffer1 != null)
            {
                freeBuffer1.Return();
                freeBuffer1 = null;
            }
        }
    }

    /// <summary>
    /// Shared flush completion tracker, when bulk-flushing many pages
    /// </summary>
    internal sealed class FlushCompletionTracker
    {
        /// <summary>
        /// Semaphore to set on flush completion
        /// </summary>
        readonly SemaphoreSlim completedSemaphore;

        /// <summary>
        /// Semaphore to wait on for flush completion
        /// </summary>
        readonly SemaphoreSlim flushSemaphore;

        /// <summary>
        /// Number of pages being flushed
        /// </summary>
        int count;

        /// <summary>
        /// Create a flush completion tracker
        /// </summary>
        /// <param name="completedSemaphore">Semaphpore to release when all flushes completed</param>
        /// <param name="flushSemaphore">Semaphpore to release when each flush completes</param>
        /// <param name="count">Number of pages to flush</param>
        public FlushCompletionTracker(SemaphoreSlim completedSemaphore, SemaphoreSlim flushSemaphore, int count)
        {
            this.completedSemaphore = completedSemaphore;
            this.flushSemaphore = flushSemaphore;
            this.count = count;
        }

        /// <summary>
        /// Complete flush of one page
        /// </summary>
        public void CompleteFlush()
        {
            flushSemaphore?.Release();
            if (Interlocked.Decrement(ref count) == 0)
                completedSemaphore.Release();
        }

        public void WaitOneFlush()
            => flushSemaphore?.Wait();
    }

    /// <summary>
    /// Page async flush result
    /// </summary>
    /// <typeparam name="TContext"></typeparam>
    public sealed class PageAsyncFlushResult<TContext>
    {
        /// <summary>
        /// The index of the log Page being written
        /// </summary>
        public long page;

        /// <summary>
        /// Context object for the callback
        /// </summary>
        public TContext context;

        /// <summary>
        /// Count of active pending flush operations; the callback decrements this and when it hits 0, the overall flush operation is complete.
        /// </summary>
        public int count;

        /// <summary>
        /// If true, this is a flush of a partial page.
        /// </summary>
        internal bool partial;

        internal long fromAddress;
        internal long untilAddress;

        /// <summary>
        /// This is the record buffer, passed through the IO process to retain a reference to it so it will not be GC'd before the Flush write completes.
        /// </summary>
        internal SectorAlignedMemory freeBuffer1;

        /// <summary>
        /// The event that is signaled by the callback so any waiting thread knows the IO has completed.
        /// </summary>
        internal AutoResetEvent done;

        internal FlushCompletionTracker flushCompletionTracker;

        /// <summary>
        /// Free
        /// </summary>
        public void Free()
        {
            if (freeBuffer1 != null)
            {
                freeBuffer1.Return();
                freeBuffer1 = null;
            }

            flushCompletionTracker?.CompleteFlush();
        }
    }
}