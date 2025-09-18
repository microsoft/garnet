// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define CALLOC

using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Result of async page read
    /// </summary>
    /// <typeparam name="TContext"></typeparam>
    public sealed class PageAsyncReadResult<TContext>
    {
        /// <summary>Index of the main-log page being read</summary>
        internal long page;

        /// <summary>Context state to be passed through the read operation</summary>
        internal TContext context;

        /// <summary>Event to be signaled when the main-log page read is complete</summary>
        internal CountdownEvent handle;

        /// <summary>The buffer to receive the main-log page being read</summary>
        internal SectorAlignedMemory mainLogPageBuffer;

        /// <summary>Callback to be called when the main-log page has completed processing; for <see cref="ObjectAllocator{TStoreFunctions}"/>
        /// this means after all Overflow or Objects on the page have been read as well.</summary>
        internal DeviceIOCompletionCallback callback;

        /// <summary>The object-log device to use; may be the recovery device.</summary>
        internal IDevice objlogDevice;

        /// <summary>If non-null, this Read is being called for an iterator</summary>
        internal object frame;

        /// <summary>The cancellation token source, if any, for the Read operation</summary>
        internal CancellationTokenSource cts;

        /// <summary>
        /// Free
        /// </summary>
        public void Free()
        {
            if (mainLogPageBuffer != null)
            {
                mainLogPageBuffer.Return();
                mainLogPageBuffer = null;
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

        public void WaitOneFlush() => flushSemaphore?.Wait();
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
        /// Flush buffers if flushing ObjectAllocator.
        /// </summary>
        public CircularDiskWriteBuffer flushBuffers;

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

    /// <summary>
    /// A class to carry callback and context through operations that may chain callbacks.
    /// </summary>
    internal sealed class DiskWriteCallbackContext
    {
        /// <summary>The countdown event to signal after the write is complete.</summary>
        public CountdownEvent countdownEvent;

        /// <summary>If this Write is from a <see cref="OverflowByteArray"/>, this <see cref="GCHandle"/> keeps its byte[] pinned during the Write.
        /// It is freed (and the array unpinned) after the Write.</summary>
        public GCHandle gcHandle;

        public DiskWriteCallbackContext(CountdownEvent countdownEvent, GCHandle gcHandle)
        {
            this.countdownEvent = countdownEvent;
            this.gcHandle = gcHandle;
        }

        public void Dispose()
        {
            if (gcHandle.IsAllocated)
                gcHandle.Free();
            _ = (countdownEvent?.Signal());
        }
    }

    /// <summary>
    /// A class to carry callback and context through operations that may chain callbacks.
    /// </summary>
    internal sealed class DiskReadCallbackContext
    {
        /// <summary>An event that can be waited for; the caller's callback will signal it if non-null.</summary>
        internal CountdownEvent countdownEvent;

        /// <summary>If we had a Read directly into the byte[] of an <see cref="OverflowByteArray"/>, this is the <see cref="GCHandle"/> that keps it pinned during the Read.
        /// After the Read it is freed (and the object unpinned).</summary>
        public GCHandle gcHandle;

        /// <summary>Constructor that also adds a reference or count to the parameters</summary>
        internal DiskReadCallbackContext(CountdownEvent countdownEvent, GCHandle gcHandle)
        {
            this.countdownEvent = countdownEvent;
            this.gcHandle = gcHandle;
        }

        public void Dispose()
        {
            if (gcHandle.IsAllocated)
                gcHandle.Free();
            _ = (countdownEvent?.Signal());
        }
    }
}