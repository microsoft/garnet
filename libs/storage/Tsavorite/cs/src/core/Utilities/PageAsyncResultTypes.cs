// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define CALLOC

using System;
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

        /* Used for iteration */
        internal long resumePtr;    // TODO unused
        internal long untilPtr;     // TODO unused
        internal long maxPtr;       // TODO set but not used

        /// <summary>Free any internal allocations</summary>
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
        /// Flush buffers if flushing ObjectAllocator.
        /// </summary>
        public CircularDiskPageWriteBuffer flushBuffers;

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
        /// <summary>If we had separate Writes for multiple spans of a single array, this is a refcounted wrapper for the <see cref="GCHandle"/>;
        /// it is released after the write and if it is the final release, all spans have been written and the GCHandle is freed (and the object unpinned).</summary>
        public RefCountedPinnedGCHandle refCountedGCHandle;

        /// <summary>If we had to Write a single span of an array, this is the <see cref="GCHandle"/>; we don't use the refcounted wrapper in this case, to avoid an
        /// unnecessary allocation. It is freed (and the object unpinned) after the Write.</summary>
        public GCHandle gcHandle;

        /// <summary> If this non-null it means a Write was at a page boundary where we had to insert a <see cref="DiskPageHeader"/> or <see cref="DiskPageFooter"/>, or for some other reason
        /// had to allocate a temp buffer to insert something.</summary>
        public SectorAlignedMemory buffer;

        /// <summary>
        /// The countdown callback for the entire partial flush, including buffers, external writes, and final sector-aligning write.
        /// </summary>
        public CountdownCallbackAndContext countdownCallbackAndContext;

        public void SetAndIncrementCountdownCallback(CountdownCallbackAndContext callbackAndContext)
        {
            countdownCallbackAndContext = callbackAndContext;
            callbackAndContext.Increment();
        }

        public void Dispose()
        {
            refCountedGCHandle?.Release();
            if (gcHandle.IsAllocated)
                gcHandle.Free();
            buffer?.Return();
            countdownCallbackAndContext.Decrement();
        }
    }

    /// <summary>
    /// Hold the callback and context for a refcounted callback and context. Used to ensure global completion of multi-buffer multiple reads or writes (which use a "local"
    /// callback) before invoking the external callback.
    /// </summary>
    /// <remarks>
    /// The sequence is illustrated for flushes, but similar logic applies to multiple reads:
    /// <list type="bullet">
    ///     <item>Initialize the field ot a new instance of this at the start of a partial flush</item>
    ///     <item>AddRef and Release for each operation (for flushes, there will be two levels of refcount:
    ///         <list type="bullet">
    ///             <item>Per-buffer, to control buffer reuse within and across multiple partial flushes</item>
    ///             <item>Globally (within the <see cref="CircularDiskPageWriteBuffer"/>), to await the completion of all partial flushes before invoking the external callback.</item>
    ///         </list>
    ///     </item>    
    /// </list>
    /// When the count hits zero, if the callback is not null, call it; it will only be set to non-null when we have completed a partial flush. This allows the count to drop to 0 and
    /// be increased again throughout the partial flush, as various data spans are written.
    /// </remarks>
    internal sealed class CountdownCallbackAndContext
    {
        public DeviceIOCompletionCallback callback;
        public object context;
        private uint numBytes;
        internal long count;

        public void Set(DeviceIOCompletionCallback callback, object context, uint numBytes)
        {
            this.callback = callback;
            this.context = context;
            this.numBytes = numBytes;
        }

        internal void Increment() => _ = Interlocked.Increment(ref count);

        internal void Decrement()
        {
            if (Interlocked.Decrement(ref count) == 0)
                callback?.Invoke(errorCode: 0, numBytes, context);
        }
    }

    /// <summary>
    /// Hold a <see cref="GCHandle"/> and a refcount; release the handle when the refcount reaches 0.
    /// </summary>
    internal sealed class RefCountedPinnedGCHandle
    {
        private GCHandle handle;
        private long count;

        internal RefCountedPinnedGCHandle(object targetObject, long initialCount)
        {
            handle = GCHandle.Alloc(targetObject, GCHandleType.Pinned);
            count = initialCount;
        }

        internal RefCountedPinnedGCHandle(GCHandle gcHandle, long initialCount)
        {
            handle = gcHandle;
            count = initialCount;
        }

        internal void AddRef()
        {
            ObjectDisposedException.ThrowIf(count <= 0, $"Uninitialized or final-released {nameof(RefCountedPinnedGCHandle)}");
            _ = Interlocked.Increment(ref count);
        }

        internal void Release()
        {
            ObjectDisposedException.ThrowIf(count <= 0, $"Uninitialized or final-released {nameof(RefCountedPinnedGCHandle)}");
            if (Interlocked.Decrement(ref count) == 0 && handle.IsAllocated)
                handle.Free();
        }

        internal object Target
        {
            get
            {
                ObjectDisposedException.ThrowIf(count <= 0 || !handle.IsAllocated, $"Uninitialized or final-released {nameof(RefCountedPinnedGCHandle)}");
                return handle.Target;
            }
        }

        internal bool IsAllocated => handle.IsAllocated;
    }

    /// <summary>
    /// A class to carry callback and context through operations that may chain callbacks.
    /// </summary>
    internal sealed class DiskReadCallbackContext
    {
        /// <summary>If we had separate Reads directly into multiple spans of a single array, this is a refcounted wrapper for the <see cref="GCHandle"/>;
        /// it is released after the write and if it is the final release, all spans have been written and the GCHandle is freed (and the object unpinned).</summary>
        public RefCountedPinnedGCHandle refCountedGCHandle;

        /// <summary>If we had a Read directly into a single span of an array, this is the <see cref="GCHandle"/>; we don't use the refcounted wrapper in this case, to avoid an
        /// unnecessary allocation. It is freed (and the object unpinned) after the read.</summary>
        public GCHandle gcHandle;

        /// <summary>If this non-null it means a Read was either not at a page boundary, or is small enough that we will read and then shift-copy over the
        /// <see cref="DiskPageHeader"/> or <see cref="DiskPageFooter"/> because it can't be part of the data returned to the caller.
        /// </summary>
        public SectorAlignedMemory buffer;

        /// <summary>If <see cref="CopyTarget"/> is not null, these are the start offset(s) of the range(s) to copy to it from <see cref="buffer"/>;
        /// i.e. the start(s) of the range(s) before or after the page boundary header+footer combo in the read that will be skipped in the copy to a final buffer
        /// (this copy will be done by the caller's callback). If negative, no copy is done.</summary>
        public int copyTargetFirstSourceOffset, copyTargetSecondSourceOffset;

        /// <summary>If <see cref="CopyTarget"/> is not null, these are the length(s) of the range(s) to copy to it from <see cref="buffer"/>;
        /// i.e. the length(s) of the range(s) before or after the page boundary header+footer combo in the read that will be skipped in the copy to a final buffer
        /// (this copy will be done by the caller's callback).</summary>
        public int copyTargetFirstSourceLength, copyTargetSecondSourceLength;

        /// <summary>If non-null, this is the target buffer to copy data to (the copy is done by the caller's callback).</summary>
        public byte[] CopyTarget => (byte[])(gcHandle.IsAllocated ? gcHandle.Target : refCountedGCHandle.Target);

        /// <summary>If <see cref="CopyTarget"/> is not null, this is the offset within it to start receiving copies.</summary>
        public int copyTargetDestinationOffset;

        /// <summary>An event that can be waited for; the caller's callback will signal it if non-null.</summary>
        internal CountdownEvent countdownEvent;

        internal int TotalCopyLength => copyTargetFirstSourceLength + copyTargetSecondSourceLength;

        /// <summary>Constructor that also adds a reference or count to the parameters</summary>
        internal DiskReadCallbackContext(CountdownEvent countdownEvent, RefCountedPinnedGCHandle refCountedGCHandle)
        {
            this.countdownEvent = countdownEvent;
            this.countdownEvent?.AddCount(1);
            this.refCountedGCHandle = refCountedGCHandle;
            this.refCountedGCHandle?.AddRef();
        }

        public void Dispose()
        {
            refCountedGCHandle?.Release();
            if (gcHandle.IsAllocated)
                gcHandle.Free();
            buffer?.Return();
        }
    }
}