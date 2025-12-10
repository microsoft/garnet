// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define CALLOC

using System;
using System.Diagnostics;
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

        /// <summary>Recovery device page offset</summary>
        internal long devicePageOffset;

        /// <summary>Context state to be passed through the read operation</summary>
        internal TContext context;

        /// <summary>Event to be signaled when the main-log page read is complete</summary>
        internal CountdownEvent handle;

        /// <summary>Callback to be called when the main-log page has completed processing; for <see cref="ObjectAllocator{TStoreFunctions}"/>
        /// this means after all Overflow or Objects on the page have been read as well.</summary>
        internal DeviceIOCompletionCallback callback;

        /// <summary>The destination pointer being read into.</summary>
        internal IntPtr destinationPtr;

        /// <summary>The cancellation token source, if any, for the Read operation</summary>
        internal CancellationTokenSource cts;

        /// <summary>Read buffers if Reading ObjectAllocator.</summary>
        public CircularDiskReadBuffer readBuffers;

        /// <summary>Number of bytes read.</summary>
        public uint numBytesRead;

        /// <summary>The max offset on the main log page to iterate records when determining how many bytes in the ObjectLog to read.</summary>
        internal long maxPtr;

        /// <summary>If true, we are called from recovery, and should use the non-transient <see cref="ObjectIdMap"/>.</summary>
        internal bool isForRecovery;

        public override string ToString()
            => $"page {page}, isRecov {isForRecovery}, devPgOffset {devicePageOffset}, ctx {context}, countdown {handle?.CurrentCount}, destPtr {destinationPtr} ({destinationPtr:X}), maxPtr {maxPtr}";

        /// <summary>Currently nothing to free.</summary>
        public void Free()
        {
        }

        /// <inheritdoc/>
        public void DisposeHandle()
        {
            handle?.Dispose();
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

        public override string ToString()
        {
            var compSemCount = completedSemaphore?.CurrentCount.ToString() ?? "null";
            var flushSemCount = completedSemaphore?.CurrentCount.ToString() ?? "null";
            return $"count {count}, compSemCount {compSemCount}, flushSemCount {flushSemCount}";
        }

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
        /// <summary>The index of the log Page being written</summary>
        public long page;

        /// <summary>Context object for the callback</summary>
        public TContext context;

        /// <summary>Flush buffers if flushing ObjectAllocator.</summary>
        public CircularDiskWriteBuffer flushBuffers;

        /// <summary>Count of active pending flush operations; the callback decrements this and when it hits 0, the overall flush operation is complete.</summary>
        public int count;

        /// <summary>If true, this is a flush of a partial page.</summary>
        internal bool partial;

        internal long fromAddress;
        internal long untilAddress;

        /// <summary>If true, we are called from recovery via AsyncFlushPagesForRecovery, so the object log files have already been written; we must reuse the
        /// deserialized object lengths to update the LogRecord's ObjectLogPosition rather than serialize again.</summary>
        internal bool isForRecovery;

        /// <summary>The record buffer, passed through the IO process to retain a reference to it so it will not be GC'd before the Flush write completes.</summary>
        internal SectorAlignedMemory freeBuffer1;

        /// <summary>The event that is signaled by the callback so any waiting thread knows the IO has completed.</summary>
        internal AutoResetEvent done;

        internal FlushCompletionTracker flushCompletionTracker;

        /// <summary>If this is set then we are using a different objectLog device from that in the allocator, and do not use the allocator's <see cref="ObjectLogFilePositionInfo"/>.</summary>
        internal ObjectLogFilePositionInfo objectLogFilePositionInfo;

        public override string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            return $"page {page}, isRecov {isForRecovery}, ctx {context}, count {count}, partial {bstr(partial)}, fromAddr {fromAddress} ({fromAddress:X}), untilAddr {untilAddress} ({untilAddress:X})," 
                 + $" flushCompTrack [{flushCompletionTracker}], circFlushBufs [{flushBuffers}]";
        }

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
        public RefCountedPinnedGCHandle refCountedGCHandle { get; private set; }

        /// <summary>Separate public Set() call so we ensure it is AddRef'd</summary>
        /// <param name="refGcHandle"></param>
        public void SetRefCountedHandle(RefCountedPinnedGCHandle refGcHandle)
        {
            Debug.Assert(!gcHandle.IsAllocated, "Cannot have both GCHandle and RefCountedPinnedGCHandle");
            refCountedGCHandle = refGcHandle;
            refCountedGCHandle.AddRef();
        }

        /// <summary>If this Write is from a <see cref="OverflowByteArray"/>, this <see cref="GCHandle"/> keeps its byte[] pinned during the Write.
        /// It is freed (and the array unpinned) after the Write. Used instead of <see cref="refCountedGCHandle"/> for only a single span of the array to avoid a heap allocation.</summary>
        private readonly GCHandle gcHandle;

        /// <summary>The countdown callback for the entire partial flush, including <see cref="DiskWriteBuffer"/>s, external writes, and final sector-aligning write.</summary>
        private readonly CountdownCallbackAndContext countdownCallbackAndContext;

        /// <summary>The countdown event if this write is associated with a <see cref="DiskWriteBuffer"/>.</summary>
        private CountdownEvent bufferCountdownEvent;

        public override string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            var countdownString = bufferCountdownEvent is null ? "null" : bufferCountdownEvent.CurrentCount.ToString();
            var cbcString = countdownCallbackAndContext is null ? "null" : countdownCallbackAndContext.ToString();
            return $"refCntGcH [{refCountedGCHandle}], gcH {bstr(gcHandle.IsAllocated)}, countdown {countdownString}, cb&c {cbcString}";
        }

        public DiskWriteCallbackContext(CountdownCallbackAndContext callbackAndContext)
        {
            countdownCallbackAndContext = callbackAndContext;
            callbackAndContext.Increment();
        }

        public DiskWriteCallbackContext(CountdownCallbackAndContext callbackAndContext, RefCountedPinnedGCHandle refGcHandle) : this(callbackAndContext)
            => SetRefCountedHandle(refGcHandle);

        public DiskWriteCallbackContext(CountdownCallbackAndContext callbackAndContext, GCHandle gcHandle) : this(callbackAndContext)
            => this.gcHandle = gcHandle;

        /// <summary>This write is associated with a <see cref="DiskWriteBuffer"/> so we need to signal the countdown event for that buffer when we are done.</summary>
        public void SetBufferCountdownEvent(CountdownEvent countdownEvent) => bufferCountdownEvent = countdownEvent;

        public long Release()
        {
            refCountedGCHandle?.Release();
            if (gcHandle.IsAllocated)
                gcHandle.Free();
            _ = bufferCountdownEvent?.Signal();
            return countdownCallbackAndContext?.Decrement() ?? 0;
        }
    }

    /// <summary>
    /// Hold the callback and context for a refcounted callback and context. Used to ensure global completion of multi-buffer writes (which use a "local"
    /// callback) before invoking the external callback.
    /// </summary>
    /// <remarks>
    /// The sequence is illustrated for flushes:
    /// <list type="bullet">
    ///     <item>Initialize the field to a new instance of this at the start of a partial flush</item>
    ///     <item>AddRef and Release for each operation (for flushes, there will be two levels of refcount:
    ///         <list type="bullet">
    ///             <item>Per-buffer</item>
    ///             <item>Globally (within the <see cref="CircularDiskWriteBuffer"/>), to await the completion of all partial flushes before invoking the external callback.</item>
    ///         </list>
    ///     </item>    
    /// </list>
    /// When the count hits zero, if the callback is not null, call it; it will only be set to non-null when we have completed a partial flush. This allows the count to drop to 0 and
    /// be increased again throughout the partial flush, as various data spans are written.
    /// </remarks>
    internal sealed class CountdownCallbackAndContext
    {
        /// <summary>Original caller's callback</summary>
        public DeviceIOCompletionCallback callback;
        /// <summary>Original caller's callback context</summary>
        public object context;
        /// <summary>Number of bytes written</summary>
        private uint numBytes;
        /// <summary>Number of in-flight operations</summary>
        internal long count;

        public override string ToString()
        {
            var callbackString = callback is null ? "null" : callback.ToString();
            var contextString = callback is null ? "null" : context.ToString();
            return $"numBytes {numBytes}, count {count}, callback {callbackString}, context {context}";
        }

        public void Set(DeviceIOCompletionCallback callback, object context, uint numBytes)
        {
            this.callback = callback;
            this.context = context;
            this.numBytes = numBytes;
        }

        internal void Increment() => _ = Interlocked.Increment(ref count);

        internal long Decrement()
        {
            var remaining = Interlocked.Decrement(ref count);
            if (remaining == 0)
                callback?.Invoke(errorCode: 0, numBytes, context);
            return remaining;
        }
    }

    /// <summary>
    /// Hold a <see cref="GCHandle"/> and a refcount; free the handle when the refcount reaches 0. Used when multiple sections of the
    /// same byte[] are being written, such as when it is split across segments.
    /// </summary>
    internal sealed class RefCountedPinnedGCHandle
    {
        /// <summary>The <see cref="GCHandle"/> being held.</summary>
        internal GCHandle gcHandle;
        /// <summary>Number of in-flight operations</summary>
        private long count;

        public override string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            return $"gcH {bstr(gcHandle.IsAllocated)}, count {count}";
        }

        internal RefCountedPinnedGCHandle(object targetObject, long initialCount)
        {
            gcHandle = GCHandle.Alloc(targetObject, GCHandleType.Pinned);
            count = initialCount;
        }

        internal RefCountedPinnedGCHandle(GCHandle gcHandle, long initialCount)
        {
            this.gcHandle = gcHandle;
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
            if (Interlocked.Decrement(ref count) == 0 && gcHandle.IsAllocated)
                gcHandle.Free();
        }

        internal object Target
        {
            get
            {
                ObjectDisposedException.ThrowIf(count <= 0 || !gcHandle.IsAllocated, $"Uninitialized or final-released {nameof(RefCountedPinnedGCHandle)}");
                return gcHandle.Target;
            }
        }

        internal bool IsAllocated => gcHandle.IsAllocated;
    }

    /// <summary>
    /// A class to carry callback and context through operations that may chain callbacks.
    /// </summary>
    internal sealed class DiskReadCallbackContext
    {
        /// <summary>If we had separate Reads directly into multiple spans of a single byte[], such as across segments, this is a refcounted wrapper for the <see cref="GCHandle"/>;
        /// it is released after the write and if it is the final release, all spans have been written and the GCHandle is freed (and the object unpinned).</summary>
        public RefCountedPinnedGCHandle refCountedGCHandle { get; private set; }

        /// <summary>Separate public Set() call so we ensure it is AddRef'd</summary>
        /// <param name="refGcHandle"></param>
        public void SetRefCountedHandle(RefCountedPinnedGCHandle refGcHandle)
        {
            Debug.Assert(!gcHandle.IsAllocated, "Cannot have both GCHandle and RefCountedPinnedGCHandle");
            refCountedGCHandle = refGcHandle;
            refCountedGCHandle.AddRef();
        }

        /// <summary>An event that can be waited for; the caller's callback will signal it if non-null.</summary>
        internal CountdownEvent countdownEvent;

        /// <summary>If we had a Read directly into the byte[] of an <see cref="OverflowByteArray"/>, this is the <see cref="GCHandle"/> that keps it pinned during the Read.
        /// After the Read it is freed (and the object unpinned).</summary>
        public GCHandle gcHandle;

        public override string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            return $"refCntGcH {refCountedGCHandle}, gcH {bstr(gcHandle.IsAllocated)}, countdown {countdownEvent?.CurrentCount}";
        }

        /// <summary>If non-null, this is the target buffer to copy data to (the copy is done by the caller's callback).</summary>
        public byte[] CopyTarget => (byte[])(gcHandle.IsAllocated ? gcHandle.Target : refCountedGCHandle.Target);

        internal DiskReadCallbackContext(CountdownEvent countdownEvent) => this.countdownEvent = countdownEvent;

        internal DiskReadCallbackContext(CountdownEvent countdownEvent, RefCountedPinnedGCHandle refGcHandle) : this(countdownEvent)
            => SetRefCountedHandle(refGcHandle);

        internal DiskReadCallbackContext(CountdownEvent countdownEvent, GCHandle gcHandle) : this(countdownEvent)
            => this.gcHandle = gcHandle;

        public void Dispose()
        {
            if (gcHandle.IsAllocated)
                gcHandle.Free();
            _ = (countdownEvent?.Signal());
        }
    }
}