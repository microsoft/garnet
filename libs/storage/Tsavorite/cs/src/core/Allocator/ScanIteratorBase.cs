// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static LogAddress;

    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    public abstract class ScanIteratorBase<TAllocatorWrapper>
        where TAllocatorWrapper : IAllocator
    {
        /// <summary>Frame size (1 or 2)</summary>
        protected readonly int frameSize;

        /// <summary>Begin address of the scan. Cannot be readonly due to SnapCursorToLogicalAddress</summary>
        protected long beginAddress;

        /// <summary>End address of the scan</summary>
        protected readonly long endAddress;

        /// <summary>Epoch from the store</summary>
        protected readonly LightEpoch epoch;

        /// <summary>Number of deferred DoReadPage drain callbacks that have been registered but not yet executed.</summary>
        protected int pendingDrainCallbacks;

        /// <summary>Current address for iteration</summary>
        protected long currentAddress;
        /// <summary>Next address for iteration</summary>
        protected long nextAddress;

        /// <summary><see cref="CountdownEvent"/> vector for waiting for frame-load completion.</summary>
        /// <remarks>This array is in parallel with <see cref="loadCTSs"/>, <see cref="loadedPages"/>, and <see cref="nextLoadedPages"/>.</remarks>
        private CountdownEvent[] loadCompletionEvents;

        /// <summary><see cref="CancellationTokenSource"/> vector for canceling the wait for frame-load completion.</summary>
        /// <remarks>This array is in parallel with <see cref="loadCompletionEvents"/>, <see cref="loadedPages"/>, and <see cref="nextLoadedPages"/>.</remarks>
        private CancellationTokenSource[] loadCTSs;

        /// <summary>Vector of endAddresses for the currently loaded pages of the frames.</summary>
        /// <remarks>This array is in parallel with <see cref="loadCompletionEvents"/>, <see cref="loadCTSs"/>, and <see cref="nextLoadedPages"/>.</remarks>
        private long[] loadedPages;

        /// <summary>Vector of endAddresses for the currently in-flight, and possibly completed, loading of pages of the frames.
        /// This is updated atomically when we start the <see cref="BufferAndLoad"/> of a page.</summary>
        /// <remarks>This array is in parallel with <see cref="loadCompletionEvents"/>, <see cref="loadCTSs"/>, and <see cref="loadedPages"/>.</remarks>
        private long[] nextLoadedPages;

        /// <summary>The circular buffer we cycle through for object-log deserialization.</summary>
        CircularDiskReadBuffer[] objectReadBuffers;

        /// <summary>Number of bits in the size of the log page</summary>
        private readonly int logPageSizeBits;

        /// <summary>The allocator struct wrapper used to map a logical address to its page via the allocator-specific address
        /// interpretation. Stored as the concrete <typeparamref name="TAllocatorWrapper"/> rather than the <see cref="IAllocator"/>
        /// interface so the call is inlined instead of a virtual interface dispatch. Main-store allocators mask off the
        /// read-cache bit; TsavoriteLog uses the full range.</summary>
        private readonly TAllocatorWrapper allocator;

        /// <summary>Whether to include closed records in the scan</summary>
        protected readonly bool includeClosedRecords;

        /// <summary>
        /// Current address
        /// </summary>
        public long CurrentAddress => currentAddress;

        /// <summary>
        /// Next address
        /// </summary>
        public long NextAddress => nextAddress;

        /// <summary>
        /// The starting address of the scan
        /// </summary>
        public long BeginAddress => beginAddress;

        /// <summary>
        /// The ending address of the scan
        /// </summary>
        public long EndAddress => endAddress;

        /// <summary>
        /// Logger instance
        /// </summary>
        protected ILogger logger;

        /// <summary>
        /// Buffering for holding copies of in-memory records
        /// </summary>
        protected InMemoryScanBufferingMode memScanBufferingMode;

        /// <summary>
        /// Constructor
        /// </summary>
        public unsafe ScanIteratorBase(long beginAddress, long endAddress, DiskScanBufferingMode diskScanBufferingMode, InMemoryScanBufferingMode memScanBufferingMode,
                bool includeClosedRecords, LightEpoch epoch, int logPageSizeBits, TAllocatorWrapper allocator, bool initForReads = true, ILogger logger = null)
        {
            this.logger = logger;
            this.memScanBufferingMode = memScanBufferingMode;

            // If we are protected when creating the iterator, we do not need per-GetNext protection
            if (epoch != null && !epoch.ThisInstanceProtected())
                this.epoch = epoch;

            this.beginAddress = beginAddress;
            this.endAddress = endAddress;
            this.logPageSizeBits = logPageSizeBits;
            this.allocator = allocator;

            this.includeClosedRecords = includeClosedRecords;
            currentAddress = -1;
            nextAddress = beginAddress;

            if (diskScanBufferingMode == DiskScanBufferingMode.SinglePageBuffering)
                frameSize = 1;
            else if (diskScanBufferingMode == DiskScanBufferingMode.DoublePageBuffering)
                frameSize = 2;
            else if (diskScanBufferingMode == DiskScanBufferingMode.NoBuffering)
            {
                frameSize = 0;
                return;
            }
            if (initForReads)
                InitializeForReads();
        }

        /// <summary>Initialize fields for read callback management</summary>
        public virtual void InitializeForReads()
        {
            loadCompletionEvents = new CountdownEvent[frameSize];
            loadCTSs = new CancellationTokenSource[frameSize];
            loadedPages = new long[frameSize];
            nextLoadedPages = new long[frameSize];
            for (var i = 0; i < frameSize; i++)
            {
                loadedPages[i] = -1;
                nextLoadedPages[i] = -1;
                loadCTSs[i] = new CancellationTokenSource();
            }
            currentAddress = -1;
            nextAddress = beginAddress;
        }

        /// <summary>Initialize read buffers</summary>
        public virtual void InitializeReadBuffers(AllocatorBase allocatorBase = default)
        {
            objectReadBuffers = new CircularDiskReadBuffer[frameSize];
            for (var i = 0; i < frameSize; i++)
                objectReadBuffers[i] = allocatorBase?.CreateCircularReadBuffers();
        }

        /// <summary>
        /// Buffer and load
        /// </summary>
        /// <param name="currentIterationAddress">The current logical address</param>
        /// <param name="currentPage">The page containing the current logical address</param>
        /// <param name="currentFrame">The frame index of the current page (the page modulo the number of frames)</param>
        /// <param name="headAddress">Head address of the log</param>
        /// <param name="endIterationAddress">Address to stop the scan at</param>
        /// <returns>True we had to await the event here; </returns>
        /// <returns></returns>
        protected bool BufferAndLoad(long currentIterationAddress, long currentPage, long currentFrame, long headAddress, long endIterationAddress)
        {
            for (var i = 0; i < frameSize; i++)
            {
                // Read the next page. If i == 0 this is the page we are about to iterate; if i > 0, then we are issuing read-ahead for efficiency.
                var nextPage = currentPage + i;

                // Cannot load nextPage if it is entirely in memory or beyond the end address
                var pageStartAddress = GetLogicalAddressOfStartOfPage(nextPage, logPageSizeBits);
                if (pageStartAddress >= headAddress || pageStartAddress >= endIterationAddress)
                    continue;

                // Determine the endAddress on nextPage, which may be limited by endAddress or headAddress to be before end of page.
                var pageEndAddress = GetLogicalAddressOfStartOfPage(nextPage + 1, logPageSizeBits);
                if (endIterationAddress < pageEndAddress)
                    pageEndAddress = endIterationAddress;

                // With HeadAddress now possibly in the middle of the page, we have to ensure we handle re-entering with the same currentFrame while
                // a previous request on currentFrame is ongoing; this is ensured by CalculateReadOnlyAddress. So just read the entire page regardless
                // of headAddress; the entire page will have been flushed to disk already. TODO Leaving this here in case we change to record-aligned ReadOnlyAddress.
                //if (headAddress < pageEndAddress)
                //    pageEndAddress = headAddress;

                // Calculate the nextFrame we will load nextPage into
                var nextFrame = (currentFrame + i) % frameSize;

                // Loop using CAS as a latch-free way to ensure only one thread issues the load for nextPage into nextFrame.
                while (true)
                {
                    // Get the endAddress of the next page being loaded for this frame. If it is already loaded, as indicated by being >= the required endAddress, we're done.
                    var val = nextLoadedPages[nextFrame];
                    if (val >= pageEndAddress && loadedPages[nextFrame] >= pageEndAddress)
                        break;

                    // If the endAddress of the next page being loaded is less than the endAddress we need for the next page for this frame,
                    // try to atomically exchange it with the endAddress we need. If successful, issue the load.
                    if (val < pageEndAddress && Interlocked.CompareExchange(ref nextLoadedPages[nextFrame], pageEndAddress, val) == val)
                    {
                        Debug.Assert(loadCompletionEvents[nextFrame] is null || loadCompletionEvents[nextFrame].IsSet,
                            $"i {i}, currentAddress {currentIterationAddress}, currentFrame {currentFrame}, nextFrame {nextFrame} overwriting unset completion event");
                        var readBuffer = objectReadBuffers is not null ? objectReadBuffers[nextFrame] : default;

                        var frameIndex = i;
                        var frameRepairLatch = 0;
                        _ = Interlocked.Increment(ref pendingDrainCallbacks);
                        if (epoch != null)
                        {
                            try
                            {
                                epoch.BumpCurrentEpoch(() => DoReadPage(frameIndex));
                            }
                            catch (Exception ex)
                            {
                                // BumpCurrentEpoch runs other threads' registered drain actions, so it can throw either
                                // before or after our action is registered, and the caller cannot tell which. The latch
                                // makes the repair idempotent: whichever of DoReadPage or this handler runs first owns
                                // the frame, and the other is a no-op.
                                if (Interlocked.Exchange(ref frameRepairLatch, 1) == 0)
                                {
                                    FailFrameLoad(nextFrame, pageEndAddress, ex);
                                    _ = Interlocked.Decrement(ref pendingDrainCallbacks);
                                }
                                throw;
                            }
                        }
                        else
                            DoReadPage(frameIndex);

                        void DoReadPage(int frameIndex)
                        {
                            // The failure handler above already repaired the frame and accounted for the callback.
                            if (Interlocked.Exchange(ref frameRepairLatch, 1) != 0)
                                return;

                            try
                            {
                                AsyncReadPageFromDeviceToFrame(readBuffer, readPage: frameIndex + allocator.GetPageOfAddress(currentIterationAddress, logPageSizeBits), untilAddress: endIterationAddress,
                                    context: Empty.Default, out loadCompletionEvents[nextFrame], devicePageOffset: 0, device: null, objectLogDevice: null, loadCTSs[nextFrame]);
                            }
                            catch (Exception ex)
                            {
                                // Publish the terminal state before releasing pendingDrainCallbacks: Dispose treats a
                                // zero count as license to dispose the completion event and token source.
                                FailFrameLoad(nextFrame, pageEndAddress, ex);
                                _ = Interlocked.Decrement(ref pendingDrainCallbacks);
                                return;
                            }
                            loadedPages[nextFrame] = pageEndAddress;
                        }
                    }
                    else
                    {
                        // Someone else incremented nextLoadedPage[nextFrame] or the BumpCE has not completed and set loadedPages, so give things a chance to work and try again.
                        epoch?.ProtectAndDrain();
                    }
                }
            }

            // Wait only for currentFrame; nextFrame(s, if we ever have frameSize > 2) will process in the background until we actually need its data,
            // in which case it will come in here as currentFrame, see that nextLoadedPage is already set, and then this line will wait for it.
            // WaitForFrameLoad returns immediately if the wait has already been satisfied.
            return WaitForFrameLoad(currentIterationAddress, currentFrame);
        }

        /// <summary>
        /// Whether we need to buffer new page from disk
        /// </summary>
        protected bool NeedBufferAndLoad(long currentAddress, long currentPage, long currentFrame, long headAddress, long endAddress)
        {
            for (var i = 0; i < frameSize; i++)
            {
                // Read the next page. If i == 0 this is the page we are about to iterate; if i > 0, then we are issuing read-ahead for efficiency.
                var nextPage = currentPage + i;

                var pageStartAddress = GetLogicalAddressOfStartOfPage(nextPage, logPageSizeBits);

                // Cannot load nextPage if it is entirely in memory or beyond the end address
                if (pageStartAddress >= headAddress || pageStartAddress >= endAddress)
                    continue;

                // Determine the endAddress on nextPage, which may be limited by endAddress or headAddress to be before end of page.
                var pageEndAddress = GetLogicalAddressOfStartOfPage(nextPage + 1, logPageSizeBits);
                if (endAddress < pageEndAddress)
                    pageEndAddress = endAddress;
                if (headAddress < pageEndAddress)
                    pageEndAddress = headAddress;

                // Calculate the nextFrame we will load nextPage into
                var nextFrame = (currentFrame + i) % frameSize;

                // If the endAddress of the next page being loaded for this frame is already loaded, as indicated by being >= the required endAddress,
                // we don't need to load.
                if (nextLoadedPages[nextFrame] < pageEndAddress || loadedPages[nextFrame] < pageEndAddress)
                    return true;
            }
            return false;
        }

        internal abstract void AsyncReadPageFromDeviceToFrame<TContext>(CircularDiskReadBuffer readBuffers, long readPage, long untilAddress, TContext context, out CountdownEvent completed,
                long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null);

        /// <summary>
        /// Publish the failure of a page read that could not be issued, so no completion callback will fire for
        /// <paramref name="frame"/>.
        /// </summary>
        /// <remarks>
        /// Runs inside the deferred <see cref="LightEpoch.BumpCurrentEpoch(Action)"/> action, so nothing may escape: an
        /// exception here aborts an arbitrary thread's drain pass and never reaches the scanning thread. The failure is
        /// instead routed through the channel a failed I/O uses -- cancelling the frame's <see cref="loadCTSs"/> -- so
        /// <see cref="WaitForFrameLoad"/> skips the page, and <see cref="loadedPages"/> is advanced so
        /// <see cref="BufferAndLoad"/>'s CAS loop does not wait on a load that will never be retried.
        /// </remarks>
        private void FailFrameLoad(long frame, long pageEndAddress, Exception ex)
        {
            // Repair the frame before logging, which is external code and guarded separately.
            try
            {
                // Leave the frame as an asynchronously failed load does: an unset completion event and a cancelled
                // token, so WaitForFrameLoad's wait throws immediately and its catch makes the frame reusable. A fresh
                // event keeps that state unambiguous when the device threw before assigning the out parameter.
                loadCompletionEvents[frame] = new CountdownEvent(1);
            }
            catch { }

            try
            {
                loadCTSs[frame]?.Cancel();
            }
            catch { }

            loadedPages[frame] = pageEndAddress;

            try
            {
                logger?.LogError(ex, "Failed to issue page read from storage during scan, skipping page. Frame: {frame}, pageEndAddress: {pageEndAddress}", frame, AddressString(pageEndAddress));
            }
            catch { }
        }

        /// <summary>
        /// Mark <paramref name="frame"/> as having no page load in flight.
        /// </summary>
        private void SignalFrameLoadCompletion(long frame)
        {
            var completionEvent = loadCompletionEvents[frame];
            if (completionEvent is not null && !completionEvent.IsSet)
                _ = completionEvent.Signal();
        }

        protected void AsyncReadPageFromDeviceToFrameCallback(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            try
            {
                var result = (PageAsyncReadResult<Empty>)context;

                if (errorCode == 0)
                    _ = result.handle?.Signal();
                else
                {
                    if (ioException is null)
                        logger?.LogError($"{nameof(AsyncReadPageFromDeviceToFrameCallback)} error: {{errorCode}}", errorCode);
                    else
                        logger?.LogError($"{nameof(AsyncReadPageFromDeviceToFrameCallback)} error: {{exception}}", Utility.GetCallbackExceptionDetail(ioException));
                    result.cts?.Cancel();
                }
            }
            finally
            {
                _ = Interlocked.Decrement(ref pendingDrainCallbacks);
            }
        }

        /// <summary>
        /// Wait for the current frame to complete loading
        /// </summary>
        /// <param name="currentAddress"></param>
        /// <param name="currentFrame"></param>
        /// <returns>True if we had to wait for the current frame load to complete; else false</returns>
        /// <exception cref="TsavoriteException"></exception>
        private bool WaitForFrameLoad(long currentAddress, long currentFrame)
        {
            if (loadCompletionEvents[currentFrame].IsSet)
                return false;

            try
            {
                epoch?.Suspend();
                WaitForFrameLoadCompletion(currentAddress, currentFrame); // Ensure we have completed ongoing load
            }
            catch (Exception e)
            {
                // Exception occurred so skip the page containing the currentAddress, and reinitialize the loaded page and cancellation token for the current frame.
                // The exception may have been an OperationCanceledException.
                // A load that fails asynchronously cancels the token without signaling the completion event, so signal
                // it here to leave the frame reusable.
                SignalFrameLoadCompletion(currentFrame);
                loadedPages[currentFrame] = -1;
                loadCTSs[currentFrame] = new CancellationTokenSource();
                _ = Utility.MonotonicUpdate(ref nextAddress, GetLogicalAddressOfStartOfPage(1 + allocator.GetPageOfAddress(currentAddress, logPageSizeBits), logPageSizeBits), out _);

                // Callers may be looking for an OCE so throw that if it's what we got.
                if (e is OperationCanceledException)
                {
                    logger?.LogWarning(e, "Wait for frame load was canceled, skipping page. CurrentAddress: {currentAddress}, currentFrame: {currentFrame}", AddressString(currentAddress), currentFrame);
                    throw;
                }
                else
                    throw new TsavoriteException("Page read from storage failed, skipping page. Inner exception: " + e.ToString());
            }
            finally
            {
                epoch?.Resume();
            }
            return true;
        }

        /// <summary>
        /// Interval at which an outstanding frame load is reported, so a device that never delivers its completion
        /// callback surfaces as a logged stall rather than a silent wait.
        /// </summary>
        private const int FrameLoadWaitReportIntervalMs = 15_000;

        /// <summary>
        /// Wait for an outstanding page load into <paramref name="currentFrame"/> to complete, reporting periodically
        /// while it remains outstanding.
        /// </summary>
        /// <remarks>
        /// Recovery runs this path before the server accepts connections, so a stall here is otherwise
        /// indistinguishable from a healthy startup. The wait stays unbounded, since a slow device is not a failed one.
        /// </remarks>
        private void WaitForFrameLoadCompletion(long currentAddress, long currentFrame)
        {
            var completionEvent = loadCompletionEvents[currentFrame];
            var token = loadCTSs[currentFrame].Token;
            for (var elapsedMs = 0L; !completionEvent.Wait(FrameLoadWaitReportIntervalMs, token); elapsedMs += FrameLoadWaitReportIntervalMs)
            {
                logger?.LogWarning("Still waiting for page read from storage to complete after {elapsedSeconds} seconds. CurrentAddress: {currentAddress}, currentFrame: {currentFrame}",
                    (elapsedMs + FrameLoadWaitReportIntervalMs) / 1000, AddressString(currentAddress), currentFrame);
            }
        }

        /// <summary>
        /// Dispose iterator
        /// </summary>
        public virtual void Dispose()
        {
            // Wait for all deferred DoReadPage callbacks and their async I/O to complete before freeing
            // resources. The counter is incremented before BumpCurrentEpoch registration and decremented
            // in AsyncReadPageFromDeviceToFrameCallback when I/O completes, so reaching zero guarantees
            // no outstanding access to our state. The deferred callbacks will be drained by other threads'
            // epoch operations (Resume, Suspend, ProtectAndDrain). Report periodically so a device that never
            // delivers a completion shows up as a logged stall.
            var reportIntervalTicks = Stopwatch.Frequency * FrameLoadWaitReportIntervalMs / 1000;
            var startTimestamp = Stopwatch.GetTimestamp();
            var nextReportTimestamp = startTimestamp + reportIntervalTicks;
            while (Volatile.Read(ref pendingDrainCallbacks) > 0)
            {
                // A page read registered on the drain list only runs when some thread drains the epoch. If this
                // thread holds it, drain here rather than wait for another thread that may not exist. A drain action
                // registered by other code may throw, which aborts the pass but not this wait; the remaining actions,
                // including ours, are picked up by the next one.
                if (epoch is not null && epoch.ThisInstanceProtected())
                {
                    try
                    {
                        epoch.ProtectAndDrain();
                    }
                    catch (Exception ex)
                    {
                        logger?.LogWarning(ex, "Draining the epoch while disposing scan iterator failed");
                    }
                }

                Thread.Yield();

                var nowTimestamp = Stopwatch.GetTimestamp();
                if (nowTimestamp < nextReportTimestamp)
                    continue;
                nextReportTimestamp = nowTimestamp + reportIntervalTicks;
                logger?.LogWarning("Still waiting for {pendingDrainCallbacks} pending page read(s) from storage after {elapsedSeconds} seconds while disposing scan iterator",
                    Volatile.Read(ref pendingDrainCallbacks), (nowTimestamp - startTimestamp) / Stopwatch.Frequency);
            }

            for (var i = 0; i < frameSize; i++)
            {
                // Wait for ongoing reads to complete/fail; if the wait throws (e.g. due to cancellation), we still
                // need to dispose the event, CTS, and read buffers below.
                try
                {
                    if (loadCompletionEvents != null && loadedPages[i] != -1)
                        loadCompletionEvents[i]?.Wait(loadCTSs[i].Token);
                }
                catch { }

                // Always dispose resources regardless of whether the wait succeeded.
                loadCompletionEvents?[i]?.Dispose();
                loadCTSs?[i]?.Dispose();
                loadCTSs?[i] = null;

                // Do not null this; we didn't hold onto the hlogBase to recreate. CircularDiskReadBuffer.Dispose() clears
                // things and leaves it in an "initialized" state.
                objectReadBuffers?[i]?.Dispose();
            }
            loadCompletionEvents = default;
        }

        /// <summary>
        /// Reset iterator
        /// </summary>
        public void Reset()
        {
            Dispose();
            loadCompletionEvents = new CountdownEvent[frameSize];
            loadCTSs = new CancellationTokenSource[frameSize];
            loadedPages = new long[frameSize];
            nextLoadedPages = new long[frameSize];
            for (var i = 0; i < frameSize; i++)
            {
                loadedPages[i] = -1;
                nextLoadedPages[i] = -1;
                loadCTSs[i] = new CancellationTokenSource();
                // readBuffers do not need to be reset because that is done in its Dispose, leaving it in an "initialized" state.
                // Also, OnBeginReadRecords() will do reinitialization internally.
            }
            currentAddress = -1;
            nextAddress = beginAddress;
        }

        /// <inheritdoc/>
        public override string ToString() => $"BA {AddressString(BeginAddress)}, EA {AddressString(EndAddress)}, CA {AddressString(CurrentAddress)}, NA {AddressString(NextAddress)}";
    }
}