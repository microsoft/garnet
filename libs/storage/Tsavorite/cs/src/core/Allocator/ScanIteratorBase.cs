// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    using static LogAddress;

    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    public abstract class ScanIteratorBase
    {
        /// <summary>
        /// Frame size
        /// </summary>
        protected readonly int frameSize;

        /// <summary>
        /// Begin address. Cannot be readonly due to SnapCursorToLogicalAddress
        /// </summary>
        protected long beginAddress;

        /// <summary>
        /// End address
        /// </summary>
        protected readonly long endAddress;

        /// <summary>
        /// Epoch
        /// </summary>
        protected readonly LightEpoch epoch;

        /// <summary>
        /// Current and next address for iteration
        /// </summary>
        protected long currentAddress, nextAddress;

        /// <summary>
        /// <see cref="CountdownEvent"/> vector for waiting for frame-load completion.
        /// </summary>
        /// <remarks>This array is in parallel with <see cref="loadCTSs"/>, <see cref="loadedPages"/>, and <see cref="nextLoadedPages"/>.</remarks>
        private CountdownEvent[] loadCompletionEvents;

        /// <summary>
        /// <see cref="CancellationTokenSource"/> vector for canceling the wait for frame-load completion.
        /// </summary>
        /// <remarks>This array is in parallel with <see cref="loadCompletionEvents"/>, <see cref="loadedPages"/>, and <see cref="nextLoadedPages"/>.</remarks>
        private CancellationTokenSource[] loadCTSs;

        /// <summary>
        /// Vector of endAddresses for the currently loaded pages of the frames.
        /// </summary>
        /// <remarks>This array is in parallel with <see cref="loadCompletionEvents"/>, <see cref="loadCTSs"/>, and <see cref="nextLoadedPages"/>.</remarks>
        private long[] loadedPages;

        /// <summary>
        /// Vector of endAddresses for the currently in-flight, and possibly completed, loading of pages of the frames.
        /// This is updated atomically when we start the <see cref="BufferAndLoad"/> of a page.
        /// </summary>
        /// <remarks>This array is in parallel with <see cref="loadCompletionEvents"/>, <see cref="loadCTSs"/>, and <see cref="loadedPages"/>.</remarks>
        private long[] nextLoadedPages;

        private readonly int logPageSizeBits;
        protected readonly bool includeClosedRecords;
        protected readonly bool returnTombstoned;

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
                bool includeClosedRecords, LightEpoch epoch, int logPageSizeBits, bool initForReads = true, ILogger logger = null)
        {
            this.logger = logger;
            this.memScanBufferingMode = memScanBufferingMode;

            // If we are protected when creating the iterator, we do not need per-GetNext protection
            if (epoch != null && !epoch.ThisInstanceProtected())
                this.epoch = epoch;

            this.beginAddress = beginAddress;
            this.endAddress = endAddress;
            this.logPageSizeBits = logPageSizeBits;

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

        /// <summary>
        /// Initialize for reads
        /// </summary>
        public virtual void InitializeForReads()
        {
            loadCompletionEvents = new CountdownEvent[frameSize];
            loadCTSs = new CancellationTokenSource[frameSize];
            loadedPages = new long[frameSize];
            nextLoadedPages = new long[frameSize];
            for (int i = 0; i < frameSize; i++)
            {
                loadedPages[i] = -1;
                nextLoadedPages[i] = -1;
                loadCTSs[i] = new CancellationTokenSource();
            }
            currentAddress = -1;
            nextAddress = beginAddress;
        }

        /// <summary>
        /// Buffer and load
        /// </summary>
        /// <param name="currentAddress">The current logical address</param>
        /// <param name="currentPage">The page containing the current logical address</param>
        /// <param name="currentFrame">The frame index of the current page (the page modulo the number of frames)</param>
        /// <param name="headAddress">Head address of the log</param>
        /// <param name="endAddress">Address to stop the scan at</param>
        /// <returns>True we had to await the event here; </returns>
        /// <returns></returns>
        protected unsafe bool BufferAndLoad(long currentAddress, long currentPage, long currentFrame, long headAddress, long endAddress)
        {
            for (int i = 0; i < frameSize; i++)
            {
                var nextPage = currentPage + i;

                // Convert to absolute addresses as we are going to disk. The LogAddress methods (GetPage, etc.) work with both absolute and AddressType-prefixed addresses.
                currentAddress = AbsoluteAddress(currentAddress);
                headAddress = AbsoluteAddress(headAddress);
                endAddress = AbsoluteAddress(endAddress);

                // Cannot load page if it is entirely in memory or beyond the end address
                var pageStartAddress = GetStartAbsoluteLogicalAddressOfPage(nextPage, logPageSizeBits);
                if (pageStartAddress >= headAddress || pageStartAddress >= endAddress)
                    continue;

                var pageEndAddress = GetStartAbsoluteLogicalAddressOfPage(nextPage + 1, logPageSizeBits);
                if (endAddress < pageEndAddress)
                    pageEndAddress = endAddress;
                if (headAddress < pageEndAddress)
                    pageEndAddress = headAddress;

                var nextFrame = (currentFrame + i) % frameSize;

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
                        var tmp_page = i;
                        if (epoch != null)
                        {
                            epoch.BumpCurrentEpoch(() =>
                            {
                                AsyncReadPagesFromDeviceToFrame(tmp_page + GetPage(currentAddress, logPageSizeBits), 1, endAddress, Empty.Default, out loadCompletionEvents[nextFrame], 0, null, null, loadCTSs[nextFrame]);
                                loadedPages[nextFrame] = pageEndAddress;
                            });
                        }
                        else
                        {
                            AsyncReadPagesFromDeviceToFrame(tmp_page + GetPage(currentAddress, logPageSizeBits), 1, endAddress, Empty.Default, out loadCompletionEvents[nextFrame], 0, null, null, loadCTSs[nextFrame]);
                            loadedPages[nextFrame] = pageEndAddress;
                        }
                    }
                    else
                    {
                        // Someone else already incremented nextLoadedPage[nextFrame], so give them a chance to work, then try again.
                        epoch?.ProtectAndDrain();
                    }
                }
            }
            return WaitForFrameLoad(currentAddress, currentFrame);
        }

        /// <summary>
        /// Whether we need to buffer new page from disk
        /// </summary>
        protected unsafe bool NeedBufferAndLoad(long currentAddress, long currentPage, long currentFrame, long headAddress, long endAddress)
        {
            for (int i = 0; i < frameSize; i++)
            {
                var nextPage = currentPage + i;

                var pageStartAddress = GetStartAbsoluteLogicalAddressOfPage(nextPage, logPageSizeBits);

                // Cannot load page if it is entirely in memory or beyond the end address
                if (pageStartAddress >= headAddress || pageStartAddress >= endAddress)
                    continue;

                var pageEndAddress = GetStartAbsoluteLogicalAddressOfPage(nextPage + 1, logPageSizeBits);
                if (endAddress < pageEndAddress)
                    pageEndAddress = endAddress;
                if (headAddress < pageEndAddress)
                    pageEndAddress = headAddress;

                var nextFrame = (currentFrame + i) % frameSize;

                if (nextLoadedPages[nextFrame] < pageEndAddress || loadedPages[nextFrame] < pageEndAddress)
                    return true;
            }
            return false;
        }

        internal abstract void AsyncReadPagesFromDeviceToFrame<TContext>(long readPageStart, int numPages, long untilAddress, TContext context, out CountdownEvent completed,
                long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null);

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
                loadCompletionEvents[currentFrame].Wait(loadCTSs[currentFrame].Token); // Ensure we have completed ongoing load
            }
            catch (Exception e)
            {
                // Exception occurred so skip the page containing the currentAddress, and reinitialize the loaded page and cancellation token for the current frame.
                // The exception may have been an OperationCanceledException.
                loadedPages[currentFrame] = -1;
                loadCTSs[currentFrame] = new CancellationTokenSource();
                Utility.MonotonicUpdate(ref nextAddress, GetStartAbsoluteLogicalAddressOfPage(1 + GetPage(currentAddress, logPageSizeBits), logPageSizeBits), out _);
                throw new TsavoriteException("Page read from storage failed, skipping page. Inner exception: " + e.ToString());
            }
            finally
            {
                epoch?.Resume();
            }
            return true;
        }

        /// <summary>
        /// Dispose iterator
        /// </summary>
        public virtual void Dispose()
        {
            if (loadCompletionEvents != null)
            {
                // Wait for ongoing reads to complete/fail
                for (int i = 0; i < frameSize; i++)
                {
                    if (loadedPages[i] != -1)
                    {
                        try
                        {
                            loadCompletionEvents[i].Wait(loadCTSs[i].Token);
                        }
                        catch { }
                    }
                }
            }
        }

        /// <summary>
        /// Reset iterator
        /// </summary>
        public void Reset()
        {
            loadCompletionEvents = new CountdownEvent[frameSize];
            loadCTSs = new CancellationTokenSource[frameSize];
            loadedPages = new long[frameSize];
            nextLoadedPages = new long[frameSize];
            for (int i = 0; i < frameSize; i++)
            {
                loadedPages[i] = -1;
                nextLoadedPages[i] = -1;
                loadCTSs[i] = new CancellationTokenSource();
            }
            currentAddress = -1;
            nextAddress = beginAddress;
        }

        /// <inheritdoc/>
        public override string ToString() => $"BA {AddressString(BeginAddress)}, EA {AddressString(EndAddress)}, CA {AddressString(CurrentAddress)}, NA {AddressString(NextAddress)}";
    }
}