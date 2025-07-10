// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
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

        private CountdownEvent[] loaded;
        private CancellationTokenSource[] loadedCancel;
        private long[] loadedPage;
        private long[] nextLoadedPage;
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
        /// Constructor
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <param name="includeClosedRecords"></param>
        /// <param name="epoch"></param>
        /// <param name="logPageSizeBits"></param>
        /// <param name="initForReads"></param>
        /// <param name="logger"></param>
        public unsafe ScanIteratorBase(long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode, bool includeClosedRecords, LightEpoch epoch, int logPageSizeBits, bool initForReads = true, ILogger logger = null)
        {
            this.logger = logger;
            // If we are protected when creating the iterator, we do not need per-GetNext protection
            if (epoch != null && !epoch.ThisInstanceProtected())
                this.epoch = epoch;

            this.beginAddress = beginAddress;
            this.endAddress = endAddress;
            this.logPageSizeBits = logPageSizeBits;

            this.includeClosedRecords = includeClosedRecords;
            currentAddress = -1;
            nextAddress = beginAddress;

            if (scanBufferingMode == ScanBufferingMode.SinglePageBuffering)
                frameSize = 1;
            else if (scanBufferingMode == ScanBufferingMode.DoublePageBuffering)
                frameSize = 2;
            else if (scanBufferingMode == ScanBufferingMode.NoBuffering)
            {
                frameSize = 0;
                return;
            }
            if (initForReads) InitializeForReads();
        }

        /// <summary>
        /// Initialize for reads
        /// </summary>
        public virtual void InitializeForReads()
        {
            loaded = new CountdownEvent[frameSize];
            loadedCancel = new CancellationTokenSource[frameSize];
            loadedPage = new long[frameSize];
            nextLoadedPage = new long[frameSize];
            for (int i = 0; i < frameSize; i++)
            {
                loadedPage[i] = -1;
                nextLoadedPage[i] = -1;
                loadedCancel[i] = new CancellationTokenSource();
            }
            currentAddress = -1;
            nextAddress = beginAddress;
        }

        /// <summary>
        /// Buffer and load
        /// </summary>
        /// <param name="currentAddress"></param>
        /// <param name="currentPage"></param>
        /// <param name="currentFrame"></param>
        /// <param name="headAddress"></param>
        /// <param name="endAddress"></param>
        /// <returns></returns>
        protected unsafe bool BufferAndLoad(long currentAddress, long currentPage, long currentFrame, long headAddress, long endAddress)
        {
            for (int i = 0; i < frameSize; i++)
            {
                var nextPage = currentPage + i;

                var pageStartAddress = nextPage << logPageSizeBits;
                // Cannot load page if it is entirely in memory or beyond the end address
                if (pageStartAddress >= headAddress || pageStartAddress >= endAddress)
                    continue;

                var pageEndAddress = (nextPage + 1) << logPageSizeBits;
                if (endAddress < pageEndAddress)
                    pageEndAddress = endAddress;
                if (headAddress < pageEndAddress)
                    pageEndAddress = headAddress;

                var nextFrame = (currentFrame + i) % frameSize;

                long val;
                while ((val = nextLoadedPage[nextFrame]) < pageEndAddress || loadedPage[nextFrame] < pageEndAddress)
                {
                    if (val < pageEndAddress && Interlocked.CompareExchange(ref nextLoadedPage[nextFrame], pageEndAddress, val) == val)
                    {
                        var tmp_i = i;
                        if (epoch != null)
                        {
                            epoch.BumpCurrentEpoch(() =>
                            {
                                AsyncReadPagesFromDeviceToFrame(tmp_i + (currentAddress >> logPageSizeBits), 1, endAddress, Empty.Default, out loaded[nextFrame], 0, null, null, loadedCancel[nextFrame]);
                                loadedPage[nextFrame] = pageEndAddress;
                            });
                        }
                        else
                        {
                            AsyncReadPagesFromDeviceToFrame(tmp_i + (currentAddress >> logPageSizeBits), 1, endAddress, Empty.Default, out loaded[nextFrame], 0, null, null, loadedCancel[nextFrame]);
                            loadedPage[nextFrame] = pageEndAddress;
                        }
                    }
                    else
                        epoch?.ProtectAndDrain();
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

                var pageStartAddress = nextPage << logPageSizeBits;

                // Cannot load page if it is entirely in memory or beyond the end address
                if (pageStartAddress >= headAddress || pageStartAddress >= endAddress)
                    continue;

                var pageEndAddress = (nextPage + 1) << logPageSizeBits;
                if (endAddress < pageEndAddress)
                    pageEndAddress = endAddress;
                if (headAddress < pageEndAddress)
                    pageEndAddress = headAddress;

                var nextFrame = (currentFrame + i) % frameSize;

                if (nextLoadedPage[nextFrame] < pageEndAddress || loadedPage[nextFrame] < pageEndAddress)
                    return true;
            }
            return false;
        }

        internal abstract void AsyncReadPagesFromDeviceToFrame<TContext>(long readPageStart, int numPages, long untilAddress, TContext context, out CountdownEvent completed,
                long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null);

        private bool WaitForFrameLoad(long currentAddress, long currentFrame)
        {
            if (loaded[currentFrame].IsSet)
                return false;

            try
            {
                epoch?.Suspend();
                loaded[currentFrame].Wait(loadedCancel[currentFrame].Token); // Ensure we have completed ongoing load
            }
            catch (Exception e)
            {
                loadedPage[currentFrame] = -1;
                loadedCancel[currentFrame] = new CancellationTokenSource();
                Utility.MonotonicUpdate(ref nextAddress, (1 + (currentAddress >> logPageSizeBits)) << logPageSizeBits, out _);
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
            if (loaded != null)
            {
                // Wait for ongoing reads to complete/fail
                for (int i = 0; i < frameSize; i++)
                {
                    if (loadedPage[i] != -1)
                    {
                        try
                        {
                            loaded[i].Wait(loadedCancel[i].Token);
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
            loaded = new CountdownEvent[frameSize];
            loadedCancel = new CancellationTokenSource[frameSize];
            loadedPage = new long[frameSize];
            nextLoadedPage = new long[frameSize];
            for (int i = 0; i < frameSize; i++)
            {
                loadedPage[i] = -1;
                nextLoadedPage[i] = -1;
                loadedCancel[i] = new CancellationTokenSource();
            }
            currentAddress = -1;
            nextAddress = beginAddress;
        }

        /// <inheritdoc/>
        public override string ToString() => $"BA {BeginAddress}, EA {EndAddress}, CA {CurrentAddress}, NA {NextAddress}";
    }
}