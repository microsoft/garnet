// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    internal sealed class GenericScanIterator<TKey, TValue, TStoreFunctions> : ScanIteratorBase, ITsavoriteScanIterator<TKey, TValue>, IPushScanIterator<TKey>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
    {
        private readonly TsavoriteKV<TKey, TValue, TStoreFunctions, GenericAllocator<TKey, TValue, TStoreFunctions>> store;
        private readonly GenericAllocatorImpl<TKey, TValue, TStoreFunctions> hlog;
        private readonly GenericFrame<TKey, TValue> frame;
        private readonly int recordSize;

        private TKey currentKey;
        private TValue currentValue;

        private long currentPage = -1, currentOffset = -1, currentFrame = -1;

        /// <summary>
        /// Constructor
        /// </summary>
        public GenericScanIterator(TsavoriteKV<TKey, TValue, TStoreFunctions, GenericAllocator<TKey, TValue, TStoreFunctions>> store, GenericAllocatorImpl<TKey, TValue, TStoreFunctions> hlog,
                long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode, bool includeClosedRecords, LightEpoch epoch, ILogger logger = null)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddress(0) : beginAddress, endAddress, scanBufferingMode, includeClosedRecords, epoch, hlog.LogPageSizeBits, logger: logger)
        {
            this.store = store;
            this.hlog = hlog;
            recordSize = hlog.GetRecordSize(0).allocatedSize;
            if (frameSize > 0)
                frame = new GenericFrame<TKey, TValue>(frameSize, hlog.PageSize);
        }

        /// <summary>
        /// Constructor for use with tail-to-head push iteration of the passed key's record versions
        /// </summary>
        public GenericScanIterator(TsavoriteKV<TKey, TValue, TStoreFunctions, GenericAllocator<TKey, TValue, TStoreFunctions>> store, GenericAllocatorImpl<TKey, TValue, TStoreFunctions> hlog,
                long beginAddress, LightEpoch epoch, ILogger logger = null)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddress(0) : beginAddress, hlog.GetTailAddress(), ScanBufferingMode.SinglePageBuffering, false, epoch, hlog.LogPageSizeBits, logger: logger)
        {
            this.store = store;
            this.hlog = hlog;
            recordSize = hlog.GetRecordSize(0).allocatedSize;
            if (frameSize > 0)
                frame = new GenericFrame<TKey, TValue>(frameSize, hlog.PageSize);
        }

        /// <summary>
        /// Gets reference to current key
        /// </summary>
        /// <returns></returns>
        public ref TKey GetKey() => ref currentKey;

        /// <summary>
        /// Gets reference to current value
        /// </summary>
        /// <returns></returns>
        public ref TValue GetValue() => ref currentValue;

        /// <inheritdoc/>
        public bool SnapCursorToLogicalAddress(ref long cursor)
        {
            Debug.Assert(currentAddress == -1, "SnapCursorToLogicalAddress must be called before GetNext()");
            beginAddress = nextAddress = hlog.SnapToFixedLengthLogicalAddressBoundary(ref cursor, GenericAllocatorImpl<TKey, TValue, TStoreFunctions>.RecordSize);
            return true;
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        public unsafe bool GetNext(out RecordInfo recordInfo)
        {
            recordInfo = default;
            currentKey = default;
            currentValue = default;
            currentPage = currentOffset = currentFrame = -1;

            while (true)
            {
                currentAddress = nextAddress;
                var stopAddress = endAddress < hlog.GetTailAddress() ? endAddress : hlog.GetTailAddress();
                if (currentAddress >= stopAddress)
                    return false;

                epoch?.Resume();
                try
                {
                    var headAddress = hlog.HeadAddress;

                    if (currentAddress < hlog.BeginAddress)
                        currentAddress = hlog.BeginAddress;

                    // If currentAddress < headAddress and we're not buffering, fail.
                    if (frameSize == 0 && currentAddress < headAddress)
                    {
                        throw new TsavoriteException("Iterator address is less than log HeadAddress in memory-scan mode");
                    }

                    currentPage = currentAddress >> hlog.LogPageSizeBits;
                    currentOffset = (currentAddress & hlog.PageSizeMask) / recordSize;

                    if (currentAddress < headAddress)
                        _ = BufferAndLoad(currentAddress, currentPage, currentPage % frameSize, headAddress, stopAddress);

                    // Check if record fits on page, if not skip to next page
                    if ((currentAddress & hlog.PageSizeMask) + recordSize > hlog.PageSize)
                    {
                        nextAddress = (1 + (currentAddress >> hlog.LogPageSizeBits)) << hlog.LogPageSizeBits;
                        continue;
                    }

                    nextAddress = currentAddress + recordSize;

                    if (currentAddress >= headAddress)
                    {
                        // Read record from cached page memory
                        currentPage %= hlog.BufferSize;
                        currentFrame = -1;      // Frame is not used in this case.

                        recordInfo = hlog.values[currentPage][currentOffset].info;
                        bool _skipOnScan = includeClosedRecords ? false : recordInfo.SkipOnScan;
                        if (_skipOnScan || recordInfo.IsNull())
                        {
                            continue;
                        }

                        // Copy the object values from cached page memory to data members; we have no ref into the log after the epoch.Suspend().
                        // These are pointer-sized shallow copies but we need to lock to ensure no value tearing inside the object while copying to temp storage.
                        OperationStackContext<TKey, TValue, TStoreFunctions, GenericAllocator<TKey, TValue, TStoreFunctions>> stackCtx = default;
                        try
                        {
                            // We cannot use GetKey() because it has not yet been set.
                            if (currentAddress >= headAddress && store is not null)
                                store.LockForScan(ref stackCtx, ref hlog.values[currentPage][currentOffset].key);

                            recordInfo = hlog.values[currentPage][currentOffset].info;
                            currentKey = hlog.values[currentPage][currentOffset].key;
                            currentValue = hlog.values[currentPage][currentOffset].value;
                        }
                        finally
                        {
                            if (stackCtx.recSrc.HasLock)
                                store.UnlockForScan(ref stackCtx);
                        }

                        // Success
                        return true;
                    }

                    currentFrame = currentPage % frameSize;
                    recordInfo = frame.GetInfo(currentFrame, currentOffset);
                    bool skipOnScan = includeClosedRecords ? false : recordInfo.SkipOnScan;
                    if (skipOnScan || recordInfo.IsNull())
                    {
                        continue;
                    }

                    // Copy the object values from the frame to data members.
                    currentKey = frame.GetKey(currentFrame, currentOffset);
                    currentValue = frame.GetValue(currentFrame, currentOffset);
                    currentPage = currentOffset = -1;
                }
                finally
                {
                    // Success
                    epoch?.Suspend();
                }
                return true;
            }
        }

        /// <summary>
        /// Get previous record and keep the epoch held while we call the user's scan functions
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        bool IPushScanIterator<TKey>.BeginGetPrevInMemory(ref TKey key, out RecordInfo recordInfo, out bool continueOnDisk)
        {
            recordInfo = default;
            currentKey = default;
            currentValue = default;
            currentPage = currentOffset = currentFrame = -1;
            continueOnDisk = false;

            while (true)
            {
                // "nextAddress" is reused as "previous address" for this operation.
                currentAddress = nextAddress;
                if (currentAddress < hlog.HeadAddress)
                {
                    continueOnDisk = currentAddress >= hlog.BeginAddress;
                    return false;
                }

                epoch?.Resume();

                currentPage = currentAddress >> hlog.LogPageSizeBits;
                currentOffset = (currentAddress & hlog.PageSizeMask) / recordSize;

                // Read record from cached page memory
                currentPage %= hlog.BufferSize;
                currentFrame = -1;      // Frame is not used in this case.

                recordInfo = hlog.values[currentPage][currentOffset].info;
                nextAddress = currentAddress + recordSize;

                bool skipOnScan = includeClosedRecords ? false : recordInfo.SkipOnScan;
                if (skipOnScan || recordInfo.IsNull() || !hlog._storeFunctions.KeysEqual(ref hlog.values[currentPage][currentOffset].key, ref key))
                {
                    epoch?.Suspend();
                    continue;
                }

                // Copy the object values from cached page memory to data members; we have no ref into the log after the epoch.Suspend().
                // These are pointer-sized shallow copies.
                recordInfo = hlog.values[currentPage][currentOffset].info;
                currentKey = hlog.values[currentPage][currentOffset].key;
                currentValue = hlog.values[currentPage][currentOffset].value;

                // Success; defer epoch?.Suspend(); to EndGet
                return true;
            }
        }

        bool IPushScanIterator<TKey>.EndGetPrevInMemory()
        {
            epoch?.Suspend();
            return true;
        }

        /// <summary>
        /// Get next record using iterator
        /// </summary>
        /// <param name="recordInfo"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool GetNext(out RecordInfo recordInfo, out TKey key, out TValue value)
        {
            if (GetNext(out recordInfo))
            {
                key = currentKey;
                value = currentValue;
                return true;
            }

            key = default;
            value = default;
            return false;
        }

        /// <summary>
        /// Dispose iterator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            frame?.Dispose();
        }

        internal override void AsyncReadPagesFromDeviceToFrame<TContext>(long readPageStart, int numPages, long untilAddress, TContext context, out CountdownEvent completed,
                long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null)
            => hlog.AsyncReadPagesFromDeviceToFrame(readPageStart, numPages, untilAddress, AsyncReadPagesCallback, context, frame, out completed, devicePageOffset, device, objectLogDevice);

        private unsafe void AsyncReadPagesCallback(uint errorCode, uint numBytes, object context)
        {
            var result = (PageAsyncReadResult<Empty>)context;

            if (errorCode != 0)
            {
                logger?.LogError($"{nameof(AsyncReadPagesCallback)} error: {{errorCode}}", errorCode);
                result.cts?.Cancel();
            }

            if (result.freeBuffer1 != null)
            {
                hlog.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, ref frame.GetPage(result.page % frame.frameSize));
                result.freeBuffer1.Return();
                result.freeBuffer1 = null;
            }

            if (errorCode == 0)
                _ = result.handle?.Signal();

            Interlocked.MemoryBarrier();
        }
    }
}