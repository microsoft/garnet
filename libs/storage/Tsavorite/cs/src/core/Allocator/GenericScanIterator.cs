﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    internal sealed class GenericScanIterator<Key, Value, TStoreFunctions> : ScanIteratorBase, ITsavoriteScanIterator<Key, Value>, IPushScanIterator<Key>
        where TStoreFunctions : IStoreFunctions<Key, Value>
    {
        private readonly TsavoriteKV<Key, Value, TStoreFunctions, GenericAllocator<Key, Value, TStoreFunctions>> store;
        private readonly GenericAllocatorImpl<Key, Value, TStoreFunctions> hlog;
        private readonly GenericFrame<Key, Value> frame;
        private readonly int recordSize;

        private Key currentKey;
        private Value currentValue;

        private long currentPage = -1, currentOffset = -1, currentFrame = -1;

        /// <summary>
        /// Constructor
        /// </summary>
        public GenericScanIterator(TsavoriteKV<Key, Value, TStoreFunctions, GenericAllocator<Key, Value, TStoreFunctions>> store, GenericAllocatorImpl<Key, Value, TStoreFunctions> hlog,
                long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode, bool includeSealedRecords, LightEpoch epoch, ILogger logger = null)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddress(0) : beginAddress, endAddress, scanBufferingMode, includeSealedRecords, epoch, hlog.LogPageSizeBits, logger: logger)
        {
            this.store = store;
            this.hlog = hlog;
            recordSize = hlog.GetRecordSize(0).allocatedSize;
            if (frameSize > 0)
                frame = new GenericFrame<Key, Value>(frameSize, hlog.PageSize);
        }

        /// <summary>
        /// Constructor for use with tail-to-head push iteration of the passed key's record versions
        /// </summary>
        public GenericScanIterator(TsavoriteKV<Key, Value, TStoreFunctions, GenericAllocator<Key, Value, TStoreFunctions>> store, GenericAllocatorImpl<Key, Value, TStoreFunctions> hlog,
                long beginAddress, LightEpoch epoch, ILogger logger = null)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddress(0) : beginAddress, hlog.GetTailAddress(), ScanBufferingMode.SinglePageBuffering, false, epoch, hlog.LogPageSizeBits, logger: logger)
        {
            this.store = store;
            this.hlog = hlog;
            recordSize = hlog.GetRecordSize(0).allocatedSize;
            if (frameSize > 0)
                frame = new GenericFrame<Key, Value>(frameSize, hlog.PageSize);
        }

        /// <summary>
        /// Gets reference to current key
        /// </summary>
        /// <returns></returns>
        public ref Key GetKey() => ref currentKey;

        /// <summary>
        /// Gets reference to current value
        /// </summary>
        /// <returns></returns>
        public ref Value GetValue() => ref currentValue;

        /// <inheritdoc/>
        public bool SnapCursorToLogicalAddress(ref long cursor)
        {
            Debug.Assert(currentAddress == -1, "SnapCursorToLogicalAddress must be called before GetNext()");
            beginAddress = nextAddress = hlog.SnapToFixedLengthLogicalAddressBoundary(ref cursor, GenericAllocatorImpl<Key, Value, TStoreFunctions>.RecordSize);
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
                var headAddress = hlog.HeadAddress;

                if (currentAddress < hlog.BeginAddress)
                    currentAddress = hlog.BeginAddress;

                // If currentAddress < headAddress and we're not buffering, fail.
                if (frameSize == 0 && currentAddress < headAddress)
                {
                    epoch?.Suspend();
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
                    epoch?.Suspend();
                    continue;
                }

                nextAddress = currentAddress + recordSize;

                if (currentAddress >= headAddress)
                {
                    // Read record from cached page memory
                    currentPage %= hlog.BufferSize;
                    currentFrame = -1;      // Frame is not used in this case.

                    recordInfo = hlog.values[currentPage][currentOffset].info;
                    bool _skipOnScan = includeSealedRecords ? recordInfo.Invalid : recordInfo.SkipOnScan;
                    if (_skipOnScan)
                    {
                        epoch?.Suspend();
                        continue;
                    }

                    // Copy the object values from cached page memory to data members; we have no ref into the log after the epoch.Suspend().
                    // These are pointer-sized shallow copies but we need to lock to ensure no value tearing inside the object while copying to temp storage.
                    OperationStackContext<Key, Value, TStoreFunctions, GenericAllocator<Key, Value, TStoreFunctions>> stackCtx = default;
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
                    epoch?.Suspend();
                    return true;
                }

                currentFrame = currentPage % frameSize;
                recordInfo = frame.GetInfo(currentFrame, currentOffset);
                bool skipOnScan = includeSealedRecords ? recordInfo.Invalid : recordInfo.SkipOnScan;
                if (skipOnScan || recordInfo.IsNull())
                {
                    epoch?.Suspend();
                    continue;
                }

                // Copy the object values from the frame to data members.
                currentKey = frame.GetKey(currentFrame, currentOffset);
                currentValue = frame.GetValue(currentFrame, currentOffset);
                currentPage = currentOffset = -1;

                // Success
                epoch?.Suspend();
                return true;
            }
        }

        /// <summary>
        /// Get previous record and keep the epoch held while we call the user's scan functions
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        bool IPushScanIterator<Key>.BeginGetPrevInMemory(ref Key key, out RecordInfo recordInfo, out bool continueOnDisk)
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

                bool skipOnScan = includeSealedRecords ? recordInfo.Invalid : recordInfo.SkipOnScan;
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

        bool IPushScanIterator<Key>.EndGetPrevInMemory()
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
        public bool GetNext(out RecordInfo recordInfo, out Key key, out Value value)
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