// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    public sealed class BlittableScanIterator<TKey, TValue, TStoreFunctions> : ScanIteratorBase, ITsavoriteScanIterator<TKey, TValue>, IPushScanIterator<TKey>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
    {
        private readonly TsavoriteKV<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> store;
        private readonly BlittableAllocatorImpl<TKey, TValue, TStoreFunctions> hlog;
        private readonly BlittableFrame frame;
        private readonly bool forceInMemory;

        private TKey currentKey;
        private TValue currentValue;
        private long framePhysicalAddress;

        /// <summary>
        /// Constructor for use with head-to-tail scan
        /// </summary>
        /// <param name="store"></param>
        /// <param name="hlog">The fully derived log implementation</param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <param name="includeClosedRecords"></param>
        /// <param name="epoch">Epoch to use for protection; may be null if <paramref name="forceInMemory"/> is true.</param>
        /// <param name="forceInMemory">Provided address range is known by caller to be in memory, even if less than HeadAddress</param>
        /// <param name="logger"></param>
        internal BlittableScanIterator(TsavoriteKV<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> store, BlittableAllocatorImpl<TKey, TValue, TStoreFunctions> hlog,
                long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode, bool includeClosedRecords, LightEpoch epoch, bool forceInMemory = false, ILogger logger = null)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddress(0) : beginAddress, endAddress, scanBufferingMode, includeClosedRecords, epoch, hlog.LogPageSizeBits, logger: logger)
        {
            this.store = store;
            this.hlog = hlog;
            this.forceInMemory = forceInMemory;
            if (frameSize > 0)
                frame = new BlittableFrame(frameSize, hlog.PageSize, hlog.GetDeviceSectorSize());
        }

        /// <summary>
        /// Constructor for use with tail-to-head push iteration of the passed key's record versions
        /// </summary>
        internal BlittableScanIterator(TsavoriteKV<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> store, BlittableAllocatorImpl<TKey, TValue, TStoreFunctions> hlog,
                long beginAddress, LightEpoch epoch, ILogger logger = null)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddress(0) : beginAddress, hlog.GetTailAddress(), ScanBufferingMode.SinglePageBuffering, false, epoch, hlog.LogPageSizeBits, logger: logger)
        {
            this.store = store;
            this.hlog = hlog;
            forceInMemory = false;
            if (frameSize > 0)
                frame = new BlittableFrame(frameSize, hlog.PageSize, hlog.GetDeviceSectorSize());
        }

        /// <summary>
        /// Get a reference to the current key
        /// </summary>
        public ref TKey GetKey() => ref framePhysicalAddress != 0 ? ref hlog._wrapper.GetKey(framePhysicalAddress) : ref currentKey;

        /// <summary>
        /// Get a reference to the current value
        /// </summary>
        public ref TValue GetValue() => ref framePhysicalAddress != 0 ? ref hlog._wrapper.GetValue(framePhysicalAddress) : ref currentValue;

        /// <inheritdoc/>
        public bool SnapCursorToLogicalAddress(ref long cursor)
        {
            Debug.Assert(currentAddress == -1, "SnapCursorToLogicalAddress must be called before GetNext()");
            beginAddress = nextAddress = hlog.SnapToFixedLengthLogicalAddressBoundary(ref cursor, BlittableAllocatorImpl<TKey, TValue, TStoreFunctions>.RecordSize);
            return true;
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        public unsafe bool GetNext(out RecordInfo recordInfo)
        {
            recordInfo = default;

            while (true)
            {
                currentAddress = nextAddress;
                var stopAddress = endAddress < hlog.GetTailAddress() ? endAddress : hlog.GetTailAddress();
                if (currentAddress >= stopAddress)
                    return false;

                epoch?.Resume();
                var headAddress = hlog.HeadAddress;

                if (currentAddress < hlog.BeginAddress && !forceInMemory)
                    currentAddress = hlog.BeginAddress;

                // If currentAddress < headAddress and we're not buffering and not guaranteeing the records are in memory, fail.
                if (frameSize == 0 && currentAddress < headAddress && !forceInMemory)
                {
                    epoch?.Suspend();
                    throw new TsavoriteException("Iterator address is less than log HeadAddress in memory-scan mode");
                }

                var currentPage = currentAddress >> hlog.LogPageSizeBits;
                var offset = currentAddress & hlog.PageSizeMask;

                if (currentAddress < headAddress && !forceInMemory)
                    BufferAndLoad(currentAddress, currentPage, currentPage % frameSize, headAddress, stopAddress);

                long physicalAddress = GetPhysicalAddress(currentAddress, headAddress, currentPage, offset);
                var recordSize = hlog._wrapper.GetRecordSize(physicalAddress).Item2;

                // If record does not fit on page, skip to the next page.
                if ((currentAddress & hlog.PageSizeMask) + recordSize > hlog.PageSize)
                {
                    nextAddress = (1 + (currentAddress >> hlog.LogPageSizeBits)) << hlog.LogPageSizeBits;
                    epoch?.Suspend();
                    continue;
                }

                nextAddress = currentAddress + recordSize;

                recordInfo = hlog._wrapper.GetInfo(physicalAddress);
                bool skipOnScan = includeClosedRecords ? false : recordInfo.SkipOnScan;
                if (skipOnScan || recordInfo.IsNull())
                {
                    epoch?.Suspend();
                    continue;
                }

                OperationStackContext<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> stackCtx = default;
                try
                {
                    // Lock to ensure no value tearing while copying to temp storage. We cannot use GetKey() because it has not yet been set.
                    if (currentAddress >= headAddress && store is not null)
                        store.LockForScan(ref stackCtx, ref hlog._wrapper.GetKey(physicalAddress));
                    _ = CopyDataMembers(physicalAddress);
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
        }

        /// <summary>
        /// Get previous record and keep the epoch held while we call the user's scan functions
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        bool IPushScanIterator<TKey>.BeginGetPrevInMemory(ref TKey key, out RecordInfo recordInfo, out bool continueOnDisk)
        {
            recordInfo = default;
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
                var headAddress = hlog.HeadAddress;

                var currentPage = currentAddress >> hlog.LogPageSizeBits;
                var offset = currentAddress & hlog.PageSizeMask;

                long physicalAddress = GetPhysicalAddress(currentAddress, headAddress, currentPage, offset);

                recordInfo = hlog._wrapper.GetInfo(physicalAddress);
                nextAddress = recordInfo.PreviousAddress;

                // Do not SkipOnScan here; we Seal previous versions.
                if (recordInfo.IsNull() || !hlog._storeFunctions.KeysEqual(ref hlog._wrapper.GetKey(physicalAddress), ref key))
                {
                    epoch?.Suspend();
                    continue;
                }

                // Success; defer epoch?.Suspend(); to EndGetPrevInMemory
                return CopyDataMembers(physicalAddress);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IPushScanIterator<TKey>.EndGetPrevInMemory()
        {
            epoch?.Suspend();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetPhysicalAddress(long currentAddress, long headAddress, long currentPage, long offset)
        {
            if (currentAddress >= headAddress)
            {
                // physicalAddress is in memory; set framePhysicalAddress to 0 so we'll set currentKey and currentValue from physicalAddress below
                framePhysicalAddress = 0;
                return hlog.GetPhysicalAddress(currentAddress);
            }

            // physicalAddress is not in memory, so we'll GetKey and GetValue will use framePhysicalAddress
            return framePhysicalAddress = frame.GetPhysicalAddress(currentPage % frameSize, offset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CopyDataMembers(long physicalAddress)
        {
            if (framePhysicalAddress == 0)
            {
                // Copy the values from the log to data members so we have no ref into the log after the epoch.Suspend().
                currentKey = hlog._wrapper.GetKey(physicalAddress);
                currentValue = hlog._wrapper.GetValue(physicalAddress);
            }
            return true;
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        public bool GetNext(out RecordInfo recordInfo, out TKey key, out TValue value)
        {
            if (GetNext(out recordInfo))
            {
                key = GetKey();
                value = GetValue();
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
                BlittableAllocatorImpl<TKey, TValue, TStoreFunctions>.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                result.freeBuffer1.Return();
                result.freeBuffer1 = null;
            }

            if (errorCode == 0)
                result.handle?.Signal();

            Interlocked.MemoryBarrier();
        }
    }
}