// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    public sealed class SpanByteScanIterator<TStoreFunctions> : ScanIteratorBase, ITsavoriteScanIterator<SpanByte, SpanByte>, IPushScanIterator<SpanByte>
        where TStoreFunctions : IStoreFunctions<SpanByte, SpanByte>
    {
        private readonly TsavoriteKV<SpanByte, SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store;
        private readonly SpanByteAllocatorImpl<TStoreFunctions> hlog;
        private readonly BlittableFrame frame;

        private SectorAlignedMemory memory;
        private readonly bool forceInMemory;

        private long currentPhysicalAddress;

        /// <summary>
        /// Constructor
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
        internal SpanByteScanIterator(TsavoriteKV<SpanByte, SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store, SpanByteAllocatorImpl<TStoreFunctions> hlog,
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
        internal SpanByteScanIterator(TsavoriteKV<SpanByte, SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store, SpanByteAllocatorImpl<TStoreFunctions> hlog,
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
        /// Gets reference to current key
        /// </summary>
        public ref SpanByte GetKey() => ref hlog._wrapper.GetKey(currentPhysicalAddress);

        /// <summary>
        /// Gets reference to current value
        /// </summary>
        public ref SpanByte GetValue() => ref hlog.GetValue(currentPhysicalAddress);

        /// <inheritdoc/>
        public bool SnapCursorToLogicalAddress(ref long cursor)
        {
            Debug.Assert(currentAddress == -1, "SnapCursorToLogicalAddress must be called before GetNext()");
            Debug.Assert(nextAddress == cursor, "SnapCursorToLogicalAddress should have nextAddress == cursor");

            if (!InitializeGetNext(out long headAddress, out long currentPage))
                return false;
            epoch?.Suspend();

            beginAddress = nextAddress = SnapToLogicalAddressBoundary(ref cursor, headAddress, currentPage);
            return true;
        }

        private bool InitializeGetNext(out long headAddress, out long currentPage)
        {
            currentAddress = nextAddress;
            var stopAddress = endAddress < hlog.GetTailAddress() ? endAddress : hlog.GetTailAddress();
            if (currentAddress >= stopAddress)
            {
                headAddress = currentPage = 0;
                return false;
            }

            epoch?.Resume();
            headAddress = hlog.HeadAddress;

            if (currentAddress < hlog.BeginAddress && !forceInMemory)
                currentAddress = hlog.BeginAddress;

            // If currentAddress < headAddress and we're not buffering and not guaranteeing the records are in memory, fail.
            if (frameSize == 0 && currentAddress < headAddress && !forceInMemory)
            {
                epoch?.Suspend();
                throw new TsavoriteException("Iterator address is less than log HeadAddress in memory-scan mode");
            }

            currentPage = currentAddress >> hlog.LogPageSizeBits;
            if (currentAddress < headAddress && !forceInMemory)
                _ = BufferAndLoad(currentAddress, currentPage, currentPage % frameSize, headAddress, stopAddress);

            // Success; keep the epoch held for GetNext (SnapCursorToLogicalAddress will Suspend()).
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long SnapToLogicalAddressBoundary(ref long logicalAddress, long headAddress, long currentPage)
        {
            long offset = logicalAddress & hlog.PageSizeMask;
            long physicalAddress = GetPhysicalAddress(logicalAddress, headAddress, currentPage, offset) - offset;
            long totalSizes = 0;
            if (currentPage == 0)
            {
                if (logicalAddress < hlog.BeginAddress)
                    return logicalAddress = hlog.BeginAddress;
                physicalAddress += hlog.BeginAddress;
                totalSizes = (int)hlog.BeginAddress;
            }

            while (totalSizes <= offset)
            {
                var (_, allocatedSize) = hlog.GetRecordSize(physicalAddress);
                if (totalSizes + allocatedSize > offset)
                    break;
                totalSizes += allocatedSize;
                physicalAddress += allocatedSize;
            }

            return logicalAddress += totalSizes - offset;
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
                if (!InitializeGetNext(out long headAddress, out long currentPage))
                    return false;

                var offset = currentAddress & hlog.PageSizeMask;
                long physicalAddress = GetPhysicalAddress(currentAddress, headAddress, currentPage, offset);
                int recordSize = hlog.GetRecordSize(physicalAddress).Item2;

                // If record does not fit on page, skip to the next page.
                if ((currentAddress & hlog.PageSizeMask) + recordSize > hlog.PageSize)
                {
                    nextAddress = (1 + (currentAddress >> hlog.LogPageSizeBits)) << hlog.LogPageSizeBits;
                    epoch?.Suspend();
                    continue;
                }

                nextAddress = currentAddress + recordSize;

                recordInfo = hlog._wrapper.GetInfo(physicalAddress);

                var skipOnScan = includeClosedRecords ? false : recordInfo.SkipOnScan;
                if (skipOnScan || recordInfo.IsNull())
                {
                    epoch?.Suspend();
                    continue;
                }

                currentPhysicalAddress = physicalAddress;

                // We will return control to the caller, which means releasing epoch protection, and we don't want the caller to lock.
                // Copy the entire record into bufferPool memory, so we do not have a ref to log data outside epoch protection.
                // Lock to ensure no value tearing while copying to temp storage.
                if (currentAddress >= headAddress || forceInMemory)
                {
                    OperationStackContext<SpanByte, SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> stackCtx = default;
                    try
                    {
                        if (memory == null)
                        {
                            memory = hlog.bufferPool.Get(recordSize);
                        }
                        else
                        {
                            if (memory.AlignedTotalCapacity < recordSize)
                            {
                                memory.Return();
                                memory = hlog.bufferPool.Get(recordSize);
                            }
                        }

                        // GetKey() should work but for safety and consistency with other allocators use physicalAddress.
                        if (currentAddress >= headAddress && store is not null)
                            store.LockForScan(ref stackCtx, ref hlog._wrapper.GetKey(physicalAddress));

                        unsafe
                        {
                            Buffer.MemoryCopy((byte*)currentPhysicalAddress, memory.aligned_pointer, recordSize, recordSize);
                            currentPhysicalAddress = (long)memory.aligned_pointer;
                        }
                    }
                    finally
                    {
                        if (stackCtx.recSrc.HasLock)
                            store.UnlockForScan(ref stackCtx);
                    }
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
        bool IPushScanIterator<SpanByte>.BeginGetPrevInMemory(ref SpanByte key, out RecordInfo recordInfo, out bool continueOnDisk)
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
                bool skipOnScan = includeClosedRecords ? false : recordInfo.SkipOnScan;
                if (skipOnScan || recordInfo.IsNull() || !hlog._storeFunctions.KeysEqual(ref hlog._wrapper.GetKey(physicalAddress), ref key))
                {
                    epoch?.Suspend();
                    continue;
                }

                // Success; defer epoch?.Suspend(); to EndGet
                currentPhysicalAddress = physicalAddress;
                return true;
            }
        }

        bool IPushScanIterator<SpanByte>.EndGetPrevInMemory()
        {
            epoch?.Suspend();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetPhysicalAddress(long currentAddress, long headAddress, long currentPage, long offset)
        {
            long physicalAddress;
            if (currentAddress >= headAddress || forceInMemory)
                physicalAddress = hlog.GetPhysicalAddress(currentAddress);
            else
                physicalAddress = frame.GetPhysicalAddress(currentPage % frameSize, offset);
            return physicalAddress;
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <returns></returns>
        public bool GetNext(out RecordInfo recordInfo, out SpanByte key, out SpanByte value)
            => throw new NotSupportedException("Use GetNext(out RecordInfo) to retrieve references to key/value");

        /// <summary>
        /// Dispose iterator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            memory?.Return();
            memory = null;
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
                hlog.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                result.freeBuffer1.Return();
                result.freeBuffer1 = null;
            }

            if (errorCode == 0)
                result.handle?.Signal();

            Interlocked.MemoryBarrier();
        }
    }
}