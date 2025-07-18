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
    public sealed class SpanByteScanIterator<TStoreFunctions, TAllocator> : ScanIteratorBase, ITsavoriteScanIterator, IPushScanIterator
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        private readonly TsavoriteKV<TStoreFunctions, TAllocator> store;
        private readonly AllocatorBase<TStoreFunctions, TAllocator> hlogBase;
        private readonly BlittableFrame frame;

        private readonly bool assumeInMemory;

        private DiskLogRecord diskLogRecord;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="store"></param>
        /// <param name="hlogBase">The fully derived log implementation</param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="diskScanBufferingMode"></param>
        /// <param name="memScanBufferingMode"></param>
        /// <param name="includeSealedRecords"></param>
        /// <param name="epoch">Epoch to use for protection; may be null if <paramref name="assumeInMemory"/> is true.</param>
        /// <param name="assumeInMemory">Provided address range is known by caller to be in memory, even if less than HeadAddress</param>
        /// <param name="logger"></param>
        internal SpanByteScanIterator(TsavoriteKV<TStoreFunctions, TAllocator> store, AllocatorBase<TStoreFunctions, TAllocator> hlogBase,
                long beginAddress, long endAddress, LightEpoch epoch,
                DiskScanBufferingMode diskScanBufferingMode, InMemoryScanBufferingMode memScanBufferingMode = InMemoryScanBufferingMode.NoBuffering,
                bool includeSealedRecords = false, bool assumeInMemory = false, ILogger logger = null)
            : base(beginAddress == 0 ? hlogBase.GetFirstValidLogicalAddressOnPage(0) : beginAddress, endAddress, diskScanBufferingMode, memScanBufferingMode, includeSealedRecords, epoch, hlogBase.LogPageSizeBits, logger: logger)
        {
            this.store = store;
            this.hlogBase = hlogBase;
            this.assumeInMemory = assumeInMemory;
            if (frameSize > 0)
                frame = new BlittableFrame(frameSize, hlogBase.PageSize, hlogBase.GetDeviceSectorSize());
        }

        /// <summary>
        /// Constructor for use with tail-to-head push iteration of the passed key's record versions
        /// </summary>
        internal SpanByteScanIterator(TsavoriteKV<TStoreFunctions, TAllocator> store, AllocatorBase<TStoreFunctions, TAllocator> hlogBase,
                long beginAddress, LightEpoch epoch, ILogger logger = null)
            : base(beginAddress == 0 ? hlogBase.GetFirstValidLogicalAddressOnPage(0) : beginAddress, hlogBase.GetTailAddress(), DiskScanBufferingMode.SinglePageBuffering, InMemoryScanBufferingMode.NoBuffering,
                false, epoch, hlogBase.LogPageSizeBits, logger: logger)
        {
            this.store = store;
            this.hlogBase = hlogBase;
            assumeInMemory = false;
            if (frameSize > 0)
                frame = new BlittableFrame(frameSize, hlogBase.PageSize, hlogBase.GetDeviceSectorSize());
        }

        /// <inheritdoc/>
        public bool SnapCursorToLogicalAddress(ref long cursor)
        {
            Debug.Assert(currentAddress == -1, "SnapCursorToLogicalAddress must be called before GetNext()");
            Debug.Assert(nextAddress == cursor, "SnapCursorToLogicalAddress should have nextAddress == cursor");

            if (!InitializeGetNextAndAcquireEpoch(out var stopAddress))
                return false;
            try
            {
                if (!LoadPageIfNeeded(stopAddress, out var headAddress, out var currentPage))
                    return false;
                beginAddress = nextAddress = SnapToLogicalAddressBoundary(ref cursor, headAddress, currentPage);
            }
            catch
            {
                epoch?.Suspend();
                throw;
            }
            return true;
        }

        private bool InitializeGetNextAndAcquireEpoch(out long stopAddress)
        {
            if (diskLogRecord.IsSet)
                hlogBase._wrapper.DisposeRecord(ref diskLogRecord, DisposeReason.DeserializedFromDisk);
            diskLogRecord = default;

            currentAddress = nextAddress;
            stopAddress = endAddress < hlogBase.GetTailAddress() ? endAddress : hlogBase.GetTailAddress();
            if (currentAddress >= stopAddress)
                return false;

            // Success; acquire the epoch. Caller will suspend the epoch as needed.
            epoch?.Resume();
            return true;
        }

        private bool LoadPageIfNeeded(long stopAddress, out long headAddress, out long currentPage)
        {
            headAddress = hlogBase.HeadAddress;

            if (currentAddress < hlogBase.BeginAddress && !assumeInMemory)
                currentAddress = hlogBase.BeginAddress;

            // If currentAddress < headAddress and we're not buffering and not guaranteeing the records are in memory, fail.
            if (frameSize == 0 && currentAddress < headAddress && !assumeInMemory)
            {
                // Caller will suspend the epoch.
                throw new TsavoriteException("Iterator address is less than log HeadAddress in memory-scan mode");
            }

            currentPage = hlogBase.GetPage(currentAddress);
            if (currentAddress < headAddress && !assumeInMemory)
                _ = BufferAndLoad(currentAddress, currentPage, currentPage % frameSize, headAddress, stopAddress);

            // Success; keep the epoch held for GetNext (SnapCursorToLogicalAddress will Suspend()).
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long SnapToLogicalAddressBoundary(ref long logicalAddress, long headAddress, long currentPage)
        {
            var offset = hlogBase.GetOffsetOnPage(logicalAddress);

            // Subtracting offset means this physicalAddress is at the start of the page.
            var physicalAddress = GetPhysicalAddress(logicalAddress, headAddress, currentPage, offset) - offset;
            long totalSizes = 0;
            if (currentPage == 0)
            {
                if (logicalAddress < hlogBase.BeginAddress)
                    return logicalAddress = hlogBase.BeginAddress;

                // Bump past the FirstValidAddress offset
                physicalAddress += hlogBase.BeginAddress;
                totalSizes = (int)hlogBase.BeginAddress;
            }

            // For SpanByteAllocator, the record format is the same in LogRecord and DiskLogRecord, so we do not need to 
            // check for whether we are above headAddress.
            while (totalSizes <= offset)
            {
                var allocatedSize = new LogRecord(physicalAddress).GetInlineRecordSizes().allocatedSize;
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
        public unsafe bool GetNext()
        {
            while (true)
            {
                if (!InitializeGetNextAndAcquireEpoch(out var stopAddress))
                    return false;

                try
                {
                    if (!LoadPageIfNeeded(stopAddress, out var headAddress, out var currentPage))
                        return false;

                    // If record does not fit on page, skip to the next page.
                    if (!GetPhysicalAddressAndAllocatedSize(currentAddress, headAddress, currentPage, out var physicalAddress, out var recordSize))
                    {
                        nextAddress = hlogBase.GetStartAbsoluteLogicalAddressOfPage(currentPage + 1);
                        epoch?.Suspend();
                        continue;
                    }

                    nextAddress = currentAddress + recordSize;

                    var recordInfo = LogRecord.GetInfo(physicalAddress);
                    var skipOnScan = includeSealedRecords ? recordInfo.Invalid : recordInfo.SkipOnScan;
                    if (skipOnScan || recordInfo.IsNull)
                    {
                        epoch?.Suspend();
                        continue;
                    }

                    var currentPhysicalAddress = physicalAddress;

                    if (currentAddress >= headAddress || assumeInMemory)
                    {
                        // TODO: for this PR we always buffer the in-memory records; pull iterators require it, and currently push iterators are implemented on top of pull.
                        // So create a disk log record from the in-memory record.
                        var logRecord = hlogBase._wrapper.CreateLogRecord(currentAddress);

                        // We will return control to the caller, which means releasing epoch protection, and we don't want the caller to lock.
                        // Copy the entire record into bufferPool memory, so we do not have a ref to log data outside epoch protection.
                        // Lock to ensure no value tearing while copying to temp storage.
                        OperationStackContext<TStoreFunctions, TAllocator> stackCtx = default;
                        try
                        {
                            store?.LockForScan(ref stackCtx, logRecord.Key);
                            diskLogRecord.CopyFrom(in logRecord, hlogBase.bufferPool);
                        }
                        finally
                        {
                            store?.UnlockForScan(ref stackCtx);
                        }
                    }
                    else
                    {
                        // We advance a record at a time in the IO frame so set the diskLogRecord to the current frame offset and advance nextAddress.
                        // We know we have the full record, so create the DiskLogRecord on the fly.
                        diskLogRecord.CopyFrom(physicalAddress, );
                        nextAddress = currentAddress + diskLogRecord.GetSerializedLength(); // GetNext()
                    }
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
        bool IPushScanIterator.BeginGetPrevInMemory(ReadOnlySpan<byte> key, out LogRecord logRecord, out bool continueOnDisk)
        {
            continueOnDisk = false;

            while (true)
            {
                epoch?.Resume();

                // "nextAddress" is reused as "previous address" for this operation.
                currentAddress = nextAddress;
                var headAddress = hlogBase.HeadAddress;
                if (currentAddress < headAddress)
                {
                    logRecord = default;
                    continueOnDisk = currentAddress >= hlogBase.BeginAddress;
                    epoch?.Suspend();
                    return false;
                }

                logRecord = hlogBase._wrapper.CreateLogRecord(currentAddress);
                nextAddress = logRecord.Info.PreviousAddress;
                var skipOnScan = includeSealedRecords ? logRecord.Info.Invalid : logRecord.Info.SkipOnScan;
                if (skipOnScan || logRecord.Info.IsNull || !hlogBase.storeFunctions.KeysEqual(logRecord.Key, key))
                {
                    epoch?.Suspend();
                    continue;
                }

                // Success; defer epoch?.Suspend(); to EndGet
                continueOnDisk = false;
                return true;
            }
        }

        void IPushScanIterator.EndGetPrevInMemory() => epoch?.Suspend();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetPhysicalAddress(long currentAddress, long headAddress, long currentPage, long offset)
        {
            if (currentAddress >= headAddress || assumeInMemory)
                return hlogBase._wrapper.CreateLogRecord(currentAddress).physicalAddress;
            return frame.GetPhysicalAddress(currentPage, offset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool GetPhysicalAddressAndAllocatedSize(long currentAddress, long headAddress, long currentPage, out long physicalAddress, out long allocatedSize)
        {
            var availableBytes = hlogBase.PageSize - (int)(currentAddress & hlogBase.PageSizeMask);
            allocatedSize = 0;
            SerializedFieldInfo fieldInfo = default;

            if (currentAddress >= headAddress || assumeInMemory)
            {
                var logRecord = hlogBase._wrapper.CreateLogRecord(currentAddress);
                physicalAddress = logRecord.physicalAddress;
                (var _, allocatedSize) = logRecord.GetInlineRecordSizes();
                return true;
            }

            physicalAddress = frame.GetPhysicalAddress(currentPage, hlogBase.GetOffsetOnPage(currentAddress));
            if (!DiskLogRecord.GetSerializedLength(physicalAddress, availableBytes, out fieldInfo))
                return false;
            allocatedSize = fieldInfo.SerializedLength;
            return true;
        }

        /// <summary>
        /// Dispose iterator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            diskLogRecord.Dispose();
            frame?.Dispose();
        }

        internal override void AsyncReadPagesFromDeviceToFrame<TContext>(long readPageStart, int numPages, long untilAddress, TContext context, out CountdownEvent completed,
                long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null)
            => hlogBase.AsyncReadPagesFromDeviceToFrame(readPageStart, numPages, untilAddress, AsyncReadPagesCallback, context, frame, out completed, devicePageOffset, device, objectLogDevice);

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