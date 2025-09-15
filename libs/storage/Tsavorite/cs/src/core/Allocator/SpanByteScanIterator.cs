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
    public sealed unsafe class SpanByteScanIterator<TStoreFunctions, TAllocator> : ScanIteratorBase, ITsavoriteScanIterator, IPushScanIterator
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        private readonly TsavoriteKV<TStoreFunctions, TAllocator> store;
        private readonly AllocatorBase<TStoreFunctions, TAllocator> hlogBase;
        private readonly BlittableFrame frame;

        private SectorAlignedMemory recordBuffer;
        private readonly bool assumeInMemory;

        private LogRecord logRecord;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="store"></param>
        /// <param name="hlogBase">The fully derived log implementation</param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="diskScanBufferingMode"></param>
        /// <param name="memScanBufferingMode"></param>
        /// <param name="includeClosedRecords"></param>
        /// <param name="epoch">Epoch to use for protection; may be null if <paramref name="assumeInMemory"/> is true.</param>
        /// <param name="assumeInMemory">Provided address range is known by caller to be in memory, even if less than HeadAddress</param>
        /// <param name="logger"></param>
        internal SpanByteScanIterator(TsavoriteKV<TStoreFunctions, TAllocator> store, AllocatorBase<TStoreFunctions, TAllocator> hlogBase,
                long beginAddress, long endAddress, LightEpoch epoch,
                DiskScanBufferingMode diskScanBufferingMode, InMemoryScanBufferingMode memScanBufferingMode = InMemoryScanBufferingMode.NoBuffering,
                bool includeClosedRecords = false, bool assumeInMemory = false, ILogger logger = null)
            : base(beginAddress == 0 ? hlogBase.GetFirstValidLogicalAddressOnPage(0) : beginAddress, endAddress, diskScanBufferingMode, memScanBufferingMode, includeClosedRecords, epoch, hlogBase.LogPageSizeBits, logger: logger)
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
            : base(beginAddress == 0 ? hlogBase.GetFirstValidLogicalAddressOnPage(0) : beginAddress, hlogBase.GetTailAddress(),
                DiskScanBufferingMode.SinglePageBuffering, InMemoryScanBufferingMode.NoBuffering, false, epoch, hlogBase.LogPageSizeBits, logger: logger)
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
                if (!LoadPageIfNeeded(out var headAddress, out var currentPage, stopAddress))
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
            if (logRecord.IsSet)
            {
                hlogBase._wrapper.DisposeRecord(ref logRecord, DisposeReason.DeserializedFromDisk);
                logRecord.Dispose(rec => { });  // We have no objects or overflow in SpanByteAllocator
                logRecord = default;
            }
            logRecord = default;
            currentAddress = nextAddress;
            stopAddress = endAddress < hlogBase.GetTailAddress() ? endAddress : hlogBase.GetTailAddress();
            if (currentAddress >= stopAddress)
                return false;

            // Success; acquire the epoch. Caller will suspend the epoch as needed.
            epoch?.Resume();
            return true;
        }

        private bool LoadPageIfNeeded(out long headAddress, out long currentPage, long stopAddress)
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
                    if (!LoadPageIfNeeded(out var headAddress, out var currentPage, stopAddress))
                        return false;

                    var offset = hlogBase.GetOffsetOnPage(currentAddress);
                    var physicalAddress = GetPhysicalAddressAndAllocatedSize(currentAddress, headAddress, currentPage, offset, out var allocatedSize);
                    var recordInfo = LogRecord.GetInfo(physicalAddress);

                    // If record does not fit on page, skip to the next page.
                    if (offset + allocatedSize > hlogBase.PageSize)
                    {
                        nextAddress = hlogBase.GetStartAbsoluteLogicalAddressOfPage(1 + hlogBase.GetPage(currentAddress));
                        epoch.Suspend();
                        continue;
                    }

                    nextAddress = currentAddress + allocatedSize;

                    var skipOnScan = !includeClosedRecords && recordInfo.SkipOnScan;
                    if (skipOnScan || recordInfo.IsNull)
                        continue;

                    if (currentAddress >= headAddress || assumeInMemory)
                    {
                        // TODO: for this PR we always buffer the in-memory records; pull iterators require it, and currently push iterators are implemented on top of pull.
                        // Copy the entire record into bufferPool memory so we don't have a ref to log data outside epoch protection.
                        OperationStackContext<TStoreFunctions, TAllocator> stackCtx = default;
                        try
                        {
                            // Lock to ensure no value tearing while copying to temp storage.
                            if (currentAddress >= headAddress && store is not null)
                                store.LockForScan(ref stackCtx, logRecord.Key);

                            if (recordBuffer == null)
                                recordBuffer = hlogBase.bufferPool.Get((int)allocatedSize);
                            else if (recordBuffer.AlignedTotalCapacity < (int)allocatedSize)
                            {
                                recordBuffer.Return();
                                recordBuffer = hlogBase.bufferPool.Get((int)allocatedSize);
                            }

                            logRecord = new((long)recordBuffer.GetValidPointer());
                        }
                        finally
                        {
                            if (stackCtx.recSrc.HasLock)
                                store.UnlockForScan(ref stackCtx);
                        }
                    }
                    else
                    {
                        // We advance a record at a time in the IO frame so set the diskLogRecord to the current frame offset and advance nextAddress.
                        logRecord = new(physicalAddress);
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
            while (true)
            {
                // "nextAddress" is reused as "previous address" for this operation.
                currentAddress = nextAddress;
                var headAddress = hlogBase.HeadAddress;
                if (currentAddress < headAddress)
                {
                    logRecord = default;
                    continueOnDisk = currentAddress >= hlogBase.BeginAddress;
                    return false;
                }

                epoch?.Resume();

                logRecord = hlogBase._wrapper.CreateLogRecord(currentAddress);
                nextAddress = logRecord.Info.PreviousAddress;
                bool skipOnScan = !includeClosedRecords && logRecord.Info.SkipOnScan;
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
            => currentAddress >= headAddress || assumeInMemory
                ? hlogBase.GetPhysicalAddress(currentAddress)
                : frame.GetPhysicalAddress(currentPage, offset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetPhysicalAddressAndAllocatedSize(long currentAddress, long headAddress, long currentPage, long offset, out long allocatedSize)
        {
            var physicalAddress = GetPhysicalAddress(currentAddress, headAddress, currentPage, offset);

            // We are just getting sizes so no need for ObjectIdMap
            var logRecord = new LogRecord(physicalAddress);
            (var _, allocatedSize) = logRecord.GetInlineRecordSizes();
            return logRecord.physicalAddress;
        }

        #region ISourceLogRecord
        /// <inheritdoc/>
        public ref RecordInfo InfoRef => ref logRecord.InfoRef;
        /// <inheritdoc/>
        public RecordInfo Info => logRecord.Info;

        /// <inheritdoc/>
        public bool IsSet => logRecord.IsSet;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> Key => logRecord.Key;

        /// <inheritdoc/>
        public bool IsPinnedKey => logRecord.IsPinnedKey;

        /// <inheritdoc/>
        public byte* PinnedKeyPointer => logRecord.PinnedKeyPointer;

        /// <inheritdoc/>
        public Span<byte> ValueSpan => logRecord.ValueSpan;

        /// <inheritdoc/>
        public IHeapObject ValueObject => logRecord.ValueObject;

        /// <inheritdoc/>
        public bool IsPinnedValue => logRecord.IsPinnedValue;

        /// <inheritdoc/>
        public byte* PinnedValuePointer => logRecord.PinnedValuePointer;

        /// <inheritdoc/>
        public long ETag => logRecord.ETag;

        /// <inheritdoc/>
        public long Expiration => logRecord.Expiration;

        /// <inheritdoc/>
        public void ClearValueObject(Action<IHeapObject> disposer) { }  // Not relevant for iterators

        /// <inheritdoc/>
        public bool AsLogRecord(out LogRecord logRecord)
        {
            logRecord = default;
            return false;
        }

        /// <inheritdoc/>
        public bool AsDiskLogRecord(out DiskLogRecord diskLogRecord)
        {
            diskLogRecord = this.logRecord;
            return true;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetRecordFieldInfo() => new()
        {
            KeySize = Key.Length,
            ValueSize = Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : ValueSpan.Length,
            ValueIsObject = Info.ValueIsObject,
            HasETag = Info.HasETag,
            HasExpiration = Info.HasExpiration
        };
        #endregion // ISourceLogRecord

        /// <summary>
        /// Dispose iterator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            if (logRecord.IsSet)
                hlogBase._wrapper.DisposeRecord(ref logRecord, DisposeReason.DeserializedFromDisk);
            recordBuffer?.Return();
            recordBuffer = null;
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

            // Deserialize valueObject in frame (if present)
            var diskLogRecord = new DiskLogRecord(frame.GetPhysicalAddress(result.page, offset: 0));
            if (diskLogRecord.Info.ValueIsObject)
                _ = diskLogRecord.DeserializeValueObject(hlogBase.storeFunctions.CreateValueObjectSerializer());

            if (errorCode == 0)
                _ = result.handle?.Signal();

            Interlocked.MemoryBarrier();
        }
    }
}