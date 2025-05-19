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
    public sealed unsafe class RecordScanIterator<TStoreFunctions, TAllocator> : ScanIteratorBase, ITsavoriteScanIterator, IPushScanIterator
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        private readonly TsavoriteKV<TStoreFunctions, TAllocator> store;
        private readonly AllocatorBase<TStoreFunctions, TAllocator> hlogBase;
        private readonly BlittableFrame frame;  // TODO remove GenericFrame

        private SectorAlignedMemory recordBuffer;
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
        internal RecordScanIterator(TsavoriteKV<TStoreFunctions, TAllocator> store, AllocatorBase<TStoreFunctions, TAllocator> hlogBase,
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
        internal RecordScanIterator(TsavoriteKV<TStoreFunctions, TAllocator> store, AllocatorBase<TStoreFunctions, TAllocator> hlogBase,
                long beginAddress, LightEpoch epoch, ILogger logger = null)
            : base(beginAddress == 0 ? hlogBase.GetFirstValidLogicalAddressOnPage(0) : beginAddress, hlogBase.GetTailAddress(), DiskScanBufferingMode.SinglePageBuffering, InMemoryScanBufferingMode.NoBuffering, false, epoch, hlogBase.LogPageSizeBits, logger: logger)
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
            if (diskLogRecord.IsSet)
            {
                hlogBase._wrapper.DisposeRecord(ref diskLogRecord, DisposeReason.DeserializedFromDisk);
                diskLogRecord = default;
            }
            diskLogRecord = default;
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

            // We don't have to worry about going past the end of the page because we're using offset to bound the scan.
            if (logicalAddress >= headAddress)
            {
                while (totalSizes <= offset)
                {
                    var allocatedSize = new LogRecord(physicalAddress).GetInlineRecordSizes().allocatedSize;
                    if (totalSizes + allocatedSize > offset)
                        break;
                    totalSizes += allocatedSize;
                    physicalAddress += allocatedSize;
                }
            }
            else
            {
                while (totalSizes <= offset)
                {
                    var allocatedSize = new DiskLogRecord(physicalAddress).GetSerializedLength();
                    if (totalSizes + allocatedSize > offset)
                        break;
                    totalSizes += allocatedSize;
                    physicalAddress += allocatedSize;
                }
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

                    // TODO: This can process past end of page if it is a too-small record at end of page and its recordInfo is not null
                    var physicalAddress = GetPhysicalAddressAndAllocatedSize(currentAddress, headAddress, currentPage, offset, out long allocatedSize);

                    // It's safe to use LogRecord here even for on-disk because both start with the RecordInfo.
                    var recordInfo = LogRecord.GetInfo(physicalAddress);
                    if (recordInfo.IsNull)  // We are probably past end of allocated records on page.
                    {
                        nextAddress = currentAddress + RecordInfo.GetLength();
                        continue;
                    }

                    // If record does not fit on page, skip to the next page.
                    if (offset + allocatedSize > hlogBase.PageSize)
                    {
                        nextAddress = hlogBase.GetStartLogicalAddressOfPage(1 + hlogBase.GetPage(currentAddress));
                        continue;
                    }

                    nextAddress = currentAddress + allocatedSize;

                    var skipOnScan = includeSealedRecords ? recordInfo.Invalid : recordInfo.SkipOnScan;
                    if (skipOnScan || recordInfo.IsNull)
                        continue;

                    if (currentAddress >= headAddress || assumeInMemory)
                    {
                        // TODO: for this PR we always buffer the in-memory records; pull iterators require it, and currently push iterators are implemented on top of pull.
                        // So create a disk log record from the in-memory record.
                        var logRecord = hlogBase._wrapper.CreateLogRecord(currentAddress);
                        nextAddress = currentAddress + logRecord.GetInlineRecordSizes().allocatedSize;

                        // We will return control to the caller, which means releasing epoch protection, and we don't want the caller to lock.
                        // Copy the entire record into bufferPool memory, so we do not have a ref to log data outside epoch protection.
                        // Lock to ensure no value tearing while copying to temp storage.
                        OperationStackContext<TStoreFunctions, TAllocator> stackCtx = default;
                        try
                        {
                            if (currentAddress >= headAddress && store is not null)
                                store.LockForScan(ref stackCtx, logRecord.Key);
                            diskLogRecord.Serialize(in logRecord, hlogBase.bufferPool, valueSerializer: default, ref recordBuffer);
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
                        diskLogRecord = new(physicalAddress);
                        if (diskLogRecord.Info.ValueIsObject)
                            _ = diskLogRecord.DeserializeValueObject(hlogBase.storeFunctions.CreateValueObjectSerializer());
                        nextAddress = currentAddress + diskLogRecord.GetSerializedLength();
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
        long GetPhysicalAddressAndAllocatedSize(long currentAddress, long headAddress, long currentPage, long offset, out long allocatedSize)
        {
            if (currentAddress >= headAddress || assumeInMemory)
            {
                var logRecord = hlogBase._wrapper.CreateLogRecord(currentAddress);
                (var _, allocatedSize) = logRecord.GetInlineRecordSizes();
                return logRecord.physicalAddress;
            }

            long physicalAddress = frame.GetPhysicalAddress(currentPage, offset);
            allocatedSize = new DiskLogRecord(physicalAddress).GetSerializedLength();
            return physicalAddress;
        }

        #region ISourceLogRecord
        /// <inheritdoc/>
        public ref RecordInfo InfoRef => ref diskLogRecord.InfoRef;
        /// <inheritdoc/>
        public RecordInfo Info => diskLogRecord.Info;

        /// <inheritdoc/>
        public bool IsSet => diskLogRecord.IsSet;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> Key => diskLogRecord.Key;

        /// <inheritdoc/>
        public bool IsPinnedKey => diskLogRecord.IsPinnedKey;

        /// <inheritdoc/>
        public byte* PinnedKeyPointer => diskLogRecord.PinnedKeyPointer;

        /// <inheritdoc/>
        public Span<byte> ValueSpan => diskLogRecord.ValueSpan;

        /// <inheritdoc/>
        public IHeapObject ValueObject => diskLogRecord.ValueObject;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> RecordSpan => diskLogRecord.RecordSpan;

        /// <inheritdoc/>
        public bool IsPinnedValue => diskLogRecord.IsPinnedValue;

        /// <inheritdoc/>
        public byte* PinnedValuePointer => diskLogRecord.PinnedValuePointer;

        /// <inheritdoc/>
        public long ETag => diskLogRecord.ETag;

        /// <inheritdoc/>
        public long Expiration => diskLogRecord.Expiration;

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
            diskLogRecord = this.diskLogRecord;
            return true;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetRecordFieldInfo() => new()
        {
            KeyDataSize = Key.Length,
            ValueDataSize = Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : ValueSpan.Length,
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
            if (diskLogRecord.IsSet)
                hlogBase._wrapper.DisposeRecord(ref diskLogRecord, DisposeReason.DeserializedFromDisk);
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