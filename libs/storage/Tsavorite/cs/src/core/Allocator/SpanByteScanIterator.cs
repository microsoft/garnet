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
        /// <param name="includeClosedRecords"></param>
        /// <param name="epoch">Epoch to use for protection; may be null if <paramref name="assumeInMemory"/> is true.</param>
        /// <param name="assumeInMemory">Provided address range is known by caller to be in memory, even if less than HeadAddress</param>
        /// <param name="logger"></param>
        internal SpanByteScanIterator(TsavoriteKV<TStoreFunctions, TAllocator> store, AllocatorBase<TStoreFunctions, TAllocator> hlogBase,
                long beginAddress, long endAddress, LightEpoch epoch,
                DiskScanBufferingMode diskScanBufferingMode, InMemoryScanBufferingMode memScanBufferingMode = InMemoryScanBufferingMode.NoBuffering,
                bool includeClosedRecords = false, bool assumeInMemory = false, ILogger logger = null)
            : base(beginAddress == 0 ? hlogBase.GetFirstValidLogicalAddressOnPage(0) : beginAddress, endAddress,
                diskScanBufferingMode, memScanBufferingMode, includeClosedRecords, epoch, hlogBase.LogPageSizeBits, logger: logger)
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

        #region TODO Unify with ObjectScanIterator
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
            finally
            {
                epoch?.Suspend();
            }
            return true;
        }

        private bool InitializeGetNextAndAcquireEpoch(out long stopAddress)
        {
            if (diskLogRecord.IsSet)
            {
                hlogBase._wrapper.DisposeRecord(ref diskLogRecord, DisposeReason.DeserializedFromDisk);
                diskLogRecord.Dispose();
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

            // Subtracting offset means this physicalAddress is at the start of the page. Adjust for PageHeader.
            long totalSizes = PageHeader.Size;
            if (currentPage == 0)
            {
                if (logicalAddress < hlogBase.BeginAddress)
                    return logicalAddress = hlogBase.BeginAddress;
                totalSizes = (int)hlogBase.BeginAddress;
            }
            var physicalAddress = GetPhysicalAddress(logicalAddress, headAddress, currentPage, offset) - offset + totalSizes;

            while (totalSizes <= offset)
            {
                var allocatedSize = new LogRecord(physicalAddress).AllocatedSize;
                if (totalSizes + allocatedSize > offset)
                    break;
                totalSizes += allocatedSize;
                physicalAddress += allocatedSize;
            }

            return logicalAddress += totalSizes - offset;
        }

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
            allocatedSize = logRecord.AllocatedSize;
            return logRecord.physicalAddress;
        }
        #endregion TODO Unify with ObjectScanIterator

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

                    // If record does not fit on page, skip to the next page. Offset should always be at least PageHeader.Size; if it's zero, it means
                    // our record size aligned perfectly with end of page, so we must move to the next page (skipping its PageHeader).
                    if (offset == 0 || offset + allocatedSize > hlogBase.PageSize)
                    {
                        var nextPage = hlogBase.GetPage(currentAddress);
                        nextAddress = hlogBase.GetFirstValidLogicalAddressOnPage(offset == 0 ? nextPage : nextPage + 1);
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
                            {
                                var logRecord = hlogBase._wrapper.CreateLogRecord(currentAddress, physicalAddress);
                                store.LockForScan(ref stackCtx, logRecord.Key);
                            }

                            if (recordBuffer == null)
                                recordBuffer = hlogBase.bufferPool.Get((int)allocatedSize);
                            else if (recordBuffer.AlignedTotalCapacity < (int)allocatedSize)
                            {
                                recordBuffer.Return();
                                recordBuffer = hlogBase.bufferPool.Get((int)allocatedSize);
                            }

                            // These objects are still alive in the log, so do not dispose the value object if any (SpanByteAllocator has none).
                            // Don't pass the recordBuffer to diskLogRecord; we reuse that here.
                            var remapPtr = recordBuffer.GetValidPointer();
                            Buffer.MemoryCopy((byte*)physicalAddress, remapPtr, allocatedSize, allocatedSize);
                            var memoryLogRecord = hlogBase._wrapper.CreateRemappedLogRecordOverPinnedTransientMemory(currentAddress, (long)remapPtr);
                            diskLogRecord = new DiskLogRecord(in memoryLogRecord, obj => { });
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
                        // We dispose the object here because it is read from the disk, unless we transfer it such as by CopyToTail (SpanByteAllocator has no objects).
                        diskLogRecord = new(new LogRecord(physicalAddress, hlogBase._wrapper.TransientObjectIdMap),
                                            obj => store.storeFunctions.DisposeValueObject(obj, DisposeReason.DeserializedFromDisk));
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
                var skipOnScan = !includeClosedRecords && logRecord.Info.SkipOnScan;
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

        #region ISourceLogRecord
        /// <inheritdoc/>
        public ref RecordInfo InfoRef => ref diskLogRecord.InfoRef;
        /// <inheritdoc/>
        public RecordInfo Info => diskLogRecord.Info;

        /// <inheritdoc/>
        public byte RecordType => diskLogRecord.RecordType;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> Namespace => diskLogRecord.Namespace;

        /// <inheritdoc/>
        public ObjectIdMap ObjectIdMap => diskLogRecord.ObjectIdMap;

        /// <inheritdoc/>
        public bool IsSet => diskLogRecord.IsSet;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> Key => diskLogRecord.Key;

        /// <inheritdoc/>
        public bool IsPinnedKey => diskLogRecord.IsPinnedKey;

        /// <inheritdoc/>
        public byte* PinnedKeyPointer => diskLogRecord.PinnedKeyPointer;

        /// <inheritdoc/>
        public OverflowByteArray KeyOverflow
        {
            get => diskLogRecord.KeyOverflow;
            set => diskLogRecord.KeyOverflow = value;
        }

        /// <inheritdoc/>
        public Span<byte> ValueSpan => diskLogRecord.ValueSpan;

        /// <inheritdoc/>
        public IHeapObject ValueObject => diskLogRecord.ValueObject;

        /// <inheritdoc/>
        public bool IsPinnedValue => diskLogRecord.IsPinnedValue;

        /// <inheritdoc/>
        public byte* PinnedValuePointer => diskLogRecord.PinnedValuePointer;

        /// <inheritdoc/>
        public OverflowByteArray ValueOverflow
        {
            get => diskLogRecord.ValueOverflow;
            set => diskLogRecord.ValueOverflow = value;
        }

        /// <inheritdoc/>
        public long ETag => diskLogRecord.ETag;

        /// <inheritdoc/>
        public long Expiration => diskLogRecord.Expiration;

        /// <inheritdoc/>
        public void ClearValueIfHeap(Action<IHeapObject> disposer) { }  // Not relevant for "iterator as logrecord"

        /// <inheritdoc/>
        public bool IsMemoryLogRecord => false;

        /// <inheritdoc/>
        public unsafe ref LogRecord AsMemoryLogRecordRef() => throw new InvalidOperationException("Cannot cast a DiskLogRecord to a memory LogRecord.");

        /// <inheritdoc/>
        public bool IsDiskLogRecord => true;

        /// <inheritdoc/>
        public unsafe ref DiskLogRecord AsDiskLogRecordRef() => ref Unsafe.AsRef(in diskLogRecord);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetRecordFieldInfo() => diskLogRecord.GetRecordFieldInfo();

        /// <inheritdoc/>
        public int AllocatedSize => diskLogRecord.AllocatedSize;

        /// <inheritdoc/>
        public int ActualSize => diskLogRecord.ActualSize;
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

        internal override void AsyncReadPageFromDeviceToFrame<TContext>(CircularDiskReadBuffer readBuffers, long readPage, long untilAddress, TContext context, out CountdownEvent completed,
                long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null)
            => hlogBase.AsyncReadPageFromDeviceToFrame(readBuffers, readPage, untilAddress, AsyncReadPageFromDeviceToFrameCallback, context, frame, out completed, devicePageOffset, device, objectLogDevice, cts);
    }
}