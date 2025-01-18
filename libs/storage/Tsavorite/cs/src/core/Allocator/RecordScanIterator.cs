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
    public sealed class RecordScanIterator<TValue, TStoreFunctions, TAllocator> : ScanIteratorBase, ITsavoriteScanIterator<TValue>, IPushScanIterator<TValue>
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        private readonly TsavoriteKV<TValue, TStoreFunctions, TAllocator> store;
        private readonly AllocatorBase<TValue, TStoreFunctions, TAllocator> hlogBase;
        private readonly BlittableFrame frame;  // TODO remove GenericFrame

        private SectorAlignedMemory recordBuffer;
        private readonly bool assumeInMemory;

        private DiskLogRecord<TValue> diskLogRecord;

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
        internal RecordScanIterator(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, AllocatorBase<TValue, TStoreFunctions, TAllocator> hlogBase,
                long beginAddress, long endAddress, LightEpoch epoch, 
                DiskScanBufferingMode diskScanBufferingMode, InMemoryScanBufferingMode memScanBufferingMode = InMemoryScanBufferingMode.NoBuffering,
                bool includeSealedRecords = false, bool assumeInMemory = false, ILogger logger = null)
            : base(beginAddress == 0 ? hlogBase.GetFirstValidLogicalAddress(0) : beginAddress, endAddress, diskScanBufferingMode, memScanBufferingMode, includeSealedRecords, epoch, hlogBase.LogPageSizeBits, logger: logger)
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
        internal RecordScanIterator(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, AllocatorBase<TValue, TStoreFunctions, TAllocator> hlogBase,
                long beginAddress, LightEpoch epoch, ILogger logger = null)
            : base(beginAddress == 0 ? hlogBase.GetFirstValidLogicalAddress(0) : beginAddress, hlogBase.GetTailAddress(), DiskScanBufferingMode.SinglePageBuffering, InMemoryScanBufferingMode.NoBuffering, false, epoch, hlogBase.LogPageSizeBits, logger: logger)
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

            if (!InitializeGetNext(out var headAddress, out var currentPage))
                return false;
            epoch?.Suspend();

            beginAddress = nextAddress = SnapToLogicalAddressBoundary(ref cursor, headAddress, currentPage);
            return true;
        }

        private bool InitializeGetNext(out long headAddress, out long currentPage)
        {
            store.DisposeRecord(ref diskLogRecord, DisposeReason.DeserializedFromDisk);
            diskLogRecord = default;
            currentAddress = nextAddress;
            var stopAddress = endAddress < hlogBase.GetTailAddress() ? endAddress : hlogBase.GetTailAddress();
            if (currentAddress >= stopAddress)
            {
                headAddress = currentPage = 0;
                return false;
            }

            epoch?.Resume();
            headAddress = hlogBase.HeadAddress;

            if (currentAddress < hlogBase.BeginAddress && !assumeInMemory)
                currentAddress = hlogBase.BeginAddress;

            // If currentAddress < headAddress and we're not buffering and not guaranteeing the records are in memory, fail.
            if (frameSize == 0 && currentAddress < headAddress && !assumeInMemory)
            {
                epoch?.Suspend();
                throw new TsavoriteException("Iterator address is less than log HeadAddress in memory-scan mode");
            }

            currentPage = currentAddress >> hlogBase.LogPageSizeBits;
            if (currentAddress < headAddress && !assumeInMemory)
                _ = BufferAndLoad(currentAddress, currentPage, currentPage % frameSize, headAddress, stopAddress);

            // Success; keep the epoch held for GetNext (SnapCursorToLogicalAddress will Suspend()).
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long SnapToLogicalAddressBoundary(ref long logicalAddress, long headAddress, long currentPage)
        {
            var offset = logicalAddress & hlogBase.PageSizeMask;
            var physicalAddress = GetPhysicalAddress(logicalAddress, headAddress, currentPage, offset) - offset;
            long totalSizes = 0;
            if (currentPage == 0)
            {
                if (logicalAddress < hlogBase.BeginAddress)
                    return logicalAddress = hlogBase.BeginAddress;
                physicalAddress += hlogBase.BeginAddress;
                totalSizes = (int)hlogBase.BeginAddress;
            }

            if (logicalAddress >= headAddress)
            {
                while (totalSizes <= offset)
                {
                    var (_, allocatedSize) = new LogRecord<TValue>(physicalAddress).GetFullRecordSizes();
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
                    var allocatedSize = new DiskLogRecord<TValue>(physicalAddress).SerializedRecordLength;
                    if (totalSizes + allocatedSize > offset)
                        break;
                    totalSizes += allocatedSize;
                    physicalAddress += allocatedSize;
                }
            }

            return logicalAddress += totalSizes  - offset;
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        public unsafe bool GetNext()
        {
            while (true)
            {
                if (!InitializeGetNext(out var headAddress, out var currentPage))
                    return false;

                var offset = currentAddress & hlogBase.PageSizeMask;
                var physicalAddress = GetPhysicalAddress(currentAddress, headAddress, currentPage, offset);

                // If record did not fit on the page its recordInfo will be Null; skip to the next page if so.
                if ((*(RecordInfo*)(physicalAddress & hlogBase.PageSizeMask)).IsNull)
                {
                    nextAddress = (1 + (currentAddress >> hlogBase.LogPageSizeBits)) << hlogBase.LogPageSizeBits;
                    epoch?.Suspend();
                    continue;
                }

                var recordInfo = LogRecord<TValue>.GetInfo(physicalAddress);
                var skipOnScan = includeSealedRecords ? recordInfo.Invalid : recordInfo.SkipOnScan;
                if (skipOnScan || recordInfo.IsNull)
                {
                    epoch?.Suspend();
                    continue;
                }

                recordBuffer?.Return();
                recordBuffer = null;
                TValue valueObject = default;
                if (currentAddress >= headAddress || assumeInMemory)
                {
                    // TODO: for this PR we always buffer the in-memory records; pull iterators require it, and currently push iterators are implemented on top of pull.
                    // So create a disk log record from the in-memory record.
                    var logRecord = hlogBase._wrapper.CreateLogRecord(currentAddress);
                    nextAddress = currentAddress + logRecord.GetFullRecordSizes().allocatedSize;

                    // We will return control to the caller, which means releasing epoch protection, and we don't want the caller to lock.
                    // Copy the entire record into bufferPool memory, so we do not have a ref to log data outside epoch protection.
                    // Lock to ensure no value tearing while copying to temp storage.
                    OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx = default;
                    try
                    {
                        if (currentAddress >= headAddress && store is not null)
                            store.LockForScan(ref stackCtx, LogRecord<TValue>.GetKey(physicalAddress));

                        hlogBase.SerializeRecordToIteratorBuffer(currentAddress, ref recordBuffer, out valueObject);
                    }
                    finally
                    {
                        if (stackCtx.recSrc.HasLock)
                            store.UnlockForScan(ref stackCtx);
                    }
                    diskLogRecord = new((long)recordBuffer.GetValidPointer());
                }
                else
                {
                    // We advance a record at a time in the IO frame so set the diskLogRecord to the current frame offset and advance nextAddress.
                    diskLogRecord = new(physicalAddress);
                    nextAddress = currentAddress + diskLogRecord.SerializedRecordLength;
                }

                if (valueObject is not null)
                    diskLogRecord.valueObject = valueObject;
                else 
                    hlogBase.DeserializeFromDiskBuffer(ref diskLogRecord, frame.GetArrayAndUnalignedOffset(currentPage, offset));

                // Success
                epoch?.Suspend();
                return true;
            }
        }

        /// <summary>
        /// Get previous record and keep the epoch held while we call the user's scan functions
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        bool IPushScanIterator<TValue>.BeginGetPrevInMemory(SpanByte key, out LogRecord<TValue> logRecord, out bool continueOnDisk)
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
                if (skipOnScan || logRecord.Info.IsNull || !hlogBase._storeFunctions.KeysEqual(logRecord.Key, key))
                {
                    epoch?.Suspend();
                    continue;
                }

                // Success; defer epoch?.Suspend(); to EndGet
                continueOnDisk = false;
                return true;
            }
        }

        void IPushScanIterator<TValue>.EndGetPrevInMemory() => epoch?.Suspend();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetPhysicalAddress(long currentAddress, long headAddress, long currentPage, long offset)
        {
            long physicalAddress;
            if (currentAddress >= headAddress || assumeInMemory)
                physicalAddress = hlogBase._wrapper.GetPhysicalAddress(currentAddress);
            else
                physicalAddress = frame.GetPhysicalAddress(currentPage, offset);
            return physicalAddress;
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <returns></returns>
        public bool GetNext(out RecordInfo recordInfo, out SpanByte key, out SpanByte value)
            => throw new NotSupportedException("Use GetNext(out RecordInfo) to retrieve references to key/value");

        #region ISourceLogRecord
        /// <inheritdoc/>
        public bool IsObjectRecord => false;
        /// <inheritdoc/>
        public ref RecordInfo InfoRef => ref diskLogRecord.InfoRef;
        /// <inheritdoc/>
        public RecordInfo Info => diskLogRecord.Info;

        /// <inheritdoc/>
        public bool IsSet => diskLogRecord.IsSet;

        /// <inheritdoc/>
        public SpanByte Key => diskLogRecord.Key;

        /// <inheritdoc/>
        public SpanByte ValueSpan => diskLogRecord.ValueSpan;

        /// <inheritdoc/>
        public TValue ValueObject => diskLogRecord.ValueObject;

        /// <inheritdoc/>
        public ref TValue GetReadOnlyValueRef() => ref diskLogRecord.GetReadOnlyValueRef();

        /// <inheritdoc/>
        public long ETag => diskLogRecord.ETag;

        /// <inheritdoc/>
        public long Expiration => diskLogRecord.Expiration;

        /// <inheritdoc/>
        public void ClearValueObject(Action<TValue> disposer) { }  // Not relevant for iterators

        /// <inheritdoc/>
        public LogRecord<TValue> AsLogRecord() => throw new TsavoriteException("Iterators cannot be converted to AsLogRecord");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetRecordFieldInfo() => new()
        {
            KeySize = Key.TotalSize,
            ValueSize = IsObjectRecord ? ObjectIdMap.ObjectIdSize : ValueSpan.TotalSize,
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
            store.DisposeRecord(ref diskLogRecord, DisposeReason.DeserializedFromDisk);
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

            if (errorCode == 0)
                _ = (result.handle?.Signal());

            Interlocked.MemoryBarrier();
        }
    }
}