// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    // Allocator for SpanByte, possibly with a Blittable Key or Value.
    internal sealed unsafe class SpanByteAllocatorImpl<TStoreFunctions> : AllocatorBase<SpanByte, SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>>
        where TStoreFunctions : IStoreFunctions<SpanByte, SpanByte>
    {
        /// <summary>Circular buffer definition, in parallel with <see cref="values"/></summary>
        /// <remarks>The long is actually a byte*, but storing as 'long' makes going through logicalAddress/physicalAddress translation more easily</remarks>
        long* pagePointers;

        /// <summary>For each in-memory page of this allocator we have an <see cref="OverflowAllocator"/> for keys and values that are too large to fit inline 
        /// into the main log.</summary>
        OverflowAllocator[] values;

        private readonly OverflowPool<PageUnit<OverflowAllocator>> freePagePool;

        readonly int maxInlineKeySize;
        readonly int maxInlineValueSize;
        readonly int overflowAllocatorPageSize;
        readonly int overflowAllocatorOversizeLimit;

        public SpanByteAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, SpanByteAllocator<TStoreFunctions>> wrapperCreator)
            : base(settings.LogSettings, storeFunctions, wrapperCreator, settings.evictCallback, settings.epoch, settings.flushCallback, settings.logger)
        {
            // TODO: Verify LogSettings.MaxInlineKeySizeBits, .MaxInlineValueSizeBits, and .OverflowPageSizeBits are in range. Do we need config for OversizeLimit?
            maxInlineKeySize = 1 << settings.LogSettings.MaxInlineKeySizeBits;
            maxInlineValueSize = 1 << settings.LogSettings.MaxInlineValueSizeBits;
            overflowAllocatorPageSize = 1 << settings.LogSettings.OverflowPageSizeBits;
            overflowAllocatorOversizeLimit = overflowAllocatorPageSize - maxInlineKeySize * 4;  // TODO should this be adjusted for the Key vs. Value space?

            freePagePool = new OverflowPool<PageUnit<OverflowAllocator>>(4, p => { });

            var bufferSizeInBytes = (nuint)RoundUp(sizeof(long*) * BufferSize, Constants.kCacheLineBytes);
            pagePointers = (long*)NativeMemory.AlignedAlloc(bufferSizeInBytes, Constants.kCacheLineBytes);
            NativeMemory.Clear(pagePointers, bufferSizeInBytes);
        }

        internal int OverflowPageCount => freePagePool.Count;

        public override void Reset()
        {
            base.Reset();
            for (int index = 0; index < BufferSize; index++)
            {
                if (IsAllocated(index))
                    FreePage(index);
            }
            Initialize();
        }

        /// <summary>
        /// Allocate memory page, pinned in memory, and in sector aligned form, if possible
        /// </summary>
        /// <param name="index"></param>
        internal void AllocatePage(int index)
        {
            IncrementAllocatedPageCount();

            if (freePagePool.TryGet(out var item))
            {
                pagePointers[index] = item.pointer;
                // unused: _ = item.value;
                return;
            }

            pagePointers[index] = (long)NativeMemory.AlignedAlloc((nuint)PageSize, (nuint)sectorSize);
            values[index] = new(overflowAllocatorPageSize, overflowAllocatorOversizeLimit);
        }

        void ReturnPage(int index)
        {
            Debug.Assert(index < BufferSize);
            if (pagePointers[index] != default)
            {
                _ = freePagePool.TryAdd(new()
                {
                    pointer = pagePointers[index],
                    value = default
                });
                pagePointers[index] = default;
                _ = Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int GetPageIndex(long logicalAddress) => (int)((logicalAddress >> LogPageSizeBits) & (BufferSize - 1));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateLogRecord(long logicalAddress, long physicalAddress) => new LogRecord(physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OverflowAllocator GetOverflowAllocator(long logicalAddress) => values[GetPageIndex(logicalAddress)];

        public override void Initialize() => Initialize(Constants.kFirstValidAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeValue(long physicalAddress, long endAddress)
        {
            // Initialize the SpanByte to the length of the entire value space, less the length of the int size prefix.
            var src = (byte*)LogRecord.GetValueAddress(physicalAddress);
            *(int*)src = (int)((byte*)endAddress - src) - sizeof(int);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetRMWCopyRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref RecordInfo recordInfo, TVariableLengthInput varlenInput)
            where TSourceLogRecord : IReadOnlyLogRecord
            where TVariableLengthInput : IVariableLengthInput<SpanByte, TInput>
        {
            // Used by RMW to determine the length of copy destination (taking Input into account), so does not need to get filler length.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetRMWModifiedFieldInfo(ref srcLogRecord, ref input) };
            PopulateRecordSizeInfo(srcLogRecord.Key, ref sizeInfo);
            return sizeInfo;
        }
        public RecordSizeInfo GetRMWInitialRecordSize<TInput, TVariableLengthInput>(SpanByte key, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<SpanByte, TInput>
        {
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetRMWInitialFieldInfo(ref input) };
            PopulateRecordSizeInfo(key, ref sizeInfo);
            return sizeInfo;
        }

        public RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(SpanByte key, SpanByte value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<SpanByte, TInput>
        {
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetUpsertFieldInfo(value, ref input) };
            PopulateRecordSizeInfo(key, ref sizeInfo);
            return sizeInfo;
        }

        public void PopulateRecordSizeInfo(SpanByte key, ref RecordSizeInfo sizeInfo)
        {
            var keySize = key.TotalSize;
            if (keySize > maxInlineKeySize)
            {
                keySize = Constants.kUnserializedSpanByteSize;
                sizeInfo.KeyIsOverflow = true;
            }
            var valueSize = sizeInfo.FieldInfo.ValueSize;
            if (valueSize > maxInlineValueSize)
            {
                valueSize = Constants.kUnserializedSpanByteSize;
                sizeInfo.ValueIsOverflow = true;
            }
            sizeInfo.ActualInlineRecordSize = RecordInfo.GetLength() + keySize + valueSize + sizeInfo.OptionalSize;
            sizeInfo.AllocatedInlineRecordSize = RoundUp(sizeInfo.ActualInlineRecordSize, Constants.kRecordAlignment);
        }

        public (int actualSize, int allocatedSize, int keySize) GetRecordSize(ref SpanByte key, ref SpanByte value)
        {
            // TODO: Adjust for key overflowing and thus taking only 12 bytes inline
            int keySize = key.TotalSize;
            var actualSize = RecordInfo.GetLength() + RoundUp(keySize, Constants.kRecordAlignment) + value.TotalSize;
            return (actualSize, RoundUp(actualSize, Constants.kRecordAlignment), keySize);
        }

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            freePagePool.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPhysicalAddress(long logicalAddress)
        {
            // Offset within page
            var offset = (int)(logicalAddress & ((1L << LogPageSizeBits) - 1));

            // Index of page within the circular buffer
            var pageIndex = GetPageIndex(logicalAddress);
            return *(pagePointers + pageIndex) + offset;
        }

        internal bool IsAllocated(int pageIndex) => pagePointers[pageIndex] != 0;

        protected override void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
        {
            WriteAsync((IntPtr)pagePointers[flushPage % BufferSize],
                    (ulong)(AlignedPageSizeBytes * flushPage),
                    (uint)AlignedPageSizeBytes,
                    callback,
                    asyncResult, device);
        }

        protected override void WriteAsyncToDevice<TContext>
            (long startPage, long flushPage, int pageSize, DeviceIOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice, long[] localSegmentOffsets, long fuzzyStartLogicalAddress)
        {
            VerifyCompatibleSectorSize(device);
            var alignedPageSize = (pageSize + (sectorSize - 1)) & ~(sectorSize - 1);

            WriteAsync((IntPtr)pagePointers[flushPage % BufferSize],
                        (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                        (uint)alignedPageSize, callback, asyncResult,
                        device);
        }

        internal void ClearPage(long page, int offset)
            => NativeMemory.Clear((byte*)pagePointers[page] + offset, (nuint)(PageSize - offset));

        internal void FreePage(long page)
        {
            ClearPage(page, 0);
            if (EmptyPageCount > 0)
                ReturnPage((int)(page % BufferSize));
        }

        protected override void ReadAsync<TContext>(ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
            DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice)
        {
            device.ReadAsync(alignedSourceAddress, (IntPtr)pagePointers[destinationPageIndex], aligned_read_length, callback, asyncResult);
        }

        /// <summary>
        /// Invoked by users to obtain a record from disk. It uses sector aligned memory to read 
        /// the record efficiently into memory.
        /// </summary>
        /// <param name="fromLogical"></param>
        /// <param name="numBytes"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="result"></param>
        protected override void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext<SpanByte, SpanByte> context, SectorAlignedMemory result = default)
        {
            throw new InvalidOperationException("AsyncReadRecordObjectsToMemory invalid for SpanByteAllocator");
        }

        internal static bool RetrievedFullRecord(byte* record, ref AsyncIOContext<SpanByte, SpanByte> ctx) => true;

        internal IHeapContainer<SpanByte> GetKeyContainer(ref SpanByte key) => new SpanByteHeapContainer(ref key, bufferPool);

        internal IHeapContainer<SpanByte> GetValueContainer(ref SpanByte value) => new SpanByteHeapContainer(ref value, bufferPool);

        internal static long[] GetSegmentOffsets() => null;

        internal void PopulatePage(byte* src, int required_bytes, long destinationPage)
        {
            throw new TsavoriteException("SpanByteAllocator memory pages are sector aligned - use direct copy");
            // Buffer.MemoryCopy(src, (void*)pointers[destinationPage % BufferSize], required_bytes, required_bytes);
        }

        /// <summary>
        /// Iterator interface for pull-scanning Tsavorite log
        /// </summary>
        public override ITsavoriteScanIterator<SpanByte, SpanByte> Scan(TsavoriteKV<SpanByte, SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode, bool includeSealedRecords)
            => new SpanByteScanIterator<TStoreFunctions>(store, this, beginAddress, endAddress, scanBufferingMode, includeSealedRecords, epoch, logger: logger);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<SpanByte, SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, ref TScanFunctions scanFunctions, ScanBufferingMode scanBufferingMode)
        {
            using SpanByteScanIterator<TStoreFunctions> iter = new(store, this, beginAddress, endAddress, scanBufferingMode, false, epoch, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<SpanByte, SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store,
                ScanCursorState<SpanByte, SpanByte> scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor)
        {
            using SpanByteScanIterator<TStoreFunctions> iter = new(store, this, cursor, endAddress, ScanBufferingMode.SinglePageBuffering, false, epoch, logger: logger);
            return ScanLookup<SpanByte, SpanByteAndMemory, TScanFunctions, SpanByteScanIterator<TStoreFunctions>>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<SpanByte, SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store,
                ref SpanByte key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using SpanByteScanIterator<TStoreFunctions> iter = new(store, this, beginAddress, epoch, logger: logger);
            return IterateKeyVersionsImpl(store, ref key, beginAddress, ref scanFunctions, iter);
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator<SpanByte, SpanByte>> observer)
        {
            using var iter = new SpanByteScanIterator<TStoreFunctions>(store: null, this, beginAddress, endAddress, ScanBufferingMode.NoBuffering, false, epoch, true, logger: logger);
            observer?.OnNext(iter);
        }

        /// <summary>
        /// Read pages from specified device
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="readPageStart"></param>
        /// <param name="numPages"></param>
        /// <param name="untilAddress"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="frame"></param>
        /// <param name="completed"></param>
        /// <param name="devicePageOffset"></param>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        internal void AsyncReadPagesFromDeviceToFrame<TContext>(
                                        long readPageStart,
                                        int numPages,
                                        long untilAddress,
                                        DeviceIOCompletionCallback callback,
                                        TContext context,
                                        BlittableFrame frame,
                                        out CountdownEvent completed,
                                        long devicePageOffset = 0,
                                        IDevice device = null, IDevice objectLogDevice = null)
        {
            var usedDevice = device;
            if (device == null)
            {
                usedDevice = this.device;
            }

            completed = new CountdownEvent(numPages);
            for (long readPage = readPageStart; readPage < (readPageStart + numPages); readPage++)
            {
                int pageIndex = (int)(readPage % frame.frameSize);
                if (frame.frame[pageIndex] == null)
                {
                    frame.Allocate(pageIndex);
                }
                else
                {
                    frame.Clear(pageIndex);
                }
                var asyncResult = new PageAsyncReadResult<TContext>()
                {
                    page = readPage,
                    context = context,
                    handle = completed,
                    frame = frame
                };

                ulong offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);

                uint readLength = (uint)AlignedPageSizeBytes;
                long adjustedUntilAddress = (AlignedPageSizeBytes * (untilAddress >> LogPageSizeBits) + (untilAddress & PageSizeMask));

                if (adjustedUntilAddress > 0 && ((adjustedUntilAddress - (long)offsetInFile) < PageSize))
                {
                    readLength = (uint)(adjustedUntilAddress - (long)offsetInFile);
                    readLength = (uint)((readLength + (sectorSize - 1)) & ~(sectorSize - 1));
                }

                if (device != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

                usedDevice.ReadAsync(offsetInFile, (IntPtr)frame.pointers[pageIndex], readLength, callback, asyncResult);
            }
        }
    }
}