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
    internal sealed unsafe class SpanByteAllocatorImpl<TStoreFunctions> : AllocatorBase<SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>>
        where TStoreFunctions : IStoreFunctions<SpanByte>
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
        readonly int overflowAllocatorFixedPageSize;

        public SpanByteAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, SpanByteAllocator<TStoreFunctions>> wrapperCreator)
            : base(settings.LogSettings, storeFunctions, wrapperCreator, settings.evictCallback, settings.epoch, settings.flushCallback, settings.logger)
        {
            // TODO: Verify LogSettings.MaxInlineKeySizeBits, .MaxInlineValueSizeBits, and .OverflowPageSizeBits are in range. Do we need config for OversizeLimit?
            maxInlineKeySize = 1 << settings.LogSettings.MaxInlineKeySizeBits;
            maxInlineValueSize = 1 << settings.LogSettings.MaxInlineValueSizeBits;
            overflowAllocatorFixedPageSize = 1 << settings.LogSettings.OverflowFixedPageSizeBits;

            freePagePool = new OverflowPool<PageUnit<OverflowAllocator>>(4, p => { });

            var bufferSizeInBytes = (nuint)RoundUp(sizeof(long*) * BufferSize, Constants.kCacheLineBytes);
            pagePointers = (long*)NativeMemory.AlignedAlloc(bufferSizeInBytes, Constants.kCacheLineBytes);
            NativeMemory.Clear(pagePointers, bufferSizeInBytes);

            values = new OverflowAllocator[BufferSize];
            for (var ii = 0; ii < BufferSize; ++ii)
                values[ii] = new(overflowAllocatorFixedPageSize);
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
            values[index] = new(overflowAllocatorFixedPageSize);
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
        internal LogRecord<SpanByte> CreateLogRecord(long logicalAddress) => CreateLogRecord(logicalAddress, GetPhysicalAddress(logicalAddress));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord<SpanByte> CreateLogRecord(long logicalAddress, long physicalAddress) => new LogRecord<SpanByte>(physicalAddress, GetOverflowAllocator(logicalAddress));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OverflowAllocator GetOverflowAllocator(long logicalAddress) => values[GetPageIndex(logicalAddress)];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SerializeKey(SpanByte key, long logicalAddress, ref LogRecord<SpanByte> logRecord) => SerializeKey(key, logicalAddress, ref logRecord, maxInlineKeySize, GetOverflowAllocator(logicalAddress));

        public override void Initialize() => Initialize(Constants.kFirstValidAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeValue(long physicalAddress, ref RecordSizeInfo sizeInfo)
        {
            var valueAddress = LogRecord.GetValueAddress(physicalAddress);
            _ = !sizeInfo.ValueIsOverflow
                        ? SpanField.SetInlineDataLength(valueAddress, sizeInfo.FieldInfo.ValueSize - SpanField.FieldLengthPrefixSize)
                        : SpanField.SetInlineDataLength(valueAddress, SpanField.OverflowDataPtrSize);   // Set the field length for the pointer, but wait for LogRecord<TValue>.TrySetValueSpan to do the allocation.
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetRMWCopyRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(ref TSourceLogRecord srcLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
            where TVariableLengthInput : IVariableLengthInput<SpanByte, TInput>
        {
            // Used by RMW to determine the length of copy destination (client uses Input to fill in whether ETag and Expiration are inluded); Filler information is not needed.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetRMWModifiedFieldInfo(ref srcLogRecord, ref input) };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetRMWInitialRecordSize<TInput, TVariableLengthInput>(SpanByte key, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<SpanByte, TInput>
        {
            // Used by RMW to determine the length of initial destination (client uses Input to fill in whether ETag and Expiration are inluded); Filler information is not needed.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetRMWInitialFieldInfo(key, ref input) };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(SpanByte key, SpanByte value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<SpanByte, TInput>
        {
            // Used by Upsert to determine the length of insert destination (client uses Input to fill in whether ETag and Expiration are inluded); Filler information is not needed.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetUpsertFieldInfo(key, value, ref input) };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetDeleteRecordSize(SpanByte key)
        {
            // Used by Delete to determine the length of a new tombstone record. Does not require an ISessionFunctions method.
            var sizeInfo = new RecordSizeInfo()
            {
                FieldInfo = new()
                {
                    KeySize = key.TotalSize,
                    ValueSize = sizeof(int), // No payload for the default value
                    HasETag = false,
                    HasExpiration = false
                }
            };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        public void PopulateRecordSizeInfo(ref RecordSizeInfo sizeInfo)
        {
            var keySize = sizeInfo.FieldInfo.KeySize;
            if (keySize > maxInlineKeySize)
            {
                keySize = SpanField.OverflowInlineSize;
                sizeInfo.KeyIsOverflow = true;
            }
            var valueSize = sizeInfo.FieldInfo.ValueSize;
            if (valueSize > maxInlineValueSize)
            {
                valueSize = SpanField.OverflowInlineSize;
                sizeInfo.ValueIsOverflow = true;
            }
            sizeInfo.MaxInlineValueSpanSize = maxInlineValueSize;
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

        /// <inheritdoc/>
        internal override void SerializeRecordToIteratorBuffer(ref LogRecord<SpanByte> logRecord, ref SectorAlignedMemory recordBuffer, out SpanByte valueObject)
        {
            var inlineRecordSize = logRecord.GetInlineRecordSizes().allocatedSize;
            if (inlineRecordSize > int.MaxValue)
                throw new TsavoriteException("Total size out of range");

            bufferPool.EnsureSize(ref recordBuffer, inlineRecordSize);
            var ptr = recordBuffer.GetValidPointer();

            *(RecordInfo*)ptr = logRecord.Info;
            ptr += RecordInfo.GetLength();

            *(long*)ptr = (long)inlineRecordSize;
            ptr += DiskLogRecord.SerializedRecordLengthSize;

            if (logRecord.Info.HasETag)
            {
                *(long*)ptr = logRecord.ETag;
                ptr += LogRecord.ETagSize;
            }

            if (logRecord.Info.HasExpiration)
            {
                *(long*)ptr = logRecord.Expiration;
                ptr += LogRecord.ExpirationSize;
            }

            var key = logRecord.Key;
            *(int*)ptr = key.Length;
            ptr += SpanField.FieldLengthPrefixSize;
            key.CopyTo(new Span<byte>(ptr, key.Length));
            ptr += key.Length;

            var value = logRecord.ValueSpan;
            *(int*)ptr = value.Length;
            ptr += SpanField.FieldLengthPrefixSize;
            value.CopyTo(new Span<byte>(ptr, value.Length));
            ptr += value.Length;

            valueObject = default;
        }

        /// <inheritdoc/>
        internal override void DeserializeFromDiskBuffer(ref DiskLogRecord<SpanByte> diskLogRecord, (byte[] array, long offset) byteStream) { /* This allocator has no IHeapObject */ }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DisposeRecord(ref LogRecord<SpanByte> logRecord, DisposeReason disposeReason)
        {
            // Release any overflow allocations for Key and possibly Value spans.
            logRecord.FreeKeyOverflow();
            logRecord.FreeValueOverflow();
        }

        internal void DisposeRecord(ref DiskLogRecord<SpanByte> logRecord, DisposeReason disposeReason) { /* This allocator has no IHeapObject */ }

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            freePagePool.Dispose();
            foreach (var value in values)
                value.Dispose();
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
        protected override void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext<SpanByte> context, SectorAlignedMemory result = default)
        {
            throw new InvalidOperationException("AsyncReadRecordObjectsToMemory invalid for SpanByteAllocator");
        }

        internal IHeapContainer<SpanByte> GetKeyContainer(ref SpanByte key) => new SpanByteHeapContainer(key, bufferPool);

        internal IHeapContainer<SpanByte> GetValueContainer(ref SpanByte value) => new SpanByteHeapContainer(value, bufferPool);

        internal static long[] GetSegmentOffsets() => null;

        internal void PopulatePage(byte* src, int required_bytes, long destinationPage)
        {
            throw new TsavoriteException("SpanByteAllocator memory pages are sector aligned - use direct copy");
            // Buffer.MemoryCopy(src, (void*)pointers[destinationPage % BufferSize], required_bytes, required_bytes);
        }

        /// <summary>
        /// Iterator interface for pull-scanning Tsavorite log
        /// </summary>
        public override ITsavoriteScanIterator<SpanByte> Scan(TsavoriteKV<SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, DiskScanBufferingMode diskScanBufferingMode, bool includeSealedRecords)
            => new RecordScanIterator<SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>>(store, this, beginAddress, endAddress, epoch, diskScanBufferingMode, includeSealedRecords: includeSealedRecords, logger: logger);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, ref TScanFunctions scanFunctions, DiskScanBufferingMode diskScanBufferingMode)
        {
            using RecordScanIterator<SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> iter = new(store, this, beginAddress, endAddress, epoch, diskScanBufferingMode, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store,
                ScanCursorState<SpanByte> scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor)
        {
            using RecordScanIterator<SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> iter = new(store, this, cursor, endAddress, epoch, DiskScanBufferingMode.SinglePageBuffering, logger: logger);
            return ScanLookup<SpanByte, SpanByteAndMemory, TScanFunctions, RecordScanIterator<SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>>>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store,
                SpanByte key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using RecordScanIterator<SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>> iter = new(store, this, beginAddress, epoch, logger: logger);
            return IterateHashChain(store, key, beginAddress, ref scanFunctions, iter);
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator<SpanByte>> observer)
        {
            using var iter = new RecordScanIterator<SpanByte, TStoreFunctions, SpanByteAllocator<TStoreFunctions>>(store: null, this, beginAddress, endAddress, epoch, DiskScanBufferingMode.NoBuffering, InMemoryScanBufferingMode.NoBuffering, 
                    includeSealedRecords: false, assumeInMemory: true, logger: logger);
            observer?.OnNext(iter);
        }
    }
}