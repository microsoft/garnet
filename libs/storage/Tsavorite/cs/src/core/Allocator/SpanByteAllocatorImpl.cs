// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    using static Utility;
    using static LogAddress;

    // Allocator for ReadOnlySpan<byte> Key and Span<byte> Value.
    internal sealed unsafe class SpanByteAllocatorImpl<TStoreFunctions> : AllocatorBase<TStoreFunctions, SpanByteAllocator<TStoreFunctions>>
        where TStoreFunctions : IStoreFunctions
    {
        /// <summary>Circular buffer definition</summary>
        /// <remarks>The long is actually a byte*, but storing as 'long' makes going through logicalAddress/physicalAddress translation more easily</remarks>
        long* pagePointers;

        private OverflowPool<PageUnit<Empty>> freePagePool;

        public SpanByteAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, SpanByteAllocator<TStoreFunctions>> wrapperCreator)
            : base(settings.LogSettings, storeFunctions, wrapperCreator, settings.evictCallback, settings.epoch, settings.flushCallback, settings.logger)
        {
            freePagePool = new OverflowPool<PageUnit<Empty>>(4, p => { });

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

        /// <summary>Allocate memory page, pinned in memory, and in sector aligned form, if possible</summary>
        internal void AllocatePage(int index)
        {
            IncrementAllocatedPageCount();

            if (freePagePool.TryGet(out var item))
            {
                pagePointers[index] = item.pointer;
                // TODO resize the values[index] arrays smaller if they are above a certain point
                return;
            }

            // No free pages are available so allocate new
            pagePointers[index] = (long)NativeMemory.AlignedAlloc((nuint)PageSize, (nuint)sectorSize);
            NativeMemory.Clear((void*)pagePointers[index], (nuint)PageSize);
        }

        void ReturnPage(int index)
        {
            Debug.Assert(index < BufferSize);
            if (pagePointers[index] != default)
            {
                _ = freePagePool.TryAdd(new()
                {
                    pointer = pagePointers[index],
                    value = Empty.Default
                });
                pagePointers[index] = default;
                _ = Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateLogRecord(long logicalAddress) => CreateLogRecord(logicalAddress, GetPhysicalAddress(logicalAddress));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateLogRecord(long logicalAddress, long physicalAddress) => new LogRecord(physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SerializeKey(ReadOnlySpan<byte> key, long logicalAddress, ref LogRecord logRecord) => SerializeKey(key, logicalAddress, ref logRecord, maxInlineKeySize: int.MaxValue, objectIdMap: null);

        public override void Initialize() => Initialize(FirstValidAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeValue(long physicalAddress, in RecordSizeInfo sizeInfo)
        {
            // Value is always inline in the SpanByteAllocator
            var valueAddress = LogRecord.GetValueAddress(physicalAddress);

            LogRecord.GetInfoRef(physicalAddress).SetValueIsInline();
            _ = LogField.SetInlineDataLength(valueAddress, sizeInfo.FieldInfo.ValueDataSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetRMWCopyRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(in TSourceLogRecord srcLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>
        {
            // Used by RMW to determine the length of copy destination (client uses Input to fill in whether ETag and Expiration are inluded); Filler information is not needed.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetRMWModifiedFieldInfo(in srcLogRecord, ref input) };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetRMWInitialRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>
        {
            // Used by RMW to determine the length of initial destination (client uses Input to fill in whether ETag and Expiration are inluded); Filler information is not needed.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetRMWInitialFieldInfo(key, ref input) };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>
        {
            // Used by Upsert to determine the length of insert destination (client uses Input to fill in whether ETag and Expiration are inluded); Filler information is not needed.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetUpsertFieldInfo(key, value, ref input) };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetUpsertRecordSize<TSourceLogRecord, TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>
        {
            // Used by Upsert to determine the length of insert destination (client uses Input to fill in whether ETag and Expiration are inluded); Filler information is not needed.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetUpsertFieldInfo(key, in inputLogRecord, ref input) };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, IHeapObject value, ref TInput input, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<TInput>
        {
            // Used by Upsert to determine the length of insert destination (client uses Input to fill in whether ETag and Expiration are inluded); Filler information is not needed.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetUpsertFieldInfo(key, value, ref input) };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetDeleteRecordSize(ReadOnlySpan<byte> key)
        {
            // Used by Delete to determine the length of a new tombstone record. Does not require an ISessionFunctions method.
            var sizeInfo = new RecordSizeInfo()
            {
                FieldInfo = new()
                {
                    KeyDataSize = key.Length,
                    ValueDataSize = 0, // No payload for the default value
                    HasETag = false,
                    HasExpiration = false
                }
            };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        public void PopulateRecordSizeInfo(ref RecordSizeInfo sizeInfo)
        {
            // For SpanByteAllocator, we are always inline.
            // Key
            sizeInfo.KeyIsInline = true;
            var keySize = sizeInfo.FieldInfo.KeyDataSize + LogField.InlineLengthPrefixSize;

            // Value
            sizeInfo.MaxInlineValueSpanSize = int.MaxValue; // Not currently doing out-of-line for SpanByteAllocator
            sizeInfo.ValueIsInline = true;
            var valueSize = sizeInfo.FieldInfo.ValueDataSize + LogField.InlineLengthPrefixSize;

            // Record
            sizeInfo.ActualInlineRecordSize = RecordInfo.GetLength() + keySize + valueSize + sizeInfo.OptionalSize;
            sizeInfo.AllocatedInlineRecordSize = RoundUp(sizeInfo.ActualInlineRecordSize, Constants.kRecordAlignment);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DisposeRecord(ref LogRecord logRecord, DisposeReason disposeReason)
        {
            logRecord.ClearOptionals();
            // Key and Value are always inline in the SpanByteAllocator so this is a no-op
        }

        internal void DisposeRecord(ref DiskLogRecord logRecord, DisposeReason disposeReason) { /* This allocator has no IHeapObject */ }

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            var localFreePagePool = Interlocked.Exchange(ref freePagePool, null);
            if (localFreePagePool != null)
            {
                base.Dispose();
                localFreePagePool.Dispose();

                if (pagePointers is not null)
                {
                    for (var ii = 0; ii < BufferSize; ii++)
                    {
                        if (pagePointers[ii] != 0)
                            NativeMemory.AlignedFree((void*)pagePointers[ii]);
                    }
                    NativeMemory.AlignedFree((void*)pagePointers);
                    pagePointers = null;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPhysicalAddress(long logicalAddress)
        {
            // Index of page within the circular buffer, and offset on the page. TODO move this (and pagePointers) to AllocatorBase)
            var pageIndex = GetPageIndexForAddress(logicalAddress);
            var offset = GetOffsetOnPage(logicalAddress);
            return *(pagePointers + pageIndex) + offset;
        }

        internal bool IsAllocated(int pageIndex) => pagePointers[pageIndex] != 0;

        protected override void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
            => WriteInlinePageAsync((IntPtr)pagePointers[flushPage % BufferSize], (ulong)(AlignedPageSizeBytes * flushPage),
                    (uint)AlignedPageSizeBytes, callback, asyncResult, device);

        protected override void WriteAsyncToDevice<TContext>(long startPage, long flushPage, int pageSize,
            DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult, IDevice device, long fuzzyStartLogicalAddress)
        {
            VerifyCompatibleSectorSize(device);
            WriteInlinePageAsync((IntPtr)pagePointers[flushPage % BufferSize], (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                    (uint)AlignedPageSizeBytes, callback, asyncResult, device);
        }

        internal void ClearPage(long page, int offset)
            => NativeMemory.Clear((byte*)pagePointers[page % BufferSize] + offset, (nuint)(PageSize - offset));

        internal void FreePage(long page)
        {
            ClearPage(page, 0);
            if (EmptyPageCount > 0)
                ReturnPage((int)(page % BufferSize));
        }

        protected override void ReadAsync<TContext>(ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
            DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice)
            => device.ReadAsync(alignedSourceAddress, (IntPtr)pagePointers[destinationPageIndex], aligned_read_length, callback, asyncResult);

        internal void PopulatePage(byte* src, int required_bytes, long destinationPage)
        {
            throw new TsavoriteException("SpanByteAllocator memory pages are sector aligned - use direct copy");
            // Buffer.MemoryCopy(src, (void*)pointers[destinationPage % BufferSize], required_bytes, required_bytes);
        }

        /// <summary>
        /// Iterator interface for pull-scanning Tsavorite log
        /// </summary>
        public override ITsavoriteScanIterator Scan(TsavoriteKV<TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, DiskScanBufferingMode diskScanBufferingMode, bool includeSealedRecords)
            => new RecordScanIterator<TStoreFunctions, SpanByteAllocator<TStoreFunctions>>(store, this, beginAddress, endAddress, epoch, diskScanBufferingMode, includeSealedRecords: includeSealedRecords, logger: logger);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, ref TScanFunctions scanFunctions, DiskScanBufferingMode diskScanBufferingMode)
        {
            using RecordScanIterator<TStoreFunctions, SpanByteAllocator<TStoreFunctions>> iter = new(store, this, beginAddress, endAddress, epoch, diskScanBufferingMode, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store,
                ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor, long maxAddress)
        {
            using RecordScanIterator<TStoreFunctions, SpanByteAllocator<TStoreFunctions>> iter = new(store, this, cursor, endAddress, epoch, DiskScanBufferingMode.SinglePageBuffering, logger: logger);
            return ScanLookup<PinnedSpanByte, SpanByteAndMemory, TScanFunctions, RecordScanIterator<TStoreFunctions, SpanByteAllocator<TStoreFunctions>>>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor, maxAddress);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<TStoreFunctions, SpanByteAllocator<TStoreFunctions>> store,
                ReadOnlySpan<byte> key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using RecordScanIterator<TStoreFunctions, SpanByteAllocator<TStoreFunctions>> iter = new(store, this, beginAddress, epoch, logger: logger);
            return IterateHashChain(store, key, beginAddress, ref scanFunctions, iter);
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator> observer)
        {
            using var iter = new RecordScanIterator<TStoreFunctions, SpanByteAllocator<TStoreFunctions>>(store: null, this, beginAddress, endAddress, epoch, DiskScanBufferingMode.NoBuffering, InMemoryScanBufferingMode.NoBuffering,
                    includeSealedRecords: false, assumeInMemory: true, logger: logger);
            observer?.OnNext(iter);
        }
    }
}