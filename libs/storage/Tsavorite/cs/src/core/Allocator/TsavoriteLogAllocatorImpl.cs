// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static LogAddress;

    // This is unused; just allows things to build. TsavoriteLog does not do key comparisons or value operations; it is just a memory allocator.
    using TsavoriteLogStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>Simple log allocator used by TsavoriteLog</summary>
    public sealed unsafe class TsavoriteLogAllocatorImpl : AllocatorBase<TsavoriteLogStoreFunctions, TsavoriteLogAllocator>
    {
        private readonly OverflowPool<PageUnit<Empty>> freePagePool;

        public TsavoriteLogAllocatorImpl(AllocatorSettings settings)
            : base(settings.LogSettings, new TsavoriteLogStoreFunctions(), @this => new TsavoriteLogAllocator(@this), evictCallback: null, settings.epoch, settings.flushCallback, settings.logger)
        {
            freePagePool = new OverflowPool<PageUnit<Empty>>(4, p => { });
        }

        public override void Reset()
        {
            base.Reset();
            for (var index = 0; index < BufferSize; index++)
            {
                if (IsAllocated(index))
                    FreePage(index);
            }
            Initialize(storeBase);
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
                _ = freePagePool.TryAdd(new ()
                {
                    pointer = pagePointers[index],
                    value = Empty.Default
                });
                pagePointers[index] = default;
                _ = Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        public override void Initialize(TsavoriteBase storeBase) => Initialize(storeBase, FirstValidAddress);

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            freePagePool.Dispose();
        }

        internal int OverflowPageCount => freePagePool.Count;

        protected override void WriteAsync<TContext>(CircularDiskPageWriteBuffer _ /*flushBuffers*/, long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
        {
            WriteInlinePageAsync((IntPtr)pagePointers[flushPage % BufferSize],
                    (ulong)(AlignedPageSizeBytes * flushPage),
                    (uint)AlignedPageSizeBytes,
                    callback,
                    asyncResult, device);
        }

        protected override void WriteAsyncToDevice<TContext> (CircularDiskPageWriteBuffer _ /*flushBuffers*/, long startPage, long flushPage, int pageSize,
            DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult, IDevice device, long fuzzyStartLogicalAddress)
        {
            VerifyCompatibleSectorSize(device);
            var alignedPageSize = (pageSize + (sectorSize - 1)) & ~(sectorSize - 1);

            WriteInlinePageAsync((IntPtr)pagePointers[flushPage % BufferSize],
                        (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                        (uint)alignedPageSize, callback, asyncResult,
                        device);
        }

        internal void ClearPage(long page, int offset)
            => NativeMemory.Clear((byte*)pagePointers[page % BufferSize] + offset, (nuint)(PageSize - offset));

        internal void FreePage(long page)
        {
            ClearPage(page, 0);
            if (EmptyPageCount > 0)
                ReturnPage((int)(page % BufferSize));
        }

        private protected override bool VerifyRecordFromDiskCallback(ref AsyncIOContext ctx, out long prevAddressToRead, out int prevLengthToRead)
            => throw new TsavoriteException("TsavoriteLogAllocator does not support VerifyRecordFromDiskCallback");

        protected override void ReadAsync<TContext>(ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
                DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device)
            => device.ReadAsync(alignedSourceAddress, (IntPtr)pagePointers[destinationPageIndex], aligned_read_length, callback, asyncResult);

        internal static void PopulatePage(byte* src, int required_bytes, long destinationPage)
            => throw new TsavoriteException("TsavoriteLogAllocator memory pages are sector aligned - use direct copy");

        /// <summary>
        /// Iterator interface for pull-scanning Tsavorite log
        /// </summary>
        public override ITsavoriteScanIterator Scan(TsavoriteKV<TsavoriteLogStoreFunctions, TsavoriteLogAllocator> store,
                long beginAddress, long endAddress, DiskScanBufferingMode diskScanBufferingMode, bool includeSealedRecords)
            => throw new TsavoriteException("TsavoriteLogAllocator Scan methods should not be used");

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<TsavoriteLogStoreFunctions, TsavoriteLogAllocator> store,
                long beginAddress, long endAddress, ref TScanFunctions scanFunctions, DiskScanBufferingMode diskScanBufferingMode)
            => throw new TsavoriteException("TsavoriteLogAllocator Scan methods should not be used");

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<TsavoriteLogStoreFunctions, TsavoriteLogAllocator> store,
                ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor, long maxAddress)
            => throw new TsavoriteException("TsavoriteLogAllocator Scan methods should not be used");

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<TsavoriteLogStoreFunctions, TsavoriteLogAllocator> store, ReadOnlySpan<byte> key, long beginAddress, ref TScanFunctions scanFunctions)
            => throw new TsavoriteException("TsavoriteLogAllocator Scan methods should not be used");

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator> observer)
            => throw new TsavoriteException("TsavoriteLogAllocator Scan methods should not be used");

        /// <summary>
        /// Read pages from specified device
        /// </summary>
        internal void AsyncReadPagesFromDeviceToFrame<TContext>(
                                        long readPageStart,
                                        int numPages,
                                        long untilAddress,
                                        DeviceIOCompletionCallback callback,
                                        TContext context,
                                        BlittableFrame frame,
                                        out CountdownEvent completed,
                                        long devicePageOffset = 0,
                                        IDevice device = null,
                                        CancellationTokenSource cts = null)
        {
            var usedDevice = device ?? this.device;

            completed = new CountdownEvent(numPages);
            for (long readPage = readPageStart; readPage < (readPageStart + numPages); readPage++)
            {
                int pageIndex = (int)(readPage % frame.frameSize);
                if (frame.frame[pageIndex] == null)
                    frame.Allocate(pageIndex);
                else
                    frame.Clear(pageIndex);

                var asyncResult = new PageAsyncReadResult<TContext>()
                {
                    page = readPage,
                    context = context,
                    handle = completed,
                    frame = frame,
                    cts = cts
                };

                ulong offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);

                uint readLength = (uint)AlignedPageSizeBytes;
                long adjustedUntilAddress = AlignedPageSizeBytes * GetPage(untilAddress) + GetOffsetOnPage(untilAddress);

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