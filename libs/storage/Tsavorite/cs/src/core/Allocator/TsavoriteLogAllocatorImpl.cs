// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    // This is unused; just allows things to build. TsavoriteLog does not do key comparisons or value operations; it is just a memory allocator.
    using TsavoriteLogStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>Simple log allocator used by TsavoriteLog</summary>
    public sealed unsafe class TsavoriteLogAllocatorImpl : AllocatorBase<TsavoriteLogStoreFunctions, TsavoriteLogAllocator>
    {
        private readonly OverflowPool<PageUnit<Empty>> freePagePool;

        /// <summary>Constructor</summary>
#pragma warning disable IDE0290 // Use primary constructor
        public TsavoriteLogAllocatorImpl(AllocatorSettings settings)
            : base(settings.LogSettings, new TsavoriteLogStoreFunctions(), @this => new TsavoriteLogAllocator(@this), evictCallback: null, settings.epoch, settings.flushCallback, settings.logger)
        {
            freePagePool = new OverflowPool<PageUnit<Empty>>(4, p => { });
        }

        /// <inheritdoc/>
        public override void Reset()
        {
            base.Reset();
            for (var index = 0; index < BufferSize; index++)
            {
                if (IsAllocated(index))
                    FreePage(index);
            }
            Initialize();
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

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            freePagePool.Dispose();
        }

        /// <summary>
        /// Allocate memory page, pinned in memory
        /// </summary>
        /// <param name="index"></param>
        internal void AllocatePage(int index)
        {
            IncrementAllocatedPageCount();

            if (freePagePool.TryGet(out var item))
                pagePointers[index] = item.pointer;
            else
            {
                // No free pages are available so allocate new
                var adjustedSize = PageSize + 2 * sectorSize;
                byte[] tmp = GC.AllocateArray<byte>(adjustedSize, true);
                long p = (long)Unsafe.AsPointer(ref tmp[0]);
                pagePointers[index] = (p + (sectorSize - 1)) & ~((long)sectorSize - 1);
                values[index] = tmp;
            }
            PageHeader.Initialize(pagePointers[index]);
        }

        internal int OverflowPageCount => freePagePool.Count;

        /// <inheritdoc/>
        protected override void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
        {
            WriteInlinePageAsync((IntPtr)pagePointers[flushPage % BufferSize],
                    (ulong)(AlignedPageSizeBytes * flushPage),
                    (uint)AlignedPageSizeBytes,
                    callback,
                    asyncResult, device);
        }

        /// <inheritdoc/>
        protected override void WriteAsyncToDevice<TContext>(long startPage, long flushPage, int pageSize, DeviceIOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice, long fuzzyStartLogicalAddress)
        {
            VerifyCompatibleSectorSize(device);
            var alignedPageSize = (pageSize + (sectorSize - 1)) & ~(sectorSize - 1);

            WriteInlinePageAsync((IntPtr)pagePointers[flushPage % BufferSize],
                        (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                        (uint)alignedPageSize, callback, asyncResult,
                        device);
        }

        internal void FreePage(long page)
        {
            ClearPage(page, 0);
            if (EmptyPageCount > 0)
                ReturnPage((int)(page % BufferSize));
        }

        protected override void ReadAsync<TContext>(ulong alignedSourceAddress, IntPtr destinationPtr, uint aligned_read_length,
                DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device)
            => device.ReadAsync(alignedSourceAddress, destinationPtr, aligned_read_length, callback, asyncResult);

        private protected override bool VerifyRecordFromDiskCallback(ref AsyncIOContext ctx, out long prevAddressToRead, out int prevLengthToRead)
            => throw new TsavoriteException("TsavoriteLogAllocator does not support VerifyRecordFromDiskCallback");

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
                ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor, long maxAddress,
                bool resetCursor = true, bool includeTombstones = false)
            => throw new TsavoriteException("TsavoriteLogAllocator Scan methods should not be used");

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<TsavoriteLogStoreFunctions, TsavoriteLogAllocator> store, ReadOnlySpan<byte> key,
                long beginAddress, ref TScanFunctions scanFunctions)
            => throw new TsavoriteException("TsavoriteLogAllocator Scan methods should not be used");

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator> observer)
            => throw new TsavoriteException("TsavoriteLogAllocator Scan methods should not be used");

        /// <summary>
        /// Read pages from specified device
        /// </summary>
        internal void AsyncReadPageFromDeviceToFrame<TContext>(
                                        long readPage,
                                        long untilAddress,
                                        DeviceIOCompletionCallback callback,
                                        TContext context,
                                        BlittableFrame frame,
                                        out CountdownEvent completed,
                                        long devicePageOffset = 0,
                                        IDevice device = null,
                                        IDevice objectLogDevice = null,
                                        CancellationTokenSource cts = null)
        {
            var usedDevice = device ?? this.device;

            completed = new CountdownEvent(1);

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