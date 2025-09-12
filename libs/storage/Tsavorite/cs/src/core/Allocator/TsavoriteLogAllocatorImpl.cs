// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    using static LogAddress;

    // This is unused; just allows things to build. TsavoriteLog does not do key comparisons or value operations; it is just a memory allocator.
    using TsavoriteLogStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>Simple log allocator used by TsavoriteLog</summary>
    public sealed unsafe class TsavoriteLogAllocatorImpl : AllocatorBase<TsavoriteLogStoreFunctions, TsavoriteLogAllocator>
    {
        // Circular buffer definition
        private readonly byte[][] values;
        private readonly long[] pointers;
        private readonly long* nativePointers;

        private readonly OverflowPool<PageUnit<byte[]>> overflowPagePool;

        public TsavoriteLogAllocatorImpl(AllocatorSettings settings)
            : base(settings.LogSettings, new TsavoriteLogStoreFunctions(), @this => new TsavoriteLogAllocator(@this), evictCallback: null, settings.epoch, settings.flushCallback, settings.logger)
        {
            overflowPagePool = new OverflowPool<PageUnit<byte[]>>(4, p => { });

            if (BufferSize > 0)
            {
                values = new byte[BufferSize][];
                pointers = GC.AllocateArray<long>(BufferSize, true);
                nativePointers = (long*)Unsafe.AsPointer(ref pointers[0]);
            }
        }

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

        void ReturnPage(int index)
        {
            Debug.Assert(index < BufferSize);
            if (values[index] != null)
            {
                _ = overflowPagePool.TryAdd(new PageUnit<byte[]>
                {
                    pointer = pointers[index],
                    value = values[index]
                });
                values[index] = null;
                pointers[index] = 0;
                _ = Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        public override void Initialize() => Initialize(FirstValidAddress);

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            overflowPagePool.Dispose();
        }

        /// <summary>
        /// Allocate memory page, pinned in memory, and in sector aligned form, if possible
        /// </summary>
        /// <param name="index"></param>
        internal void AllocatePage(int index)
        {
            IncrementAllocatedPageCount();

            if (overflowPagePool.TryGet(out var item))
            {
                pointers[index] = item.pointer;
                values[index] = item.value;
                return;
            }

            var adjustedSize = PageSize + 2 * sectorSize;

            byte[] tmp = GC.AllocateArray<byte>(adjustedSize, true);
            long p = (long)Unsafe.AsPointer(ref tmp[0]);
            pointers[index] = (p + (sectorSize - 1)) & ~((long)sectorSize - 1);
            values[index] = tmp;
        }

        internal int OverflowPageCount => overflowPagePool.Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPhysicalAddress(long logicalAddress)
        {
            // Index of page within the circular buffer, and offset on the page. TODO move this (and pagePointers) to AllocatorBase)
            var pageIndex = GetPageIndexForAddress(logicalAddress);
            var offset = GetOffsetOnPage(logicalAddress);
            return *(nativePointers + pageIndex) + offset;
        }

        internal bool IsAllocated(int pageIndex) => values[pageIndex] != null;

        protected override void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
        {
            WriteAsync((IntPtr)pointers[flushPage % BufferSize],
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

            WriteAsync((IntPtr)pointers[flushPage % BufferSize],
                        (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                        (uint)alignedPageSize, callback, asyncResult,
                        device);
        }

        internal void ClearPage(long page, int offset)
        {
            if (offset == 0)
                Array.Clear(values[page % BufferSize], offset, values[page % BufferSize].Length - offset);
            else
            {
                // Adjust array offset for cache alignment
                offset += (int)(pointers[page % BufferSize] - (long)Unsafe.AsPointer(ref values[page % BufferSize][0]));
                Array.Clear(values[page % BufferSize], offset, values[page % BufferSize].Length - offset);
            }
        }

        internal void FreePage(long page)
        {
            ClearPage(page, 0);
            if (EmptyPageCount > 0)
                ReturnPage((int)(page % BufferSize));
        }

        protected override void ReadAsync<TContext>(ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
                DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice)
            => device.ReadAsync(alignedSourceAddress, (IntPtr)pointers[destinationPageIndex], aligned_read_length, callback, asyncResult);

        /// <summary>
        /// Invoked by users to obtain a record from disk. It uses sector aligned memory to read 
        /// the record efficiently into memory.
        /// </summary>
        /// <param name="fromLogical"></param>
        /// <param name="numBytes"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="result"></param>
        protected override void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext context, SectorAlignedMemory result = default)
            => throw new InvalidOperationException("AsyncReadRecordObjectsToMemory invalid for TsavoriteLogAllocator");

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
                ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor, long maxAddress, bool resetCursor = true, bool includeTombstones = false)
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
                                        IDevice objectLogDevice = null,
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