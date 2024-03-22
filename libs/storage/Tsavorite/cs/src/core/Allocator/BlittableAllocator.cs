// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    internal sealed unsafe class BlittableAllocator<Key, Value> : AllocatorBase<Key, Value>
    {
        // Circular buffer definition
        private readonly byte[][] values;
        private readonly long[] pointers;
        private readonly long* nativePointers;

        internal static int KeySize => Unsafe.SizeOf<Key>();
        internal static int ValueSize => Unsafe.SizeOf<Value>();
        internal static int RecordSize => Unsafe.SizeOf<Record<Key, Value>>();

        private readonly OverflowPool<PageUnit> overflowPagePool;

        public BlittableAllocator(LogSettings settings, ITsavoriteEqualityComparer<Key> comparer, Action<long, long> evictCallback = null,
                LightEpoch epoch = null, Action<CommitInfo> flushCallback = null, ILogger logger = null)
            : base(settings, comparer, evictCallback, epoch, flushCallback, logger)
        {
            overflowPagePool = new OverflowPool<PageUnit>(4, p => { });

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
                ReturnPage(index);
            }
            Initialize();
        }

        void ReturnPage(int index)
        {
            Debug.Assert(index < BufferSize);
            if (values[index] != null)
            {
                overflowPagePool.TryAdd(new PageUnit
                {
                    pointer = pointers[index],
                    value = values[index]
                });
                values[index] = null;
                pointers[index] = 0;
                Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        public override void Initialize()
        {
            Initialize(Constants.kFirstValidAddress);
        }

        public override ref RecordInfo GetInfo(long physicalAddress)
        {
            return ref Unsafe.AsRef<RecordInfo>((void*)physicalAddress);
        }

        public override ref RecordInfo GetInfoFromBytePointer(byte* ptr)
        {
            return ref Unsafe.AsRef<RecordInfo>(ptr);
        }

        public override ref Key GetKey(long physicalAddress)
        {
            return ref Unsafe.AsRef<Key>((byte*)physicalAddress + RecordInfo.GetLength());
        }

        public override ref Value GetValue(long physicalAddress)
        {
            return ref Unsafe.AsRef<Value>((byte*)physicalAddress + RecordInfo.GetLength() + KeySize);
        }

        public override (int actualSize, int allocatedSize) GetRecordSize(long physicalAddress)
        {
            return (RecordSize, RecordSize);
        }

        public override (int actualSize, int allocatedSize, int keySize) GetRMWCopyDestinationRecordSize<Input, TsavoriteSession>(ref Key key, ref Input input, ref Value value, ref RecordInfo recordInfo, TsavoriteSession tsavoriteSession)
        {
            return (RecordSize, RecordSize, KeySize);
        }

        public override int GetAverageRecordSize() => RecordSize;

        public override int GetFixedRecordSize() => RecordSize;

        public override (int actualSize, int allocatedSize, int keySize) GetRMWInitialRecordSize<Input, TsavoriteSession>(ref Key key, ref Input input, TsavoriteSession tsavoriteSession)
        {
            return (RecordSize, RecordSize, KeySize);
        }

        public override (int actualSize, int allocatedSize, int keySize) GetRecordSize(ref Key key, ref Value value)
        {
            return (RecordSize, RecordSize, KeySize);
        }

        public override int GetValueLength(ref Value value) => ValueSize;

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            overflowPagePool.Dispose();
        }

        public override AddressInfo* GetKeyAddressInfo(long physicalAddress)
        {
            return (AddressInfo*)((byte*)physicalAddress + RecordInfo.GetLength());
        }

        public override AddressInfo* GetValueAddressInfo(long physicalAddress)
        {
            return (AddressInfo*)((byte*)physicalAddress + RecordInfo.GetLength() + KeySize);
        }

        /// <summary>
        /// Allocate memory page, pinned in memory, and in sector aligned form, if possible
        /// </summary>
        /// <param name="index"></param>
        internal override void AllocatePage(int index)
        {
            Interlocked.Increment(ref AllocatedPageCount);

            if (overflowPagePool.TryGet(out var item))
            {
                pointers[index] = item.pointer;
                values[index] = item.value;
                return;
            }

            var adjustedSize = PageSize + 2 * sectorSize;

            byte[] tmp = GC.AllocateArray<byte>(adjustedSize, true);
            long p = (long)Unsafe.AsPointer(ref tmp[0]);
            Array.Clear(tmp, 0, adjustedSize);
            pointers[index] = (p + (sectorSize - 1)) & ~((long)sectorSize - 1);
            values[index] = tmp;
        }

        internal override int OverflowPageCount => overflowPagePool.Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override long GetPhysicalAddress(long logicalAddress)
        {
            // Offset within page
            int offset = (int)(logicalAddress & ((1L << LogPageSizeBits) - 1));

            // Index of page within the circular buffer
            int pageIndex = (int)((logicalAddress >> LogPageSizeBits) & (BufferSize - 1));
            return *(nativePointers + pageIndex) + offset;
        }

        internal override bool IsAllocated(int pageIndex)
        {
            return values[pageIndex] != null;
        }

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

        /// <summary>
        /// Get start logical address
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        public override long GetStartLogicalAddress(long page)
        {
            return page << LogPageSizeBits;
        }


        /// <summary>
        /// Get first valid logical address
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        public override long GetFirstValidLogicalAddress(long page)
        {
            if (page == 0)
                return (page << LogPageSizeBits) + Constants.kFirstValidAddress;

            return page << LogPageSizeBits;
        }

        internal override void ClearPage(long page, int offset)
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

        internal override void FreePage(long page)
        {
            ClearPage(page, 0);
            if (EmptyPageCount > 0)
                ReturnPage((int)(page % BufferSize));
        }

        /// <summary>
        /// Delete in-memory portion of the log
        /// </summary>
        internal override void DeleteFromMemory()
        {
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = null;
            }
        }

        protected override void ReadAsync<TContext>(
            ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
            DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice)
        {
            device.ReadAsync(alignedSourceAddress, (IntPtr)pointers[destinationPageIndex],
                aligned_read_length, callback, asyncResult);
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
        protected override void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext<Key, Value> context, SectorAlignedMemory result = default)
        {
            throw new InvalidOperationException("AsyncReadRecordObjectsToMemory invalid for BlittableAllocator");
        }

        /// <summary>
        /// Retrieve objects from object log
        /// </summary>
        /// <param name="record"></param>
        /// <param name="ctx"></param>
        /// <returns></returns>
        protected override bool RetrievedFullRecord(byte* record, ref AsyncIOContext<Key, Value> ctx)
        {
            ctx.key = GetKey((long)record);
            ctx.value = GetValue((long)record);
            return true;
        }

        /// <summary>
        /// Whether KVS has keys to serialize/deserialize
        /// </summary>
        /// <returns></returns>
        public override bool KeyHasObjects()
        {
            return false;
        }

        /// <summary>
        /// Whether KVS has values to serialize/deserialize
        /// </summary>
        /// <returns></returns>
        public override bool ValueHasObjects()
        {
            return false;
        }

        public override IHeapContainer<Key> GetKeyContainer(ref Key key) => new StandardHeapContainer<Key>(ref key);
        public override IHeapContainer<Value> GetValueContainer(ref Value value) => new StandardHeapContainer<Value>(ref value);

        public override long[] GetSegmentOffsets()
        {
            return null;
        }

        internal override void PopulatePage(byte* src, int required_bytes, long destinationPage)
        {
            throw new TsavoriteException("BlittableAllocator memory pages are sector aligned - use direct copy");
        }

        /// <summary>
        /// Iterator interface for pull-scanning Tsavorite log
        /// </summary>
        public override ITsavoriteScanIterator<Key, Value> Scan(TsavoriteKV<Key, Value> store, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode)
            => new BlittableScanIterator<Key, Value>(store, this, beginAddress, endAddress, scanBufferingMode, epoch, logger: logger);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<Key, Value> store, long beginAddress, long endAddress, ref TScanFunctions scanFunctions, ScanBufferingMode scanBufferingMode)
        {
            using BlittableScanIterator<Key, Value> iter = new(store, this, beginAddress, endAddress, scanBufferingMode, epoch, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<Key, Value> store, ScanCursorState<Key, Value> scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor)
        {
            using BlittableScanIterator<Key, Value> iter = new(store, this, cursor, endAddress, ScanBufferingMode.SinglePageBuffering, epoch, logger: logger);
            return ScanLookup<long, long, TScanFunctions, BlittableScanIterator<Key, Value>>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<Key, Value> store, ref Key key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using BlittableScanIterator<Key, Value> iter = new(store, this, store.comparer, beginAddress, epoch, logger: logger);
            return IterateKeyVersionsImpl(store, ref key, beginAddress, ref scanFunctions, iter);
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator<Key, Value>> observer)
        {
            using var iter = new BlittableScanIterator<Key, Value>(store: null, this, beginAddress, endAddress, ScanBufferingMode.NoBuffering, epoch, true, logger: logger);
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
        /// <param name="cts"></param>
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
                    frame = frame,
                    cts = cts
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