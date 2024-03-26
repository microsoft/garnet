// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    // Allocator for SpanByte, possibly with a Blittable Key or Value.
    internal sealed unsafe class SpanByteAllocator : AllocatorBase<SpanByte, SpanByte>
    {
        public const int kRecordAlignment = 8; // RecordInfo has a long field, so it should be aligned to 8-bytes

        // Circular buffer definition
        private readonly byte[][] values;
        private readonly long[] pointers;
        private readonly long* nativePointers;

        private readonly OverflowPool<PageUnit> overflowPagePool;

        public SpanByteAllocator(LogSettings settings, ITsavoriteEqualityComparer<SpanByte> comparer, Action<long, long> evictCallback = null, LightEpoch epoch = null, Action<CommitInfo> flushCallback = null, ILogger logger = null)
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

        internal override int OverflowPageCount => overflowPagePool.Count;

        public override void Reset()
        {
            base.Reset();
            for (int index = 0; index < BufferSize; index++)
                ReturnPage(index);

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

        public override void Initialize() => Initialize(Constants.kFirstValidAddress);

        public override ref RecordInfo GetInfo(long physicalAddress) => ref Unsafe.AsRef<RecordInfo>((void*)physicalAddress);

        public override ref RecordInfo GetInfoFromBytePointer(byte* ptr) => ref Unsafe.AsRef<RecordInfo>(ptr);

        public override ref SpanByte GetKey(long physicalAddress) => ref Unsafe.AsRef<SpanByte>((byte*)physicalAddress + RecordInfo.GetLength());

        public override ref SpanByte GetValue(long physicalAddress) => ref Unsafe.AsRef<SpanByte>((byte*)ValueOffset(physicalAddress));

        public override ref SpanByte GetAndInitializeValue(long physicalAddress, long endAddress)
        {
            var src = (byte*)ValueOffset(physicalAddress);

            // Initialize the SpanByte to the length of the entire value space, less the length of the int size prefix.
            *(int*)src = (int)((byte*)endAddress - src) - sizeof(int);
            return ref Unsafe.AsRef<SpanByte>(src);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long KeyOffset(long physicalAddress) => physicalAddress + RecordInfo.GetLength();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long ValueOffset(long physicalAddress) => KeyOffset(physicalAddress) + AlignedKeySize(physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int AlignedKeySize(long physicalAddress) => RoundUp(KeySize(physicalAddress), kRecordAlignment);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int KeySize(long physicalAddress) => (*(SpanByte*)KeyOffset(physicalAddress)).TotalSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ValueSize(long physicalAddress) => (*(SpanByte*)ValueOffset(physicalAddress)).TotalSize;

        public override int GetValueLength(ref SpanByte value) => value.TotalSize;

        const int FieldInitialLength = sizeof(int);     // The .Length field of a SpanByte is the initial length

        public override (int actualSize, int allocatedSize) GetRecordSize(long physicalAddress)
        {
            ref var recordInfo = ref GetInfo(physicalAddress);
            if (recordInfo.IsNull())
            {
                var l = RecordInfo.GetLength();
                return (l, l);
            }

            var valueLen = ValueSize(physicalAddress);
            if (recordInfo.Filler)  // Get the extraValueLength
                valueLen += *(int*)(ValueOffset(physicalAddress) + RoundUp(valueLen, sizeof(int)));

            var size = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + valueLen;
            return (size, RoundUp(size, kRecordAlignment));
        }

        public override (int actualSize, int allocatedSize, int keySize) GetRMWCopyDestinationRecordSize<Input, TsavoriteSession>(ref SpanByte key, ref Input input, ref SpanByte value, ref RecordInfo recordInfo, TsavoriteSession tsavoriteSession)
        {
            // Used by RMW to determine the length of copy destination (taking Input into account), so does not need to get filler length.
            var keySize = key.TotalSize;
            var size = RecordInfo.GetLength() + RoundUp(keySize, kRecordAlignment) + tsavoriteSession.GetRMWModifiedValueLength(ref value, ref input);
            return (size, RoundUp(size, kRecordAlignment), keySize);
        }

        public override int GetRequiredRecordSize(long physicalAddress, int availableBytes)
        {
            // We need at least [average record size]...
            var reqBytes = GetAverageRecordSize();
            if (availableBytes < reqBytes)
                return reqBytes;

            // We need at least [RecordInfo size] + [actual key size]...
            reqBytes = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + FieldInitialLength;
            if (availableBytes < reqBytes)
                return reqBytes;

            // We need at least [RecordInfo size] + [actual key size] + [actual value size]
            var recordInfo = GetInfo(physicalAddress);
            var valueLen = ValueSize(physicalAddress);
            if (recordInfo.Filler)
            {
                // We have a filler, so the valueLen we have now is the usedValueLength; we need to offset to where the extraValueLength is and read that int
                var alignedUsedValueLength = RoundUp(valueLen, sizeof(int));
                reqBytes = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + alignedUsedValueLength + sizeof(int);
                if (availableBytes < reqBytes)
                    return reqBytes;
                valueLen += *(int*)(ValueOffset(physicalAddress) + alignedUsedValueLength);
            }

            // Now we know the full record length.
            reqBytes = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + valueLen;
            reqBytes = RoundUp(reqBytes, kRecordAlignment);
            return reqBytes;
        }

        public override int GetAverageRecordSize() => RecordInfo.GetLength() + (RoundUp(FieldInitialLength, kRecordAlignment) * 2);

        public override int GetFixedRecordSize() => GetAverageRecordSize();

        public override (int actualSize, int allocatedSize, int keySize) GetRMWInitialRecordSize<TInput, TsavoriteSession>(ref SpanByte key, ref TInput input, TsavoriteSession tsavoriteSession)
        {
            int keySize = key.TotalSize;
            var actualSize = RecordInfo.GetLength() + RoundUp(keySize, kRecordAlignment) + tsavoriteSession.GetRMWInitialValueLength(ref input);
            return (actualSize, RoundUp(actualSize, kRecordAlignment), keySize);
        }

        public override (int actualSize, int allocatedSize, int keySize) GetRecordSize(ref SpanByte key, ref SpanByte value)
        {
            int keySize = key.TotalSize;
            var actualSize = RecordInfo.GetLength() + RoundUp(keySize, kRecordAlignment) + value.TotalSize;
            return (actualSize, RoundUp(actualSize, kRecordAlignment), keySize);
        }

        public override void SerializeKey(ref SpanByte src, long physicalAddress) => src.CopyTo((byte*)KeyOffset(physicalAddress));

        public override void SerializeValue(ref SpanByte src, long physicalAddress) => src.CopyTo((byte*)ValueOffset(physicalAddress));

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
            // AddressInfo is only used in GenericAllocator - TODO remove from other allocators
            throw new NotSupportedException();
        }

        public override AddressInfo* GetValueAddressInfo(long physicalAddress)
        {
            // AddressInfo is only used in GenericAllocator - TODO remove from other allocators
            throw new NotSupportedException();
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override long GetPhysicalAddress(long logicalAddress)
        {
            // Offset within page
            int offset = (int)(logicalAddress & ((1L << LogPageSizeBits) - 1));

            // Index of page within the circular buffer
            int pageIndex = (int)((logicalAddress >> LogPageSizeBits) & (BufferSize - 1));
            return *(nativePointers + pageIndex) + offset;
        }

        internal override bool IsAllocated(int pageIndex) => values[pageIndex] != null;

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

        public override long GetStartLogicalAddress(long page) => page << LogPageSizeBits;

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
                values[i] = null;
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
        protected override void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext<SpanByte, SpanByte> context, SectorAlignedMemory result = default)
        {
            throw new InvalidOperationException("AsyncReadRecordObjectsToMemory invalid for SpanByteAllocator");
        }

        /// <summary>
        /// Retrieve objects from object log
        /// </summary>
        /// <param name="record"></param>
        /// <param name="ctx"></param>
        /// <returns></returns>
        protected override bool RetrievedFullRecord(byte* record, ref AsyncIOContext<SpanByte, SpanByte> ctx) => true;

        public override ref SpanByte GetContextRecordKey(ref AsyncIOContext<SpanByte, SpanByte> ctx) => ref GetKey((long)ctx.record.GetValidPointer());

        public override ref SpanByte GetContextRecordValue(ref AsyncIOContext<SpanByte, SpanByte> ctx) => ref GetValue((long)ctx.record.GetValidPointer());

        public override IHeapContainer<SpanByte> GetKeyContainer(ref SpanByte key) => new SpanByteHeapContainer(ref key, bufferPool);

        public override IHeapContainer<SpanByte> GetValueContainer(ref SpanByte value) => new SpanByteHeapContainer(ref value, bufferPool);

        public override bool KeyHasObjects() => false;

        public override bool ValueHasObjects() => false;

        public override long[] GetSegmentOffsets() => null;

        internal override void PopulatePage(byte* src, int required_bytes, long destinationPage)
        {
            throw new TsavoriteException("SpanByteAllocator memory pages are sector aligned - use direct copy");
            // Buffer.MemoryCopy(src, (void*)pointers[destinationPage % BufferSize], required_bytes, required_bytes);
        }

        /// <summary>
        /// Iterator interface for pull-scanning Tsavorite log
        /// </summary>
        public override ITsavoriteScanIterator<SpanByte, SpanByte> Scan(TsavoriteKV<SpanByte, SpanByte> store, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode)
            => new SpanByteScanIterator(store, this, beginAddress, endAddress, scanBufferingMode, epoch, logger: logger);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<SpanByte, SpanByte> store, long beginAddress, long endAddress, ref TScanFunctions scanFunctions, ScanBufferingMode scanBufferingMode)
        {
            using SpanByteScanIterator iter = new(store, this, beginAddress, endAddress, scanBufferingMode, epoch, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<SpanByte, SpanByte> store, ScanCursorState<SpanByte, SpanByte> scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor)
        {
            using SpanByteScanIterator iter = new(store, this, cursor, endAddress, ScanBufferingMode.SinglePageBuffering, epoch, logger: logger);
            return ScanLookup<SpanByte, SpanByteAndMemory, TScanFunctions, SpanByteScanIterator>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<SpanByte, SpanByte> store, ref SpanByte key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using SpanByteScanIterator iter = new(store, store.comparer, this, beginAddress, epoch, logger: logger);
            return IterateKeyVersionsImpl(store, ref key, beginAddress, ref scanFunctions, iter);
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator<SpanByte, SpanByte>> observer)
        {
            using var iter = new SpanByteScanIterator(store: null, this, beginAddress, endAddress, ScanBufferingMode.NoBuffering, epoch, true, logger: logger);
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