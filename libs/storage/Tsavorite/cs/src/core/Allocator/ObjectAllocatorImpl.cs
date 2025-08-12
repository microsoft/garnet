// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//#define READ_WRITE

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    using static LogAddress;
    using static Utility;

    internal sealed unsafe class ObjectAllocatorImpl<TStoreFunctions> : AllocatorBase<TStoreFunctions, ObjectAllocator<TStoreFunctions>>
        where TStoreFunctions : IStoreFunctions
    {
        /// <summary>For each in-memory page of this allocator we have an <see cref="ObjectIdMap"/> for keys that are too large to fit inline into the main log
        /// and become overflow byte[], or are Object values; this is needed to root the objects for GC.</summary>
        internal struct ObjectPage
        {
            internal readonly ObjectIdMap objectIdMap { get; init; }

            public ObjectPage() => objectIdMap = new();

            internal readonly void Clear() => objectIdMap?.Clear();       // TODO: Ensure we have already called the RecordDisposer
        }

        /// <summary>The pages of the log, containing object storage. In parallel with AllocatorBase.pagePointers</summary>
        internal ObjectPage[] values;

        /// <summary>
        /// Offset of expanded IO tail on the disk for Flush. This is added to .PreviousAddress for all records being flushed. It leads <see cref="ClosedDiskTailOffset"/>.
        /// </summary>
        long FlushedDiskTailOffset;

        /// <summary>
        /// Offset of expanded IO tail on the disk for Close. When we arrive at <see cref="AllocatorBase{TStoreFunctions, TAllocator}.OnPagesClosed(long)"/>
        /// this is used to calculate the offset for each .PreviousAddress in the in-memory log that references the record(s) being closed, so it can be patched up
        /// to the on-disk address for that record by <see cref="PatchExpandedAddresses"/>.
        /// </summary>
        long ClosedDiskTailOffset;

        private readonly OverflowPool<PageUnit<ObjectPage>> freePagePool;

        public ObjectAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, ObjectAllocator<TStoreFunctions>> wrapperCreator)
            : base(settings.LogSettings, storeFunctions, wrapperCreator, settings.evictCallback, settings.epoch, settings.flushCallback, settings.logger, isObjectAllocator: true)
        {
            freePagePool = new OverflowPool<PageUnit<ObjectPage>>(4, static p => { });

            values = new ObjectPage[BufferSize];
            for (var ii = 0; ii < BufferSize; ii++)
                values[ii] = new();

            Debug.Assert(ObjectIdMap.ObjectIdSize == sizeof(int), "InlineLengthPrefixSize must be equal to ObjectIdMap.ObjectIdSize");
        }

        internal int OverflowPageCount => freePagePool.Count;

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

        /// <summary>Allocate memory page, pinned in memory, and in sector aligned form, if possible</summary>
        internal void AllocatePage(int index)
        {
            IncrementAllocatedPageCount();

            if (freePagePool.TryGet(out var item))
            {
                pagePointers[index] = item.pointer;
                values[index] = item.value;
                // TODO resize the values[index] arrays smaller if they are above a certain point
                return;
            }

            // No free pages are available so allocate new
            pagePointers[index] = (long)NativeMemory.AlignedAlloc((nuint)PageSize, (nuint)sectorSize);
            NativeMemory.Clear((void*)pagePointers[index], (nuint)PageSize);
            values[index] = new();
        }

        void ReturnPage(int index)
        {
            Debug.Assert(index < BufferSize);
            if (pagePointers[index] != default)
            {
                _ = freePagePool.TryAdd(new()
                {
                    pointer = pagePointers[index],
                    value = values[index]
                });
                pagePointers[index] = default;
                _ = Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateLogRecord(long logicalAddress) => CreateLogRecord(logicalAddress, GetPhysicalAddress(logicalAddress));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateLogRecord(long logicalAddress, long physicalAddress) => new(physicalAddress, values[GetPageIndexForAddress(logicalAddress)].objectIdMap);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ObjectIdMap GetObjectIdMap(long logicalAddress) => values[GetPageIndexForAddress(logicalAddress)].objectIdMap;

        public override void Initialize(TsavoriteBase storeBase) => Initialize(storeBase, FirstValidAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeRecord(ReadOnlySpan<byte> key, long logicalAddress, in RecordSizeInfo sizeInfo, ref LogRecord logRecord)
            => logRecord.InitializeRecord(key, in sizeInfo, GetObjectIdMap(logicalAddress));

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
        public RecordSizeInfo GetUpsertRecordSize<TInput, TVariableLengthInput>(ReadOnlySpan<byte> key, IHeapObject value, ref TInput input, TVariableLengthInput varlenInput)
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
        public RecordSizeInfo GetDeleteRecordSize(ReadOnlySpan<byte> key)
        {
            // Used by Delete to determine the length of a new tombstone record. Does not require an ISessionFunctions method.
            var sizeInfo = new RecordSizeInfo()
            {
                FieldInfo = new()
                {
                    KeySize = key.Length,
                    ValueSize = 0,      // This will be inline, and with the length prefix and possible space when rounding up to kRecordAlignment, allows the possibility revivification can reuse the record for a Heap Field
                    HasETag = false,
                    HasExpiration = false
                }
            };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PopulateRecordSizeInfo(ref RecordSizeInfo sizeInfo)
        {
            // Object allocator may have Inline or Overflow Keys or Values; additionally, Values may be Object. Both non-inline cases are an objectId in the record.
            // Key
            sizeInfo.KeyIsInline = sizeInfo.FieldInfo.KeySize <= maxInlineKeySize;
            var keySize = sizeInfo.KeyIsInline ? sizeInfo.FieldInfo.KeySize : ObjectIdMap.ObjectIdSize;

            // Value
            sizeInfo.MaxInlineValueSize = maxInlineValueSize;
            sizeInfo.ValueIsInline = !sizeInfo.ValueIsObject && sizeInfo.FieldInfo.ValueSize <= sizeInfo.MaxInlineValueSize;
            var valueSize = sizeInfo.ValueIsInline ? sizeInfo.FieldInfo.ValueSize : ObjectIdMap.ObjectIdSize;

            // Record
            sizeInfo.CalculateSizes(keySize, valueSize);
        }

        /// <summary>
        /// Dispose an in-memory <see cref="LogRecord"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DisposeRecord(ref LogRecord logRecord, DisposeReason disposeReason)
        {
            logRecord.ClearOptionals();
            if (disposeReason != DisposeReason.Deleted)
                _ = logRecord.ClearKeyIfOverflow();
            _ = logRecord.ClearValueIfHeap(obj => storeFunctions.DisposeValueObject(obj, disposeReason));
        }

        /// <summary>
        /// Dispose a <see cref="DiskLogRecord"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DisposeRecord(ref DiskLogRecord logRecord, DisposeReason disposeReason)
        {
            // Clear the IHeapObject if we deserialized it
            if (logRecord.Info.ValueIsObject && logRecord.ValueObject is not null)
                storeFunctions.DisposeValueObject(logRecord.ValueObject, disposeReason);
        }

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            var localValues = Interlocked.Exchange(ref values, null);
            if (localValues != null)
            {
                freePagePool.Dispose();
                foreach (var value in localValues)
                    value.Clear();
                base.Dispose();
            }
        }

        internal override void PatchExpandedAddresses(long closeStartAddress, long closeEndAddress, TsavoriteBase storeBase)
        {
            // Scan forward to process each closing LogRecord and advance to the next one.
            for (var closeAddress = closeStartAddress; closeAddress < closeEndAddress; /* incremented in loop*/)
            {
                // Get the key from the logRecord being closed, and use that to get to the start of the tag chain.
                var closeLogRecord = CreateLogRecord(closeAddress);
                if (!closeLogRecord.Info.Invalid)
                {
                    HashEntryInfo hei = new(storeFunctions.GetKeyHashCode64(closeLogRecord.Key));
                    if (!storeBase.FindTag(ref hei) || hei.Address < BeginAddress)
                        continue;

                    // If the address is in the HashBucketEntry, this will update it; otherwise we will have to traverse the tag chain.
                    if (!hei.UpdateToOnDiskAddress(closeAddress, ClosedDiskTailOffset))
                    {
                        // Tag chains are singly-linked lists so we have to iterate the tag chain until we find the record pointing to closeAddress.
                        for (var prevAddress = hei.entry.Address; prevAddress >= closeAddress; /*adjusted in loop*/)
                        {
                            // Use GetPhysicalAddress() directly here for speed (we're just updating RecordInfo so don't need the overhead of an ObjectIdMap lookup)
                            var logRecord = new LogRecord(GetPhysicalAddress(prevAddress));
                            if (logRecord.Info.Valid)
                            {
                                if (logRecord.Info.PreviousAddress == closeAddress)
                                {
                                    logRecord.InfoRef.UpdateToOnDiskAddress(ClosedDiskTailOffset);
                                    Debug.Assert((*(RecordInfo*)GetPhysicalAddress(closeAddress)).PreviousAddress < BeginAddress || IsOnDisk((*(RecordInfo*)GetPhysicalAddress(closeAddress)).PreviousAddress),
                                                $"RI: Expected PrevAddress {AbsoluteAddress((*(RecordInfo*)GetPhysicalAddress(closeAddress)).PreviousAddress)} to be on Disk, but wasn't");
                                    Debug.WriteLine($"PatchAddressUpdate  RI: {ClosedDiskTailOffset}, closeAddress {AbsoluteAddress(closeAddress)} -> {AbsoluteAddress(logRecord.Info.PreviousAddress)}");
                                    break;
                                }
                            }
                            prevAddress = logRecord.Info.PreviousAddress;
                            if (prevAddress < closeAddress)
                            {
                                Debug.Fail("We should have found closeAddress");
                                break;
                            }
                        }
                    }
                    else
                    {
                        Debug.Assert((*(RecordInfo*)GetPhysicalAddress(closeAddress)).PreviousAddress < BeginAddress || IsOnDisk((*(RecordInfo*)GetPhysicalAddress(closeAddress)).PreviousAddress),
                                    $"HBE: Expected PrevAddress {AbsoluteAddress((*(RecordInfo*)GetPhysicalAddress(closeAddress)).PreviousAddress)} to be on Disk, but wasn't");
                        Debug.WriteLine($"PatchAddressUpdate HBE: {ClosedDiskTailOffset}, closeAddress {AbsoluteAddress(closeAddress)} -> {AbsoluteAddress(hei.Address)}");
                    }

                    // Now update ClosedDiskTailOffset. This is the same computation that is done during Flush, but we already know the object size here
                    // as we saved it in the object's.SerializedSize (and we ignore SerializedSizeIsExact because we know it hasn't changed) or, if it's
                    // Overflow, we already have it in the OverflowByteArray. OnPagesClosedWorker ensures only one thread is doing this update.
                    // TODO is MonotonicUpdate needed? The OnPagesClosedWorker page-fragment ordering sequence should guarantee that only one thread at a time will be doing this
                    _ = MonotonicUpdate(ref ClosedDiskTailOffset, ClosedDiskTailOffset + closeLogRecord.CalculateExpansion(), out _);
                    Debug.WriteLine($"ClosedDiskTailOffset pt 1: {ClosedDiskTailOffset}, closeAddress {AbsoluteAddress(closeAddress)}");
                }

                // Move to the next record being closed. OnPagesClosedWorker only calls this one page at a time, so we won't cross a page boundary.
                closeAddress += closeLogRecord.GetInlineRecordSizes().allocatedSize;
            }
        }

        internal void ClearPage(long page, int offset)
        {
            // This is called during recovery, not as part of normal operations, so there is no need to walk pages starting at offset to Free() ObjectIds
            NativeMemory.Clear((byte*)pagePointers[page] + offset, (nuint)(PageSize - offset));
        }

        internal void FreePage(long page)
        {
            ClearPage(page, 0);

            // Close segments
            var thisCloseSegment = page >> (LogSegmentSizeBits - LogPageSizeBits);
            var nextCloseSegment = (page + 1) >> (LogSegmentSizeBits - LogPageSizeBits);

            // If all pages are being used (i.e. EmptyPageCount == 0), nothing to re-utilize by adding
            // to overflow pool.
            if (EmptyPageCount > 0)
                ReturnPage((int)(page % BufferSize));
        }

        protected override void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
        {
            WriteAsync(flushPage, (ulong)(AlignedPageSizeBytes * flushPage), (uint)PageSize,
                    callback, asyncResult, device);
        }

        protected override void WriteAsyncToDevice<TContext>(long startPage, long flushPage, int pageSize,
            DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult, IDevice device, long fuzzyStartLogicalAddress)
        {
            VerifyCompatibleSectorSize(device);

            var epochTaken = epoch.ResumeIfNotProtected();
            try
            {
                if (HeadAddress >= GetStartLogicalAddressOfPage(flushPage) + pageSize)
                {
                    // Requested page is unavailable in memory, ignore
                    callback(0, 0, asyncResult);
                    return;
                }

                // We are writing to separate device
                WriteAsync(flushPage, (ulong)(AlignedPageSizeBytes * (flushPage - startPage)), (uint)pageSize,
                        callback, asyncResult, device, flushPage, fuzzyStartLogicalAddress);
            }
            finally
            {
                if (epochTaken)
                    epoch.Suspend();
            }
        }

        private void WriteAsync<TContext>(long flushPage, ulong alignedDestinationAddress, uint numBytesToWrite,
                        DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                        IDevice device, long intendedDestinationPage = -1, long fuzzyStartLogicalAddress = long.MaxValue)
        {
            // We flush within the DiskStreamWriteBuffer, so we do not use the asyncResult here for IO, but it has useful fields.

            // Short circuit if we are using a null device
            if ((device as NullDevice) != null)
            {
                device.WriteAsync(IntPtr.Zero, 0, 0, numBytesToWrite, callback, asyncResult);
                return;
            }

            Debug.Assert(asyncResult.page == flushPage, $"asyncResult.page {asyncResult.page} should equal flushPage {flushPage}");

            // TODO: If ObjectIdMap for this page is empty and FlushedDiskTailOffset is 0, we can write directly from the page to disk (with the sector-aligning pre-Read).

            // numBytesToWrite is calculated from start and end logical addresses, either for the full page or a subset of records (aligned to start and end of record boundaries).
            // This includes the objectId space for Overflow and Heap Objects. "Aligned" refers to sector aligned
            int startOffset = 0, alignedStartOffset = 0, endOffset = (int)numBytesToWrite;
            if (asyncResult.partial)
            {
                // We're writing only a subset of the page, so aligned_start will be rounded down to sectorSize-aligned start position.
                var pageStart = GetStartLogicalAddressOfPage(asyncResult.page);
                startOffset = (int)(asyncResult.fromAddress - pageStart);
                alignedStartOffset = RoundDown(startOffset, sectorSize);
                endOffset = (int)(asyncResult.untilAddress - pageStart);
                numBytesToWrite = (uint)(endOffset - startOffset);
            }

            // Hang onto local copies of the objects and page inline data, so they are kept valid even if the original page is reclaimed.
            // TODO: Should Page eviction null the ObjectIdMap, or its internal MultiLevelPageArray, vs. just clearing the objects
            var localObjectIdMap = values[flushPage % BufferSize].objectIdMap;
            var srcBuffer = bufferPool.Get((int)numBytesToWrite);
            var pageSpan = new Span<byte>((byte*)pagePointers[flushPage % BufferSize] + startOffset, (int)numBytesToWrite);
            pageSpan.CopyTo(srcBuffer.TotalValidSpan);
            srcBuffer.available_bytes = (int)numBytesToWrite;

            // We suspend epoch during the actual flush as that can take a long time.
            var epochWasProtected = epoch.ThisInstanceProtected();
            if (epochWasProtected)
                epoch.Suspend();

            // Object keys and values are serialized into this Stream.
            var valueObjectSerializer = storeFunctions.CreateValueObjectSerializer();
            var diskBuffer = new DiskStreamWriteBuffer(device, logger, bufferPool, IStreamBuffer.DiskWriteBufferSize, valueObjectSerializer, callback, asyncResult);
            PinnedMemoryStream<DiskStreamWriteBuffer> pinnedMemoryStream = new(diskBuffer);

            try
            {
                if (alignedStartOffset < startOffset)
                {
                    // Writes must be sector-aligned and the current write would not start on a sector boundary. Read one sector of bytes back from the disk to the start of
                    // the buffer (rather than re-serializing the start of the page; we wrote it to disk with expanded objects in a prior Flush). We'll start overwriting at
                    // startOffset, which is somewhere in the middle of that sector. Do not read back the invalid header of page 0; we'll write it below.
                    if (flushPage > 0 || startOffset > GetFirstValidLogicalAddressOnPage(flushPage))
                    {
                        using var countdownEvent = new CountdownEvent(1);
                        PageAsyncReadResult<Empty> result = new() { handle = countdownEvent };
                        alignedDestinationAddress += (ulong)alignedStartOffset;
                        device.ReadAsync(alignedDestinationAddress, (IntPtr)diskBuffer.flushBuffer.memory.GetValidPointer(), (uint)sectorSize, AsyncSimpleReadPageCallback, result);
                        result.handle.Wait();
                        if (result.numBytesRead != (uint)sectorSize)    // Our writes should always have written at least a sector
                            throw new TsavoriteException($"Expected number of bytes read {sectorSize}, actual {result.numBytesRead}");
                        diskBuffer.OnInitialSectorReadComplete(startOffset - alignedStartOffset);
                    }
                }

                diskBuffer.SetAlignedDeviceAddress(alignedDestinationAddress);
                valueObjectSerializer.BeginSerialize(pinnedMemoryStream);

                // AddressType consistency check
                Debug.Assert(IsInLogMemory(asyncResult.fromAddress), "fromAddress is not marked as in Log memory");
                Debug.Assert(IsInLogMemory(asyncResult.untilAddress), "untilAddress is not marked as in Log memory");
                Debug.Assert(fuzzyStartLogicalAddress < 0 || IsInLogMemory(fuzzyStartLogicalAddress), "fromAddress is not marked as in Log memory");

                var logicalAddress = asyncResult.fromAddress;
                var endPhysicalAddress = (long)srcBuffer.GetValidPointer() + numBytesToWrite;

                if (flushPage == 0 && startOffset == AbsoluteAddress(GetFirstValidLogicalAddressOnPage(flushPage)))
                {
                    // Write the header of page 0. This keeps the file aligned with the first logical address, and maybe we'll add something useful there.
                    diskBuffer.Write(new ReadOnlySpan<byte>((byte*)pagePointers[flushPage % BufferSize], startOffset));
                }

                for (var physicalAddress = (long)srcBuffer.GetValidPointer(); physicalAddress < endPhysicalAddress; /* incremented in loop */)
                {
                    var logRecord = new LogRecord(physicalAddress, localObjectIdMap);

                    // Use allocatedSize here because that is what LogicalAddress is based on.
                    var logRecordSize = logRecord.GetInlineRecordSizes().allocatedSize;

                    // Do not write Invalid records or v+1 records (e.g. during a checkpoint).
                    if (logRecord.Info.Invalid || (logicalAddress >= fuzzyStartLogicalAddress && logRecord.Info.IsInNewVersion))
                    {
                        // Shrink the expansion for skipped records. We still have to step over the record to get to the next one.
                        // TODO is MonotonicUpdate needed? The flush "next adjacent" sequence should guarantee that only one thread at a time will be doing this
                        _ = MonotonicUpdate(ref FlushedDiskTailOffset, FlushedDiskTailOffset - logRecordSize, out _);
                        Debug.WriteLine($"FlushedDiskTailOffset pt 1: {FlushedDiskTailOffset}, logicalAddress {AbsoluteAddress(logicalAddress)}");
                    }
                    else
                    {
                        logRecord.InfoRef.PreviousAddress += FlushedDiskTailOffset;
                        var prevPosition = diskBuffer.TotalWrittenLength;

                        // The Write will update the flush image's .PreviousAddress, but NOT logRecord.Info.PreviousAddress, as that will be set later during page eviction.
                        diskBuffer.Write(in logRecord, FlushedDiskTailOffset);
                        var streamRecordSize = diskBuffer.TotalWrittenLength - prevPosition;

                        var streamExpansion = streamRecordSize - logRecordSize;
                        Debug.Assert(streamExpansion % Constants.kRecordAlignment == 0, $"streamExpansion {streamExpansion} is not record-aligned");
#if DEBUG
                        // Note: It is OK for the "expansion" to be negative; we don't preserve Filler here, we just write only the actual data.
                        var logRecExpansion = logRecord.CalculateExpansion();
                        Debug.Assert(streamExpansion == logRecExpansion, $"logRecSize to StreamSize expansion {streamExpansion} does not equal calculated expansion {logRecExpansion}");
#endif

                        // TODO is MonotonicUpdate needed? The flush "next adjacent" sequence should guarantee that only one thread at a time will be doing this
                        _ = MonotonicUpdate(ref FlushedDiskTailOffset, FlushedDiskTailOffset + streamExpansion, out _);
                        Debug.WriteLine($"FlushedDiskTailOffset pt 2: {FlushedDiskTailOffset}, logicalAddress {AbsoluteAddress(logicalAddress)}");
                    }

                    logicalAddress += logRecordSize;    // advance in main log
                    physicalAddress += logRecordSize;   // advance in source buffer
                }
                diskBuffer.OnFlushComplete(callback, asyncResult);
            }
            finally
            {
                srcBuffer.Return();
                if (epochWasProtected)
                    epoch.Resume();
            }
        }

        protected override void ReadAsync<TContext>(
            ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
            DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device)
        {
#if READ_WRITE
            asyncResult.freeBuffer1 = bufferPool.Get((int)aligned_read_length);
            asyncResult.freeBuffer1.required_bytes = (int)aligned_read_length;

            if (!(KeyHasObjects() || ValueHasObjects()))
            {
                device.ReadAsync(alignedSourceAddress, (IntPtr)asyncResult.freeBuffer1.aligned_pointer,
                    aligned_read_length, callback, asyncResult);
                return;
            }

            asyncResult.callback = callback;

            if (objlogDevice == null)
            {
                Debug.Assert(objectLogDevice != null);
                objlogDevice = objectLogDevice;
            }
            asyncResult.objlogDevice = objlogDevice;

            device.ReadAsync(alignedSourceAddress, (IntPtr)asyncResult.freeBuffer1.aligned_pointer,
                    aligned_read_length, AsyncReadPageWithObjectsCallback<TContext>, asyncResult);
#endif // READ_WRITE
        }

        internal void PopulatePage(byte* src, int required_bytes, long destinationPage)
        {
#if READ_WRITE
            PopulatePage(src, required_bytes, ref values[destinationPage % BufferSize]);
#endif // READ_WRITE
        }

        internal void PopulatePage(byte* src, int required_bytes)
        {
#if READ_WRITE
, ref AllocatorRecord[] destinationPage
            fixed (RecordInfo* pin = &destinationPage[0].info)
            {
                Debug.Assert(required_bytes <= RecordSize * destinationPage.Length);
                Buffer.MemoryCopy(src, Unsafe.AsPointer(ref destinationPage[0]), required_bytes, required_bytes);
            }
#endif // READ_WRITE
        }

        /// <summary>
        /// Iterator interface for scanning Tsavorite log
        /// </summary>
        /// <returns></returns>
        public override ITsavoriteScanIterator Scan(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, DiskScanBufferingMode diskScanBufferingMode, bool includeSealedRecords)
            => new ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>>(store, this, beginAddress, endAddress, epoch, diskScanBufferingMode, includeSealedRecords: includeSealedRecords);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, ref TScanFunctions scanFunctions, DiskScanBufferingMode scanBufferingMode)
        {
            using ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, beginAddress, endAddress, epoch, scanBufferingMode, includeSealedRecords: false, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor, long maxAddress)
        {
            using ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, cursor, endAddress, epoch, DiskScanBufferingMode.SinglePageBuffering, includeSealedRecords: false, logger: logger);
            return ScanLookup<long, long, TScanFunctions, ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>>>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor, maxAddress);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                ReadOnlySpan<byte> key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, beginAddress, epoch, logger: logger);
            return IterateHashChain(store, key, beginAddress, ref scanFunctions, iter);
        }

        private void ComputeScanBoundaries(long beginAddress, long endAddress, out long pageStartAddress, out int start, out int end)
        {
#if READ_WRITE
            pageStartAddress = beginAddress & ~PageSizeMask;
            start = (int)(beginAddress & PageSizeMask) / RecordSize;
            var count = (int)(endAddress - beginAddress) / RecordSize;
            end = start + count;
#else
            pageStartAddress = 0;
            start = end = 0;
#endif // READ_WRITE
        }

        /// <inheritdoc />
        internal override void EvictPage(long page)
        {
#if READ_WRITE
            if (OnEvictionObserver is not null)
            {
                var beginAddress = page << LogPageSizeBits;
                var endAddress = (page + 1) << LogPageSizeBits;
                ComputeScanBoundaries(beginAddress, endAddress, out var pageStartAddress, out var start, out var end);
                using var iter = new MemoryPageScanIterator(values[(int)(page % BufferSize)], start, end, pageStartAddress, RecordSize);
                OnEvictionObserver?.OnNext(iter);
            }

            FreePage(page);
#endif // READ_WRITE
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator> observer)
        {
#if READ_WRITE
            var page = (beginAddress >> LogPageSizeBits) % BufferSize;
            ComputeScanBoundaries(beginAddress, endAddress, out var pageStartAddress, out var start, out var end);
            using var iter = new MemoryPageScanIterator(values[page], start, end, pageStartAddress, RecordSize);
            Debug.Assert(epoch.ThisInstanceProtected());
            try
            {
                epoch.Suspend();
                observer?.OnNext(iter);
            }
            finally
            {
                epoch.Resume();
            }
#endif // READ_WRITE
        }

        /// <summary>
        /// Read pages from specified device
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        internal void AsyncReadPagesFromDeviceToFrame<TContext>(
                                        long readPageStart,
                                        int numPages,
                                        long untilAddress,
                                        DeviceIOCompletionCallback callback,
                                        TContext context,
                                        ObjectFrame frame,
                                        out CountdownEvent completed,
                                        long devicePageOffset = 0,
                                        IDevice device = null)
        {
            var usedDevice = device ?? this.device;

            completed = new CountdownEvent(numPages);
            for (long readPage = readPageStart; readPage < (readPageStart + numPages); readPage++)
            {
                int pageIndex = (int)(readPage % frame.frameSize);
                if (frame.GetPage(pageIndex) == null)
                    frame.Allocate(pageIndex);
                else
                    frame.Clear(pageIndex);

                var asyncResult = new PageAsyncReadResult<TContext>()
                {
                    page = readPage,
                    context = context,
                    handle = completed,
                    maxPtr = PageSize,
                    frame = frame,
                };

                var offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);
                var readLength = (uint)AlignedPageSizeBytes;
                long adjustedUntilAddress = (AlignedPageSizeBytes * (untilAddress >> LogPageSizeBits) + (untilAddress & PageSizeMask));

                if (adjustedUntilAddress > 0 && ((adjustedUntilAddress - (long)offsetInFile) < PageSize))
                {
                    readLength = (uint)(adjustedUntilAddress - (long)offsetInFile);
                    asyncResult.maxPtr = readLength;
                    readLength = (uint)((readLength + (sectorSize - 1)) & ~(sectorSize - 1));
                }

                if (device != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));
                ReadAsync(offsetInFile, pageIndex, readLength, callback, asyncResult, usedDevice);
            }
        }

        internal override void AsyncFlushDeltaToDevice(long startAddress, long endAddress, long prevEndAddress, long version, DeltaLog deltaLog, out SemaphoreSlim completedSemaphore, int throttleCheckpointFlushDelayMs)
        {
            throw new TsavoriteException("Incremental snapshots not supported with generic allocator"); // TODO READ_WRITE
        }
    }
}