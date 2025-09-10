// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//#define READ_WRITE
#define TRACE_EXPANSION

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;

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

        // Default to max sizes so testing a size as "greater than" will always be false
        readonly int maxInlineKeySize = LogSettings.kMaxInlineKeySize;
        readonly int maxInlineValueSize = int.MaxValue;

        public ObjectAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, ObjectAllocator<TStoreFunctions>> wrapperCreator)
            : base(settings.LogSettings, storeFunctions, wrapperCreator, settings.evictCallback, settings.epoch, settings.flushCallback, settings.logger, isObjectAllocator: true)
        {
            freePagePool = new OverflowPool<PageUnit<ObjectPage>>(4, static p => { });

            maxInlineKeySize = 1 << settings.LogSettings.MaxInlineKeySizeBits;
            maxInlineValueSize = 1 << settings.LogSettings.MaxInlineValueSizeBits;

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
            // If we are closing the first record, initialize ClosedDiskTailOffset. Note that only the first in-memory page has the invalid header (which offsets all
            // addresses), but all on-disk pages have a DiskPageHeader. The on-disk address is essentially the physical address, and accounts for the growth due to
            // both DiskPageHeader and key/value expansion.
            if (closeStartAddress == GetFirstValidLogicalAddressOnPage(0))
                ClosedDiskTailOffset = DiskPageHeader.Size - GetFirstValidLogicalAddressOnPage(0);

            // Scan forward to process each closing LogRecord and advance to the next one.
            for (var closeAddress = closeStartAddress; closeAddress < closeEndAddress; /* incremented in loop*/)
            {
                // Get the key from the logRecord being closed, and use that to get to the start of the tag chain.
                var closeLogRecord = CreateLogRecord(closeAddress);
                if (closeLogRecord.Info.Invalid)
                {
                    // Shrink the expansion for skipped records, corresponding to WriteAsync()--see comments there for more details.
                    ClosedDiskTailOffset -= closeLogRecord.GetInlineRecordSizes().allocatedSize;
                    TraceExpansion($"ClosedDiskTailOffset pt 1 (IsInvalid): {ClosedDiskTailOffset}, closeAddress {AbsoluteAddress(closeAddress)}");
                }
                else
                {
                    HashEntryInfo hei = new(storeFunctions.GetKeyHashCode64(closeLogRecord.Key));
                    if (!storeBase.FindTag(ref hei) || hei.Address < BeginAddress)
                        continue;

                    // See if we need to bump to next sector alignment.
                    if (closeLogRecord.Info.IsSectorForceAligned)
                    {
                        var unalignedPatchedAddress = closeAddress + ClosedDiskTailOffset;
                        var alignedPatchedAddress = RoundUp(unalignedPatchedAddress, sectorSize);
                        if (alignedPatchedAddress > unalignedPatchedAddress)
                        {
                            ClosedDiskTailOffset += alignedPatchedAddress - unalignedPatchedAddress;
                            TraceExpansion($"ClosedDiskTailOffset pt 2 (sector-align record): {ClosedDiskTailOffset}, closeAddress {AbsoluteAddress(closeAddress)}");
                        }
                    }

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
                                    TraceExpansion($"PatchAddressUpdate  RI: {ClosedDiskTailOffset}, closeAddress {AbsoluteAddress(closeAddress)} -> {AbsoluteAddress(logRecord.Info.PreviousAddress)}");
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
                        TraceExpansion($"PatchAddressUpdate HBE: {ClosedDiskTailOffset}, closeAddress {AbsoluteAddress(closeAddress)} -> {AbsoluteAddress(hei.Address)}");
                    }

                    // Now update ClosedDiskTailOffset. This is the same computation that is done during Flush, but we already know the object size here
                    // as we saved it in the object's.SerializedSize (and we ignore SerializedSizeIsExact because we know it hasn't changed) or, if it's
                    // Overflow, we already have it in the OverflowByteArray. OnPagesClosedWorker ensures only one thread is doing this update.
                    // Note: We can just add instead of MonotonicUpdate because the OnPagesClosedWorker page/page-fragment ordering sequence guarantees that
                    // only one thread at a time will be doing this, which is important to ensure FlushDiskTailOffset and ClosedDiskTailOffset ordering.
                    // Also, this line decrements ClosedDiskTailOffset, which is a no-op in MonotonicUpdate.
                    ClosedDiskTailOffset += closeLogRecord.CalculateExpansion();
                    TraceExpansion($"ClosedDiskTailOffset pt 3 (after record): {ClosedDiskTailOffset}, closeAddress {AbsoluteAddress(closeAddress)}");
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

        /// <summary>Create the flush buffer (for <see cref="ObjectAllocator{Tsavorite}"/> only)</summary>
        protected override CircularDiskPageWriteBuffer CreateFlushBuffers(SectorAlignedBufferPool bufferPool, int pageBufferSize, int numPageBuffers, IDevice device, ILogger logger)
            => new(bufferPool, IStreamBuffer.PageBufferSize, numberOfFlushPageBuffers, device, logger);

        /// <summary>Create the flush buffer (for <see cref="ObjectAllocator{Tsavorite}"/> only)</summary>
        protected override CircularDiskPageReadBuffer CreateDeserializationBuffers(SectorAlignedBufferPool bufferPool, int pageBufferSize, int numPageBuffers, IDevice device, ILogger logger)
            => new(bufferPool, IStreamBuffer.PageBufferSize, numberOfFlushPageBuffers, device, logger);

        protected override void WriteAsync<TContext>(CircularDiskPageWriteBuffer diskPageBuffers, long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
            => WriteAsync(diskPageBuffers, flushPage, (uint)PageSize, callback, asyncResult, device);

        protected override void WriteAsyncToDevice<TContext>(CircularDiskPageWriteBuffer diskPageBuffers, long startPage, long flushPage, int pageSize,
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
                WriteAsync(diskPageBuffers, flushPage, (uint)pageSize, callback, asyncResult, device, fuzzyStartLogicalAddress);
            }
            finally
            {
                if (epochTaken)
                    epoch.Suspend();
            }
        }

        /// <summary>
        /// This writes data from a page (or pages) that does not need to be expanded.
        /// </summary>
        /// <param name="diskPageBuffers"></param>
        /// <param name="flushPage">The allocator page to be flushed</param>
        /// <param name="numBytesToWrite">Number of bytes to be written, based on allocator page range</param>
        /// <param name="callback">The callback for the operation</param>
        /// <param name="asyncResult">The callback state information, including information for the flush operation</param>
        /// <param name="device">The device to write to</param>
        /// <param name="fuzzyStartLogicalAddress">Start address of fuzzy region, which contains old and new version records (we use this to selectively flush only old-version records during snapshot checkpoint)</param>
        private void WriteAsync<TContext>(CircularDiskPageWriteBuffer diskPageBuffers, long flushPage, uint numBytesToWrite, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                        IDevice device, long fuzzyStartLogicalAddress = long.MaxValue)
        {
            // We flush within the DiskStreamWriteBuffer, so we do not use the asyncResult here for IO, but it has useful fields.

            // Short circuit if we are using a null device
            if ((device as NullDevice) != null)
            {
                device.WriteAsync(IntPtr.Zero, 0, 0, numBytesToWrite, callback, asyncResult);
                return;
            }

            Debug.Assert(asyncResult.page == flushPage, $"asyncResult.page {asyncResult.page} should equal flushPage {flushPage}");

            // TODO: If ObjectIdMap for this page is empty and FlushedDiskTailOffset is 0, we can write directly from the page to disk (with DiskPageHeader and DiskPageBuffer postion adjusted for DiskBufferSize).

            // numBytesToWrite is calculated from start and end logical addresses, either for the full page or a subset of records (aligned to start and end of record boundaries),
            // in the allocator page (including the objectId space for Overflow and Heap Objects). However, we need to align with the on-disk expanded addresses, including making
            // sure the start logical address is aligned into the page buffer we are writing, so we cleanly align with the page buffer size and thus can put the DiskPageHeaders
            // in the right place. So, first get the DiskPageBuffer offset of asyncResult.fromAddress, including FlushDiskTailOffset, which includes DiskPageHeader sizes. We do this
            // DiskPageBuffer alignment regardless of whether asyncResult.partial is true; due to expansion, we can no longer assume an allocator page starts on a disk-page boundary.
            // Note that only the first in-memory page has the invalid header, but all on-disk pages have a DiskPageHeader. The on-disk address is essentially the physical address,
            // and accounts for the growth due to both DiskPageHeader and key/value expansion. Note: "Aligned" in this discussion refers to sector aligned (as opposed to record alignment).

            // Initialize offsets into the allocator page based on full-page, then override them if partial. pageStart subtraction is needed because it offsets all logicalAddresses;
            // we must adjust them to start at zero, then add FlushDiskTailOffset (which includes Header and forced sector alignment on the final flush to disk of the partial flush).
            int startOffset = 0, endOffset = (int)numBytesToWrite;
            var pageStart = GetStartLogicalAddressOfPage(asyncResult.page);
            if (asyncResult.partial)
            {
                // We're writing only a subset of the page, so align relative to page start (which is necessary due to the invalid in-memory header of page 0).
                startOffset = (int)(asyncResult.fromAddress - pageStart);
                endOffset = (int)(asyncResult.untilAddress - pageStart);
                numBytesToWrite = (uint)(endOffset - startOffset);
            }

            // Initialize disk offset from logicalAddress to subtract the GetFirstValidLogicalAddressOnPage(), then ensure we are aligned to the DiskPageBuffer.
            // Note: Partial flushes end with a sector-aligned Flush, and this is also reflected in FlushDiskTailOffset. So even if this is
            // the first flush in this CircularDiskPageWriteBuffer, FlushedDiskTailOffset will be set so we are sector aligned for the first record.
            var logicalAddress = asyncResult.fromAddress;

            // If this is the first record, initialize FlushedDiskTailOffset to be the difference between the first allocator page's first valid address and the disk page header.
            if (logicalAddress == GetFirstValidLogicalAddressOnPage(0))
                FlushedDiskTailOffset = DiskPageHeader.Size - GetFirstValidLogicalAddressOnPage(0);

            // Destination address is the equivalent of physicalAddress, since there is no longer a connection between logicalAddress space and disk pages in the expanded disk page space.
            // Align destination address to the location we'll start the first write to the disk for this flush operation. This address does not have the AddressType prefix, as it is the
            // IO address in the file.
            var destinationAddress = AbsoluteAddress(logicalAddress) + FlushedDiskTailOffset;
            var alignedDestinationAddress = RoundDown(destinationAddress, sectorSize);
            var bufferPosition = (int)(destinationAddress & (IStreamBuffer.PageBufferSize - 1));

            // Now make sure the circular buffer is consistent with bufferPosition, including if necessary updating FlushedDiskTailOffset to account for an initial DiskPageHeader.
            var isFirstPartialFlush = diskPageBuffers.OnBeginPartialFlush(bufferPosition, ref FlushedDiskTailOffset);

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
            var diskWriter = new DiskStreamWriter(device, diskPageBuffers, valueObjectSerializer);
            PinnedMemoryStream<DiskStreamWriter> pinnedMemoryStream = new(diskWriter);

            try
            {
                diskWriter.circularPageFlushBuffers.alignedDeviceAddress = (ulong)alignedDestinationAddress;
                valueObjectSerializer.BeginSerialize(pinnedMemoryStream);

                // AddressType consistency check
                Debug.Assert(IsInLogMemory(asyncResult.fromAddress), "fromAddress is not marked as in Log memory");
                Debug.Assert(IsInLogMemory(asyncResult.untilAddress), "untilAddress is not marked as in Log memory");
                Debug.Assert(fuzzyStartLogicalAddress < 0 || IsInLogMemory(fuzzyStartLogicalAddress), "fromAddress is not marked as in Log memory");

                var endPhysicalAddress = (long)srcBuffer.GetValidPointer() + numBytesToWrite;
                var isFirstRecordInPartialFlush = true;
                for (var physicalAddress = (long)srcBuffer.GetValidPointer(); physicalAddress < endPhysicalAddress; /* incremented in loop */)
                {
                    var logRecord = new LogRecord(physicalAddress, localObjectIdMap);

                    // Use allocatedSize here because that is what LogicalAddress is based on.
                    var logRecordSize = logRecord.GetInlineRecordSizes().allocatedSize;

                    // Do not write Invalid records. This includes IsNull records.
                    if (logRecord.Info.Invalid)
                    {
                        // Shrink the expansion for skipped records. This is also critical at end-of-page, where there is no valid record, so we decrement
                        // FlushDiskTailOffset by the amount of unused space at the end of the allocator page (as it counts in the logicalAddress of the 
                        // records on subsequent pages). This avoids double-counting the sector alignment at the end of the page. PatchExpandedAddresses
                        // does the corresponding decrement of ClosedDiskTailOffset.
                        // Note: We can just add instead of MonotonicUpdate because the Flush page/page-fragment ordering sequence guarantees that
                        // only one thread at a time will be doing this, which is important to ensure FlushDiskTailOffset and ClosedDiskTailOffset ordering.
                        // Also, this line decrements FlushedDiskTailOffset, which is a no-op in MonotonicUpdate.
                        FlushedDiskTailOffset -= logRecordSize;
                        TraceExpansion($"FlushedDiskTailOffset pt 1 (IsInvalid): {FlushedDiskTailOffset}, logicalAddress {AbsoluteAddress(logicalAddress)}");
                    }
                    else
                    {
                        if (isFirstRecordInPartialFlush && logicalAddress > GetFirstValidLogicalAddressOnPage(0))
                            logRecord.InfoRef.IsSectorForceAligned = true;
                        isFirstRecordInPartialFlush = false;
                        var prevPosition = diskWriter.circularPageFlushBuffers.TotalWrittenLength;

                        Debug.Assert(((logicalAddress + FlushedDiskTailOffset) & (diskWriter.circularPageFlushBuffers.pageBufferSize - 1)) == diskWriter.pageBuffer.currentPosition, 
                            $"logicalAddress + FlushedDiskTailOffset does not match buffer.currentPosition");

                        // The Write will update the flush image's .PreviousAddress, but NOT logRecord.Info.PreviousAddress, as that will be set later during page eviction.
                        diskWriter.Write(in logRecord, FlushedDiskTailOffset);
                        var streamRecordSize = diskWriter.circularPageFlushBuffers.TotalWrittenLength - prevPosition;

                        var streamExpansion = streamRecordSize - logRecordSize;
                        Debug.Assert(streamExpansion % Constants.kRecordAlignment == 0, $"streamExpansion {streamExpansion} is not record-aligned (streamRecordSize {streamRecordSize})");
#if DEBUG
                        // Note: It is OK for the "expansion" to be negative; we don't preserve Filler here, we just write only the actual data.
                        var logRecExpansion = logRecord.CalculateExpansion();
                        Debug.Assert(streamExpansion == logRecExpansion, $"logRecSize to StreamSize expansion {streamExpansion} does not equal calculated expansion {logRecExpansion}");
#endif

                        FlushedDiskTailOffset += streamExpansion;
                        TraceExpansion($"FlushedDiskTailOffset pt 2 (IsValid): {FlushedDiskTailOffset}, logicalAddress {AbsoluteAddress(logicalAddress)}");
                    }

                    logicalAddress += logRecordSize;    // advance in main log
                    physicalAddress += logRecordSize;   // advance in source buffer
                }

                var flushedUntilAdjustment = diskWriter.circularPageFlushBuffers.OnPartialFlushComplete(callback, asyncResult);
                if (flushedUntilAdjustment > 0)
                {
                    FlushedDiskTailOffset += flushedUntilAdjustment;
                    TraceExpansion($"FlushedDiskTailOffset pt 3: {FlushedDiskTailOffset}, logicalAddress {AbsoluteAddress(logicalAddress)}");
                }
            }
            finally
            {
                srcBuffer.Return();
                if (epochWasProtected)
                    epoch.Resume();
            }
        }

        private protected override bool VerifyRecordFromDiskCallback(ref AsyncIOContext ctx, out long prevAddressToRead, out int prevLengthToRead)
        {
            prevLengthToRead = IStreamBuffer.InitialIOSize;

            var deserializationBuffers = CreateDeserializationBuffers(bufferPool, IStreamBuffer.PageBufferSize, numberOfFlushPageBuffers, device, logger);

            DiskStreamReader<TStoreFunctions>.DiskReadParameters readParams = 
                new(bufferPool, maxInlineKeySize, maxInlineValueSize, device.SectorSize, IStreamBuffer.PageBufferSize, AbsoluteAddress(ctx.logicalAddress), storeFunctions);
            var readBuffer = new DiskStreamReader<TStoreFunctions>(in readParams, device, deserializationBuffers, logger);
            if (readBuffer.Read(ref ctx.record, ctx.request_key, out ctx.diskLogRecord, out _ /*recordLength*/))
            {
                // Success; default the output arguments.
                prevAddressToRead = 0;
                return true;
            }

            // If readBuffer.Read returned false it was due to key mismatch or Invalid record, so get the previous record.
            prevAddressToRead = (*(RecordInfo*)ctx.record.GetValidPointer()).PreviousAddress;
            return false;
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
                frame.Initialize(pageIndex);

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

        [Conditional("TRACE_EXPANSION")]
        private void TraceExpansion(string message)
        {
            Debug.WriteLine(message);
            //Console.WriteLine(message);
        }
    }
}