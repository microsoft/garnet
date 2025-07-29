// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//#define READ_WRITE

using System;
using System.Diagnostics;
using System.IO;
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
        /// <summary>Circular buffer definition, in parallel with <see cref="values"/></summary>
        /// <remarks>The long is actually a byte*, but storing as 'long' makes going through logicalAddress/physicalAddress translation more easily</remarks>
        long* pagePointers;

        /// <summary>For each in-memory page of this allocator we have an <see cref="ObjectIdMap"/> for keys that are too large to fit inline into the main log
        /// and become overflow byte[], or are Object values; this is needed to root the objects for GC.</summary>
        internal struct ObjectPage
        {
            internal readonly ObjectIdMap objectIdMap { get; init; }

            public ObjectPage() => objectIdMap = new();

            internal readonly void Clear() => objectIdMap?.Clear();       // TODO: Ensure we have already called the RecordDisposer
        }

        internal ObjectPage[] values;

        /// <summary>
        /// Offset of expanded IO tail on the disk for Flush. This is added to .PreviousAddress for all records being flushed.
        /// It leads <see cref="ClosedDiskTailOffset"/>, which is recalculated after we arrive at <see cref="AllocatorBase{TStoreFunctions, TAllocator}.OnPagesClosed(long)"/>.
        /// </summary>
        long FlushedDiskTailOffset;

        /// <summary>
        /// Offset of expanded IO tail on the disk for Close. When we arrive at <see cref="AllocatorBase{TStoreFunctions, TAllocator}.OnPagesClosed(long)"/>
        /// this is used to calculate the offset for each .PreviousAddress in the in-memory log that references the page being closed, so it can be patched up
        /// to the on-disk address for that record.
        /// </summary>
        long ClosedDiskTailOffset;

        // Size of object chunks being written to storage
        private readonly int objectBlockSize = 100 * (1 << 20);

        private readonly OverflowPool<PageUnit<ObjectPage>> freePagePool;

        public ObjectAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, ObjectAllocator<TStoreFunctions>> wrapperCreator)
            : base(settings.LogSettings, storeFunctions, wrapperCreator, settings.evictCallback, settings.epoch, settings.flushCallback, settings.logger, isObjectAllocator: true)
        {
            freePagePool = new OverflowPool<PageUnit<ObjectPage>>(4, static p => { });

            var bufferSizeInBytes = (nuint)RoundUp(sizeof(long*) * BufferSize, Constants.kCacheLineBytes);
            pagePointers = (long*)NativeMemory.AlignedAlloc(bufferSizeInBytes, Constants.kCacheLineBytes);
            NativeMemory.Clear(pagePointers, bufferSizeInBytes);

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
            Initialize();
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SerializeKey(ReadOnlySpan<byte> key, long logicalAddress, ref LogRecord logRecord) => logRecord.SerializeKey(key, maxInlineKeySize, GetObjectIdMap(logicalAddress));

        public override void Initialize() => Initialize(FirstValidAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitializeValue(in RecordSizeInfo sizeInfo, ref LogRecord newLogRecord)
        {
            if (sizeInfo.ValueIsInline)
            {
                // Set the actual length indicator for inline.
                newLogRecord.InfoRef.SetValueIsInline();
                sizeInfo.SetValueInlineLength(newLogRecord.physicalAddress);
            }
            else
            {
                // If it's a revivified record, it may have ValueIsInline set, so clear that.
                newLogRecord.InfoRef.ClearValueIsInline();

                if (sizeInfo.ValueIsObject)
                    newLogRecord.InfoRef.SetValueIsObject();

                // Either an IHeapObject or an overflow byte[]; either way, it's an object, and the length is already set.
                Debug.Assert(sizeInfo.FieldInfo.ValueSize == ObjectIdMap.InvalidObjectId, $"Expected object size ({ObjectIdMap.ObjectIdSize}) for ValueSize but was {sizeInfo.FieldInfo.ValueSize}");
                sizeInfo.SetValueInlineLength(newLogRecord.physicalAddress);
                *(int*)sizeInfo.GetValueAddress(newLogRecord.physicalAddress) = ObjectIdMap.InvalidObjectId;
            }
            newLogRecord.SetFillerLength(in sizeInfo);
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
            sizeInfo.MaxInlineValueSpanSize = maxInlineValueSize;
            sizeInfo.ValueIsInline = !sizeInfo.ValueIsObject && sizeInfo.FieldInfo.ValueSize <= sizeInfo.MaxInlineValueSpanSize;
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
                base.Dispose();
                freePagePool.Dispose();
                foreach (var value in localValues)
                    value.Clear();

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
            // Index of page within the circular buffer, and offset on the page
            var pageIndex = GetPageIndexForAddress(logicalAddress);
            var offset = GetOffsetOnPage(logicalAddress);
            return *(pagePointers + pageIndex) + offset;
        }

        internal bool IsAllocated(int pageIndex) => pagePointers[pageIndex] != 0;

        protected override void TruncateUntilAddress(long toAddress)    // TODO READ_WRITE: ObjectAllocator specifics if any
        {
            base.TruncateUntilAddress(toAddress);
        }

        protected override void TruncateUntilAddressBlocking(long toAddress)    // TODO READ_WRITE: ObjectAllocator specifics if any
        {
            base.TruncateUntilAddressBlocking(toAddress);
        }

        protected override void RemoveSegment(int segment)    // TODO READ_WRITE: ObjectAllocator specifics if any
        {
            base.RemoveSegment(segment);
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

#if READ_WRITE
            if (thisCloseSegment != nextCloseSegment)
            {
                // We are clearing the last page in current segment
                segmentOffsets[thisCloseSegment % SegmentBufferSize] = 0;
            }
#endif // READ_WRITE

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

            // "Aligned" refers to sector aligned
            int startOffset = 0, alignedStartOffset = 0, endOffset = (int)numBytesToWrite;
            if (asyncResult.partial)
            {
                // We're writing only a subset of the page, so aligned_start will be rounded down to sectorSize-aligned start position.
                var pageStart = GetStartLogicalAddressOfPage(asyncResult.page);
                startOffset = (int)(asyncResult.fromAddress - pageStart);
                alignedStartOffset = RoundDown(startOffset, sectorSize);
                endOffset = (int)(asyncResult.untilAddress - pageStart);
            }

            // numBytesToWrite is calculated from start and end logical addresses, either for the full page or a subset of records (aligned to start and end of record boundaries).
            // This includes the objectId space for Overflow and Heap Objects.
            Debug.Assert(numBytesToWrite == endOffset - startOffset, $"numBytesToWrite={numBytesToWrite} does not equal endOffset - startOffset={endOffset - startOffset}");

            // Hang onto local copies of the objects and page inline data, so they are kept valid even if the original page is reclaimed.
            // TODO: Should Page eviction null the ObjectIdMap, or its internal MultiLevelPageArray, vs. just clearing the objects
            var localObjectIdMap = values[flushPage % BufferSize].objectIdMap;
            var srcBuffer = bufferPool.Get(endOffset - alignedStartOffset); // Source buffer adjusts size for alignment subtraction from startOffset.
            var pageSpan = new Span<byte>((byte*)pagePointers[flushPage % BufferSize] + startOffset, (int)numBytesToWrite);
            pageSpan.CopyTo(srcBuffer.Span);

            // We suspend epoch during the actual flush as that can take a long time.
            var epochWasProtected = epoch.ThisInstanceProtected();
            if (epochWasProtected)
                epoch.Suspend();

            // Object keys and values are serialized into this Stream.
            var valueObjectSerializer = storeFunctions.CreateValueObjectSerializer();
            var diskBuffer = new DiskStreamWriteBuffer(device, bufferPool.Get(IStreamBuffer.DiskWriteBufferSize), valueObjectSerializer, AsyncReadPageCallback);
            PinnedMemoryStream<DiskStreamWriteBuffer> pinnedMemoryStream = new(diskBuffer);

            try
            {
                if (alignedStartOffset < startOffset)
                {
                    // Writes must be sector-aligned and the current write would not start on a sector boundary. Read one sector of bytes back from the disk to the start of
                    // the buffer (rather than re-serializing the start of the page; we wrote it to disk with expanded objects in a prior Flush). We'll start overwriting at
                    // startOffset, which is somewhere in the middle of that sector. Do not read back the invalid header of page 0.
                    if ((flushPage > 0) || (startOffset > GetFirstValidLogicalAddressOnPage(flushPage)))
                    {
                        using var countdownEvent = new CountdownEvent(1);
                        PageAsyncReadResult<Empty> result = new() { handle = countdownEvent };
                        alignedDestinationAddress += (ulong)alignedStartOffset;
                        device.ReadAsync(alignedDestinationAddress, (IntPtr)diskBuffer.BufferPointer, (uint)sectorSize, AsyncReadPageCallback, result);
                        result.handle.Wait();
                        diskBuffer.OnInitialSectorReadComplete(startOffset - alignedStartOffset);
                    }
                }

                diskBuffer.SetAlignedDeviceAddress(alignedDestinationAddress);
                valueObjectSerializer.BeginSerialize(pinnedMemoryStream);

                var logicalAddress = asyncResult.fromAddress;
                var endPhysicalAddress = (long)srcBuffer.aligned_pointer + endOffset;
                for (var physicalAddress = (long)srcBuffer.aligned_pointer + startOffset; physicalAddress < endPhysicalAddress; /* incremented in loop */)
                {
                    var logRecord = new LogRecord(physicalAddress, localObjectIdMap);
                    var logRecordSize = logRecord.GetInlineRecordSizes().allocatedSize;

                    // Do not write Invalid records or v+1 records (e.g. during a checkpoint).
                    if (logRecord.Info.Invalid || (logicalAddress >= fuzzyStartLogicalAddress && logRecord.Info.IsInNewVersion))
                    {
                        FlushedDiskTailOffset -= logRecordSize;
                        continue;
                    }

                    // Update the flush image's .PreviousAddress, but NOT logRecord.Info.PreviousAddress, as that will be set later during page eviction.
                    logRecord.InfoRef.PreviousAddress += FlushedDiskTailOffset;

                    var prevPosition = pinnedMemoryStream.Position;
                    diskBuffer.Write(in logRecord);
                    var streamRecordSize = RoundUp(pinnedMemoryStream.Position, Constants.kRecordAlignment) - prevPosition;
                    Debug.Assert(streamRecordSize >= logRecordSize, $"Serialized size of record {streamRecordSize} is less than expected record size {logRecordSize}.");

                    FlushedDiskTailOffset += streamRecordSize - logRecordSize;  // The flush "next adjacent" sequence guarantees only one thread at a time will be calling this
                }
                diskBuffer.OnFlushComplete();
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
            DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice)
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


        /// <summary>
        /// IOCompletion callback for page flush
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="numBytes"></param>
        /// <param name="context"></param>
        private void AsyncFlushPartialObjectLogCallback<TContext>(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncFlushPartialObjectLogCallback)} error: {{errorCode}}", errorCode);

            // Signal the event so the waiter can continue
            var result = (PageAsyncFlushResult<TContext>)context;
            _ = result.done.Set();
        }

        private void AsyncReadPageWithObjectsCallback<TContext>(uint errorCode, uint numBytes, object context)
        {
#if READ_WRITE
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncReadPageWithObjectsCallback)} error: {{errorCode}}", errorCode);

            var result = (PageAsyncReadResult<TContext>)context;

            AllocatorRecord[] src;

            // We are reading into a frame
            if (result.frame != null)
            {
                var frame = (GenericFrame)result.frame;
                src = frame.GetPage(result.page % frame.frameSize);
            }
            else
                src = values[result.page % BufferSize];


            // Deserialize all objects until untilptr
            if (result.resumePtr < result.untilPtr)
            {
                MemoryStream ms = new(result.freeBuffer2.buffer);
                ms.Seek(result.freeBuffer2.offset, SeekOrigin.Begin);
                Deserialize(result.freeBuffer1.GetValidPointer(), result.resumePtr, result.untilPtr, src, ms);
                ms.Dispose();

                result.freeBuffer2.Return();
                result.freeBuffer2 = null;
                result.resumePtr = result.untilPtr;
            }

            // If we have processed entire page, return
            if (result.untilPtr >= result.maxPtr)
            {
                result.Free();

                // Call the "real" page read callback
                result.callback(errorCode, numBytes, context);
                return;
            }

            // We will now be able to process all records until (but not including) untilPtr
            GetObjectInfo(result.freeBuffer1.GetValidPointer(), ref result.untilPtr, result.maxPtr, objectBlockSize, out long startptr, out long alignedLength);

            // Object log fragment should be aligned by construction
            Debug.Assert(startptr % sectorSize == 0);
            Debug.Assert(alignedLength % sectorSize == 0);

            if (alignedLength > int.MaxValue)
                throw new TsavoriteException("Unable to read object page, total size greater than 2GB: " + alignedLength);

            var objBuffer = bufferPool.Get((int)alignedLength);
            result.freeBuffer2 = objBuffer;

            // Request objects from objlog
            result.objlogDevice.ReadAsync(
                (int)((result.page - result.offset) >> (LogSegmentSizeBits - LogPageSizeBits)),
                (ulong)startptr,
                (IntPtr)objBuffer.aligned_pointer, (uint)alignedLength, AsyncReadPageWithObjectsCallback<TContext>, result);
#endif // READ_WRITE
        }

        #region Page handlers for objects
        /// <summary>
        /// Deseialize part of page from stream
        /// </summary>
        /// <param name="raw"></param>
        /// <param name="ptr">From pointer</param>
        /// <param name="untilptr">Until pointer</param>
        /// <param name="stream">Stream</param>
        public void Deserialize(byte* raw, long ptr, long untilptr, Stream stream)
        {
#if READ_WRITE
AllocatorRecord[] src, 
            long streamStartPos = stream.Position;
            long start_addr = -1;
            int start_offset = -1, end_offset = -1;

            var keySerializer = KeyHasObjects() ? _storeFunctions.BeginDeserializeKey(stream) : null;
            var valueSerializer = ValueHasObjects() ? _storeFunctions.BeginDeserializeValue(stream) : null;

            while (ptr < untilptr)
            {
                ref var record = ref Unsafe.AsRef<AllocatorRecord>(raw + ptr);
                src[ptr / RecordSize].info = record.info;
                if (start_offset == -1)
                    start_offset = (int)(ptr / RecordSize);

                end_offset = (int)(ptr / RecordSize) + 1;

                if (!record.info.Invalid)
                {
                    if (KeyHasObjects())
                    {
                        var key_addr = GetKeyAddressInfo((long)raw + ptr);
                        if (start_addr == -1) start_addr = key_addr->Address & ~((long)sectorSize - 1);
                        if (stream.Position != streamStartPos + key_addr->Address - start_addr)
                            _ = stream.Seek(streamStartPos + key_addr->Address - start_addr, SeekOrigin.Begin);

                        keySerializer.Deserialize(out src[ptr / RecordSize].key);
                    }
                    else
                        src[ptr / RecordSize].key = record.key;

                    if (!record.info.Tombstone)
                    {
                        if (ValueHasObjects())
                        {
                            var value_addr = GetValueAddressInfo((long)raw + ptr);
                            if (start_addr == -1) start_addr = value_addr->Address & ~((long)sectorSize - 1);
                            if (stream.Position != streamStartPos + value_addr->Address - start_addr)
                                stream.Seek(streamStartPos + value_addr->Address - start_addr, SeekOrigin.Begin);

                            valueSerializer.Deserialize(out src[ptr / RecordSize].value);
                        }
                        else
                            src[ptr / RecordSize].value = record.value;
                    }
                }
                ptr += GetRecordSize(ptr).Item2;
            }
            if (KeyHasObjects())
                keySerializer.EndDeserialize();
            if (ValueHasObjects())
                valueSerializer.EndDeserialize();

            if (OnDeserializationObserver != null && start_offset != -1 && end_offset != -1)
            {
                using var iter = new MemoryPageScanIterator(src, start_offset, end_offset, -1, RecordSize);
                OnDeserializationObserver.OnNext(iter);
            }
#endif // READ_WRITE
        }

        /// <summary>
        /// Get location and range of object log addresses for specified log page
        /// </summary>
        /// <param name="raw"></param>
        /// <param name="ptr"></param>
        /// <param name="untilptr"></param>
        /// <param name="objectBlockSize"></param>
        /// <param name="startptr"></param>
        /// <param name="size"></param>
        public void GetObjectInfo(byte* raw, ref long ptr, long untilptr, int objectBlockSize, out long startptr, out long size)
        {
#if READ_WRITE
            var minObjAddress = long.MaxValue;
            var maxObjAddress = long.MinValue;
            var done = false;

            while (!done && (ptr < untilptr))
            {
                ref var record = ref Unsafe.AsRef<AllocatorRecord>(raw + ptr);

                if (!record.info.Invalid)
                {
                    if (KeyHasObjects())
                    {
                        var key_addr = GetKeyAddressInfo((long)raw + ptr);
                        var addr = key_addr->Address;

                        if (addr < minObjAddress) minObjAddress = addr;
                        addr += key_addr->Size;
                        if (addr > maxObjAddress) maxObjAddress = addr;

                        // If object pointer is greater than kObjectSize from starting object pointer
                        if (minObjAddress != long.MaxValue && (addr - minObjAddress > objectBlockSize))
                            done = true;
                    }


                    if (ValueHasObjects() && !record.info.Tombstone)
                    {
                        var value_addr = GetValueAddressInfo((long)raw + ptr);
                        var addr = value_addr->Address;

                        if (addr < minObjAddress) minObjAddress = addr;
                        addr += value_addr->Size;
                        if (addr > maxObjAddress) maxObjAddress = addr;

                        // If object pointer is greater than kObjectSize from starting object pointer
                        if (minObjAddress != long.MaxValue && (addr - minObjAddress > objectBlockSize))
                            done = true;
                    }
                }
                ptr += GetRecordSize(ptr).allocatedSize;
            }

            // Handle the case where no objects are to be written
            if (minObjAddress == long.MaxValue && maxObjAddress == long.MinValue)
            {
                minObjAddress = 0;
                maxObjAddress = 0;
            }

            // Align start pointer for retrieval
            minObjAddress &= ~((long)sectorSize - 1);

            // Align max address as well
            maxObjAddress = (maxObjAddress + (sectorSize - 1)) & ~((long)sectorSize - 1);

            startptr = minObjAddress;
            size = maxObjAddress - minObjAddress;
#else
            startptr = 0;
            size = 0;
#endif // READ_WRITE
        }
        #endregion

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

        internal override void AsyncFlushDeltaToDevice(long startAddress, long endAddress, long prevEndAddress, long version, DeltaLog deltaLog, out SemaphoreSlim completedSemaphore, int throttleCheckpointFlushDelayMs)
        {
            throw new TsavoriteException("Incremental snapshots not supported with generic allocator");
        }
    }
}