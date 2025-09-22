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
        internal ObjectPage[] pages;
        /// <summary>The free pages of the log</summary>

        /// <summary>The address of the next write to the device. Will always be sector-aligned.</summary>
        ulong alignedNextMainLogFlushAddress;

        /// <summary>The position information for the next write to the object log.</summary>
        ObjectLogFilePositionInfo objectLogNextRecordStartPosition;

        // Default to max sizes so testing a size as "greater than" will always be false
        readonly int maxInlineKeySize = LogSettings.kMaxInlineKeySize;
        readonly int maxInlineValueSize = int.MaxValue;

        readonly int numberOfFlushBuffers;
        readonly int numberOfDeserializationBuffers;

        private readonly IDevice objectLogDevice;

        private readonly OverflowPool<PageUnit<ObjectPage>> freePagePool;

        public ObjectAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, ObjectAllocator<TStoreFunctions>> wrapperCreator)
            : base(settings.LogSettings, storeFunctions, wrapperCreator, settings.evictCallback, settings.epoch, settings.flushCallback, settings.logger)
        {
            IsObjectAllocator = true;

            maxInlineKeySize = 1 << settings.LogSettings.MaxInlineKeySizeBits;
            maxInlineValueSize = 1 << settings.LogSettings.MaxInlineValueSizeBits;

            freePagePool = new OverflowPool<PageUnit<ObjectPage>>(4, static p => { });

            if (settings.LogSettings.NumberOfFlushBuffers < LogSettings.kMinFlushBuffers || settings.LogSettings.NumberOfFlushBuffers > LogSettings.kMaxFlushBuffers || !IsPowerOfTwo(settings.LogSettings.NumberOfFlushBuffers))
                throw new TsavoriteException($"{nameof(settings.LogSettings.NumberOfFlushBuffers)} must be between {LogSettings.kMinFlushBuffers} and {LogSettings.kMaxFlushBuffers - 1} and a power of 2");
            numberOfFlushBuffers = settings.LogSettings.NumberOfFlushBuffers;

            if (settings.LogSettings.NumberOfDeserializationBuffers < LogSettings.kMinDeserializationBuffers || settings.LogSettings.NumberOfDeserializationBuffers > LogSettings.kMaxDeserializationBuffers || !IsPowerOfTwo(settings.LogSettings.NumberOfDeserializationBuffers))
                throw new TsavoriteException($"{nameof(settings.LogSettings.NumberOfDeserializationBuffers)} must be between {LogSettings.kMinDeserializationBuffers} and {LogSettings.kMaxDeserializationBuffers - 1} and a power of 2");
            numberOfDeserializationBuffers = settings.LogSettings.NumberOfDeserializationBuffers;

            if (settings.LogSettings.ObjectLogSegmentSizeBits < LogSettings.kMinObjectLogSegmentSizeBits || settings.LogSettings.ObjectLogSegmentSizeBits > LogSettings.kMaxSegmentSizeBits)
                throw new TsavoriteException($"{nameof(settings.LogSettings.ObjectLogSegmentSizeBits)} must be between {LogSettings.kMinObjectLogSegmentSizeBits} and {LogSettings.kMaxSegmentSizeBits}");
            objectLogNextRecordStartPosition.SegmentSizeBits = settings.LogSettings.ObjectLogSegmentSizeBits;

            pages = new ObjectPage[BufferSize];
            for (var ii = 0; ii < BufferSize; ii++)
                pages[ii] = new();
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
                pages[index] = item.value;
            }
            else
            {
                // No free pages are available so allocate new
                pagePointers[index] = (long)NativeMemory.AlignedAlloc((nuint)PageSize, (nuint)sectorSize);
                NativeMemory.Clear((void*)pagePointers[index], (nuint)PageSize);
                pages[index] = new();
            }
            PageHeader.Initialize(pagePointers[index]);
        }

        void ReturnPage(int index)
        {
            Debug.Assert(index < BufferSize);
            if (pagePointers[index] != default)
            {
                _ = freePagePool.TryAdd(new()
                {
                    pointer = pagePointers[index],
                    value = pages[index]
                });
                pagePointers[index] = default;
                _ = Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateLogRecord(long logicalAddress) => CreateLogRecord(logicalAddress, GetPhysicalAddress(logicalAddress));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateLogRecord(long logicalAddress, long physicalAddress) => new(physicalAddress, pages[GetPageIndexForAddress(logicalAddress)].objectIdMap);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ObjectIdMap GetObjectIdMap(long logicalAddress) => pages[GetPageIndexForAddress(logicalAddress)].objectIdMap;

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
                    ValueSize = 0,          // This will be inline, and with the length prefix and possible space when rounding up to kRecordAlignment, allows the possibility revivification can reuse the record for a Heap Field
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
            var localValues = Interlocked.Exchange(ref pages, null);
            if (localValues != null)
            {
                freePagePool.Dispose();
                foreach (var value in localValues)
                    value.Clear();
                base.Dispose();
            }
        }

        protected override void TruncateUntilAddress(long toAddress)
        {
            base.TruncateUntilAddress(toAddress);
        }

        protected override void TruncateUntilAddressBlocking(long toAddress)
        {
            base.TruncateUntilAddressBlocking(toAddress);
        }

        protected override void RemoveSegment(int segment)
        {
            TODO("Switch Segments over to per-page recording of the segment extent of each page");
            base.RemoveSegment(segment);
        }

        protected override void WriteAsync<TContext>(CircularDiskWriteBuffer flushBuffers, long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
            => WriteAsync(flushBuffers, flushPage, (uint)PageSize, callback, asyncResult, device, objectLogDevice);

        protected override void WriteAsyncToDevice<TContext>(CircularDiskWriteBuffer flushBuffers, long startPage, long flushPage, int possiblyPartialPageSize, DeviceIOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice, long fuzzyStartLogicalAddress)
        {
            VerifyCompatibleSectorSize(device);
            VerifyCompatibleSectorSize(objectLogDevice);

            var epochTaken = epoch.ResumeIfNotProtected();
            try
            {
                if (HeadAddress >= GetAbsoluteLogicalAddressOfStartOfPage(flushPage) + possiblyPartialPageSize)
                {
                    // Requested page is unavailable in memory, ignore
                    callback(0, 0, asyncResult);
                }
                else
                {
                    // We are writing to a separate device
                    WriteAsync(flushBuffers, flushPage, (uint)possiblyPartialPageSize, callback, asyncResult, device, objectLogDevice, fuzzyStartLogicalAddress);
                }
            }
            finally
            {
                if (epochTaken)
                    epoch.Suspend();
            }
        }

        internal void FreePage(long page)
        {
            pages[page].objectIdMap.Clear();

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

        /// <summary>Create the flush buffer (for <see cref="ObjectAllocator{Tsavorite}"/> only)</summary>
        protected override CircularDiskWriteBuffer CreateFlushBuffers(SectorAlignedBufferPool bufferPool, IDevice device, ILogger logger)
            => new(bufferPool, IStreamBuffer.BufferSize, numberOfFlushBuffers, device, logger);

        /// <summary>Create the flush buffer (for <see cref="ObjectAllocator{Tsavorite}"/> only)</summary>
        protected override CircularDiskReadBuffer CreateDeserializationBuffers(SectorAlignedBufferPool bufferPool, IDevice device, ILogger logger)
            => new(bufferPool, IStreamBuffer.BufferSize, numberOfDeserializationBuffers, device, logger);

        private void WriteAsync<TContext>(CircularDiskWriteBuffer flushBuffers, long flushPage, uint numBytesToWrite,
                        DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                        IDevice device, IDevice objectLogDevice, long fuzzyStartLogicalAddress = long.MaxValue)
        {
            // We flush within the DiskStreamWriteBuffer, so we do not use the asyncResult here for IO (until the final callback), but it has necessary fields.

            // Short circuit if we are using a null device
            if ((device as NullDevice) != null)
            {
                device.WriteAsync(IntPtr.Zero, 0, 0, numBytesToWrite, callback, asyncResult);
                return;
            }

            Debug.Assert(asyncResult.page == flushPage, $"asyncResult.page {asyncResult.page} should equal flushPage {flushPage}");
            var allocatorPage = pages[flushPage];

            // numBytesToWrite is calculated from start and end logical addresses, either for the full page or a subset of records (aligned to start and end of record boundaries),
            // in the allocator page (including the objectId space for Overflow and Heap Objects). Note: "Aligned" in this discussion refers to sector (as opposed to record) alignment.

            // Initialize offsets into the allocator page based on full-page, then override them if partial.
            int startOffset = 0, endOffset = (int)numBytesToWrite;
            var pageStart = GetAbsoluteLogicalAddressOfStartOfPage(asyncResult.page);
            if (asyncResult.partial)
            {
                // We're writing only a subset of the page, so align relative to page start (so we include PageHeader, which is important).
                startOffset = (int)(asyncResult.fromAddress - pageStart);
                endOffset = (int)(asyncResult.untilAddress - pageStart);
                numBytesToWrite = (uint)(endOffset - startOffset);
            }

            // Initialize disk offset from logicalAddress to subtract the GetFirstValidLogicalAddressOnPage(), then ensure we are aligned to the PageHeader
            // (for the first record on the page the caller probably passed the address of the start of the page rather than the offset of the header position).
            var logicalAddress = asyncResult.fromAddress;
            var hasPageHeader = startOffset == 0;
            Debug.Assert(asyncResult.fromAddress == pageStart || asyncResult.fromAddress >= pageStart + PageHeader.Size, $"fromAddress ({asyncResult.fromAddress}, offset {asyncResult.fromAddress - pageStart}) is in the middle of the PageHeader");
            Debug.Assert(!hasPageHeader || asyncResult.fromAddress == pageStart, "fromAddress should not be start of page if startOffset is 0");
            if (hasPageHeader)
                logicalAddress = pageStart + PageHeader.Size;

            _ = flushBuffers.OnBeginPartialFlush();

            // Create a local copy of the main-log page inline data. Space for ObjectIds and the ObjectLogFilePosition will be updated as we go
            // (ObjectId space and the length-metadata space will combine for 5 bytes or 1TB of object size, which is our max).
            // Note: The ShiftHeadAddress check to always remain below FlushedUntilAddress means the actual log page, inluding ObjectIdMap, will remain valid until we
            // complete this partial flush; but we need to create the disk image (and do so without changing record lengths).
            // TODO: We could make this initial buffer copy smaller by looping on successive subsets of the records.
            var localObjectIdMap = pages[flushPage % BufferSize].objectIdMap;
            var srcBuffer = bufferPool.Get((int)numBytesToWrite);
            var pageSpan = new Span<byte>((byte*)pagePointers[flushPage % BufferSize] + startOffset, (int)numBytesToWrite);
            pageSpan.CopyTo(srcBuffer.TotalValidSpan);
            srcBuffer.available_bytes = (int)numBytesToWrite;

            // We suspend epoch during the time-consuming actual flush. As noted in the preceding comment, we don't need the epoch to keep the log page stable.
            var epochWasProtected = epoch.ThisInstanceProtected();
            if (epochWasProtected)
                epoch.Suspend();

            // Object keys and values are serialized into this Stream.
            var valueObjectSerializer = storeFunctions.CreateValueObjectSerializer();
            var logWriter = new ObjectLogWriter(device, flushBuffers, valueObjectSerializer);
            PinnedMemoryStream<ObjectLogWriter> pinnedMemoryStream = new(logWriter);

            try
            {
                flushBuffers.filePosition = objectLogNextRecordStartPosition;
                valueObjectSerializer.BeginSerialize(pinnedMemoryStream);

                var pageHeaderPtr = (PageHeader*)srcBuffer.GetValidPointer();
                var endPhysicalAddress = (long)srcBuffer.GetValidPointer() + numBytesToWrite;
                for (var physicalAddress = (long)srcBuffer.GetValidPointer() + (hasPageHeader ? PageHeader.Size : 0); physicalAddress < endPhysicalAddress; /* incremented in loop */)
                {
                    // LogRecord is in the *copy of* the log buffer. We will update it without affecting the actual record in the log.
                    var logRecord = new LogRecord(physicalAddress, localObjectIdMap);

                    // Use allocatedSize here because that is what LogicalAddress is based on.
                    var logRecordSize = logRecord.GetInlineRecordSizes().allocatedSize;

                    // Do not write Invalid records. This includes IsNull records.
                    if (!logRecord.Info.Invalid)
                    {
                        // Do not write v+1 records (e.g. during a checkpoint)
                        if (logicalAddress < fuzzyStartLogicalAddress || !logRecord.Info.IsInNewVersion)
                        {
                            var recordStartPosition = logWriter.GetNextRecordStartPosition();
                            if (hasPageHeader)
                                pageHeaderPtr->SetLowestObjectLogPosition(recordStartPosition);
                            var recordObjectLength = logWriter.Write(in logRecord);
                            logRecord.SetObjectLogRecordStartPositionAndLength(recordStartPosition, recordObjectLength);
                        }
                        else
                        {
                            // Mark v+1 records as invalid to avoid deserializing them on recovery
                            logRecord.InfoRef.SetInvalid();
                        }
                    }

                    logicalAddress += logRecordSize;    // advance in main log
                    physicalAddress += logRecordSize;   // advance in source buffer
                }

                // We are done with the per-record objectlog flushes and we've updated the copy of the allocator page. Now write that updated page
                // to the main log file.
                var alignedStartOffset = RoundDown(startOffset, (int)device.SectorSize);
                if (asyncResult.partial)
                {
                    // We're writing only a subset of the page, so update our count of bytes to write.
                    var aligned_end = (int)RoundUp(asyncResult.untilAddress - pageStart, (int)device.SectorSize);
                    numBytesToWrite = (uint)(aligned_end - alignedStartOffset);
                }

                // Round up the number of byte to write to sector alignment.
                var alignedNumBytesToWrite = RoundUp(numBytesToWrite, (int)device.SectorSize);

                // Finally write the main log page as part of OnPartialFlushComplete.
                TODO("This will potentially overwrite sectors");
                var mainLogSpan = new ReadOnlySpan<byte>(srcBuffer.GetValidPointer() + alignedStartOffset, (int)alignedNumBytesToWrite);
                logWriter.OnPartialFlushComplete(mainLogSpan, device, ref alignedNextMainLogFlushAddress, callback, asyncResult, out objectLogNextRecordStartPosition);
            }
            finally
            {
                srcBuffer.Return();
                if (epochWasProtected)
                    epoch.Resume();
            }
        }

        private void AsyncReadPageCallback(uint errorCode, uint numBytes, object context)
        {
            TODO("Is this still needed");
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncReadPageCallback)} error: {{errorCode}}", errorCode);

            // Set the page status to flushed
            var result = (PageAsyncReadResult<Empty>)context;
            _ = result.handle.Signal();
        }

        /// <inheritdoc />
        /// <remarks>This override of the base function reads Overflow keys or values, or Object values.</remarks>
        private protected override bool VerifyRecordFromDiskCallback(ref AsyncIOContext ctx, out long prevAddressToRead, out int prevLengthToRead)
        {
            // If this fails it is either too-short main-log record or a key mismatch. Let the top-level retry handle it.
            if (!base.VerifyRecordFromDiskCallback(ref ctx, out prevAddressToRead, out prevLengthToRead) && ctx.diskLogRecord.IsSet)
                return false;

            // If the record is inline, we have not Overflow or Objects to retrieve.
            ref var diskLogRecord = ref ctx.diskLogRecord;
            if (diskLogRecord.Info.RecordIsInline)
                return true;

            var deserializationBuffers = diskLogRecord.Info.ValueIsObject ? CreateDeserializationBuffers(bufferPool, device, logger) : default;

            ObjectLogReader<TStoreFunctions>.DiskReadParameters readParams =
                new(bufferPool, maxInlineKeySize, maxInlineValueSize, device.SectorSize, IStreamBuffer.BufferSize, ctx.logicalAddress, storeFunctions);
            var readBuffer = new ObjectLogReader<TStoreFunctions>(in readParams, device, deserializationBuffers, logger);
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

        protected override void ReadAsync<TContext>(CircularDiskReadBuffer readBuffers, ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
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


        private void AsyncReadPageWithObjectsCallback<TContext>(uint errorCode, uint numBytes, object context)
        {
            TODO("Ensure cts is passed through to Deserialize and CircularDiskReadBuffer.Read");
            TODO("Do not track deserialization size changes if we are deserializing to a frame");
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
                // We do not track deserialization size changes if we are deserializing to a frame
                Deserialize(result.freeBuffer1.GetValidPointer(), result.resumePtr, result.untilPtr, src, ms, result.frame != null);
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
        {
#if READ_WRITE
            var fileOffset = (ulong)(AlignedPageSizeBytes * (fromLogical >> LogPageSizeBits) + (fromLogical & PageSizeMask));
            var alignedFileOffset = (ulong)(((long)fileOffset / sectorSize) * sectorSize);

            var alignedReadLength = (uint)((long)fileOffset + numBytes - (long)alignedFileOffset);
            alignedReadLength = (uint)((alignedReadLength + (sectorSize - 1)) & ~(sectorSize - 1));

            var record = bufferPool.Get((int)alignedReadLength);
            record.valid_offset = (int)(fileOffset - alignedFileOffset);
            record.available_bytes = (int)(alignedReadLength - (fileOffset - alignedFileOffset));
            record.required_bytes = numBytes;

            var asyncResult = default(AsyncGetFromDiskResult<AsyncIOContext>);
            asyncResult.context = context;
            asyncResult.context.record = result;
            asyncResult.context.objBuffer = record;
            objectLogDevice.ReadAsync(
                (int)(context.logicalAddress >> LogSegmentSizeBits),
                alignedFileOffset,
                (IntPtr)asyncResult.context.objBuffer.aligned_pointer,
                alignedReadLength,
                callback,
                asyncResult);
#endif // READ_WRITE
        }

        public struct AllocatorRecord   // TODO remove
        {
            public RecordInfo info;
            public byte[] key;
            public byte[] value;
        }

        #region Page handlers for objects
        /// <summary>
        /// Deseialize part of page from stream
        /// </summary>
        /// <param name="raw"></param>
        /// <param name="ptr">From pointer</param>
        /// <param name="untilptr">Until pointer</param>
        /// <param name="src"></param>
        /// <param name="stream">Stream</param>
        public void Deserialize(byte* raw, long ptr, long untilptr, AllocatorRecord[] src, Stream stream)
        {
#if READ_WRITE
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

            if (OnDeserializationObserver != null && start_offset != -1 && end_offset != -1 && !doNotObserve)
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

        public long[] GetSegmentOffsets() => null;

        internal void PopulatePage(byte* src, int required_bytes, long destinationPage)
        {
#if READ_WRITE
            PopulatePage(src, required_bytes, ref values[destinationPage % BufferSize]);
#endif // READ_WRITE
        }

        internal void PopulatePage(byte* src, int required_bytes, ref AllocatorRecord[] destinationPage)
        {
#if READ_WRITE
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
                long beginAddress, long endAddress, DiskScanBufferingMode diskScanBufferingMode, bool includeClosedRecords)
            => new SpanByteScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>>(store, this, beginAddress, endAddress, epoch, diskScanBufferingMode, includeClosedRecords: includeClosedRecords);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, ref TScanFunctions scanFunctions, DiskScanBufferingMode scanBufferingMode)
        {
            using SpanByteScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, beginAddress, endAddress, epoch, scanBufferingMode, includeClosedRecords: false, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor, long maxAddress,
                bool resetCursor = true, bool includeTombstones = false)
        {
            using SpanByteScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, cursor, endAddress, epoch, DiskScanBufferingMode.SinglePageBuffering,
                includeClosedRecords: maxAddress < long.MaxValue, logger: logger);
            return ScanLookup<long, long, TScanFunctions, SpanByteScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>>>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor,
                maxAddress, resetCursor: resetCursor, includeTombstones: includeTombstones);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                ReadOnlySpan<byte> key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using SpanByteScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, beginAddress, epoch, logger: logger);
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