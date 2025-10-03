﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//#define READ_WRITE

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
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

            public override readonly string ToString() => $"oidMap {objectIdMap}";
        }

        /// <summary>The pages of the log, containing object storage. In parallel with AllocatorBase.pagePointers</summary>
        internal ObjectPage[] pages;

        /// <summary>The position information for the next write to the object log.</summary>
        ObjectLogFilePositionInfo objectLogNextRecordStartPosition;

        // Default to max sizes so testing a size as "greater than" will always be false
        readonly int maxInlineKeySize = LogSettings.kMaxInlineKeySize;
        readonly int maxInlineValueSize = int.MaxValue;

        readonly int numberOfFlushBuffers;
        readonly int numberOfDeserializationBuffers;

        private readonly IDevice objectLogDevice;

        /// <summary>The free pages of the log</summary>
        private readonly OverflowPool<PageUnit<ObjectPage>> freePagePool;

        public ObjectAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, ObjectAllocator<TStoreFunctions>> wrapperCreator)
            : base(settings.LogSettings, storeFunctions, wrapperCreator, settings.evictCallback, settings.epoch, settings.flushCallback, settings.logger, transientObjectIdMap: new ObjectIdMap())
        {
            objectLogDevice = settings.LogSettings.ObjectLogDevice;

            maxInlineKeySize = 1 << settings.LogSettings.MaxInlineKeySizeBits;
            maxInlineValueSize = 1 << settings.LogSettings.MaxInlineValueSizeBits;

            freePagePool = new OverflowPool<PageUnit<ObjectPage>>(4, static p => { });

            if (settings.LogSettings.NumberOfFlushBuffers < LogSettings.kMinFlushBuffers || settings.LogSettings.NumberOfFlushBuffers > LogSettings.kMaxFlushBuffers || !IsPowerOfTwo(settings.LogSettings.NumberOfFlushBuffers))
                throw new TsavoriteException($"{nameof(settings.LogSettings.NumberOfFlushBuffers)} must be between {LogSettings.kMinFlushBuffers} and {LogSettings.kMaxFlushBuffers - 1} and a power of 2");
            numberOfFlushBuffers = settings.LogSettings.NumberOfFlushBuffers;

            if (settings.LogSettings.NumberOfDeserializationBuffers < LogSettings.kMinDeserializationBuffers || settings.LogSettings.NumberOfDeserializationBuffers > LogSettings.kMaxDeserializationBuffers || !IsPowerOfTwo(settings.LogSettings.NumberOfDeserializationBuffers))
                throw new TsavoriteException($"{nameof(settings.LogSettings.NumberOfDeserializationBuffers)} must be between {LogSettings.kMinDeserializationBuffers} and {LogSettings.kMaxDeserializationBuffers - 1} and a power of 2");
            numberOfDeserializationBuffers = settings.LogSettings.NumberOfDeserializationBuffers;

            if (settings.LogSettings.ObjectLogSegmentSizeBits is < LogSettings.kMinObjectLogSegmentSizeBits or > LogSettings.kMaxSegmentSizeBits)
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
        internal LogRecord CreateRemappedLogRecordOverTransientMemory(long logicalAddress, long physicalAddress)
            => LogRecord.CreateRemappedOverTransientMemory(physicalAddress, pages[GetPageIndexForAddress(logicalAddress)].objectIdMap, transientObjectIdMap);

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
            logRecord.ClearHeapFields(disposeReason != DisposeReason.Deleted, obj => storeFunctions.DisposeValueObject(obj, disposeReason));
            logRecord.ClearOptionals();
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
            //TODOnow("Get the object log segment information from this main-log segment");
            base.RemoveSegment(segment);
        }

        protected override void WriteAsync<TContext>(CircularDiskWriteBuffer flushBuffers, long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
            => WriteAsync(flushBuffers, flushPage, (ulong)(AlignedPageSizeBytes * flushPage), (uint)PageSize, callback, asyncResult, device, objectLogDevice);

        protected override void WriteAsyncToDevice<TContext>(CircularDiskWriteBuffer flushBuffers, long startPage, long flushPage, int possiblyPartialPageSize, DeviceIOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice, long fuzzyStartLogicalAddress)
        {
            VerifyCompatibleSectorSize(device);
            VerifyCompatibleSectorSize(objectLogDevice);

            var epochTaken = epoch.ResumeIfNotProtected();
            try
            {
                if (HeadAddress >= GetLogicalAddressOfStartOfPage(flushPage) + possiblyPartialPageSize)
                {
                    // Requested page is unavailable in memory, ignore
                    callback(0, 0, asyncResult);
                }
                else
                {
                    // We are writing to a separate device which starts at "startPage"
                    WriteAsync(flushBuffers, flushPage, (ulong)(AlignedPageSizeBytes * (flushPage - startPage)), (uint)possiblyPartialPageSize,
                               callback, asyncResult, device, objectLogDevice, fuzzyStartLogicalAddress);
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
            pages[page % BufferSize].objectIdMap.Clear();

            ClearPage(page, 0);

            // If all pages are being used (i.e. EmptyPageCount == 0), nothing to re-utilize by adding
            // to overflow pool.
            if (EmptyPageCount > 0)
                ReturnPage((int)(page % BufferSize));
        }

        /// <summary>Create the flush buffer (for <see cref="ObjectAllocator{Tsavorite}"/> only)</summary>
        internal override CircularDiskWriteBuffer CreateCircularFlushBuffers(IDevice objectLogDevice, ILogger logger)
            => new(bufferPool, IStreamBuffer.BufferSize, numberOfFlushBuffers, objectLogDevice ?? this.objectLogDevice, logger);

        /// <summary>Create the flush buffer (for <see cref="ObjectAllocator{Tsavorite}"/> only)</summary>
        internal override CircularDiskReadBuffer CreateCircularReadBuffers(IDevice objectLogDevice, ILogger logger)
            => new(bufferPool, IStreamBuffer.BufferSize, numberOfDeserializationBuffers, objectLogDevice ?? this.objectLogDevice, logger);

        private CircularDiskReadBuffer CreateCircularReadBuffers()
            => new(bufferPool, IStreamBuffer.BufferSize, numberOfDeserializationBuffers, objectLogDevice, logger);

        private void WriteAsync<TContext>(CircularDiskWriteBuffer flushBuffers, long flushPage, ulong alignedMainLogFlushPageAddress, uint numBytesToWrite,
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
            var allocatorPage = pages[flushPage % BufferSize];

            // numBytesToWrite is calculated from start and end logical addresses, either for the full page or a subset of records (aligned to start and end of record boundaries),
            // in the allocator page (including the objectId space for Overflow and Heap Objects). Note: "Aligned" in this discussion refers to sector (as opposed to record) alignment.

            // Initialize offsets into the allocator page based on full-page (including the page header), then override them if partial.
            // asyncResult.fromAddress is either start of page or start of a record past the page header
            var pageStart = GetLogicalAddressOfStartOfPage(asyncResult.page);
            Debug.Assert(asyncResult.fromAddress - pageStart is >= PageHeader.Size or 0, $"fromAddress ({asyncResult.fromAddress}, offset {asyncResult.fromAddress - pageStart}) must be 0 or after the PageHeader");
            Debug.Assert(asyncResult.untilAddress - pageStart >= PageHeader.Size, $"untilAddress ({asyncResult.untilAddress}, offset {asyncResult.untilAddress - pageStart}) must be past PageHeader {flushPage}");
            int startOffset = (int)(asyncResult.fromAddress - pageStart), endOffset = startOffset + (int)numBytesToWrite;
            if (asyncResult.partial)
            {
                // We're writing only a subset of the page.
                endOffset = (int)(asyncResult.untilAddress - pageStart);
                numBytesToWrite = (uint)(endOffset - startOffset);
            }

            // Adjust so the first record on the page includes the page header. We've already asserted fromAddress such that startOffset is either 0 or >= PageHeader.
            var logicalAddress = asyncResult.fromAddress;
            var isFirstRecordOnPage = startOffset <= PageHeader.Size;
            var firstRecordOffset = startOffset;
            if (isFirstRecordOnPage)
            {
                if (startOffset == 0)
                {
                    // For the first record on the page the caller may have passed the address of the start of the page rather than the offset at the end of the PageHeader.
                    firstRecordOffset = PageHeader.Size;
                    logicalAddress += firstRecordOffset;
                }
                else
                {
                    startOffset = 0;    // Include the PageHeader
                    numBytesToWrite = (uint)(endOffset - startOffset);
                }
            }

            var alignedStartOffset = RoundDown(startOffset, (int)device.SectorSize);
            var startPadding = startOffset - alignedStartOffset;
            var alignedBufferSize = RoundUp(startPadding + (int)numBytesToWrite, (int)device.SectorSize);

            // We suspend epoch during the time-consuming actual flush. Note: The ShiftHeadAddress check to always remain below FlushedUntilAddress
            // means the actual log page, inluding ObjectIdMap, will remain valid until we complete this partial flush.
            var epochWasProtected = epoch.SuspendIfProtected();

            // Do everything below here in the try{} to be sure the epoch is Resumed()d if we Suspended it.
            SectorAlignedMemory srcBuffer = default;
            try
            {
                // Create a local copy of the main-log page inline data. Space for ObjectIds and the ObjectLogPosition will be updated as we go
                // (ObjectId space and a byte of the length-metadata space will combine for 5 bytes or 1TB of object size, which is our max). This does
                // not change record sizes, so the logicalAddress space is unchanged. Also, we will not advance HeadAddress until this flush is complete
                // and has updated FlushedUntilAddress, so we don't have to worry about the page being yanked out from underneath us (and Objects
                // won't be disposed before we're done). TODO: Loop on successive subsets of the page's records to make this initial copy buffer smaller.
                var objectIdMap = pages[flushPage % BufferSize].objectIdMap;
                srcBuffer = bufferPool.Get(alignedBufferSize);

                // Read back the first sector if the start is not aligned (this means we already wrote a partially-filled sector with ObjectLog fields set).
                if (startPadding > 0)
                {
                    // TODO: This will potentially overwrite partial sectors if this is a partial flush; a workaround would be difficult.
                    // TODO: Cache the last sector flushed in readBuffers so we can avoid this Read.
                    PageAsyncReadResult<Empty> result = new() { handle = new CountdownEvent(1) };
                    device.ReadAsync(alignedMainLogFlushPageAddress + (ulong)alignedStartOffset, (IntPtr)srcBuffer.aligned_pointer, (uint)sectorSize, AsyncReadPageCallback, result);
                    result.handle.Wait();
                    result.DisposeHandle();
                }

                // Copy from the record start position (startOffset) in the main log page to the src buffer starting at its offset in the first sector (startPadding).
                var allocatorPageSpan = new Span<byte>((byte*)pagePointers[flushPage % BufferSize] + startOffset, (int)numBytesToWrite);
                allocatorPageSpan.CopyTo(srcBuffer.TotalValidSpan.Slice(startPadding));
                srcBuffer.available_bytes = (int)numBytesToWrite + startPadding;

                // Overflow Keys and Values are written to, and Object values are serialized to, this Stream.
                var logWriter = new ObjectLogWriter<TStoreFunctions>(device, flushBuffers, storeFunctions);
                _ = logWriter.OnBeginPartialFlush(objectLogNextRecordStartPosition);

                // Include page header when calculating end address.
                var endPhysicalAddress = (long)srcBuffer.GetValidPointer() + startPadding + numBytesToWrite;
                var physicalAddress = (long)srcBuffer.GetValidPointer() + firstRecordOffset - alignedStartOffset;
                while (physicalAddress < endPhysicalAddress)
                {
                    // LogRecord is in the *copy of* the log buffer. We will update it (for objectIds) without affecting the actual record in the log.
                    var logRecord = new LogRecord(physicalAddress, objectIdMap);

                    // Use allocatedSize here because that is what LogicalAddress is based on.
                    var logRecordSize = logRecord.GetInlineRecordSizes().allocatedSize;

                    // Do not write Invalid records. This includes IsNull records.
                    if (!logRecord.Info.Invalid)
                    {
                        // Do not write v+1 records (e.g. during a checkpoint)
                        if (logicalAddress < fuzzyStartLogicalAddress || !logRecord.Info.IsInNewVersion)
                        {
                            // Do not write objects for fully-inline records
                            if (logRecord.Info.RecordHasObjects)
                            {
                                var recordStartPosition = logWriter.GetNextRecordStartPosition();
                                if (isFirstRecordOnPage)
                                {
                                    ((PageHeader*)srcBuffer.GetValidPointer())->SetLowestObjectLogPosition(recordStartPosition);
                                    isFirstRecordOnPage = false;
                                }
                                var valueObjectLength = logWriter.WriteRecordObjects(in logRecord);
                                logRecord.SetObjectLogRecordStartPositionAndLength(recordStartPosition, valueObjectLength);
                            }
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
                if (asyncResult.partial)
                {
                    // We're writing only a subset of the page, so update our count of bytes to write.
                    var aligned_end = (int)RoundUp(asyncResult.untilAddress - pageStart, (int)device.SectorSize);
                    numBytesToWrite = (uint)(aligned_end - alignedStartOffset);
                }

                // Finally write the main log page as part of OnPartialFlushComplete.
                // TODO: This will potentially overwrite partial sectors if this is a partial flush; a workaround would be difficult.
                var mainLogSpan = new ReadOnlySpan<byte>(srcBuffer.GetValidPointer(), alignedBufferSize);
                logWriter.OnPartialFlushComplete(mainLogSpan, device, alignedMainLogFlushPageAddress + (uint)alignedStartOffset, callback, asyncResult, out objectLogNextRecordStartPosition);
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
            if (!base.VerifyRecordFromDiskCallback(ref ctx, out prevAddressToRead, out prevLengthToRead))
                return false;

            // If the record is inline, we have no Overflow or Objects to retrieve.
            ref var diskLogRecord = ref ctx.diskLogRecord;
            if (diskLogRecord.Info.RecordIsInline)
                return true;

            var startPosition = new ObjectLogFilePositionInfo(ctx.diskLogRecord.logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength),
                                                              objectLogNextRecordStartPosition.SegmentSizeBits);
            var totalBytesToRead = (ulong)keyLength + valueLength;

            using var readBuffers = diskLogRecord.Info.RecordHasObjects ? CreateCircularReadBuffers(objectLogDevice, logger) : default;

            var logReader = new ObjectLogReader<TStoreFunctions>(readBuffers, storeFunctions);
            logReader.OnBeginReadRecords(startPosition, totalBytesToRead);
            if (logReader.ReadRecordObjects(ref diskLogRecord.logRecord, ctx.request_key, transientObjectIdMap, startPosition.SegmentSizeBits))
            {
                // Success; set the DiskLogRecord objectDisposer. We dispose the object here because it is read from the disk, unless we transfer it such as by CopyToTail.
                ctx.diskLogRecord.objectDisposer = obj => storeFunctions.DisposeValueObject(obj, DisposeReason.DeserializedFromDisk);

                // Default the output arguments for reading a previous record.
                prevAddressToRead = 0;
                return true;
            }

            // If readBuffer.Read returned false it was due to an Overflow key mismatch or an Invalid record, so get the previous record.
            prevAddressToRead = (*(RecordInfo*)ctx.record.GetValidPointer()).PreviousAddress;
            return false;
        }

        protected override void ReadAsync<TContext>(CircularDiskReadBuffer readBuffers, ulong alignedSourceAddress, IntPtr destinationPtr, uint aligned_read_length,
            DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device)
        {
            //TODOnow("Add CancellationToken to the ReadAsync path");

            asyncResult.callback = callback;
            asyncResult.destinationPtr = destinationPtr;
            asyncResult.readBuffers = readBuffers;
            asyncResult.maxPtr = aligned_read_length;

            device.ReadAsync(alignedSourceAddress, destinationPtr, aligned_read_length, AsyncReadPageWithObjectsCallback<TContext>, asyncResult);
        }

        private void AsyncReadPageWithObjectsCallback<TContext>(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncReadPageWithObjectsCallback)} error: {{errorCode}}", errorCode);

            var result = (PageAsyncReadResult<TContext>)context;
            var pageStartAddress = (long)result.destinationPtr;
            result.maxPtr = numBytes;

            // Iterate all records in range to determine how many bytes we need to read from objlog.
            ObjectLogFilePositionInfo startPosition = new();
            ulong totalBytesToRead = 0;
            var recordAddress = pageStartAddress + PageHeader.Size;
            while (true)
            {
                var logRecord = new LogRecord(recordAddress);

                // Use allocatedSize here because that is what LogicalAddress is based on.
                var logRecordSize = logRecord.GetInlineRecordSizes().allocatedSize;
                recordAddress += logRecordSize;

                if (logRecord.Info.Invalid || logRecord.Info.RecordIsInline)
                    continue;

                if (!startPosition.IsSet)
                {
                    startPosition = new(logRecord.GetObjectLogRecordStartPositionAndLengths(out _, out _), objectLogNextRecordStartPosition.SegmentSizeBits);
                    continue;
                }

                // We have already incremented record address to get to the next record; if it is at or beyond the maxPtr, we have processed all records.
                if (recordAddress >= pageStartAddress + result.maxPtr)
                {
                    ObjectLogFilePositionInfo endPosition = new(logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength),
                                                                objectLogNextRecordStartPosition.SegmentSizeBits);
                    endPosition.Advance((ulong)keyLength + valueLength);
                    totalBytesToRead = endPosition - startPosition;
                    break;
                }
            }

            // The page may not have contained any records with objects
            if (startPosition.IsSet)
            {
                // Iterate all records again to actually do the deserialization.
                result.readBuffers.nextReadFilePosition = startPosition;
                recordAddress = pageStartAddress + PageHeader.Size;
                ReadOnlySpan<byte> noKey = default;
                var logReader = new ObjectLogReader<TStoreFunctions>(result.readBuffers, storeFunctions);
                logReader.OnBeginReadRecords(startPosition, totalBytesToRead);

                do
                {
                    var logRecord = new LogRecord(recordAddress);

                    // Use allocatedSize here because that is what LogicalAddress is based on.
                    var logRecordSize = logRecord.GetInlineRecordSizes().allocatedSize;
                    recordAddress += logRecordSize;

                    if (logRecord.Info.Invalid || logRecord.Info.RecordIsInline)
                        continue;

                    // We don't need the DiskLogRecord here; we're either iterating (and will create it in GetNext()) or recovering
                    // (and do not need one; we're just populating the record ObjectIds and ObjectIdMap). objectLogDevice is in readBuffers.
                    _ = logReader.ReadRecordObjects(pageStartAddress, logRecordSize, noKey, transientObjectIdMap, startPosition.SegmentSizeBits, out _ /*diskLogRecord*/);

                    // If the incremented record address is at or beyond the maxPtr, we have processed all records.
                } while (recordAddress < pageStartAddress + result.maxPtr);
            }

            // Call the "real" page read callback
            result.callback(errorCode, numBytes, context);
            result.Free();
            return;
        }

        /// <summary>
        /// Iterator interface for scanning Tsavorite log
        /// </summary>
        /// <returns></returns>
        public override ITsavoriteScanIterator Scan(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, DiskScanBufferingMode diskScanBufferingMode, bool includeClosedRecords)
            => new ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>>(CreateCircularReadBuffers(), store, this, beginAddress, endAddress, epoch, diskScanBufferingMode, includeClosedRecords: includeClosedRecords);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, ref TScanFunctions scanFunctions, DiskScanBufferingMode scanBufferingMode)
        {
            using ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(CreateCircularReadBuffers(), store, this, beginAddress, endAddress, epoch, scanBufferingMode, includeClosedRecords: false, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor, long maxAddress,
                bool resetCursor = true, bool includeTombstones = false)
        {
            using ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(CreateCircularReadBuffers(), store, this, cursor, endAddress, epoch, DiskScanBufferingMode.SinglePageBuffering,
                includeClosedRecords: maxAddress < long.MaxValue, logger: logger);
            return ScanLookup<long, long, TScanFunctions, ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>>>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor,
                maxAddress, resetCursor: resetCursor, includeTombstones: includeTombstones);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                ReadOnlySpan<byte> key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(CreateCircularReadBuffers(), store, this, beginAddress, epoch, logger: logger);
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