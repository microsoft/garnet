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
    using static LogAddress;

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

        readonly int maxInlineKeySize;
        readonly int maxInlineValueSize;

        // Size of object chunks being written to storage
        // TODO: private readonly int objectBlockSize = 100 * (1 << 20);

        private readonly OverflowPool<PageUnit<ObjectPage>> freePagePool;

        public ObjectAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, ObjectAllocator<TStoreFunctions>> wrapperCreator)
            : base(settings.LogSettings, storeFunctions, wrapperCreator, settings.evictCallback, settings.epoch, settings.flushCallback, settings.logger)
        {
            IsObjectAllocator = true;

            maxInlineKeySize = 1 << settings.LogSettings.MaxInlineKeySizeBits;
            maxInlineValueSize = 1 << settings.LogSettings.MaxInlineValueSizeBits;

            freePagePool = new OverflowPool<PageUnit<ObjectPage>>(4, static p => { });

            var bufferSizeInBytes = (nuint)RoundUp(sizeof(long*) * BufferSize, Constants.kCacheLineBytes);
            pagePointers = (long*)NativeMemory.AlignedAlloc(bufferSizeInBytes, Constants.kCacheLineBytes);
            NativeMemory.Clear(pagePointers, bufferSizeInBytes);

            values = new ObjectPage[BufferSize];
            for (var ii = 0; ii < BufferSize; ii++)
                values[ii] = new();

            // For LogField conversions between inline and heap fields, we assume the inline field size prefix is the same size as objectId size
            Debug.Assert(LogField.InlineLengthPrefixSize == ObjectIdMap.ObjectIdSize, "InlineLengthPrefixSize must be equal to ObjectIdMap.ObjectIdSize");
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
        internal void SerializeKey(ReadOnlySpan<byte> key, long logicalAddress, ref LogRecord logRecord) => SerializeKey(key, logicalAddress, ref logRecord, maxInlineKeySize, GetObjectIdMap(logicalAddress));

        public override void Initialize() => Initialize(FirstValidAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitializeValue(long physicalAddress, in RecordSizeInfo sizeInfo)
        {
            var valueAddress = LogRecord.GetValueAddress(physicalAddress);
            if (sizeInfo.ValueIsInline)
            {
                // Set the actual length indicator for inline.
                LogRecord.GetInfoRef(physicalAddress).SetValueIsInline();
                _ = LogField.SetInlineDataLength(valueAddress, sizeInfo.FieldInfo.ValueDataSize);
            }
            else
            {
                // If it's a revivified record, it may have ValueIsInline set, so clear that.
                LogRecord.GetInfoRef(physicalAddress).ClearValueIsInline();

                if (sizeInfo.ValueIsObject)
                    LogRecord.GetInfoRef(physicalAddress).SetValueIsObject();

                // Either an IHeapObject or a byte[]
                *LogRecord.GetValueObjectIdAddress(physicalAddress) = ObjectIdMap.InvalidObjectId;
            }
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
                    KeyDataSize = key.Length,
                    ValueDataSize = 0,      // This will be inline, and with the length prefix and possible space when rounding up to kRecordAlignment, allows the possibility revivification can reuse the record for a Heap Field
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
            // Key
            sizeInfo.KeyIsInline = sizeInfo.FieldInfo.KeyDataSize <= maxInlineKeySize;
            var keySize = sizeInfo.KeyIsInline ? sizeInfo.FieldInfo.KeyDataSize + LogField.InlineLengthPrefixSize : ObjectIdMap.ObjectIdSize;

            // Value
            sizeInfo.MaxInlineValueSpanSize = maxInlineValueSize;
            sizeInfo.ValueIsInline = !sizeInfo.ValueIsObject && sizeInfo.FieldInfo.ValueDataSize <= sizeInfo.MaxInlineValueSpanSize;
            var valueSize = sizeInfo.ValueIsInline ? sizeInfo.FieldInfo.ValueDataSize + LogField.InlineLengthPrefixSize : ObjectIdMap.ObjectIdSize;

            // Record
            sizeInfo.ActualInlineRecordSize = RecordInfo.GetLength() + keySize + valueSize + sizeInfo.OptionalSize;
            sizeInfo.AllocatedInlineRecordSize = RoundUp(sizeInfo.ActualInlineRecordSize, Constants.kRecordAlignment);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DisposeRecord(ref LogRecord logRecord, DisposeReason disposeReason)
        {
            logRecord.ClearOptionals();
            if (disposeReason != DisposeReason.Deleted)
                _ = logRecord.FreeKeyOverflow();

            if (!logRecord.Info.ValueIsInline)
            {
                if (!logRecord.FreeValueOverflow() && logRecord.ValueObjectId != ObjectIdMap.InvalidObjectId)
                {
                    var heapObj = logRecord.ValueObject;
                    if (heapObj is not null)
                        storeFunctions.DisposeValueObject(heapObj, disposeReason);
                    logRecord.objectIdMap.Free(logRecord.ValueObjectId);
                }
            }
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

        protected override void TruncateUntilAddress(long toAddress)    // TODO: ObjectAllocator specifics if any
        {
            base.TruncateUntilAddress(toAddress);
        }

        protected override void TruncateUntilAddressBlocking(long toAddress)    // TODO: ObjectAllocator specifics if any
        {
            base.TruncateUntilAddressBlocking(toAddress);
        }

        protected override void RemoveSegment(int segment)    // TODO: ObjectAllocator specifics if any
        {
            base.RemoveSegment(segment);
        }

        protected override void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
        {
            WriteAsync(flushPage,
                    (ulong)(AlignedPageSizeBytes * flushPage),
                    (uint)PageSize,
                    callback,
                    asyncResult, device);
        }

        protected override void WriteAsyncToDevice<TContext>
            (long startPage, long flushPage, int pageSize, DeviceIOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice, long[] localSegmentOffsets, long fuzzyStartLogicalAddress)
        {
#if READ_WRITE
            VerifyCompatibleSectorSize(device);
            VerifyCompatibleSectorSize(objectLogDevice);

            var epochTaken = false;
            if (!epoch.ThisInstanceProtected())
            {
                epochTaken = true;
                epoch.Resume();
            }
            try
            {
                if (HeadAddress >= (flushPage << LogPageSizeBits) + pageSize)
                {
                    // Requested page is unavailable in memory, ignore
                    callback(0, 0, asyncResult);
                }
                else
                {
                    // We are writing to separate device, so use fresh segment offsets
                    WriteAsync(flushPage,
                            (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                            (uint)pageSize, callback, asyncResult,
                            device, objectLogDevice, flushPage, localSegmentOffsets, fuzzyStartLogicalAddress);
                }
            }
            finally
            {
                if (epochTaken)
                    epoch.Suspend();
            }
#endif // READ_WRITE
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

        private void WriteAsync<TContext>(long flushPage, ulong alignedDestinationAddress, uint numBytesToWrite,
                        DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                        IDevice device, long intendedDestinationPage = -1, long[] localSegmentOffsets = null, long fuzzyStartLogicalAddress = long.MaxValue)
        {
#if READ_WRITE
            // Short circuit if we are using a null device
            if ((device as NullDevice) != null)
            {
                device.WriteAsync(IntPtr.Zero, 0, 0, numBytesToWrite, callback, asyncResult);
                return;
            }

            int start = 0, aligned_start = 0, end = (int)numBytesToWrite;
            if (asyncResult.partial)
            {
                // We're writing only a subset of the page
                start = (int)(asyncResult.fromAddress - (asyncResult.page << LogPageSizeBits));
                aligned_start = (start / sectorSize) * sectorSize;
                end = (int)(asyncResult.untilAddress - (asyncResult.page << LogPageSizeBits));
            }

            // Check if user did not override with special segment offsets
            localSegmentOffsets ??= segmentOffsets;

            // This is the in-memory buffer page to be written
            var src = values[flushPage % BufferSize];

            // We create a shadow copy of the page if we are under epoch protection.
            // This copy ensures that object references are kept valid even if the original page is reclaimed.
            // We suspend epoch during the actual flush as that can take a long time.
            var epochProtected = false;
            if (epoch.ThisInstanceProtected())
            {
                epochProtected = true;
                src = new AllocatorRecord[values[flushPage % BufferSize].Length];
                Array.Copy(values[flushPage % BufferSize], src, values[flushPage % BufferSize].Length);
                epoch.Suspend();
            }
            try
            {
                // Temporary storage to hold the image "template" we'll write to disk: It will have RecordInfos and object pointers that will be overwritten by addresses
                // when writing to the main log (both object pointers and addresses are 8 bytes).
                var buffer = bufferPool.Get((int)numBytesToWrite);

                if (aligned_start < start && (KeyHasObjects() || ValueHasObjects()))
                {
                    // Do not read back the invalid header of page 0
                    if ((flushPage > 0) || (start > GetFirstValidLogicalAddress(flushPage)))
                    {
                        // Get the overlapping HLOG from disk as we wrote it with object pointers previously. This avoids object reserialization
                        PageAsyncReadResult<Empty> result = new()
                        {
                            handle = new CountdownEvent(1)
                        };
                        device.ReadAsync(alignedDestinationAddress + (ulong)aligned_start, (IntPtr)buffer.aligned_pointer + aligned_start,
                            (uint)sectorSize, AsyncReadPageCallback, result);
                        result.handle.Wait();
                    }
                    fixed (RecordInfo* pin = &src[0].info)
                    {
                        // Write all the RecordInfos on one operation. This also includes object pointers, but for valid records we will overwrite those below.
                        Debug.Assert(buffer.aligned_pointer + numBytesToWrite <= (byte*)Unsafe.AsPointer(ref buffer.buffer[0]) + buffer.buffer.Length);

                        Buffer.MemoryCopy((void*)((long)Unsafe.AsPointer(ref src[0]) + start), buffer.aligned_pointer + start,
                            numBytesToWrite - start, numBytesToWrite - start);
                    }
                }
                else
                {
                    fixed (RecordInfo* pin = &src[0].info)
                    {
                        // Write all the RecordInfos on one operation. This also includes object pointers, but for valid records we will overwrite those below.
                        Debug.Assert(buffer.aligned_pointer + numBytesToWrite <= (byte*)Unsafe.AsPointer(ref buffer.buffer[0]) + buffer.buffer.Length);

                        Buffer.MemoryCopy((void*)((long)Unsafe.AsPointer(ref src[0]) + aligned_start), buffer.aligned_pointer + aligned_start,
                            numBytesToWrite - aligned_start, numBytesToWrite - aligned_start);
                    }
                }

                // In the main log, we write addresses to pages in the object log. This array saves the addresses of the key and/or value fields in 'buffer',
                // which again is the image we're building from the 'values' "page" for this write. The "addresses into 'buffer'" are cast below to AddressInfo
                // structures and stored in the sequence we'll write them: alternating series of key then value if both are object types, else keys or values only.
                var addr = new List<long>();
                asyncResult.freeBuffer1 = buffer;

                // Object keys and values are serialized into this MemoryStream.
                MemoryStream ms = new();
                var keySerializer = KeyHasObjects() ? _storeFunctions.BeginSerializeKey(ms) : null;
                var valueSerializer = ValueHasObjects() ? _storeFunctions.BeginSerializeValue(ms) : null;

                // Track the size to be written to the object log.
                long endPosition = 0;

                for (int i = start / RecordSize; i < end / RecordSize; i++)
                {
                    byte* recordPtr = buffer.aligned_pointer + i * RecordSize;

                    // Retrieve reference to record struct
                    ref var record = ref Unsafe.AsRef<AllocatorRecord>(recordPtr);
                    AddressInfo* key_address = null, value_address = null;

                    // Zero out object reference addresses (AddressInfo) in the planned disk image
                    if (KeyHasObjects())
                    {
                        key_address = GetKeyAddressInfo((long)recordPtr);
                        *key_address = default;
                    }
                    if (ValueHasObjects())
                    {
                        value_address = GetValueAddressInfo((long)recordPtr);
                        *value_address = default;
                    }

                    // Now fill in AddressInfo data for the valid records
                    if (!record.info.Invalid)
                    {
                        // Calculate the logical address of the 'values' page currently being written.
                        var address = (flushPage << LogPageSizeBits) + i * RecordSize;

                        // Do not write v+1 records (e.g. during a checkpoint)
                        if (address < fuzzyStartLogicalAddress || !record.info.IsInNewVersion)
                        {
                            if (KeyHasObjects())
                            {
                                long pos = ms.Position;
                                keySerializer.Serialize(ref src[i].key);

                                // Store the key address into the 'buffer' AddressInfo image as an offset into 'ms'.
                                key_address->Address = pos;
                                key_address->Size = (int)(ms.Position - pos);
                                addr.Add((long)key_address);
                                endPosition = pos + key_address->Size;
                            }

                            if (ValueHasObjects() && !record.info.Tombstone)
                            {
                                long pos = ms.Position;
                                try
                                {
                                    valueSerializer.Serialize(ref src[i].value);
                                }
                                catch (Exception ex)
                                {
                                    logger?.LogError(ex, "Failed to serialize value");
                                    ms.Position = pos;
                                    TValue defaultValue = default;
                                    valueSerializer.Serialize(ref defaultValue);
                                }

                                // Store the value address into the 'buffer' AddressInfo image as an offset into 'ms'.
                                value_address->Address = pos;
                                value_address->Size = (int)(ms.Position - pos);
                                addr.Add((long)value_address);
                                endPosition = pos + value_address->Size;
                            }
                        }
                        else
                        {
                            // Mark v+1 records as invalid to avoid deserializing them on recovery
                            record.info.SetInvalid();
                        }
                    }

                    // If this record's serialized size surpassed ObjectBlockSize or it's the last record to be written, write to the object log.
                    if (endPosition > objectBlockSize || i == (end / RecordSize) - 1)
                    {
                        var memoryStreamActualLength = ms.Position;
                        var memoryStreamTotalLength = (int)endPosition;
                        endPosition = 0;

                        if (KeyHasObjects())
                            keySerializer.EndSerialize();
                        if (ValueHasObjects())
                            valueSerializer.EndSerialize();
                        ms.Close();

                        // Get the total serialized length rounded up to sectorSize
                        var _alignedLength = (memoryStreamTotalLength + (sectorSize - 1)) & ~(sectorSize - 1);

                        // Reserve the current address in the object log segment offsets for this chunk's write operation.
                        var _objAddr = Interlocked.Add(ref localSegmentOffsets[(long)(alignedDestinationAddress >> LogSegmentSizeBits) % SegmentBufferSize], _alignedLength) - _alignedLength;

                        // Allocate the object-log buffer to build the image we'll write to disk, then copy to it from the memory stream.
                        SectorAlignedMemory _objBuffer = null;
                        if (memoryStreamTotalLength > 0)
                        {
                            _objBuffer = bufferPool.Get(memoryStreamTotalLength);

                            fixed (void* src_ = ms.GetBuffer())
                                Buffer.MemoryCopy(src_, _objBuffer.aligned_pointer, memoryStreamTotalLength, memoryStreamActualLength);
                        }

                        // Each address we calculated above is now an offset to objAddr; convert to the actual address.
                        foreach (var address in addr)
                            ((AddressInfo*)address)->Address += _objAddr;

                        // If we have not written all records, prepare for the next chunk of records to be written.
                        if (i < (end / RecordSize) - 1)
                        {
                            // Create a new MemoryStream for the next chunk of records to be written.
                            ms = new MemoryStream();
                            if (KeyHasObjects())
                                keySerializer.BeginSerialize(ms);
                            if (ValueHasObjects())
                                valueSerializer.BeginSerialize(ms);

                            // Reset address list for the next chunk of records to be written.
                            addr = new List<long>();

                            // Write this chunk of records to the object log device.
                            asyncResult.done = new AutoResetEvent(false);
                            Debug.Assert(memoryStreamTotalLength > 0);
                            objlogDevice.WriteAsync(
                                (IntPtr)_objBuffer.aligned_pointer,
                                (int)(alignedDestinationAddress >> LogSegmentSizeBits),
                                (ulong)_objAddr, (uint)_alignedLength, AsyncFlushPartialObjectLogCallback<TContext>, asyncResult);

                            // Wait for write to complete before resuming next write
                            _ = asyncResult.done.WaitOne();
                            _objBuffer.Return();
                        }
                        else
                        {
                            // We have written all records in this 'values' "page".
                            if (memoryStreamTotalLength > 0)
                            {
                                // Increment the count because we need to write both page and object cache.
                                _ = Interlocked.Increment(ref asyncResult.count);

                                asyncResult.freeBuffer2 = _objBuffer;
                                objlogDevice.WriteAsync(
                                    (IntPtr)_objBuffer.aligned_pointer,
                                    (int)(alignedDestinationAddress >> LogSegmentSizeBits),
                                    (ulong)_objAddr, (uint)_alignedLength, callback, asyncResult);
                            }
                        }
                    }
                }

                if (asyncResult.partial)
                {
                    // We're writing only a subset of the page, so update our count of bytes to write.
                    var aligned_end = (int)(asyncResult.untilAddress - (asyncResult.page << LogPageSizeBits));
                    aligned_end = (aligned_end + (sectorSize - 1)) & ~(sectorSize - 1);
                    numBytesToWrite = (uint)(aligned_end - aligned_start);
                }

                // Round up the number of byte to write to sector alignment.
                var alignedNumBytesToWrite = (uint)((numBytesToWrite + (sectorSize - 1)) & ~(sectorSize - 1));

                // Finally write the hlog page
                device.WriteAsync((IntPtr)buffer.aligned_pointer + aligned_start, alignedDestinationAddress + (ulong)aligned_start,
                    alignedNumBytesToWrite, callback, asyncResult);
            }
            finally
            {
                if (epochProtected)
                    epoch.Resume();
            }
#endif // READ_WRITE
        }

        private void AsyncReadPageCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncReadPageCallback)} error: {{errorCode}}", errorCode);

            // Set the page status to flushed
            var result = (PageAsyncReadResult<Empty>)context;
            _ = result.handle.Signal();
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

            // Set the page status to flushed
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
            => new RecordScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>>(store, this, beginAddress, endAddress, epoch, diskScanBufferingMode, includeClosedRecords: includeClosedRecords);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, ref TScanFunctions scanFunctions, DiskScanBufferingMode scanBufferingMode)
        {
            using RecordScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, beginAddress, endAddress, epoch, scanBufferingMode, includeClosedRecords: false, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor, long maxAddress,
                bool resetCursor = true, bool includeTombstones = false)
        {
            using RecordScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, cursor, endAddress, epoch, DiskScanBufferingMode.SinglePageBuffering,
                includeClosedRecords: maxAddress < long.MaxValue, logger: logger);
            return ScanLookup<long, long, TScanFunctions, RecordScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>>>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor,
                maxAddress, resetCursor: resetCursor, includeTombstones: includeTombstones);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                ReadOnlySpan<byte> key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using RecordScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, beginAddress, epoch, logger: logger);
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