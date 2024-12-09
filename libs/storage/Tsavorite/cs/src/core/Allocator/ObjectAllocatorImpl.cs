// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    internal sealed unsafe class ObjectAllocatorImpl<TStoreFunctions> : AllocatorBase<SpanByte, IHeapObject, TStoreFunctions, ObjectAllocator<TStoreFunctions>>
        where TStoreFunctions : IStoreFunctions<SpanByte, IHeapObject>
    {
        // Circular buffer definition (the long is actually a byte*, but storing as 'long' makes going through logicalAddress/physicalAddress translation easier).
        long* pagePointers;

        // Size of object chunks being written to storage
        private readonly int ObjectBlockSize = 100 * (1 << 20);

        // RecordSize and Value size are constant; only the key size is variable.
        private static int FixedValueSize => ObjectLogRecord.ObjectIdSize;

        // The values array, per-page.
        private IHeapObject[][] values;

        private readonly OverflowPool<PageUnit> overflowPagePool;

        int adjustedPageSize;

        public ObjectAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, ObjectAllocator<TStoreFunctions>> wrapperCreator)
            : base(settings.LogSettings, storeFunctions, wrapperCreator, settings.evictCallback, settings.epoch, settings.flushCallback, settings.logger)
        {
            values = new IHeapObject[BufferSize][];
            overflowPagePool = new OverflowPool<PageUnit>(4, p => { });

            adjustedPageSize = PageSize + 2 * sectorSize;

            var bufferSizeInBytes = (nuint)RoundUp(sizeof(long*) * BufferSize, Constants.kCacheLineBytes);
            pagePointers = (long*)NativeMemory.AlignedAlloc(bufferSizeInBytes, (nuint)Constants.kCacheLineBytes);
            NativeMemory.Clear(pagePointers, bufferSizeInBytes);
        }

        internal int OverflowPageCount => overflowPagePool.Count;

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

            if (overflowPagePool.TryGet(out var item))
                pagePointers[index] = item.pointer;
            else
                pagePointers[index] = (long)NativeMemory.AlignedAlloc((nuint)adjustedPageSize, (nuint)sectorSize);
        }

        void ReturnPage(int index)
        {
            Debug.Assert(index < BufferSize);
            if (pagePointers[index] != default)
            {
                _ = overflowPagePool.TryAdd(new PageUnit
                {
                    pointer = pagePointers[index],
                    value = default
                });
                pagePointers[index] = default;
                _ = Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        public override void Initialize() => Initialize(Constants.kFirstValidAddress);

        /// <summary>Get start logical address</summary>
        public long GetStartLogicalAddress(long page) => page << LogPageSizeBits;

        /// <summary>Get first valid address</summary>
        public long GetFirstValidLogicalAddress(long page) => page == 0 ? Constants.kFirstValidAddress : page << LogPageSizeBits;

        public static ref RecordInfo GetInfoRef(long physicalAddress) => ref new ObjectLogRecord(physicalAddress).InfoRef;

        public static ref RecordInfo GetInfoFromBytePointer(byte* ptr) => ref Unsafe.AsRef<RecordInfo>(ptr);

        internal static SpanByte GetKey(long physicalAddress) => new ObjectLogRecord(physicalAddress).Key;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref IHeapObject GetValue(long physicalAddress)
        {
            var valuePtr = (long*)ValueOffset(physicalAddress);
            if (*valuePtr == ObjectIdMap.InvalidObjectId)
                *valuePtr = objectIdMap.Allocate();
            return ref objectIdMap.GetRef(*valuePtr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref IHeapObject GetAndInitializeValue(long physicalAddress, long _ /* endAddress */) => ref GetValue(physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long KeyOffset(long physicalAddress) => physicalAddress + RecordInfo.GetLength();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long ValueOffset(long physicalAddress) => KeyOffset(physicalAddress) + AlignedKeySize(physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int AlignedKeySize(long physicalAddress) => RoundUp(KeySize(physicalAddress), Constants.kRecordAlignment);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int KeySize(long physicalAddress) => (*(SpanByte*)KeyOffset(physicalAddress)).TotalSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ValueSize(long physicalAddress) => FixedValueSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetValueLength(ref IHeapObject value) => FixedValueSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (int actualSize, int allocatedSize) GetRecordSize(long physicalAddress)
        {
            ref var recordInfo = ref GetInfo(physicalAddress);
            if (recordInfo.IsNull())
                return (RecordInfo.GetLength(), RecordInfo.GetLength());

            var size = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + ValueSize(physicalAddress);
            return (size, RoundUp(size, Constants.kRecordAlignment));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SerializeKey(ref SpanByte src, long physicalAddress) => src.CopyTo((byte*)KeyOffset(physicalAddress));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (int actualSize, int allocatedSize, int keySize) GetRMWCopyDestinationRecordSize<TInput, TVariableLengthInput>(ref SpanByte key, ref TInput input, ref IHeapObject value, ref RecordInfo recordInfo, TVariableLengthInput varlenInput)
            where TVariableLengthInput : IVariableLengthInput<SpanByte, TInput>
            => GetRecordSize(ref key);

        const int KeyInitialLength = sizeof(int) * 2;     // The .Length field of a SpanByte is the initial length, but the key won't be empty, so add another int

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetAverageRecordSize() => RecordInfo.GetLength() + RoundUp(KeyInitialLength, Constants.kRecordAlignment) + FixedValueSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetFixedRecordSize() => GetAverageRecordSize();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (int actualSize, int allocatedSize, int keySize) GetRMWInitialRecordSize<TInput, TSessionFunctionsWrapper>(ref SpanByte key, ref TInput input, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : IVariableLengthInput<SpanByte, TInput>
            => GetRecordSize(ref key);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (int actualSize, int allocatedSize, int keySize) GetRecordSize(ref SpanByte key, ref IHeapObject _ /* value */)
            => GetRecordSize(ref key);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (int actualSize, int allocatedSize, int keySize) GetRecordSize(ref SpanByte key)
        {
            var keySize = key.TotalSize;
            var size = RecordInfo.GetLength() + RoundUp(keySize, Constants.kRecordAlignment) + FixedValueSize;
            return (size, RoundUp(size, Constants.kRecordAlignment), keySize);
        }

        internal (int actualSize, int allocatedSize, int keySize) GetUpsertRecordSize<TInput, TSessionFunctionsWrapper>(ref SpanByte key, ref IHeapObject value, ref TInput input, TSessionFunctionsWrapper sessionFunctions)
            => GetRecordSize(ref key);

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            var localMap = Interlocked.Exchange(ref objectIdMap, null);
            if (localMap != null)
            {
                localMap.Clear();
                overflowPagePool.Dispose();
                base.Dispose();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPhysicalAddress(long logicalAddress)
        {
            // Offset within page
            var offset = (int)(logicalAddress & ((1L << LogPageSizeBits) - 1));

            // Index of page within the circular buffer
            var pageIndex = (int)((logicalAddress >> LogPageSizeBits) & (BufferSize - 1));
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
                    asyncResult, device, objectLogDevice);
        }

        protected override void WriteAsyncToDevice<TContext>
            (long startPage, long flushPage, int pageSize, DeviceIOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice, long[] localSegmentOffsets, long fuzzyStartLogicalAddress)
        {
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
        }

        internal void ClearPage(long page, int offset)
        {
            // This is called during recovery, not as part of normal operations, so there is no need to walk pages starting at offset to Free() ObjectIds
            NativeMemory.Clear((byte*)pagePointers[page] + offset, (nuint)(adjustedPageSize - offset));
        }

        internal void FreePage(long page)
        {
            ClearPage(page, 0);

            // Close segments
            var thisCloseSegment = page >> (LogSegmentSizeBits - LogPageSizeBits);
            var nextCloseSegment = (page + 1) >> (LogSegmentSizeBits - LogPageSizeBits);

            if (thisCloseSegment != nextCloseSegment)
            {
                // We are clearing the last page in current segment
                segmentOffsets[thisCloseSegment % SegmentBufferSize] = 0;
            }

            // If all pages are being used (i.e. EmptyPageCount == 0), nothing to re-utilize by adding
            // to overflow pool.
            if (EmptyPageCount > 0)
                ReturnPage((int)(page % BufferSize));
        }

        private void WriteAsync<TContext>(long flushPage, ulong alignedDestinationAddress, uint numBytesToWrite,
                        DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                        IDevice device, IDevice objlogDevice, long intendedDestinationPage = -1, long[] localSegmentOffsets = null, long fuzzyStartLogicalAddress = long.MaxValue)
        {
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
                src = new AllocatorRecord<TKey, TValue>[values[flushPage % BufferSize].Length];
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
                    ref var record = ref Unsafe.AsRef<AllocatorRecord<TKey, TValue>>(recordPtr);
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
                                valueSerializer.Serialize(ref src[i].value);

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
                    if (endPosition > ObjectBlockSize || i == (end / RecordSize) - 1)
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
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncReadPageWithObjectsCallback)} error: {{errorCode}}", errorCode);

            var result = (PageAsyncReadResult<TContext>)context;

            AllocatorRecord<TKey, TValue>[] src;

            // We are reading into a frame
            if (result.frame != null)
            {
                var frame = (GenericFrame<TKey, TValue>)result.frame;
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
            GetObjectInfo(result.freeBuffer1.GetValidPointer(), ref result.untilPtr, result.maxPtr, ObjectBlockSize, out long startptr, out long alignedLength);

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
        protected override void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext<TKey, TValue> context, SectorAlignedMemory result = default)
        {
            var fileOffset = (ulong)(AlignedPageSizeBytes * (fromLogical >> LogPageSizeBits) + (fromLogical & PageSizeMask));
            var alignedFileOffset = (ulong)(((long)fileOffset / sectorSize) * sectorSize);

            var alignedReadLength = (uint)((long)fileOffset + numBytes - (long)alignedFileOffset);
            alignedReadLength = (uint)((alignedReadLength + (sectorSize - 1)) & ~(sectorSize - 1));

            var record = bufferPool.Get((int)alignedReadLength);
            record.valid_offset = (int)(fileOffset - alignedFileOffset);
            record.available_bytes = (int)(alignedReadLength - (fileOffset - alignedFileOffset));
            record.required_bytes = numBytes;

            var asyncResult = default(AsyncGetFromDiskResult<AsyncIOContext<TKey, TValue>>);
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
                                        GenericFrame<TKey, TValue> frame,
                                        out CountdownEvent completed,
                                        long devicePageOffset = 0,
                                        IDevice device = null, IDevice objectLogDevice = null)
        {
            var usedDevice = device ?? this.device;
            IDevice usedObjlogDevice = objectLogDevice;

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

                ReadAsync(offsetInFile, pageIndex, readLength, callback, asyncResult, usedDevice, usedObjlogDevice);
            }
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
        public void Deserialize(byte* raw, long ptr, long untilptr, AllocatorRecord<TKey, TValue>[] src, Stream stream)
        {
            long streamStartPos = stream.Position;
            long start_addr = -1;
            int start_offset = -1, end_offset = -1;

            var keySerializer = KeyHasObjects() ? _storeFunctions.BeginDeserializeKey(stream) : null;
            var valueSerializer = ValueHasObjects() ? _storeFunctions.BeginDeserializeValue(stream) : null;

            while (ptr < untilptr)
            {
                ref var record = ref Unsafe.AsRef<AllocatorRecord<TKey, TValue>>(raw + ptr);
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
                using var iter = new MemoryPageScanIterator<TKey, TValue>(src, start_offset, end_offset, -1, RecordSize);
                OnDeserializationObserver.OnNext(iter);
            }
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
            var minObjAddress = long.MaxValue;
            var maxObjAddress = long.MinValue;
            var done = false;

            while (!done && (ptr < untilptr))
            {
                ref var record = ref Unsafe.AsRef<AllocatorRecord<TKey, TValue>>(raw + ptr);

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
        }

        /// <summary>Retrieve objects from object log</summary>
        internal bool RetrievedFullRecord(byte* record, ref AsyncIOContext<SpanByte, IHeapObject> ctx)
        {
            if (!KeyHasObjects())
                ctx.key = Unsafe.AsRef<AllocatorRecord<TKey, TValue>>(record).key;
            if (!ValueHasObjects())
                ctx.value = Unsafe.AsRef<AllocatorRecord<TKey, TValue>>(record).value;

            if (!(KeyHasObjects() || ValueHasObjects()))
                return true;

            if (ctx.objBuffer == null)
            {
                // Issue IO for objects
                long startAddress = -1;
                long endAddress = -1;
                if (KeyHasObjects())
                {
                    var x = GetKeyAddressInfo((long)record);
                    startAddress = x->Address;
                    endAddress = x->Address + x->Size;
                }

                if (ValueHasObjects() && !GetInfoFromBytePointer(record).Tombstone)
                {
                    var x = GetValueAddressInfo((long)record);
                    if (startAddress == -1)
                        startAddress = x->Address;
                    endAddress = x->Address + x->Size;
                }

                // We are limited to a 2GB size per key-value
                if (endAddress - startAddress > int.MaxValue)
                    throw new TsavoriteException("Size of key-value exceeds max of 2GB: " + (endAddress - startAddress));

                if (startAddress < 0)
                    startAddress = 0;

                AsyncGetFromDisk(startAddress, (int)(endAddress - startAddress), ctx, ctx.record);
                return false;
            }

            // Parse the key and value objects
            var ms = new MemoryStream(ctx.objBuffer.buffer);
            _ = ms.Seek(ctx.objBuffer.offset + ctx.objBuffer.valid_offset, SeekOrigin.Begin);

            if (KeyHasObjects())
            {
                var keySerializer = _storeFunctions.BeginDeserializeKey(ms);
                keySerializer.Deserialize(out ctx.key);
                keySerializer.EndDeserialize();
            }

            if (ValueHasObjects() && !GetInfoFromBytePointer(record).Tombstone)
            {
                var valueSerializer = _storeFunctions.BeginDeserializeValue(ms);
                valueSerializer.Deserialize(out ctx.value);
                valueSerializer.EndDeserialize();
            }

            ctx.objBuffer.Return();
            return true;
        }

        internal static ref SpanByte GetContextRecordKey(ref AsyncIOContext<SpanByte, IHeapObject> ctx) => ref GetKey((long)ctx.record.GetValidPointer());

        internal IHeapContainer<SpanByte> GetKeyContainer(ref SpanByte key) => new SpanByteHeapContainer(ref key, bufferPool);
        #endregion

        public long[] GetSegmentOffsets() => segmentOffsets;

        internal void PopulatePage(byte* src, int required_bytes, long destinationPage)
            => PopulatePage(src, required_bytes, ref values[destinationPage % BufferSize]);

        internal void PopulatePageFrame(byte* src, int required_bytes, AllocatorRecord<TKey, TValue>[] frame)
            => PopulatePage(src, required_bytes, ref frame);

        internal void PopulatePage(byte* src, int required_bytes, ref AllocatorRecord<TKey, TValue>[] destinationPage)
        {
            fixed (RecordInfo* pin = &destinationPage[0].info)
            {
                Debug.Assert(required_bytes <= RecordSize * destinationPage.Length);
                Buffer.MemoryCopy(src, Unsafe.AsPointer(ref destinationPage[0]), required_bytes, required_bytes);
            }
        }

        /// <summary>
        /// Iterator interface for scanning Tsavorite log
        /// </summary>
        /// <returns></returns>
        public override ITsavoriteScanIterator<SpanByte, IHeapObject> Scan(TsavoriteKV<SpanByte, IHeapObject, TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode, bool includeSealedRecords)
            => new ObjectScanIterator<TStoreFunctions>(store, this, beginAddress, endAddress, scanBufferingMode, includeSealedRecords, epoch);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<SpanByte, IHeapObject, TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, ref TScanFunctions scanFunctions, ScanBufferingMode scanBufferingMode)
        {
            using ObjectScanIterator<TStoreFunctions> iter = new(store, this, beginAddress, endAddress, scanBufferingMode, false, epoch, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<SpanByte, IHeapObject, TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                ScanCursorState<SpanByte, IHeapObject> scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor)
        {
            using ObjectScanIterator<TStoreFunctions> iter = new(store, this, cursor, endAddress, ScanBufferingMode.SinglePageBuffering, false, epoch, logger: logger);
            return ScanLookup<long, long, TScanFunctions, ObjectScanIterator<TStoreFunctions>>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<SpanByte, IHeapObject, TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                ref SpanByte key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using ObjectScanIterator<TStoreFunctions> iter = new(store, this, beginAddress, epoch, logger: logger);
            return IterateKeyVersionsImpl(store, ref key, beginAddress, ref scanFunctions, iter);
        }

        private void ComputeScanBoundaries(long beginAddress, long endAddress, out long pageStartAddress, out int start, out int end)
        {
            pageStartAddress = beginAddress & ~PageSizeMask;
            start = (int)(beginAddress & PageSizeMask) / RecordSize;
            var count = (int)(endAddress - beginAddress) / RecordSize;
            end = start + count;
        }

        /// <inheritdoc />
        internal override void EvictPage(long page)
        {
            if (OnEvictionObserver is not null)
            {
                var beginAddress = page << LogPageSizeBits;
                var endAddress = (page + 1) << LogPageSizeBits;
                ComputeScanBoundaries(beginAddress, endAddress, out var pageStartAddress, out var start, out var end);
                using var iter = new MemoryPageScanIterator<TKey, TValue>(values[(int)(page % BufferSize)], start, end, pageStartAddress, RecordSize);
                OnEvictionObserver?.OnNext(iter);
            }

            FreePage(page);
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator<TKey, TValue>> observer)
        {
            var page = (beginAddress >> LogPageSizeBits) % BufferSize;
            ComputeScanBoundaries(beginAddress, endAddress, out var pageStartAddress, out var start, out var end);
            using var iter = new MemoryPageScanIterator<TKey, TValue>(values[page], start, end, pageStartAddress, RecordSize);
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
        }

        internal override void AsyncFlushDeltaToDevice(long startAddress, long endAddress, long prevEndAddress, long version, DeltaLog deltaLog, out SemaphoreSlim completedSemaphore, int throttleCheckpointFlushDelayMs)
        {
            throw new TsavoriteException("Incremental snapshots not supported with generic allocator");
        }
    }
}