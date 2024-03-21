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

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct Record<Key, Value>
    {
        public RecordInfo info;
        public Key key;
        public Value value;

        public override string ToString()
        {
            var keyString = key?.ToString() ?? "null";
            if (keyString.Length > 20)
                keyString = keyString.Substring(0, 20) + "...";
            var valueString = value?.ToString() ?? "null"; ;
            if (valueString.Length > 20)
                valueString = valueString.Substring(0, 20) + "...";
            return $"{keyString} | {valueString} | {info}";
        }
    }

    internal sealed unsafe class GenericAllocator<Key, Value> : AllocatorBase<Key, Value>
    {
        // Circular buffer definition
        internal Record<Key, Value>[][] values;

        // Object log related variables
        private readonly IDevice objectLogDevice;
        // Size of object chunks being written to storage
        private readonly int ObjectBlockSize = 100 * (1 << 20);
        // Tail offsets per segment, in object log
        public readonly long[] segmentOffsets;
        // Record sizes
        private readonly SerializerSettings<Key, Value> SerializerSettings;
        private readonly bool keyBlittable = Utility.IsBlittable<Key>();
        private readonly bool valueBlittable = Utility.IsBlittable<Value>();


        // We do not support variable-length keys in GenericAllocator
        internal static int KeySize => Unsafe.SizeOf<Key>();
        internal static int ValueSize => Unsafe.SizeOf<Value>();
        internal static int RecordSize => Unsafe.SizeOf<Record<Key, Value>>();

        private readonly OverflowPool<Record<Key, Value>[]> overflowPagePool;

        public GenericAllocator(LogSettings settings, SerializerSettings<Key, Value> serializerSettings, ITsavoriteEqualityComparer<Key> comparer,
                Action<long, long> evictCallback = null, LightEpoch epoch = null, Action<CommitInfo> flushCallback = null, ILogger logger = null)
            : base(settings, comparer, evictCallback, epoch, flushCallback, logger)
        {
            overflowPagePool = new OverflowPool<Record<Key, Value>[]>(4);

            if (settings.ObjectLogDevice == null)
            {
                throw new TsavoriteException("LogSettings.ObjectLogDevice needs to be specified (e.g., use Devices.CreateLogDevice, AzureStorageDevice, or NullDevice)");
            }

            if (typeof(Key) == typeof(SpanByte))
                throw new TsavoriteException("SpanByte Keys cannot be mixed with object Values");
            if (typeof(Value) == typeof(SpanByte))
                throw new TsavoriteException("SpanByte Values cannot be mixed with object Keys");

            SerializerSettings = serializerSettings ?? new SerializerSettings<Key, Value>();

            if ((!keyBlittable) && (settings.LogDevice as NullDevice == null) && ((SerializerSettings == null) || (SerializerSettings.keySerializer == null)))
            {
#if DEBUG
                if (typeof(Key) != typeof(byte[]) && typeof(Key) != typeof(string))
                    Debug.WriteLine("Key is not blittable, but no serializer specified via SerializerSettings. Using (slow) DataContractSerializer as default.");
#endif
                SerializerSettings.keySerializer = ObjectSerializer.Get<Key>();
            }

            if ((!valueBlittable) && (settings.LogDevice as NullDevice == null) && ((SerializerSettings == null) || (SerializerSettings.valueSerializer == null)))
            {
#if DEBUG
                if (typeof(Value) != typeof(byte[]) && typeof(Value) != typeof(string))
                    Debug.WriteLine("Value is not blittable, but no serializer specified via SerializerSettings. Using (slow) DataContractSerializer as default.");
#endif
                SerializerSettings.valueSerializer = ObjectSerializer.Get<Value>();
            }

            values = new Record<Key, Value>[BufferSize][];
            segmentOffsets = new long[SegmentBufferSize];

            objectLogDevice = settings.ObjectLogDevice;

            if ((settings.LogDevice as NullDevice == null) && (KeyHasObjects() || ValueHasObjects()))
            {
                if (objectLogDevice == null)
                    throw new TsavoriteException("Objects in key/value, but object log not provided during creation of Tsavorite instance");
                if (objectLogDevice.SegmentSize != -1)
                    throw new TsavoriteException("Object log device should not have fixed segment size. Set preallocateFile to false when calling CreateLogDevice for object log");
            }
        }

        internal override int OverflowPageCount => overflowPagePool.Count;

        public override void Reset()
        {
            base.Reset();
            objectLogDevice.Reset();
            for (int index = 0; index < BufferSize; index++)
            {
                ReturnPage(index);
            }

            Array.Clear(segmentOffsets, 0, segmentOffsets.Length);
            Initialize();
        }

        void ReturnPage(int index)
        {
            Debug.Assert(index < BufferSize);
            if (values[index] != default)
            {
                overflowPagePool.TryAdd(values[index]);
                values[index] = default;
                Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        public override void Initialize()
        {
            Initialize(RecordSize);
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
                return (page << LogPageSizeBits) + RecordSize;

            return page << LogPageSizeBits;
        }

        public override ref RecordInfo GetInfo(long physicalAddress)
        {
            // Offset within page
            int offset = (int)(physicalAddress & PageSizeMask);

            // Index of page within the circular buffer
            int pageIndex = (int)((physicalAddress >> LogPageSizeBits) & BufferSizeMask);

            return ref values[pageIndex][offset / RecordSize].info;
        }

        public override ref RecordInfo GetInfoFromBytePointer(byte* ptr)
        {
            return ref Unsafe.AsRef<Record<Key, Value>>(ptr).info;
        }

        public override ref Key GetKey(long physicalAddress)
        {
            // Offset within page
            int offset = (int)(physicalAddress & PageSizeMask);

            // Index of page within the circular buffer
            int pageIndex = (int)((physicalAddress >> LogPageSizeBits) & BufferSizeMask);

            return ref values[pageIndex][offset / RecordSize].key;
        }

        public override ref Value GetValue(long physicalAddress)
        {
            // Offset within page
            int offset = (int)(physicalAddress & PageSizeMask);

            // Index of page within the circular buffer
            int pageIndex = (int)((physicalAddress >> LogPageSizeBits) & BufferSizeMask);

            return ref values[pageIndex][offset / RecordSize].value;
        }

        public override (int actualSize, int allocatedSize) GetRecordSize(long physicalAddress)
        {
            return (RecordSize, RecordSize);
        }

        public override int GetValueLength(ref Value value) => ValueSize;

        public override (int actualSize, int allocatedSize, int keySize) GetRMWCopyDestinationRecordSize<Input, TsavoriteSession>(ref Key key, ref Input input, ref Value value, ref RecordInfo recordInfo, TsavoriteSession tsavoriteSession)
        {
            return (RecordSize, RecordSize, KeySize);
        }

        public override int GetAverageRecordSize()
        {
            return RecordSize;
        }

        public override int GetFixedRecordSize() => RecordSize;

        public override (int actualSize, int allocatedSize, int keySize) GetRMWInitialRecordSize<Input, TsavoriteSession>(ref Key key, ref Input input, TsavoriteSession tsavoriteSession)
        {
            return (RecordSize, RecordSize, KeySize);
        }

        public override (int actualSize, int allocatedSize, int keySize) GetRecordSize(ref Key key, ref Value value)
        {
            return (RecordSize, RecordSize, KeySize);
        }

        internal override bool TryComplete()
        {
            var b1 = objectLogDevice.TryComplete();
            var b2 = base.TryComplete();
            return b1 || b2;
        }

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            if (values != null)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    values[i] = null;
                }
                values = null;
            }
            overflowPagePool.Dispose();
            base.Dispose();
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
            values = null;
        }

        public override AddressInfo* GetKeyAddressInfo(long physicalAddress)
        {
            return (AddressInfo*)Unsafe.AsPointer(ref Unsafe.AsRef<Record<Key, Value>>((byte*)physicalAddress).key);
        }

        public override AddressInfo* GetValueAddressInfo(long physicalAddress)
        {
            return (AddressInfo*)Unsafe.AsPointer(ref Unsafe.AsRef<Record<Key, Value>>((byte*)physicalAddress).value);
        }

        /// <summary>
        /// Allocate memory page, pinned in memory, and in sector aligned form, if possible
        /// </summary>
        /// <param name="index"></param>
        internal override void AllocatePage(int index)
        {
            values[index] = AllocatePage();
        }

        internal Record<Key, Value>[] AllocatePage()
        {
            Interlocked.Increment(ref AllocatedPageCount);

            if (overflowPagePool.TryGet(out var item))
                return item;

            Record<Key, Value>[] tmp;
            if (PageSize % RecordSize == 0)
                tmp = new Record<Key, Value>[PageSize / RecordSize];
            else
                tmp = new Record<Key, Value>[1 + (PageSize / RecordSize)];
            Array.Clear(tmp, 0, tmp.Length);
            return tmp;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long SnapToLogicalAddressBoundary(ref long logicalAddress)
        {
            return logicalAddress = ((logicalAddress - Constants.kFirstValidAddress) / RecordSize) * RecordSize + Constants.kFirstValidAddress;
        }

        public override long GetPhysicalAddress(long logicalAddress)
        {
            return logicalAddress;
        }

        internal override bool IsAllocated(int pageIndex)
        {
            return values[pageIndex] != null;
        }

        protected override void TruncateUntilAddress(long toAddress)
        {
            base.TruncateUntilAddress(toAddress);
            objectLogDevice.TruncateUntilSegment((int)(toAddress >> LogSegmentSizeBits));
        }

        protected override void TruncateUntilAddressBlocking(long toAddress)
        {
            base.TruncateUntilAddressBlocking(toAddress);
            objectLogDevice.TruncateUntilSegment((int)(toAddress >> LogSegmentSizeBits));
        }

        protected override void RemoveSegment(int segment)
        {
            base.RemoveSegment(segment);
            objectLogDevice.RemoveSegment(segment);
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

            bool epochTaken = false;
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

        internal override void ClearPage(long page, int offset)
        {
            Array.Clear(values[page % BufferSize], offset / RecordSize, values[page % BufferSize].Length - offset / RecordSize);
        }

        internal override void FreePage(long page)
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
            if (device as NullDevice != null)
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
            if (localSegmentOffsets == null)
                localSegmentOffsets = segmentOffsets;

            // This is the in-memory buffer page to be written
            var src = values[flushPage % BufferSize];

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
            List<long> addr = new List<long>();
            asyncResult.freeBuffer1 = buffer;

            // Object keys and values are serialized into this MemoryStream.
            MemoryStream ms = new();
            IObjectSerializer<Key> keySerializer = null;
            IObjectSerializer<Value> valueSerializer = null;

            if (KeyHasObjects())
            {
                keySerializer = SerializerSettings.keySerializer();
                keySerializer.BeginSerialize(ms);
            }
            if (ValueHasObjects())
            {
                valueSerializer = SerializerSettings.valueSerializer();
                valueSerializer.BeginSerialize(ms);
            }

            // Track the size to be written to the object log.
            long endPosition = 0;

            for (int i = start / RecordSize; i < end / RecordSize; i++)
            {
                if (!src[i].info.Invalid)
                {
                    // Calculate the logical address of the 'values' page currently being written.
                    var address = (flushPage << LogPageSizeBits) + i * RecordSize;

                    // Do not write v+1 records (e.g. during a checkpoint)
                    if (address < fuzzyStartLogicalAddress || !src[i].info.IsInNewVersion)
                    {
                        if (KeyHasObjects())
                        {
                            long pos = ms.Position;
                            keySerializer.Serialize(ref src[i].key);

                            // Store the key address into the 'buffer' AddressInfo image as an offset into 'ms'.
                            var key_address = GetKeyAddressInfo((long)(buffer.aligned_pointer + i * RecordSize));
                            key_address->Address = pos;
                            key_address->Size = (int)(ms.Position - pos);
                            addr.Add((long)key_address);
                            endPosition = pos + key_address->Size;
                        }

                        if (ValueHasObjects() && !src[i].info.Tombstone)
                        {
                            long pos = ms.Position;
                            valueSerializer.Serialize(ref src[i].value);

                            // Store the value address into the 'buffer' AddressInfo image as an offset into 'ms'.
                            var value_address = GetValueAddressInfo((long)(buffer.aligned_pointer + i * RecordSize));
                            value_address->Address = pos;
                            value_address->Size = (int)(ms.Position - pos);
                            addr.Add((long)value_address);
                            endPosition = pos + value_address->Size;
                        }
                    }
                    else
                    {
                        // Mark v+1 records as invalid to avoid deserializing them on recovery
                        ref var record = ref Unsafe.AsRef<Record<Key, Value>>(buffer.aligned_pointer + i * RecordSize);
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
                        asyncResult.done.WaitOne();
                        _objBuffer.Return();
                    }
                    else
                    {
                        // We have written all records in this 'values' "page".
                        if (memoryStreamTotalLength > 0)
                        {
                            // Increment the count because we need to write both page and object cache.
                            Interlocked.Increment(ref asyncResult.count);

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

        private void AsyncReadPageCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                logger?.LogError($"AsyncReadPageCallback error: {errorCode}");
            }

            // Set the page status to flushed
            var result = (PageAsyncReadResult<Empty>)context;

            result.handle.Signal();
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
            {
                logger?.LogError($"AsyncFlushPartialObjectLogCallback error: {errorCode}");
            }

            // Set the page status to flushed
            PageAsyncFlushResult<TContext> result = (PageAsyncFlushResult<TContext>)context;
            result.done.Set();
        }

        private void AsyncReadPageWithObjectsCallback<TContext>(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                logger?.LogError($"AsyncReadPageWithObjectsCallback error: {errorCode}");
            }

            PageAsyncReadResult<TContext> result = (PageAsyncReadResult<TContext>)context;

            Record<Key, Value>[] src;

            // We are reading into a frame
            if (result.frame != null)
            {
                var frame = (GenericFrame<Key, Value>)result.frame;
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
        protected override void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext<Key, Value> context, SectorAlignedMemory result = default)
        {
            ulong fileOffset = (ulong)(AlignedPageSizeBytes * (fromLogical >> LogPageSizeBits) + (fromLogical & PageSizeMask));
            ulong alignedFileOffset = (ulong)(((long)fileOffset / sectorSize) * sectorSize);

            uint alignedReadLength = (uint)((long)fileOffset + numBytes - (long)alignedFileOffset);
            alignedReadLength = (uint)((alignedReadLength + (sectorSize - 1)) & ~(sectorSize - 1));

            var record = bufferPool.Get((int)alignedReadLength);
            record.valid_offset = (int)(fileOffset - alignedFileOffset);
            record.available_bytes = (int)(alignedReadLength - (fileOffset - alignedFileOffset));
            record.required_bytes = numBytes;

            var asyncResult = default(AsyncGetFromDiskResult<AsyncIOContext<Key, Value>>);
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
                                        GenericFrame<Key, Value> frame,
                                        out CountdownEvent completed,
                                        long devicePageOffset = 0,
                                        IDevice device = null, IDevice objectLogDevice = null)
        {
            var usedDevice = device;
            IDevice usedObjlogDevice = objectLogDevice;

            if (device == null)
            {
                usedDevice = this.device;
            }

            completed = new CountdownEvent(numPages);
            for (long readPage = readPageStart; readPage < (readPageStart + numPages); readPage++)
            {
                int pageIndex = (int)(readPage % frame.frameSize);
                if (frame.GetPage(pageIndex) == null)
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
                    maxPtr = PageSize,
                    frame = frame,
                };

                ulong offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);
                uint readLength = (uint)AlignedPageSizeBytes;
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
        public void Deserialize(byte* raw, long ptr, long untilptr, Record<Key, Value>[] src, Stream stream)
        {
            IObjectSerializer<Key> keySerializer = null;
            IObjectSerializer<Value> valueSerializer = null;

            long streamStartPos = stream.Position;
            long start_addr = -1;
            if (KeyHasObjects())
            {
                keySerializer = SerializerSettings.keySerializer();
                keySerializer.BeginDeserialize(stream);
            }
            if (ValueHasObjects())
            {
                valueSerializer = SerializerSettings.valueSerializer();
                valueSerializer.BeginDeserialize(stream);
            }

            while (ptr < untilptr)
            {
                ref Record<Key, Value> record = ref Unsafe.AsRef<Record<Key, Value>>(raw + ptr);
                src[ptr / RecordSize].info = record.info;

                if (!record.info.Invalid)
                {
                    if (KeyHasObjects())
                    {
                        var key_addr = GetKeyAddressInfo((long)raw + ptr);
                        if (start_addr == -1) start_addr = key_addr->Address & ~((long)sectorSize - 1);
                        if (stream.Position != streamStartPos + key_addr->Address - start_addr)
                        {
                            stream.Seek(streamStartPos + key_addr->Address - start_addr, SeekOrigin.Begin);
                        }

                        keySerializer.Deserialize(out src[ptr / RecordSize].key);
                    }
                    else
                    {
                        src[ptr / RecordSize].key = record.key;
                    }

                    if (!record.info.Tombstone)
                    {
                        if (ValueHasObjects())
                        {
                            var value_addr = GetValueAddressInfo((long)raw + ptr);
                            if (start_addr == -1) start_addr = value_addr->Address & ~((long)sectorSize - 1);
                            if (stream.Position != streamStartPos + value_addr->Address - start_addr)
                            {
                                stream.Seek(streamStartPos + value_addr->Address - start_addr, SeekOrigin.Begin);
                            }

                            valueSerializer.Deserialize(out src[ptr / RecordSize].value);
                        }
                        else
                        {
                            src[ptr / RecordSize].value = record.value;
                        }
                    }
                }
                ptr += GetRecordSize(ptr).Item2;
            }
            if (KeyHasObjects())
            {
                keySerializer.EndDeserialize();
            }
            if (ValueHasObjects())
            {
                valueSerializer.EndDeserialize();
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
            long minObjAddress = long.MaxValue;
            long maxObjAddress = long.MinValue;
            bool done = false;

            while (!done && (ptr < untilptr))
            {
                ref Record<Key, Value> record = ref Unsafe.AsRef<Record<Key, Value>>(raw + ptr);

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
                ptr += GetRecordSize(ptr).Item2;
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

        /// <summary>
        /// Retrieve objects from object log
        /// </summary>
        /// <param name="record"></param>
        /// <param name="ctx"></param>
        /// <returns></returns>
        protected override bool RetrievedFullRecord(byte* record, ref AsyncIOContext<Key, Value> ctx)
        {
            if (!KeyHasObjects())
            {
                ctx.key = Unsafe.AsRef<Record<Key, Value>>(record).key;
            }
            if (!ValueHasObjects())
            {
                ctx.value = Unsafe.AsRef<Record<Key, Value>>(record).value;
            }

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
            MemoryStream ms = new MemoryStream(ctx.objBuffer.buffer);
            ms.Seek(ctx.objBuffer.offset + ctx.objBuffer.valid_offset, SeekOrigin.Begin);

            if (KeyHasObjects())
            {
                var keySerializer = SerializerSettings.keySerializer();
                keySerializer.BeginDeserialize(ms);
                keySerializer.Deserialize(out ctx.key);
                keySerializer.EndDeserialize();
            }

            if (ValueHasObjects() && !GetInfoFromBytePointer(record).Tombstone)
            {
                var valueSerializer = SerializerSettings.valueSerializer();
                valueSerializer.BeginDeserialize(ms);
                valueSerializer.Deserialize(out ctx.value);
                valueSerializer.EndDeserialize();
            }

            ctx.objBuffer.Return();
            return true;
        }

        /// <summary>
        /// Whether KVS has keys to serialize/deserialize
        /// </summary>
        /// <returns></returns>
        public override bool KeyHasObjects()
        {
            return SerializerSettings.keySerializer != null;
        }

        /// <summary>
        /// Whether KVS has values to serialize/deserialize
        /// </summary>
        /// <returns></returns>
        public override bool ValueHasObjects()
        {
            return SerializerSettings.valueSerializer != null;
        }
        #endregion

        public override IHeapContainer<Key> GetKeyContainer(ref Key key) => new StandardHeapContainer<Key>(ref key);
        public override IHeapContainer<Value> GetValueContainer(ref Value value) => new StandardHeapContainer<Value>(ref value);

        public override long[] GetSegmentOffsets()
        {
            return segmentOffsets;
        }

        internal override void PopulatePage(byte* src, int required_bytes, long destinationPage)
        {
            PopulatePage(src, required_bytes, ref values[destinationPage % BufferSize]);
        }

        internal void PopulatePageFrame(byte* src, int required_bytes, Record<Key, Value>[] frame)
        {
            PopulatePage(src, required_bytes, ref frame);
        }

        internal void PopulatePage(byte* src, int required_bytes, ref Record<Key, Value>[] destinationPage)
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
        public override ITsavoriteScanIterator<Key, Value> Scan(TsavoriteKV<Key, Value> store, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode)
            => new GenericScanIterator<Key, Value>(store, this, beginAddress, endAddress, scanBufferingMode, epoch);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<Key, Value> store, long beginAddress, long endAddress, ref TScanFunctions scanFunctions, ScanBufferingMode scanBufferingMode)
        {
            using GenericScanIterator<Key, Value> iter = new(store, this, beginAddress, endAddress, scanBufferingMode, epoch, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<Key, Value> store, ScanCursorState<Key, Value> scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor)
        {
            using GenericScanIterator<Key, Value> iter = new(store, this, cursor, endAddress, ScanBufferingMode.SinglePageBuffering, epoch, logger: logger);
            return ScanLookup<long, long, TScanFunctions, GenericScanIterator<Key, Value>>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<Key, Value> store, ref Key key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using GenericScanIterator<Key, Value> iter = new(store, this, store.comparer, beginAddress, epoch, logger: logger);
            return IterateKeyVersionsImpl(store, ref key, beginAddress, ref scanFunctions, iter);
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator<Key, Value>> observer)
        {
            var page = (beginAddress >> LogPageSizeBits) % BufferSize;
            long pageStartAddress = beginAddress & ~PageSizeMask;
            int start = (int)(beginAddress & PageSizeMask) / RecordSize;
            int count = (int)(endAddress - beginAddress) / RecordSize;
            int end = start + count;
            using var iter = new MemoryPageScanIterator<Key, Value>(values[page], start, end, pageStartAddress, RecordSize);
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