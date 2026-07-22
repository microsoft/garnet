// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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
        internal ObjectPage[] objectPages;

        /// <summary>The position information for the next write to the object log.</summary>
        ObjectLogFilePositionInfo objectLogTail;

        /// <summary>
        /// We use the LastIssued here because we don't want <see cref="OnPagesMarkedReadOnlyWorker"/> to wait for IO to complete which is when
        /// FlushedUntilAddress is updated. Instead, LastIssuedFlushedUntilAddress is the proxy for it: it's updated with the flushEndAddress
        /// after the flush has been issued, without waiting for it to complete.
        /// </summary>
        long LastIssuedFlushedUntilAddress;

        /// <summary>
        /// Dynamically extended Flush end address, used by <see cref="OnPagesMarkedReadOnlyWorker"/>
        /// </summary>
        long OngoingFlushedUntilAddress;

        /// <summary>
        /// If the "noFlush" option on <see cref="AllocatorBase{TStoreFunctions, TAllocator}.ShiftReadOnlyAddress(long, bool)"/> is true, we won't try to flush anything below that.
        /// </summary>
        long NoFlushUntilAddress;

        /// <summary>The lowest object-log segment in use; adjusted with Truncate to remain consistent with BeginAddress.</summary>
        internal int lowestObjectLogSegmentInUse = 0;

        // Default to max sizes so testing a size as "greater than" will always be false
        readonly int maxInlineKeySize;
        readonly int maxInlineValueSize;

        readonly int numberOfFlushBuffers;
        readonly int numberOfDeserializationBuffers;

        private readonly IDevice objectLogDevice;

        /// <summary>The free pages of the log</summary>
        private readonly OverflowPool<PageUnit<ObjectPage>> freePagePool;

        /// <summary>Segment size</summary>
        private long ObjectLogSegmentSize;

        /// <inheritdoc/>
        public override string ToString() => BaseToString($" (LI {LastIssuedFlushedUntilAddress}, OG {OngoingFlushedUntilAddress}, No {NoFlushUntilAddress})");

        public ObjectAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, ObjectAllocator<TStoreFunctions>> wrapperCreator)
            : base(settings, storeFunctions, wrapperCreator, settings.logger, transientObjectIdMap: new ObjectIdMap())
        {
            objectLogDevice = settings.LogSettings.ObjectLogDevice;

            maxInlineKeySize = settings.LogSettings.MaxInlineKeySize;
            maxInlineValueSize = settings.LogSettings.MaxInlineValueSize;

            ObjectLogSegmentSize = 1L << settings.LogSettings.ObjectLogSegmentSizeBits;

            freePagePool = new OverflowPool<PageUnit<ObjectPage>>(4, static p => { });
            pageHeaderSize = PageHeader.Size;

            if (settings.LogSettings.NumberOfFlushBuffers < LogSettings.kMinFlushBuffers || settings.LogSettings.NumberOfFlushBuffers > LogSettings.kMaxFlushBuffers || !IsPowerOfTwo(settings.LogSettings.NumberOfFlushBuffers))
                throw new TsavoriteException($"{nameof(settings.LogSettings.NumberOfFlushBuffers)} must be between {LogSettings.kMinFlushBuffers} and {LogSettings.kMaxFlushBuffers - 1} and a power of 2");
            numberOfFlushBuffers = settings.LogSettings.NumberOfFlushBuffers;

            if (settings.LogSettings.NumberOfDeserializationBuffers < LogSettings.kMinDeserializationBuffers || settings.LogSettings.NumberOfDeserializationBuffers > LogSettings.kMaxDeserializationBuffers || !IsPowerOfTwo(settings.LogSettings.NumberOfDeserializationBuffers))
                throw new TsavoriteException($"{nameof(settings.LogSettings.NumberOfDeserializationBuffers)} must be between {LogSettings.kMinDeserializationBuffers} and {LogSettings.kMaxDeserializationBuffers - 1} and a power of 2");
            numberOfDeserializationBuffers = settings.LogSettings.NumberOfDeserializationBuffers;

            if (settings.LogSettings.ObjectLogSegmentSizeBits is < LogSettings.kMinObjectLogSegmentSizeBits or > LogSettings.kMaxSegmentSizeBits)
                throw new TsavoriteException($"{nameof(settings.LogSettings.ObjectLogSegmentSizeBits)} must be between {LogSettings.kMinObjectLogSegmentSizeBits} and {LogSettings.kMaxSegmentSizeBits}");
            objectLogTail = new(0, settings.LogSettings.ObjectLogSegmentSizeBits);

            objectPages = new ObjectPage[BufferSize];
            for (var ii = 0; ii < BufferSize; ii++)
                objectPages[ii] = new();
        }

        /// <summary>Initialize allocator</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        protected internal override void Initialize()
        {
            base.Initialize();
            LastIssuedFlushedUntilAddress = FlushedUntilAddress;
            OngoingFlushedUntilAddress = 0;
            NoFlushUntilAddress = 0;
        }

        internal int OverflowPageCount => freePagePool.Count;

        /// <inheritdoc />
        protected override void FreeAllAllocatedPages()
        {
            for (var index = 0; index < BufferSize; index++)
            {
                if (IsAllocated(index))
                    FreePage(index);
            }
        }

        /// <summary>Allocate memory page, pinned in memory, and in sector aligned form, if possible</summary>
        internal void AllocatePage(int index)
        {
            IncrementAllocatedPageCount();

            if (freePagePool.TryGet(out var item))
            {
                pageArrays[index] = item.array;
                pagePointers[index] = item.pointer;
                objectPages[index] = item.value;
            }
            else
            {
                // No free pages are available so allocate new
                AllocatePinnedPageArray(index);
                objectPages[index] = new();
            }
            PageHeader.Initialize(pagePointers[index]);
        }

        void ReturnPage(int index)
        {
            Debug.Assert(index < BufferSize);
            if (pagePointers[index] != default)
            {
                var enqueued = freePagePool.TryAdd(new()
                {
                    array = pageArrays[index],
                    pointer = pagePointers[index],
                    value = objectPages[index]
                });

                // We only need to clear the page if it's enqueued; otherwise we don't reuse the page, so can save the time
                if (enqueued)
                    ClearPage(index, 0);
                else
                    objectPages[index].Clear();
                pageArrays[index] = default;
                pagePointers[index] = default;
                _ = Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        internal void FreePage(long page)
        {
            // If the logSizeTracker is not active, then all pages are used once allocated so there's nothing to add to the overflow pool.
            if (logSizeTracker is not null)
                ReturnPage((int)(page % BufferSize));
            else
            {
                objectPages[page % BufferSize].Clear();
                ClearPage(page, 0);
            }
        }

        internal override void ClearPage(long page, int offset = 0)
        {
            var index = page % BufferSize;

            // Offset is nonzero only for RecoveryReset, to zero out the page past offset (which is tailAddress).
            // In this case, we want to keep the objectPage information for the used (so far) part of the page.
            if (offset == 0)
                objectPages[index].Clear();
            base.ClearPage(index, offset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateLogRecord(long logicalAddress) => CreateLogRecord(logicalAddress, GetPhysicalAddress(logicalAddress));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateLogRecord(long logicalAddress, long physicalAddress) => new(physicalAddress, objectPages[GetPageIndexForAddress(logicalAddress)].objectIdMap);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateRemappedLogRecordOverPinnedTransientMemory(long logicalAddress, long physicalAddress)
            => LogRecord.CreateRemappedOverPinnedTransientMemory(physicalAddress, objectPages[GetPageIndexForAddress(logicalAddress)].objectIdMap, transientObjectIdMap);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ObjectIdMap GetObjectIdMap(long logicalAddress) => objectPages[GetPageIndexForAddress(logicalAddress)].objectIdMap;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeRecord<TKey>(TKey key, long logicalAddress, in RecordSizeInfo sizeInfo, ref LogRecord logRecord)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
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
        public RecordSizeInfo GetRMWInitialRecordSize<TKey, TInput, TVariableLengthInput>(TKey key, ref TInput input, TVariableLengthInput varlenInput)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TVariableLengthInput : IVariableLengthInput<TInput>
        {
            // Used by RMW to determine the length of initial destination (client uses Input to fill in whether ETag and Expiration are inluded); Filler information is not needed.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetRMWInitialFieldInfo(key, ref input) };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetUpsertRecordSize<TKey, TInput, TVariableLengthInput>(TKey key, ReadOnlySpan<byte> value, ref TInput input, TVariableLengthInput varlenInput)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TVariableLengthInput : IVariableLengthInput<TInput>
        {
            // Used by Upsert to determine the length of insert destination (client uses Input to fill in whether ETag and Expiration are inluded); Filler information is not needed.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetUpsertFieldInfo(key, value, ref input) };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetUpsertRecordSize<TKey, TInput, TVariableLengthInput>(TKey key, IHeapObject value, ref TInput input, TVariableLengthInput varlenInput)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TVariableLengthInput : IVariableLengthInput<TInput>
        {
            // Used by Upsert to determine the length of insert destination (client uses Input to fill in whether ETag and Expiration are inluded); Filler information is not needed.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetUpsertFieldInfo(key, value, ref input) };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetUpsertRecordSize<TKey, TSourceLogRecord, TInput, TVariableLengthInput>(TKey key, in TSourceLogRecord inputLogRecord, ref TInput input, TVariableLengthInput varlenInput)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>
        {
            // Used by Upsert to determine the length of insert destination (client uses Input to fill in whether ETag and Expiration are inluded); Filler information is not needed.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetUpsertFieldInfo(key, in inputLogRecord, ref input) };
            PopulateRecordSizeInfo(ref sizeInfo);
            return sizeInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordSizeInfo GetDeleteRecordSize<TKey>(TKey key)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            // Used by Delete to determine the length of a new tombstone record. Does not require an ISessionFunctions method.
            var sizeInfo = new RecordSizeInfo()
            {
                FieldInfo = new()
                {
                    KeySize = key.KeyBytes.Length,
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
            Debug.Assert(sizeInfo.word == 0, "RecordSizeInfo should not be resused");

            // Object allocator may have Inline or Overflow Keys or Values; additionally, Values may be Object. Both non-inline cases are an objectId in the record.
            // Key
            if (sizeInfo.FieldInfo.KeySize <= maxInlineKeySize)
                sizeInfo.SetKeyIsInline();
            var keySize = sizeInfo.KeyIsInline ? sizeInfo.FieldInfo.KeySize : ObjectIdMap.ObjectIdSize;

            // Value
            sizeInfo.MaxInlineValueSize = maxInlineValueSize;
            if (!sizeInfo.ValueIsObject && sizeInfo.FieldInfo.ValueSize <= sizeInfo.MaxInlineValueSize)
                sizeInfo.SetValueIsInline();
            var valueSize = sizeInfo.ValueIsInline ? sizeInfo.FieldInfo.ValueSize : ObjectIdMap.ObjectIdSize;

            // Record
            sizeInfo.CalculateSizes(keySize, valueSize);
        }

        /// <summary>
        /// Dispose an in-memory <see cref="LogRecord"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void OnDispose(ref LogRecord logRecord, DisposeReason disposeReason)
        {
            if (logRecord.IsSet)
            {
                // Decrement heap from the tracker before clearing fields. The amount depends on
                // what's still alive on the record at dispose time:
                //  - Deleted: value only (key stays for chain traversal; key accounted at eviction).
                //    Tombstone is NOT yet set, so GetValueHeapMemorySize returns the correct value.
                //  - Elided / RevivificationFreeList: full remaining heap (key + value). The record
                //    is being removed from the chain entirely, so both key and value are freed.
                //    Eviction skips invalid records, so this is the last chance to account for them.
                //  - CAS failures / InsertAbandoned: no decrement needed — the record was never
                //    CAS'd into the chain, so +key/+value were never added to the tracker.
                if (disposeReason == DisposeReason.Deleted)
                {
                    var valueHeap = logRecord.GetValueHeapMemorySize();
                    if (valueHeap != 0)
                        logSizeTracker?.IncrementSize(-valueHeap);
                }
                else if (disposeReason is DisposeReason.Elided or DisposeReason.RevivificationFreeList)
                {
                    // Subtract whatever heap remains. The record is being removed from the chain
                    // entirely (or transferred to the freelist), so eviction will never visit it.
                    // For tombstoned records, CalculateHeapMemorySize returns 0, but key overflow
                    // is still alive and needs to be subtracted.
                    long remainingHeap;
                    if (!logRecord.Info.Tombstone)
                        remainingHeap = logRecord.CalculateHeapMemorySize();
                    else
                        remainingHeap = logRecord.DataHeader.KeyIsOverflow ? logRecord.KeyOverflow.HeapMemorySize : 0;
                    if (remainingHeap != 0)
                        logSizeTracker?.IncrementSize(-remainingHeap);
                }

                storeFunctions.OnDispose(ref logRecord, disposeReason);

                logRecord.ClearHeapFields(disposeReason != DisposeReason.Deleted);
                logRecord.ClearOptionals();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason disposeReason)
        {
            // Route to the store-level trigger; the app (e.g. Garnet) decides whether to Dispose()
            // the value object. DiskLogRecord.Dispose() is responsible for releasing the record buffer.
            storeFunctions.OnDisposeDiskRecord(ref logRecord, disposeReason);
        }

        /// <summary>
        /// Iterate records in the given logical address range and call <see cref="IStoreFunctions.OnEvict"/>
        /// on each non-null, non-invalid, non-tombstoned record — including sealed source records that
        /// may still own heap. Used during page eviction to allow cleanup of external resources.
        /// The caller constrains <paramref name="startAddress"/> / <paramref name="endAddress"/> to lie on a single page
        /// (see <see cref="AllocatorBase{TStoreFunctions, TAllocator}.OnPagesClosedWorker"/> and
        /// <see cref="AllocatorBase{TStoreFunctions, TAllocator}.EvictPageForRecovery"/>), so this routine walks records
        /// within that single page only.
        /// </summary>
        internal void EvictRecordsInRange(long startAddress, long endAddress, EvictionSource source, bool isRecovery)
        {
            var startPage = GetPage(startAddress);
            var firstValidAddress = GetFirstValidLogicalAddressOnPage(startPage);
            var address = startAddress < firstValidAddress ? firstValidAddress : startAddress;
            var pageEndAddress = GetLogicalAddressOfStartOfPage(startPage + 1);
            var stopAddress = endAddress < pageEndAddress ? endAddress : pageEndAddress;

            // During recovery a page whose object load was deferred has an empty ObjectIdMap, and its per-record object/overflow slots still hold raw
            // on-disk values, not valid ObjectIdMap ids; they must not be dereferenced and the records are not yet materialized, so skip the page.
            // (In normal eviction an empty map merely means an object-free page, whose inline records must still be visited for OnEvict.)
            var objectIdMap = objectPages[GetPageIndexForAddress(address)].objectIdMap;
            if (isRecovery && objectIdMap.IsEmpty)
                return;

            while (address < stopAddress)
            {
                var physicalAddress = GetPhysicalAddress(address);
                var logRecord = new LogRecord(physicalAddress, objectIdMap);
                var allocatedSize = logRecord.AllocatedSize;

                if (allocatedSize <= 0)
                    break;
                var offset = GetOffsetOnPage(address);
                if (offset == 0 || offset + allocatedSize > PageSize)
                    break;

                // Skip null and invalid records (elided/disposed, heap already cleaned up).
                if (logRecord.Info.IsNull || logRecord.Info.Invalid)
                {
                    address += allocatedSize;
                    continue;
                }

                // Decrement the record's heap contribution in a single call.
                // For non-tombstoned records, CalculateHeapMemorySize returns key + value heap.
                // For tombstoned records, it returns 0 (by design), but the key overflow is still
                // alive — value was already decremented at the delete site via OnDispose.
                long heapSize;
                if (!logRecord.Info.Tombstone)
                {
                    heapSize = logRecord.CalculateHeapMemorySize();

                    if (storeFunctions.CallOnEvict)
                        storeFunctions.OnEvict(ref logRecord, source);
                }
                else
                {
                    heapSize = logRecord.DataHeader.KeyIsOverflow ? logRecord.KeyOverflow.HeapMemorySize : 0;
                }

                if (heapSize != 0)
                    logSizeTracker?.IncrementSize(-heapSize);

                address += allocatedSize;
            }
        }

        /// <summary>
        /// Call <see cref="IStoreFunctions.OnFlush(ref LogRecord, long)"/> on original in-memory records
        /// before they are flushed to disk. This allows the application to snapshot external resources
        /// (e.g. BfTree data files) and set flags on the live record while it is still in memory.
        /// </summary>
        internal void FlushRecordsInRange(long startAddress, long endAddress)
        {
            var page = GetPage(startAddress);
            var firstValidAddress = GetFirstValidLogicalAddressOnPage(page);
            var address = startAddress < firstValidAddress ? firstValidAddress : startAddress;

            while (address < endAddress)
            {
                var physicalAddress = GetPhysicalAddress(address);
                var logRecord = new LogRecord(physicalAddress, objectPages[GetPageIndexForAddress(address)].objectIdMap);
                var allocatedSize = logRecord.AllocatedSize;

                if (allocatedSize <= 0)
                    break;

                var offset = GetOffsetOnPage(address);
                if (offset + allocatedSize > PageSize)
                    break;

                if (logRecord.Info.Valid && !logRecord.Info.IsNull && !logRecord.Info.SkipOnScan && !logRecord.Info.Tombstone)
                    storeFunctions.OnFlush(ref logRecord, address);

                address += allocatedSize;
            }
        }

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            var localValues = Interlocked.Exchange(ref objectPages, null);
            if (localValues != null)
            {
                freePagePool.Dispose();
                foreach (var value in localValues)
                    value.Clear();
                base.Dispose();
            }
        }

        protected override void TruncateUntilAddressBlocking(long toAddress)
        {
            // First get the segment of the object log to remove. We've put the lowest object log position used by a page into its PageHeader.
            // If toAddress is in the middle of a main log page, we must limit objectlog truncation to the lowest segment used by the that page.
            // If toAddress is not past the PageHeader, then assume it is the start of the page and its PageHeader hasn't been written, so use
            // the previous page.
            var objectLogSegment = -1;
            if (objectLogDevice is not null)
            {
                var addressOfStartOfMainLogPage = GetAddressOfStartOfPageOfAddress(toAddress);
                if (GetOffsetOnPage(toAddress) <= PageHeader.Size && addressOfStartOfMainLogPage >= PageSize)
                    addressOfStartOfMainLogPage -= PageSize;
                objectLogSegment = GetLowestObjectLogSegmentInUse(addressOfStartOfMainLogPage);
            }

            // Now do the actual truncations.
            base.TruncateUntilAddressBlocking(toAddress);
            if (objectLogSegment >= 0)
            {
                objectLogDevice.TruncateUntilSegment(objectLogSegment);
                _ = MonotonicUpdate(ref lowestObjectLogSegmentInUse, objectLogSegment, out _);
            }
        }

        protected override void RemoveSegment(int segment)
        {
            // if segment is not the last segment (which should be the case), we can use the page header of the start of the segment to get
            // the highest object log segment to remove because we know its PageHeader has been written. Otherwise, we have to use the previous
            // page's PageHeader to get the object log segment to remove.
            var objectLogSegment = -1;
            if (objectLogDevice is not null)
            {
                var addressOfStartOfMainLogPage = GetStartLogicalAddressOfSegment(segment);
                if (segment >= device.EndSegment)
                    addressOfStartOfMainLogPage -= PageSize;
                objectLogSegment = GetLowestObjectLogSegmentInUse(addressOfStartOfMainLogPage);
            }

            // Now do the actual truncations; TruncateUntilSegment does not remove the passed segment.
            base.RemoveSegment(segment);
            if (objectLogSegment >= 0)
            {
                objectLogDevice.TruncateUntilSegment(objectLogSegment);
                _ = MonotonicUpdate(ref lowestObjectLogSegmentInUse, objectLogSegment, out _);
            }
        }

        private int GetLowestObjectLogSegmentInUse(long addressOfStartOfMainLogPage)
        {
            Debug.Assert(objectLogDevice is not null, "GetHighestObjectLogSegmentToRemove should not be called if there is no objectLogDevice");
            var objectLogSegment = -1;

            // If we're on the first main-log page, we won't be able to remove any object log segments.
            // If we're not past the PageHeader of the second page, then the PageHeader probably hasn't been written, so we can't read it.
            if (addressOfStartOfMainLogPage <= PageSize + PageHeader.Size)
                return objectLogSegment;

            var buffer = bufferPool.Get(sectorSize);
            PageAsyncReadResult<Empty> result = new() { handle = new CountdownEvent(1) };
            try
            {
                device.ReadAsync((ulong)addressOfStartOfMainLogPage, (IntPtr)buffer.aligned_pointer, (uint)sectorSize, AsyncReadPageCallback, result);
                result.handle.Wait();
                if (result.numBytesRead >= PageHeader.Size)
                {
                    var pageHeader = *(PageHeader*)buffer.aligned_pointer;
                    if (pageHeader.objectLogLowestPositionWord != ObjectLogFilePositionInfo.NotSet)
                    {
                        var objectLogPosition = new ObjectLogFilePositionInfo(pageHeader.objectLogLowestPositionWord, objectLogTail.SegmentSizeBits);   // TODO verify SegmentSizeBits is correct
                        objectLogSegment = objectLogPosition.SegmentId;
                    }
                }
            }
            finally
            {
                bufferPool.Return(buffer);
                result.DisposeHandle();
            }

            return objectLogSegment;
        }

        /// <inheritdoc/>
        internal override CircularDiskWriteBuffer CreateCircularFlushBuffers(IDevice objectLogDevice, ILogger logger)
        {
            var localObjectLogDevice = objectLogDevice ?? this.objectLogDevice;
            return localObjectLogDevice is not null
                ? new(bufferPool, IStreamBuffer.BufferSize, numberOfFlushBuffers, localObjectLogDevice, logger)
                : null;
        }

        /// <inheritdoc/>
        internal override CircularDiskReadBuffer CreateCircularReadBuffers(IDevice objectLogDevice, ILogger logger)
            => new(bufferPool, IStreamBuffer.BufferSize, numberOfDeserializationBuffers, objectLogDevice ?? this.objectLogDevice, logger);

        /// <inheritdoc/>
        internal override CircularDiskReadBuffer CreateCircularReadBuffers()
            => new(bufferPool, IStreamBuffer.BufferSize, numberOfDeserializationBuffers, objectLogDevice, logger);

        /// <inheritdoc/>
        internal override int LowestObjectLogSegmentInUse => lowestObjectLogSegmentInUse;
        /// <inheritdoc/>
        internal override ObjectLogFilePositionInfo GetObjectLogTail() => objectLogTail;
        /// <inheritdoc/>
        internal override void SetObjectLogTail(ObjectLogFilePositionInfo tail)
        {
            Debug.Assert(!objectLogTail.HasData, $"SetObjectLogTail should be called only when we have not already set objectLogTail, such as in Recovery");
            objectLogTail = tail;
        }

        /// <summary>Object log segment size</summary>
        public override long GetObjectLogSegmentSize() => ObjectLogSegmentSize;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.NoInlining)]
        protected internal override void RecoveryReset(long tailAddress, long headAddress, long beginAddress, long readonlyAddress)
        {
            base.RecoveryReset(tailAddress, headAddress, beginAddress, readonlyAddress);
            LastIssuedFlushedUntilAddress = readonlyAddress;
            OngoingFlushedUntilAddress = 0;
            NoFlushUntilAddress = 0;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal override void AsyncFlushPagesForReadOnly(long fromAddress, long untilAddress, bool noFlush = false)
        {
            // We do not need to ensure page alignment of the ReadOnlyAddres for correctness, and in fact that is impossible since we support setting it to whatever
            // the current TailAddress is, but for normal flush operations we do set it to page alignment to eliminate concerns about rewriting partial sectors.
            GetFlushPageRange(fromAddress, untilAddress, out var startPage, out var numPages);

            // Create the buffers we will use for all ranges of the flush. This calls our callback and disposes itself when the last write of a range completes.
            var flushBuffers = CreateCircularFlushBuffers(objectLogDevice: null, logger);

            // Write each page (or partial page) in the range.
            for (var flushPage = startPage; flushPage < (startPage + numPages); flushPage++)
            {
                // The result from PrepareFlushAsyncResult indicates whether we are to perform an actual flush--but asyncResult will be set anyway.
                if (PrepareFlushAsyncResult(fromAddress, untilAddress, noFlush, flushPage, out var asyncResult))
                {
                    asyncResult.flushBuffers = flushBuffers;

                    // TsavoriteKV using ObjectAllocator always moves ReadOnlyAddress in page alignment, so if we have a partial first page, it can be written
                    // in the same loop as full pages, because there are no adjacent fragments. Write the entire page up to asyncResult.untilAddress.
                    Debug.Assert(PendingFlush[GetPageIndexForAddress(asyncResult.fromAddress)].list.Count == 0,
                        $"Expected PendingFlush count {PendingFlush[GetPageIndexForAddress(asyncResult.fromAddress)].list.Count} to be 0 for ObjectAllocator");

                    WriteAsync(flushPage, AsyncFlushPageCallback, asyncResult);
                }
            }
        }

        protected override void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
            => WriteAsync(flushPage, (ulong)(AlignedPageSizeBytes * flushPage), (uint)PageSize, callback, asyncResult, device, objectLogDevice);

        protected override void WriteAsyncToDeviceForSnapshot<TContext>(long startPage, long flushPage, int pageFlushSize, DeviceIOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice, long fuzzyStartLogicalAddress)
        {
            VerifyCompatibleSectorSize(device);
            VerifyCompatibleSectorSize(objectLogDevice);

            var epochTaken = epoch.ResumeIfNotProtected();
            try
            {
                var headAddress = HeadAddress;

                if (headAddress >= asyncResult.untilAddress)
                {
                    // Requested span on page is entirely unavailable in memory; ignore it and call the callback directly.
                    callback(0, 0, asyncResult);
                    return;
                }

                // If requested page span is only partly available in memory, adjust the start position
                // and mark as partial so WriteAsync recalculates the flush size from the adjusted range.
                if (headAddress > asyncResult.fromAddress)
                {
                    asyncResult.fromAddress = headAddress;
                    asyncResult.partial = true;
                }

                // We are writing to a separate device which starts at startPage. Eventually, startPage becomes the basis of
                // HybridLogRecoveryInfo.snapshotStartFlushedLogicalAddress, which is the page starting at offset 0 of the snapshot file.
                WriteAsync(flushPage, (ulong)(AlignedPageSizeBytes * (flushPage - startPage)), (uint)pageFlushSize,
                            callback, asyncResult, device, objectLogDevice, fuzzyStartLogicalAddress);
            }
            finally
            {
                if (epochTaken)
                    epoch.Suspend();
            }
        }

        private void WriteAsync<TContext>(long flushPage, ulong alignedMainLogFlushPageAddress, uint numBytesToWrite,
                        DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                        IDevice device, IDevice objectLogDevice, long fuzzyStartLogicalAddress = long.MaxValue)
        {
            // We flush within the DiskStreamWriteBuffer, so we do not use the asyncResult here for IO (until the final callback), but it has necessary fields.

            // Short circuit if we are using a null device
            if (device is NullDevice)
            {
                device.WriteAsync(IntPtr.Zero, 0, 0, numBytesToWrite, callback, asyncResult);
                return;
            }

            var pageStart = GetLogicalAddressOfStartOfPage(asyncResult.page);
            var logPagePointer = (byte*)pagePointers[flushPage % BufferSize];

            // asyncResult.fromAddress is either start of page or start of a record past the page header
            Debug.Assert(asyncResult.fromAddress - pageStart is >= PageHeader.Size or 0, $"fromAddress ({asyncResult.fromAddress}, offset {asyncResult.fromAddress - pageStart}) must be 0 or after the PageHeader");
            int startOffset = (int)(asyncResult.fromAddress - pageStart), endOffset = startOffset + (int)numBytesToWrite;
            var isFirstRecordOnPage = startOffset <= PageHeader.Size;

            // Write the object log position into the header if this is the first record on the page. If there are no records on the page, we will
            // call through to WriteInlinePageAsync so we want the header updated regardless of whether we have objects (this may be a page with no
            // objects after some pages with objects, and so we want Truncate() to know it has to preserve those object log segments).
            if (isFirstRecordOnPage)
                ((PageHeader*)logPagePointer)->SetLowestObjectLogPosition(objectLogTail);

            // Short circuit if we are not using flushBuffers and not in recovery (e.g. using ObjectAllocator for string-only purposes).
            if (asyncResult.flushBuffers is null)
            {
                if (asyncResult.flushRequestState != FlushRequestState.Recovery)
                {
                    WriteInlinePageAsync((nint)pagePointers[flushPage % BufferSize], (ulong)(AlignedPageSizeBytes * flushPage), (uint)AlignedPageSizeBytes, callback, asyncResult, device);
                    return;
                }
                Debug.Assert(!asyncResult.partial, "Partial flush should not be requested for recovery flushes");
            }

            Debug.Assert(asyncResult.page == flushPage, $"asyncResult.page {asyncResult.page} should equal flushPage {flushPage}");

            // numBytesToWrite is calculated from start and end logical addresses, either for the full page or a subset of records (aligned to start and end of record boundaries),
            // in the allocator page (including the objectId space for Overflow and Heap Objects). Note: "Aligned" in this discussion refers to sector (as opposed to record) alignment.

            // Initialize offsets into the allocator page based on full-page (including the page header), then override them if partial.
            Debug.Assert(asyncResult.untilAddress - pageStart >= PageHeader.Size, $"untilAddress ({asyncResult.untilAddress}, offset {asyncResult.untilAddress - pageStart}) must be past PageHeader {flushPage}");
            if (asyncResult.partial)
            {
                // We're writing only a subset of the page.
                endOffset = (int)(asyncResult.untilAddress - pageStart);
                numBytesToWrite = (uint)(endOffset - startOffset);
            }

            // Adjust so the first record on the page includes the page header. We've already asserted fromAddress such that startOffset is either 0 or >= PageHeader.
            var logicalAddress = asyncResult.fromAddress;
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
                    startOffset = 0;    // Include the PageHeader in the page output
                    numBytesToWrite = (uint)(endOffset - startOffset);
                }
            }
            else
                Debug.Assert(asyncResult.flushRequestState != FlushRequestState.Recovery, "FlushRequestState.IsForRecovery should always be done an entire page at a time");

            var alignedStartOffset = RoundDown(startOffset, (int)device.SectorSize);
            var startPadding = startOffset - alignedStartOffset;
            var alignedBufferSize = RoundUp(startPadding + (int)numBytesToWrite, (int)device.SectorSize);

            // The srcBuffer path copies the record bytes out, mutates the copy's objectId slots with the on-disk lengths,
            // then writes the copy to disk. The live main-log record stays intact for subsequent in-memory operations.

            // If we are in snapshot checkpoint we will need to acquire the epoch whenever we access the log record or oidMap; we will not have the epoch
            // when we enter here. If we are in recovery, we will not have the epoch either, but we don't need to acquire it as there are no other operations
            // happening. Otherwise, we are here because we are moving the read-only address (FoldOver checkpoint is a special case of this). In that case
            // we will have the epoch on entry, but we will not need to remain protected because ShiftHeadAddress always remains below FlushedUntilAddress
            // so the actual log page, inluding ObjectIdMap, will remain valid until we complete this partial flush. So we release the epoch if we have it;
            // we don't need it and don't want to hold it during the time-consuming actual flush.
            var pulseEpoch = asyncResult.flushRequestState == FlushRequestState.Snapshot;
            var protectEpochWhenDone = epoch.TrySuspend();

            // Overflow Keys and Values are written to, and Object values are serialized to, this Stream, if we have flushBuffers.
            ObjectLogWriter<TStoreFunctions> logWriter = null;

            // For a snapshot-region recovery flush, the reader over the snapshot object-log device from which each record's object bytes are
            // copied into the main object-log (appended via logWriter). Null for non-recovery flushes and for the hybrid-log region (whose
            // objects are already durable in the main object-log).
            var isSnapshotRecoveryCopy = asyncResult.recoverySnapshotObjectLogDevice is not null;
            var formerFlushedUntilAddress = asyncResult.recoveryFormerFlushedUntilAddress;
            CircularDiskReadBuffer snapshotObjectReadBuffers = null;
            ObjectLogReader<TStoreFunctions> snapshotObjectReader = null;

            // Do everything below here in the try{} to be sure the epoch is Resumed()d if we Suspended it.
            SectorAlignedMemory srcBuffer = default;
            try
            {
                // Create a local copy of the main-log page inline data. Space for ObjectIds and the ObjectLogPosition will be updated as we go
                // (ObjectId space and the RecordDataHeader length fields will combine for the full range of object sizes). This does
                // not change record sizes, so the logicalAddress space is unchanged. Also, we will not advance HeadAddress until this flush is complete
                // and has updated FlushedUntilAddress, so we don't have to worry about the page being yanked out from underneath us (and Objects
                // won't be disposed before we're done). TODO: Loop on successive subsets of the page's records to make this initial copy buffer smaller.
                var objectIdMap = objectPages[flushPage % BufferSize].objectIdMap;

                srcBuffer = bufferPool.Get(alignedBufferSize);
                asyncResult.freeBuffer1 = srcBuffer;

                // Read back the first sector if the start is not aligned (this means we already wrote a partially-filled sector with ObjectLog fields set).
                if (startPadding > 0)
                {
                    // TODO: This will potentially overwrite partial sectors (with the same data) if this is a partial flush; a workaround would be difficult.
                    // TODO: Cache the last sector flushed in readBuffers so we can avoid this Read.
                    PageAsyncReadResult<Empty> result = new() { handle = new CountdownEvent(1) };
                    device.ReadAsync(alignedMainLogFlushPageAddress + (ulong)alignedStartOffset, (IntPtr)srcBuffer.aligned_pointer, (uint)sectorSize, AsyncReadPageCallback, result);
                    result.handle.Wait();
                    result.DisposeHandle();
                }

                try
                {
                    if (pulseEpoch)
                        epoch.Resume();

                    // Copy from the record start position (startOffset) in the main log page to the src buffer starting at its offset in the first sector (startPadding).
                    var allocatorPageSpan = new Span<byte>((byte*)logPagePointer + startOffset, (int)numBytesToWrite);
                    allocatorPageSpan.CopyTo(srcBuffer.TotalValidSpan.Slice(startPadding));
                    srcBuffer.available_bytes = (int)numBytesToWrite + startPadding;
                }
                finally
                {
                    if (pulseEpoch)
                        epoch.Suspend();
                }

                // recordsBasePtr is the page-relative base pointer such that (recordsBasePtr + pageOffset) points to the record at that page offset.
                // srcBuffer holds data starting at alignedStartOffset, so we subtract alignedStartOffset to get the same page-relative addressing.
                var recordsBasePtr = srcBuffer.GetValidPointer() - alignedStartOffset;
                // diskWritePtr is the pointer passed to device.WriteAsync — the sector-aligned start of the data to write.
                var diskWritePtr = srcBuffer.GetValidPointer();

                if (asyncResult.flushBuffers is not null)
                {
                    logWriter = new(device, asyncResult.flushBuffers, storeFunctions);
                    _ = logWriter.OnBeginPartialFlush(objectLogTail);
                }

                // Include page header when calculating end address. Using page-relative addressing makes both paths look identical.
                var endPhysicalAddress = (long)recordsBasePtr + startOffset + numBytesToWrite;
                var physicalAddress = (long)recordsBasePtr + firstRecordOffset;

                // For recovery flushes we don't re-serialize; rather we just update the object lengths and positions in the log file using deserialized
                // Overflow and/or Object information. That means we also have to track the increasing object log position "as if" we were re-serializing
                // the objects (because it is recovery, the lengths will not change--even if this is a page from snapshot, in which case we still don't
                // want to write to an object-log segment; that is ONLY done on OnPagesMarkedReadOnly.
                ref var pageHeader = ref *(PageHeader*)recordsBasePtr;

                var recoveryOngoingPageHeader = asyncResult.flushRequestState == FlushRequestState.Recovery ? pageHeader.GetLowestObjectLogPosition(objectLogTail.SegmentSizeBits) : default;
                var endLogicalAddress = logicalAddress + (endPhysicalAddress - physicalAddress);

                while (physicalAddress < endPhysicalAddress)
                {
                    // Increment for next iteration; use allocatedSize because that is what LogicalAddress is based on.
                    var logRecord = new LogRecord(physicalAddress, objectIdMap);
                    var logRecordSize = logRecord.AllocatedSize;
                    var extraRecordOffset = 0;

                    // Do not write Invalid records. This includes IsNull records. By the time we get here, ReadOnlyAddress has been advanced, so the
                    // record's state (IsValid, IsInNewVersion, inline data, etc.) will not change.
                    if (logRecord.Info.Valid)
                    {
                        // Do not write v+1 records (e.g. during a checkpoint). For non-Snapshot flushes, fuzzyStartLogicalAddress is long.MaxValue
                        // so this condition is always true and the SetInvalid branch is unreachable.
                        if (logicalAddress < fuzzyStartLogicalAddress || !logRecord.Info.IsInNewVersion)
                        {
                            // Do not write objects for fully-inline records. This should always be false if we don't have a logWriter (i.e. no flushBuffers),
                            // which would be the case where we were created to be used for inline string records only.
                            if (logRecord.DataHeader.RecordHasObjects)
                            {
                                if (asyncResult.flushRequestState != FlushRequestState.Recovery)
                                {
                                    var recordStartPosition = logWriter.GetNextRecordStartPosition();
                                    Debug.Assert(asyncResult.flushRequestState != FlushRequestState.ReadOnly || !isFirstRecordOnPage || recordStartPosition.CurrentAddress == objectLogTail.CurrentAddress,
                                        $"ObjectLogPosition mismatch on first record for ReadOnly flush: rec {recordStartPosition.CurrentAddress}, tail {objectLogTail.CurrentAddress}");

                                    OverflowByteArray keyOverflow = default, valueOverflow = default;
                                    IHeapObject valueObject = default;
                                    try
                                    {
                                        if (pulseEpoch)
                                        {
                                            epoch.Resume();

                                            // Check to see if HeadAddress (which can change while we're here) has moved past this record.
                                            var headAddress = HeadAddress;
                                            if (headAddress > logicalAddress)
                                            {
                                                if (headAddress <= endLogicalAddress)
                                                {
                                                    // Jump ahead to HeadAddress. Recover() will start recovery at the last FlushedUntilAddress of the main log,
                                                    // which will never be less than HeadAddress. So we do not need to worry about whatever values are in the inline
                                                    // record space between the current logicalAddress and HeadAddress.
                                                    extraRecordOffset = (int)(headAddress - (logicalAddress + logRecordSize));
                                                    // Skip object serialization
                                                    goto NextRecord;
                                                }
                                                else
                                                {
                                                    asyncResult.flushRequestState = FlushRequestState.WriteNotIssued;
                                                    goto WritePage;
                                                }
                                            }
                                        }

                                        if (logRecord.DataHeader.KeyIsOverflow)
                                            keyOverflow = logRecord.KeyOverflow;

                                        if (logRecord.DataHeader.ValueIsOverflow)
                                            valueOverflow = logRecord.ValueOverflow;
                                        else if (logRecord.DataHeader.ValueIsObject)
                                            valueObject = logRecord.ValueObject;
                                    }
                                    finally
                                    {
                                        if (pulseEpoch)
                                            epoch.Suspend();
                                    }

                                    // WriteRecordObjects can do disk IO and must not hold the epoch. SetObjectLogRecordStartPositionAndLength
                                    // writes the on-disk-encoded lengths and ObjectLogPosition into the disk-image record (srcBuffer copy).
                                    // The main-log allocator page is left untouched by these calls.
                                    logRecord.SetReuseObjectIdForSize();

                                    var valueObjectLength = logWriter.WriteRecordObjects(in keyOverflow, in valueOverflow, in valueObject);
                                    logRecord.SetObjectLogRecordStartPositionAndLength(recordStartPosition, valueObjectLength);
                                }
                                else
                                {
                                    if (isSnapshotRecoveryCopy && logicalAddress >= formerFlushedUntilAddress)
                                    {
                                        // Snapshot-region recovery flush: the record's objects live only in the snapshot object-log. Copy their bytes
                                        // into the main object-log (appended at the current objectLogTail via logWriter) so the page becomes durable and
                                        // can be evicted, then repoint the disk-image record to that main object-log position. The objects are NOT
                                        // deserialized at this point, so read the position/lengths from the R11 encoding (not from objectIdMap), and use
                                        // RepointObjectLogPosition (which preserves the unchanged lengths) rather than SetRecoveredObjectLogRecordStartPosition.
                                        var snapshotPositionWord = logRecord.GetObjectLogRecordStartPositionAndLengths(out var copyKeyLength, out var copyValueLength);
                                        var copyObjectLength = (ulong)copyKeyLength + copyValueLength;

                                        // Demand-load the snapshot object reader on the first valid record with objects, so pages with few or no object
                                        // records avoid an up-front full-page pre-pass. The read-ahead range is sized by scanning forward from here.
                                        snapshotObjectReader ??= CreateSnapshotObjectReader(physicalAddress + logRecordSize, endPhysicalAddress, snapshotPositionWord,
                                            copyKeyLength, copyValueLength, asyncResult.recoverySnapshotObjectLogDevice, out snapshotObjectReadBuffers);

                                        var mainRecordPosition = logWriter.GetNextRecordStartPosition();

                                        // Position/await the snapshot read buffers at this record (skips sector padding and waits for the read-ahead IO),
                                        // then stream the record's bytes verbatim into the main object-log.
                                        if (!snapshotObjectReadBuffers.OnBeginRecord(new ObjectLogFilePositionInfo(snapshotPositionWord, objectLogTail.SegmentSizeBits)))
                                            throw new TsavoriteException("No snapshot object-log data available while copying objects during recovery");
                                        logWriter.CopyRecoveredObjectBytes(snapshotObjectReader, copyObjectLength);
                                        logRecord.RepointObjectLogPosition(mainRecordPosition);
                                        recoveryOngoingPageHeader.Advance(copyObjectLength);
                                    }
                                    else
                                    {
                                        // In recovery we just need to update the disk-image LogRecord with the object lengths and file position, and then
                                        // advance the recoveryOngoingPageHeader position. This advancement will also take care of segment breaks if needed.
                                        var objectLengths = logRecord.SetRecoveredObjectLogRecordStartPosition(recoveryOngoingPageHeader);
                                        recoveryOngoingPageHeader.Advance(objectLengths);
                                    }
                                }

                                // Do this for both cases so it's clear when debugging
                                isFirstRecordOnPage = false;
                            }
                        }
                        else
                        {
                            // Mark v+1 records as invalid to avoid deserializing them on recovery. This is only reachable when fuzzyStartLogicalAddress < long.MaxValue,
                            // which only happens for Snapshot flushes. We are only setting Invalid in the disk image (srcBuffer), not the main log.
                            logRecord.InfoRef.SetInvalid();
                        }
                    } // endif record id Valid

                NextRecord:
                    logicalAddress += logRecordSize + extraRecordOffset;    // advance in main log
                    physicalAddress += logRecordSize + extraRecordOffset;   // advance in source buffer
                }

            WritePage:
                // We are done with the per-record objectlog flushes and we've updated the page (in-place or in srcBuffer). Now write that page
                // to the main log file (or snapshot file) unless we are to skip it because HeadAddress advanced.
                if (asyncResult.flushRequestState != FlushRequestState.WriteNotIssued)
                {
                    if (asyncResult.partial)
                    {
                        // We're writing only a subset of the page, so update our count of bytes to write.
                        var aligned_end = (int)RoundUp(asyncResult.untilAddress - alignedStartOffset, (int)device.SectorSize);
                        numBytesToWrite = (uint)(aligned_end - alignedStartOffset);
                    }

                    // Finally write the main log page as part of OnPartialFlushComplete, or directly if we had no flushBuffers.
                    // TODO: This will potentially overwrite partial sectors if this is a partial flush; a workaround would be difficult.
                    if (logWriter is not null)
                        logWriter.OnPartialFlushComplete(diskWritePtr, alignedBufferSize, device, alignedMainLogFlushPageAddress + (uint)alignedStartOffset, callback, asyncResult, ref objectLogTail);
                    else
                        device.WriteAsync((IntPtr)diskWritePtr, alignedMainLogFlushPageAddress + (uint)alignedStartOffset, (uint)alignedBufferSize, callback, asyncResult);
                }
            }
            finally
            {
                if (protectEpochWhenDone)
                    epoch.Resume();
                logWriter?.Dispose();
                snapshotObjectReader?.OnEndReadRecords();
                snapshotObjectReadBuffers?.Dispose();
            }
        }

        /// <summary>
        /// Demand-loads (creates and seeds) the reader over the snapshot object-log for a snapshot-region recovery flush, on the first valid record
        /// with objects on the page. The read-ahead range is sized by scanning forward from <paramref name="nextRecordAddress"/> to the last object
        /// record on the page, so pages with few or no object records avoid an up-front full-page pre-pass.
        /// </summary>
        /// <param name="nextRecordAddress">The (disk-image) address of the record just after the first object record.</param>
        /// <param name="endPhysicalAddress">The end of the page's records in the disk image.</param>
        /// <param name="firstPositionWord">The snapshot object-log position word of the first object record (the read-ahead start).</param>
        /// <param name="firstKeyLength">The first object record's key length.</param>
        /// <param name="firstValueLength">The first object record's value length.</param>
        /// <param name="snapshotObjectLogDevice">The snapshot object-log device to read from.</param>
        /// <param name="readBuffers">Outputs the created read buffers; the caller disposes them.</param>
        private ObjectLogReader<TStoreFunctions> CreateSnapshotObjectReader(long nextRecordAddress, long endPhysicalAddress, ulong firstPositionWord,
            int firstKeyLength, ulong firstValueLength, IDevice snapshotObjectLogDevice, out CircularDiskReadBuffer readBuffers)
        {
            var startPosition = new ObjectLogFilePositionInfo(firstPositionWord, objectLogTail.SegmentSizeBits);
            var endPosition = startPosition;
            var endKeyLength = firstKeyLength;
            var endValueLength = firstValueLength;
            for (var scanAddress = nextRecordAddress; scanAddress < endPhysicalAddress;)
            {
                var scanRecord = new LogRecord(scanAddress);
                scanAddress += scanRecord.AllocatedSize;
                if (scanRecord.Info.Valid && scanRecord.DataHeader.RecordHasObjects)
                    endPosition = new(scanRecord.GetObjectLogRecordStartPositionAndLengths(out endKeyLength, out endValueLength), objectLogTail.SegmentSizeBits);
            }
            endPosition.Advance((ulong)endKeyLength + endValueLength);

            readBuffers = CreateCircularReadBuffers(snapshotObjectLogDevice, logger);
            var reader = new ObjectLogReader<TStoreFunctions>(readBuffers, storeFunctions);
            reader.OnBeginReadRecords(startPosition, endPosition - startPosition);
            return reader;
        }

        /// <summary>
        /// Action to be performed when pages move into the immutable region.
        /// Seal: make sure there are no longer any threads writing to the page
        /// Flush: send page to secondary store
        /// </summary>
        internal override void OnPagesMarkedReadOnly(long newSafeReadOnlyAddress, bool noFlush = false)
        {
            Debug.Assert(newSafeReadOnlyAddress > HeadAddress);
            Debug.Assert(newSafeReadOnlyAddress <= GetTailAddress());
            if (noFlush)
                _ = MonotonicUpdate(ref NoFlushUntilAddress, newSafeReadOnlyAddress, out _);
            if (MonotonicUpdate(ref SafeReadOnlyAddress, newSafeReadOnlyAddress, out var oldSafeReadOnlyAddress))
            {
                // This thread is responsible for [oldSafeReadOnlyAddress -> newSafeReadOnlyddress]
                while (true)
                {
                    var _ongoingFlushedUntilAddress = OngoingFlushedUntilAddress;

                    // If we are closing in the middle of an ongoing OPMROWorker loop, exit.
                    if (_ongoingFlushedUntilAddress >= newSafeReadOnlyAddress)
                        break;

                    // We'll continue the loop if we fail the CAS here; that means another thread extended the Ongoing range.
                    if (Interlocked.CompareExchange(ref OngoingFlushedUntilAddress, newSafeReadOnlyAddress, _ongoingFlushedUntilAddress) == _ongoingFlushedUntilAddress)
                    {
                        // If _ongoingFlushedUntilAddress != 0 then another thread is runnning the OPMROWorker loop and will see the OnGoingFlushedUntilAddress increment to
                        // include newSafeReadOnlyAddress so we are done here. Otherwise, this thread is responsible for flushing [LastIssuedFlushedUntilAddress -> newSafeHeadAddress]
                        // and any other ranges that OnGoingFlushedUntilAddress is incremented to, and we are done here when that concludes.
                        if (_ongoingFlushedUntilAddress == 0)
                            OnPagesMarkedReadOnlyWorker();
                        return;
                    }
                    _ = Thread.Yield();
                }
            }
        }

        private void OnPagesMarkedReadOnlyWorker()
        {
            while (true)
            {
                var flushStartAddress = LastIssuedFlushedUntilAddress;
                var flushEndAddress = OngoingFlushedUntilAddress;

                // Notify the application per record before flushing, so it can snapshot external
                // resources (e.g. BfTree data files) and/or set flags on the live in-memory records.
                // This runs on the ORIGINAL records (not a copy), under epoch protection.
                if (storeFunctions.CallOnFlush)
                    FlushRecordsInRange(flushStartAddress, flushEndAddress);

                // Debug.WriteLine("SafeReadOnly shifted from {0:X} to {1:X}", oldSafeReadOnlyAddress, newSafeReadOnlyAddress);
                if (onReadOnlyObserver != null)
                {
                    // This scan does not need a store because it does not lock; it is epoch-protected so by the time it runs no current thread
                    // will have seen a record below the new ReadOnlyAddress as "in mutable region".
                    using var iter = Scan(store: null, flushStartAddress, flushEndAddress, DiskScanBufferingMode.NoBuffering);
                    onReadOnlyObserver?.OnNext(iter);
                }

                var noFlushUntilAddress = NoFlushUntilAddress;
                if (flushEndAddress > noFlushUntilAddress && flushStartAddress < noFlushUntilAddress)
                {
                    // NoFlushUntilAddress is in the middle of the flush range, so we flush in two parts: <= NoFUA (noFlush) and > NoFUA (!noFlush)
                    AsyncFlushPagesForReadOnly(flushStartAddress, noFlushUntilAddress, noFlush: true);
                    AsyncFlushPagesForReadOnly(noFlushUntilAddress, flushEndAddress, noFlush: false);
                }
                else
                {
                    // We're entirely above or below NoFUA, so we can flush in one go with the appropriate noFlush value
                    AsyncFlushPagesForReadOnly(flushStartAddress, flushEndAddress, noFlush: flushEndAddress <= NoFlushUntilAddress);
                }

                var updatedLIFUA = MonotonicUpdate(ref LastIssuedFlushedUntilAddress, flushEndAddress, out var oldLastIssuedFlushedUntilAddress);
                Debug.Assert(updatedLIFUA, $"Failed to update LIFUA");
                Debug.Assert(oldLastIssuedFlushedUntilAddress == flushStartAddress, $"Expected LastIssuedFlushedUntilAddress to be {flushStartAddress} but was {oldLastIssuedFlushedUntilAddress}");

                // End if we have exhausted co-operative work. This includes the case where OngoingFUA and flushEndAddress are already 0.
                if (Interlocked.CompareExchange(ref OngoingFlushedUntilAddress, 0, flushEndAddress) == flushEndAddress)
                    break;
                _ = Thread.Yield();
            }
        }

        private void AsyncReadPageCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncReadPageCallback)} error: {{errorCode}}", errorCode);

            // Set the page status to flushed
            var result = (PageAsyncReadResult<Empty>)context;
            result.numBytesRead = numBytes;
            _ = result.handle.Signal();
        }

        /// <inheritdoc />
        /// <remarks>This override of the base function reads Overflow keys or values, or Object values.</remarks>
        private protected override bool VerifyRecordFromDiskCallback(ref AsyncIOContext ctx, out long prevAddressToRead, out int prevLengthToRead)
        {
            // If this fails it is either too-short main-log record or a key mismatch. Let the top-level retry handle it. This will always
            // use the transientObjectIdMap (unless we are copying to tail, in which case we will remap to the allocator page's objectIdMap).
            if (!base.VerifyRecordFromDiskCallback(ref ctx, out prevAddressToRead, out prevLengthToRead))
                return false;

            // If the record is inline, we have no Overflow or Objects to retrieve.
            ref var diskLogRecord = ref ctx.diskLogRecord;
            if (diskLogRecord.DataHeader.RecordIsInline)
                return true;

            var startPosition = new ObjectLogFilePositionInfo(ctx.diskLogRecord.logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength), objectLogTail.SegmentSizeBits);
            var totalBytesToRead = (ulong)keyLength + valueLength;

            // 'using' is OK here as we complete the object reads before returning.
            using var readBuffers = CreateCircularReadBuffers(objectLogDevice, logger);

            var logReader = new ObjectLogReader<TStoreFunctions>(readBuffers, storeFunctions);
            logReader.OnBeginReadRecords(startPosition, totalBytesToRead);
            if (logReader.ReadRecordObjects(ref diskLogRecord.logRecord, ctx.requestKey, startPosition.SegmentSizeBits))
            {
                // Success. The deserialized heap object's Dispose() will be invoked when the DiskLogRecord
                // is disposed (ObjectIdMap.Free → IHeapObject.Dispose), unless the object is transferred out
                // (e.g. via CopyToTail) in which case the transient ObjectIdMap slot is cleared without disposing.
                // Default the output arguments for reading a previous record.
                prevAddressToRead = 0;
                return true;
            }

            // Ensure we have finished all object reads
            logReader.OnEndReadRecords();

            // If readBuffer.Read returned false it was due to an Overflow key mismatch or an Invalid record, so get the previous record.
            prevAddressToRead = (*(RecordInfo*)ctx.record.GetValidPointer()).PreviousAddress;
            return false;
        }

        protected override void ReadAsync<TContext>(ulong alignedSourceAddress, IntPtr destinationPtr, uint aligned_read_length,
            DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device)
        {
            //TODO("Add CancellationToken to the ReadAsync and WriteAsync paths");

            asyncResult.callback = callback;
            asyncResult.destinationPtr = destinationPtr;

            // Bound the object-record walk (in DeserializeObjectsOnPage) to the caller's valid-data extent when one was provided.
            // For a partial page read the caller sets maxAddressOffsetOnPage to the scan/recovery end (untilAddress), which can be
            // mid-page and is smaller than the sector-aligned device read length. Walking to the aligned length would process records
            // that lie ABOVE untilAddress (the page physically continues past the requested range). Such a record can straddle the
            // read end, so the fields GetObjectLogRecordStartPositionAndLengths reads near its tail -- the ObjectLogPosition word and
            // the R11 value-length high bits -- fall past the bytes actually transferred and are read from the un-read buffer tail.
            // That tail is only incidentally zero (the read buffer is cleared before the read); a different boundary alignment could
            // bisect a field and yield arbitrary bytes. Either way the resulting position/length is bogus and corrupts the computed
            // object-log range. Only fall back to the read length when the caller left the extent unset.
            if (asyncResult.maxAddressOffsetOnPage == 0)
                asyncResult.maxAddressOffsetOnPage = aligned_read_length;

            device.ReadAsync(alignedSourceAddress, destinationPtr, aligned_read_length, AsyncReadPageWithObjectsCallback<TContext>, asyncResult);
        }

        private void AsyncReadPageWithObjectsCallback<TContext>(uint errorCode, uint numBytes, object context)
        {
            var result = (PageAsyncReadResult<TContext>)context;

            if (errorCode != 0)
            {
                // The page read failed or was cancelled. The destination buffer may contain partial or
                // garbage data, so do not attempt to parse record headers or deserialize objects from it
                // (doing so can compute bogus lengths and throw OutOfMemoryException/AccessViolation).
                // Surface the error to the real page-read callback, which handles the failure.
                logger?.LogError($"{nameof(AsyncReadPageWithObjectsCallback)} error: {{errorCode}}", errorCode);
                result.callback(errorCode, numBytes, context);
                return;
            }

            // If this is Recovery Pass 1 we skip object deserialization (frame reads are in RecoveryPhase.None).
            if (result.recoveryPhase != RecoveryPhase.Pass1)
            {
                var objectIdMapToUse = result.recoveryPhase != RecoveryPhase.None ? objectPages[result.page % BufferSize].objectIdMap : transientObjectIdMap;
                DeserializeObjectsOnPage((long)result.destinationPtr, result.maxAddressOffsetOnPage, objectIdMapToUse, result.readBuffers);
            }

            // Call the "real" page read callback
            result.callback(errorCode, numBytes, context);
            result.Free();
        }

        /// <summary>
        /// Deserialize objects on a page that has already been loaded into memory (physical addresses).
        /// Scans records to determine object log ranges, reads from the object log via the provided readBuffers, and deserializes objects.
        /// </summary>
        /// <param name="pageStartPhysicalAddress">Physical start address of the page in memory</param>
        /// <param name="maxAddressOffsetOnPage">Maximum offset on the page (PageSize or less for partial pages)</param>
        /// <param name="objectIdMap">The ObjectIdMap to use for deserialized objects</param>
        /// <param name="readBuffers">The circular read buffers for object log reading</param>
        private void DeserializeObjectsOnPage(long pageStartPhysicalAddress, long maxAddressOffsetOnPage, ObjectIdMap objectIdMap, CircularDiskReadBuffer readBuffers)
        {
            ObjectLogFilePositionInfo startPosition = new(), endPosition = new();
            var endKeyLength = 0;
            ulong endValueLength = 0;
            var recordAddress = pageStartPhysicalAddress + PageHeader.Size;
            var endAddress = pageStartPhysicalAddress + maxAddressOffsetOnPage;

            // First pass: determine the range of object log bytes to read
            while (recordAddress < endAddress)
            {
                var logRecord = new LogRecord(recordAddress);
                recordAddress += logRecord.AllocatedSize;

                if (logRecord.DataHeader.RecordHasObjects && logRecord.Info.Valid)
                {
                    endPosition = new(logRecord.GetObjectLogRecordStartPositionAndLengths(out endKeyLength, out endValueLength), objectLogTail.SegmentSizeBits);
                    if (!startPosition.IsSet)
                        startPosition = endPosition;
                }
            }

            // The page may not have contained any records with objects
            if (!startPosition.IsSet)
                return;

            endPosition.Advance((ulong)endKeyLength + endValueLength);
            var totalBytesToRead = endPosition - startPosition;

            // Second pass: deserialize objects
            readBuffers.nextFileReadPosition = startPosition;
            recordAddress = pageStartPhysicalAddress + PageHeader.Size;
            var logReader = new ObjectLogReader<TStoreFunctions>(readBuffers, storeFunctions);
            logReader.OnBeginReadRecords(startPosition, totalBytesToRead);

            try
            {
                while (recordAddress < endAddress)
                {
                    var logRecord = new LogRecord(recordAddress, objectIdMap);
                    recordAddress += logRecord.AllocatedSize;

                    if (logRecord.DataHeader.RecordHasObjects && logRecord.Info.Valid)
                    {
                        _ = logReader.ReadRecordObjects(ref logRecord, default(EmptyKey), startPosition.SegmentSizeBits);
                        TrackRecoveredObjectRecord(in logRecord);
                    }
                }
            }
            finally
            {
                logReader.OnEndReadRecords();
            }
        }

        private void TrackRecoveredObjectRecord(in LogRecord logRecord)
        {
            if (logSizeTracker is null)
                return;

            // CalculateHeapMemorySize returns 0 for tombstones, but eviction subtracts
            // key overflow for tombstoned records. Add it here so the tracker stays balanced.
            if (logRecord.Info.Tombstone)
            {
                if (logRecord.DataHeader.KeyIsOverflow)
                    logSizeTracker.IncrementSize(logRecord.KeyOverflow.HeapMemorySize);
            }
            else
                logSizeTracker.UpdateSize(in logRecord, add: true);
        }

        /// <inheritdoc/>
        internal override long CalculatePageObjectSizes(long page, long startAddress, long untilAddress)
        {
            var recordAddress = Math.Max(startAddress, GetFirstValidLogicalAddressOnPage(page));
            var endAddress = Math.Min(untilAddress, GetLogicalAddressOfStartOfPage(page + 1));
            long totalSize = 0;

            while (recordAddress < endAddress)
            {
                var logRecord = new LogRecord(GetPhysicalAddress(recordAddress));
                var allocatedSize = logRecord.AllocatedSize;
                if (allocatedSize <= 0)
                    ThrowTsavoriteException($"LogRecord size should be > 0; encountered {allocatedSize}");

                recordAddress += allocatedSize;
                if (recordAddress > endAddress)
                    ThrowTsavoriteException($"Unaligned end of page; record exceeded page by {recordAddress - endAddress} bytes");

                if (logRecord.Info.Valid && logRecord.DataHeader.RecordHasObjects)
                {
                    _ = logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength);
                    totalSize += keyLength + (long)valueLength;
                }
            }

            return totalSize;
        }

        /// <summary>
        /// Determine if this is the last valid record on the page.
        /// </summary>
        /// <param name="recordAddress">Address of the current record</param>
        /// <param name="endAddress">Address of the end of the page</param>
        /// <returns>True if this is the last valid record on the page, otherwise false</returns>
        private bool IsLastRecordOnPage(long recordAddress, long endAddress, out long nextRecordAddress)
        {
            while (recordAddress < endAddress)
            {
                var logRecord = new LogRecord(GetPhysicalAddress(recordAddress));
                var allocatedSize = logRecord.AllocatedSize;
                if (allocatedSize <= 0)
                    ThrowTsavoriteException($"LogRecord size should be > 0; encountered {allocatedSize}");

                recordAddress += allocatedSize;
                if (recordAddress > endAddress)
                    ThrowTsavoriteException($"Unaligned end of page; record exceeded page by {recordAddress - endAddress} bytes");

                if (logRecord.Info.Valid)
                {
                    nextRecordAddress = recordAddress;
                    return false;
                }
            }
            nextRecordAddress = -1L;
            return true;
        }

        /// <inheritdoc/>
        internal override void LoadObjectsForRecoveryPass2(long page, long fromAddress, long untilAddress, IDevice objectLogDevice)
        {
            var pageStartAddress = GetFirstValidLogicalAddressOnPage(page);
            var address = Math.Max(fromAddress, pageStartAddress);
            var endAddress = Math.Min(untilAddress, GetLogicalAddressOfStartOfPage(page + 1));
            if (address >= endAddress)
                return;

            var pagePhysicalAddress = GetPhysicalAddress(GetLogicalAddressOfStartOfPage(page));
            var maxOffset = endAddress - GetLogicalAddressOfStartOfPage(page);
            var objectIdMapToUse = objectPages[page % BufferSize].objectIdMap;
            using var readBuffers = CreateCircularReadBuffers(objectLogDevice, logger);
            DeserializeObjectsOnPage(pagePhysicalAddress, maxOffset, objectIdMapToUse, readBuffers);
        }

        /// <inheritdoc/>
        internal override long FindHeadAddressCutoffOnPage(long page, long untilAddress, long totalPageObjectSize, int numPagesBelowCurrentPage, long remainingBudget, out int numPagesBelowToEvict)
        {
            var recordAddress = GetFirstValidLogicalAddressOnPage(page);
            var stopAddress = Math.Min(untilAddress, GetLogicalAddressOfStartOfPage(page + 1));
            var overBudgetAmount = totalPageObjectSize - remainingBudget;
            if (overBudgetAmount <= 0)
            {
                numPagesBelowToEvict = 0;
                return recordAddress;
            }

            // We are over budget. First see if we can evict enough pages to get below budget.
            var pagesToEvictToGetUnderBudget = (int)((overBudgetAmount + PageSize - 1) / PageSize);
            if (pagesToEvictToGetUnderBudget <= numPagesBelowCurrentPage)
            {
                // We can, and may even still have some pages left below us that can remain.
                numPagesBelowToEvict = pagesToEvictToGetUnderBudget;
                return recordAddress;
            }

            // We cannot evict enough pages to get under budget. Evict all pages below, and then skip records on this page until we are under budget.
            numPagesBelowToEvict = numPagesBelowCurrentPage;
            overBudgetAmount -= (long)numPagesBelowToEvict * PageSize;

            while (recordAddress < stopAddress)
            {
                var logRecord = new LogRecord(GetPhysicalAddress(recordAddress));
                var allocatedSize = logRecord.AllocatedSize;
                if (allocatedSize <= 0)
                    ThrowTsavoriteException($"LogRecord size should be > 0; encountered {allocatedSize}");

                recordAddress += allocatedSize;
                if (recordAddress > stopAddress)
                    ThrowTsavoriteException($"Unaligned end of page; record exceeded page by {recordAddress - stopAddress} bytes");

                if (logRecord.Info.Valid && logRecord.DataHeader.RecordHasObjects)
                {
                    _ = logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength);
                    overBudgetAmount -= keyLength + (long)valueLength;
                    if (overBudgetAmount <= 0)
                        return recordAddress;
                }
            }

            return stopAddress;
        }

        /// <summary>
        /// Iterator interface for scanning Tsavorite log
        /// </summary>
        /// <returns></returns>
        public override ITsavoriteScanIterator Scan(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, DiskScanBufferingMode diskScanBufferingMode = DiskScanBufferingMode.DoublePageBuffering, bool includeClosedRecords = false)
            => new ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>>(store, this, beginAddress, endAddress, epoch, diskScanBufferingMode, includeClosedRecords: includeClosedRecords);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                long beginAddress, long endAddress, ref TScanFunctions scanFunctions, DiskScanBufferingMode scanBufferingMode)
        {
            using ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, beginAddress, endAddress, epoch, scanBufferingMode, includeClosedRecords: false, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor, long maxAddress,
                bool resetCursor = true, bool includeTombstones = false)
        {
            using ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, cursor, endAddress, epoch, DiskScanBufferingMode.SinglePageBuffering,
                includeClosedRecords: maxAddress < long.MaxValue, logger: logger);
            return ScanLookup<long, long, TScanFunctions, ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>>>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor,
                maxAddress, resetCursor: resetCursor, includeTombstones: includeTombstones);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TKey, TScanFunctions>(TsavoriteKV<TStoreFunctions, ObjectAllocator<TStoreFunctions>> store,
                TKey key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, beginAddress, epoch, logger: logger);
            return IterateHashChain(store, key, beginAddress, ref scanFunctions, iter);
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator> observer)
        {
            using var iter = new ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>>(store: null, this, beginAddress, endAddress, epoch, DiskScanBufferingMode.NoBuffering, InMemoryScanBufferingMode.NoBuffering,
                    includeClosedRecords: false, assumeInMemory: true, logger: logger);
            observer?.OnNext(iter);
        }

    }
}