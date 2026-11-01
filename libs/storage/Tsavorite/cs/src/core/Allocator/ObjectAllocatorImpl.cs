// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
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

        /// <summary>The object log that ReadOnly flushes write and reads resolve against; null until first needed, and always null when
        /// <see cref="HasPersistentObjectLog"/> is false. While a downlevel checkpoint is being up-converted, flushes write
        /// <see cref="upgradeObjectLog"/> instead.</summary>
        ObjectLog mainObjectLog;

        /// <summary>Destination of a downlevel object-log up-conversion while it runs: an object log over <see cref="upgradeObjectLogDevice"/>, or
        /// <see cref="mainObjectLog"/> itself for a self-contained snapshot. Null otherwise.</summary>
        ObjectLog upgradeObjectLog;

        /// <summary>A checkpoint's snapshot object log, opened by recovery to read snapshot-region objects and to copy them into the main object
        /// log; and the device it is over, which recovery passes to select it.</summary>
        ObjectLog recoverySnapshotObjectLog;
        IDevice recoverySnapshotObjectLogDevice;

        /// <summary>The captures of the Snapshot checkpoint in progress: published at its PREPARE, before any (v+1) CopyUpdate can supersede a (v)
        /// value, and withdrawn when it completes or aborts. Null otherwise.</summary>
        SnapshotObjectLogState snapshotObjectLogState;

        /// <summary>The tag of the last Snapshot checkpoint published; see <see cref="SnapshotObjectLogState.Tag"/>.</summary>
        int lastSnapshotCheckpointTag;

        ObjectLogRecordRef recoveryHighWaterReference;

        /// <summary>The epoch shared by this allocator's object logs, created with the first of them. Sharing it keeps each store to one more
        /// <see cref="LightEpoch"/> instance, of which a process has a fixed number.</summary>
        LightEpoch objectLogEpoch;

        /// <summary>Serializes object-log creation and replacement.</summary>
        readonly object objectLogLock = new();

        /// <summary>Page size, in bits, of this allocator's object logs.</summary>
        readonly int objectLogPageSizeBits;

        /// <summary>Pages in the in-memory ring of each of this allocator's object logs.</summary>
        readonly int objectLogPageCount;

        /// <summary>Segment size, in bits, of this allocator's object-log devices.</summary>
        readonly int objectLogSegmentSizeBits;

        /// <summary>Chunking parameters for writes to this allocator's object logs.</summary>
        readonly ObjectLogWriterSettings objectLogWriterSettings;

        /// <summary>
        /// We use the LastIssued here because we don't want <see cref="OnPagesMarkedReadOnlyWorker"/> to wait for IO to complete which is when
        /// FlushedUntilAddress is updated. Instead, LastIssuedFlushedUntilAddress is the proxy for it: it is updated with flushEndAddress
        /// immediately before issuing the flush. Publishing before WriteAsync can suspend the epoch also lets Snapshot PREPARE's epoch
        /// barrier capture every worker that took the pre-coordination fast path.
        /// </summary>
        long LastIssuedFlushedUntilAddress;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private protected override long GetLastIssuedReadOnlyFlushAddress()
            => Volatile.Read(ref LastIssuedFlushedUntilAddress);

        /// <summary>
        /// Whether <paramref name="logicalAddress"/> is in a read-only flush that is committed but not yet durable, so that its record
        /// image must not be mutated.
        /// </summary>
        /// <remarks>
        /// This allocator resolves objectIds against the live <see cref="ObjectIdMap"/>, stamps ObjectLogPosition and size hints
        /// into the live record, and writes the live page to the device with the epoch suspended. Disposal in that window
        /// corrupts the image two ways: releasing the heap can persist a Valid record whose ObjectLogPosition refers to a freed -
        /// or recycled, hence unrelated - object, and <see cref="LogRecord.ClearOptionals"/> zeroes the ObjectLogPosition field
        /// this flush just stamped. Such a record keeps its heap and metadata until the window closes; its release is marked
        /// with <see cref="RecordInfo.DeferredDispose"/> and run by the drain on flush completion, or by eviction.
        /// <para>
        /// The upper bound is <see cref="AllocatorBase{TStoreFunctions, TAllocator}.SafeReadOnlyAddress"/>, not
        /// <see cref="LastIssuedFlushedUntilAddress"/>: <see cref="OnPagesMarkedReadOnly"/> advances SafeReadOnlyAddress first and
        /// only then publishes LastIssued and issues the flush, so a thread that already sees an address as immutable can still
        /// see LastIssued below it. SafeReadOnlyAddress is where a flush of the address becomes committed, so it is the first
        /// bound that cannot be observed too late.
        /// </para>
        /// <para>
        /// A record below <see cref="AllocatorBase{TStoreFunctions, TAllocator}.FlushedUntilAddress"/> is free, even one that shares a
        /// sector with it. The next flush starts in that sector and writes it whole, but it takes the bytes below its start from
        /// <see cref="PartialSectorImage"/> or the device, never from the live page, so a later change to such a record cannot reach
        /// the image a completed checkpoint recovers.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsFrozenForFlush(long logicalAddress)
            => logicalAddress < SafeReadOnlyAddress && logicalAddress >= FlushedUntilAddress;

        /// <summary>
        /// Dynamically extended Flush end address, used by <see cref="OnPagesMarkedReadOnlyWorker"/>
        /// </summary>
        long OngoingFlushedUntilAddress;

        /// <summary>
        /// If the "noFlush" option on <see cref="AllocatorBase{TStoreFunctions, TAllocator}.ShiftReadOnlyAddress(long, bool)"/> is true, we won't try to flush anything below that.
        /// </summary>
        long NoFlushUntilAddress;

        /// <summary>
        /// The durable part of the sector the last ReadOnly flush ended in, which the next flush writes again. Null if that flush ended on a
        /// sector boundary, or if no ReadOnly flush has run since the allocator was initialized, reset, or recovered.
        /// </summary>
        PartialSectorImage lastPartialSector;

        /// <summary>
        /// The bytes a ReadOnly flush wrote for [<see cref="SectorStart"/>, <see cref="SectorStart"/> + <see cref="Length"/>), the durable
        /// part of the sector that flush ended in.
        /// </summary>
        /// <remarks>
        /// Device writes are sector-aligned, so the next flush, which starts where this one ended, writes the whole sector again. It must not
        /// take these bytes from the live page. They are durable and may be a completed checkpoint's image, while the records they hold may
        /// since have changed in memory: released after being superseded, or rewritten by recovery's object load. So the next flush writes
        /// them from here, or reads them back from the device when there is no image, as for the first flush after recovery.
        /// </remarks>
        sealed class PartialSectorImage
        {
            /// <summary>Logical address of the start of the sector.</summary>
            internal readonly long SectorStart;

            /// <summary>Number of bytes, from the start of the sector, that the flush wrote.</summary>
            internal readonly int Length;

            /// <summary>The bytes written; null if the range ending here was skipped rather than written. A range is skipped when it lies
            /// below BeginAddress or below a no-flush boundary, and nothing reads such bytes, so the next flush may take them from the live page.</summary>
            internal readonly byte[] Bytes;

            internal PartialSectorImage(long sectorStart, int length, byte[] bytes)
            {
                SectorStart = sectorStart;
                Length = length;
                Bytes = bytes;
            }
        }

        // Default to max sizes so testing a size as "greater than" will always be false
        readonly int maxInlineKeySize;
        readonly int maxInlineValueSize;

        /// <summary>Read buffers for the downlevel (cv7) object-log reader; current-format reads go through <see cref="ObjectLog"/>.</summary>
        readonly int numberOfDeserializationBuffers;

        /// <summary>Default number of concurrently issued Snapshot pages; tune it from checkpoint throughput and foreground-latency
        /// measurements.</summary>
        const int DefaultSnapshotFlushWindowSize = 64;

        /// <summary>The device under <see cref="mainObjectLog"/>. While a downlevel checkpoint is being up-converted it holds the downlevel object
        /// log, which the legacy reader reads; <see cref="CompleteObjectLogUpgrade"/> repoints it to <see cref="upgradeObjectLogDevice"/>.</summary>
        private IDevice objectLogDevice;

        /// <summary>Device receiving up-converted object bytes while recovering a downlevel checkpoint; null otherwise.
        /// <see cref="objectLogDevice"/> is the read source for that conversion.</summary>
        private readonly IDevice upgradeObjectLogDevice;

        /// <summary>Whether a downlevel object-log up-conversion is in progress; set for the duration of the ascending conversion pass.</summary>
        private bool isUpgradingObjectLog;

        /// <summary>Whether a downlevel object-log up-conversion has completed, so the live object log is the one the conversion wrote.</summary>
        private bool objectLogWasUpgraded;

        /// <summary>The free pages of the log</summary>
        private readonly OverflowPool<PageUnit<ObjectPage>> freePagePool;

        /// <summary>Segment size</summary>
        private readonly long ObjectLogSegmentSize;

        /// <inheritdoc/>
        public override string ToString() => BaseToString($" (LI {LastIssuedFlushedUntilAddress}, OG {OngoingFlushedUntilAddress}, No {NoFlushUntilAddress})");

        public ObjectAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, ObjectAllocator<TStoreFunctions>> wrapperCreator)
            : base(settings, storeFunctions, wrapperCreator, settings.logger, transientObjectIdMap: new ObjectIdMap())
        {
            objectLogDevice = settings.LogSettings.ObjectLogDevice;
            upgradeObjectLogDevice = settings.LogSettings.UpgradeObjectLogDevice;

            maxInlineKeySize = settings.LogSettings.MaxInlineKeySize;
            maxInlineValueSize = settings.LogSettings.MaxInlineValueSize;

            ObjectLogSegmentSize = 1L << settings.LogSettings.ObjectLogSegmentSizeBits;

            freePagePool = new OverflowPool<PageUnit<ObjectPage>>(4, static p => { });
            pageHeaderSize = PageHeader.Size;

            SnapshotFlushWindowSize = Math.Min(DefaultSnapshotFlushWindowSize, BufferSize);

            if (settings.LogSettings.NumberOfDeserializationBuffers < LogSettings.kMinDeserializationBuffers || settings.LogSettings.NumberOfDeserializationBuffers > LogSettings.kMaxDeserializationBuffers || !IsPowerOfTwo(settings.LogSettings.NumberOfDeserializationBuffers))
                throw new TsavoriteException($"{nameof(settings.LogSettings.NumberOfDeserializationBuffers)} must be between {LogSettings.kMinDeserializationBuffers} and {LogSettings.kMaxDeserializationBuffers - 1} and a power of 2");
            numberOfDeserializationBuffers = settings.LogSettings.NumberOfDeserializationBuffers;

            objectLogSegmentSizeBits = settings.LogSettings.ObjectLogSegmentSizeBits;
            if (objectLogSegmentSizeBits is < LogSettings.kMinObjectLogSegmentSizeBits or > LogSettings.kMaxSegmentSizeBits)
                throw new TsavoriteException($"{nameof(KVSettings.ObjectLogSegmentSize)} (2^{objectLogSegmentSizeBits}) must be between 2^{LogSettings.kMinObjectLogSegmentSizeBits} and 2^{LogSettings.kMaxSegmentSizeBits} bytes");

            // An object-log page bounds a single chunk, and a device write never spans a segment, so the page is at most the segment.
            objectLogPageSizeBits = settings.LogSettings.ObjectLogPageSizeBits > 0
                ? settings.LogSettings.ObjectLogPageSizeBits
                : Math.Min(ObjectLog.DefaultPageSizeBits, objectLogSegmentSizeBits);
            if (objectLogPageSizeBits < LogSettings.kMinPageSizeBits || objectLogPageSizeBits > LogSettings.kMaxPageSizeBits || objectLogPageSizeBits > objectLogSegmentSizeBits)
                throw new TsavoriteException($"{nameof(KVSettings.ObjectLogPageSize)} (2^{objectLogPageSizeBits}) must be between 2^{LogSettings.kMinPageSizeBits} and 2^{LogSettings.kMaxPageSizeBits} bytes, and no larger than {nameof(KVSettings.ObjectLogSegmentSize)} (2^{objectLogSegmentSizeBits})");

            objectLogPageCount = ObjectLog.DefaultPageCount;
            var objectLogMemorySizeBits = settings.LogSettings.ObjectLogMemorySizeBits;
            if (objectLogMemorySizeBits > 0)
            {
                var pageCount = objectLogMemorySizeBits >= objectLogPageSizeBits ? 1L << (objectLogMemorySizeBits - objectLogPageSizeBits) : 0;
                if (pageCount is < ObjectLog.MinPageCount or > ObjectLog.MaxPageCount)
                    throw new TsavoriteException($"{nameof(KVSettings.ObjectLogMemorySize)} (2^{objectLogMemorySizeBits}) must hold between {ObjectLog.MinPageCount} and {ObjectLog.MaxPageCount} pages of {nameof(KVSettings.ObjectLogPageSize)} (2^{objectLogPageSizeBits})");
                objectLogPageCount = (int)pageCount;
            }
            objectLogWriterSettings = settings.LogSettings.ObjectLogWriterSettings ?? ObjectLogWriterSettings.Default;

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
            lastPartialSector = null;
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
                if (useNativeLogPages)
                {
                    // Zero the inline page bytes and clear the ObjectIdMap before pooling the direct-VM block:
                    // FreeNativeLogPage recycles it, and AllocatePinnedPageArray reuses a pooled block without
                    // re-zeroing. Leaving stale bytes would let a new page that does not fully overwrite them retain
                    // stale record headers that eviction reads as valid records. ClearPage does both.
                    ClearPage(index, 0);
                    FreeNativeLogPage(index);
                }
                else
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
                }
                pageArrays[index] = default;
                pagePointers[index] = default;
                _ = Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        internal void FreePage(int page)
        {
            // If the logSizeTracker is not active, then all pages are used once allocated so there's nothing to add to the overflow pool.
            if (logSizeTracker is not null)
                ReturnPage(page % BufferSize);
            else
            {
                objectPages[page % BufferSize].Clear();
                ClearPage(page, 0);
            }
        }

        internal override void ClearPage(int page, int offset = 0)
        {
            var index = page % BufferSize;

            // Offset is nonzero only for RecoveryReset, to zero out the page past offset (which is tailAddress).
            // In this case, we want to keep the objectPage information for the used (so far) part of the page.
            if (offset == 0)
                objectPages[index].Clear();
            base.ClearPage(index, offset);

            // A page cleared from its start may be reused for a later page without being allocated again, so it needs a fresh header. A zeroed
            // header's floor reads as a stamped floor of 0, which a flush's stamp does not replace and which holds main object-log truncation back.
            if (offset == 0 && pagePointers[index] != 0)
                PageHeader.Initialize(pagePointers[index]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateLogRecord(long logicalAddress) => CreateLogRecord(logicalAddress, GetPhysicalAddress(logicalAddress));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateLogRecord(long logicalAddress, long physicalAddress) => new(physicalAddress, objectPages[GetPageIndexForAddress(logicalAddress)].objectIdMap);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryCreateRemappedLogRecordOverPinnedTransientMemory(long logicalAddress, long physicalAddress, out LogRecord logRecord)
            => LogRecord.TryCreateRemappedOverPinnedTransientMemory(physicalAddress, objectPages[GetPageIndexForAddress(logicalAddress)].objectIdMap, transientObjectIdMap,
                out logRecord);

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
                    ExtendedNamespaceSize = RecordNamespace.GetExtendedNamespaceSize(in key),
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
            // Key. A zero-length key is inline by definition, so it never takes the overflow path.
            if (sizeInfo.FieldInfo.KeySize < 0)
                ThrowTsavoriteException($"Key length cannot be negative (got {sizeInfo.FieldInfo.KeySize})");
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

        /// <inheritdoc/>
        internal override void DrainDeferredDisposes(long fromAddress, long untilAddress)
        {
            // Anything below HeadAddress has been evicted, and EvictRecordsInRange honors the mark, so clamping drops no obligation --
            // eviction is the fallback for every record this drain does not reach. Reading HeadAddress inside the epoch-protected
            // region is what makes the pages at or above it safe to walk: their eviction cannot run until the caller suspends.
            var address = fromAddress < HeadAddress ? HeadAddress : fromAddress;

            while (address < untilAddress)
            {
                var page = GetPage(address);
                var pageEndAddress = GetLogicalAddressOfStartOfPage(page + 1);
                var stopAddress = untilAddress < pageEndAddress ? untilAddress : pageEndAddress;

                var firstValidAddress = GetFirstValidLogicalAddressOnPage(page);
                if (address < firstValidAddress)
                    address = firstValidAddress;

                var pageIndex = GetPageIndexForAddress(address);
                if (IsAllocated(pageIndex))
                {
                    var objectIdMap = objectPages[pageIndex].objectIdMap;
                    while (address < stopAddress)
                    {
                        var logRecord = new LogRecord(GetPhysicalAddress(address), objectIdMap);
                        var allocatedSize = logRecord.AllocatedSize;
                        if (allocatedSize <= 0)
                            break;

                        var offset = GetOffsetOnPage(address);
                        if (offset == 0 || offset + allocatedSize > PageSize)
                            break;

                        ref var recordInfo = ref logRecord.InfoRef;
                        if (recordInfo.TryTakeDeferredDispose(out var copyUpdate))
                        {

                            // Valid matters as much as Sealed. SealAndInvalidate yields Sealed and not Valid, which is an elided or
                            // free-listed record whose disposal already ran, and a marked record can be elided once the freeze lifts
                            // but before this drain fires. Requiring Valid is what keeps it from being disposed a second time.
                            // Captured CopyUpdate sources may also be sealed, so their explicit transient classification takes priority.
                            if (recordInfo.Valid && copyUpdate)
                                ReleaseCopyUpdatedSourceValue(ref logRecord);
                            else if (recordInfo.IsValidAndSealed)
                                OnDispose(ref logRecord, DisposeReason.Deleted);
                            else if (recordInfo.Valid)
                                ReleaseCopyUpdatedSourceValue(ref logRecord);
                        }

                        address += allocatedSize;
                    }
                }

                address = stopAddress;
            }
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

                // Honor a deferred disposal here if eviction reaches the record before the drain does. Clearing the mark without
                // disposing would resurrect exactly the leak this exists to close: eviction calls only OnEvict, so a deleted
                // record's external resources would never be released. Running it first reproduces the ordinary sequence --
                // disposal at the supersede site, then eviction -- so the heap accounting below sees a value-cleared record and
                // does not double-count. Invalid records were skipped above, which is what keeps an elided record, whose
                // disposal already ran, from being disposed twice.
                if (logRecord.InfoRef.TryTakeDeferredDispose(out var copyUpdate))
                {
                    if (copyUpdate)
                        ReleaseCopyUpdatedSourceValue(ref logRecord);
                    else if (logRecord.Info.IsValidAndSealed)
                        OnDispose(ref logRecord, DisposeReason.Deleted);
                    else
                        ReleaseCopyUpdatedSourceValue(ref logRecord);   // A frozen CopyUpdate source; Valid, as Invalid was skipped above
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
            // Stop the resizer before clearing objectPages, for the same reason AllocatorBase.Dispose() stops it before
            // tearing down the resources it owns: a resizer already inside ShiftAddresses can reach EvictRecordsInRange,
            // which dereferences objectPages, and the resulting NullReferenceException is escalated to FailFast by
            // OnPagesClosedWorker. This is the same helper base.Dispose() uses, so it sets disposed first (releasing a
            // resizer parked in an eviction spin-wait) and is a no-op when base.Dispose() calls it again below.
            StopSizeTrackerForDispose();

            var localValues = Interlocked.Exchange(ref objectPages, null);
            if (localValues != null)
            {
                freePagePool.Dispose();
                foreach (var value in localValues)
                    value.Clear();

                // Before base.Dispose: disposing an object log fails the flush waiters of pages still in flight, whose callbacks run against
                // this allocator.
                DisposeObjectLogs();
                base.Dispose();
            }
        }

        /// <summary>Dispose every object log this allocator owns, then their shared epoch.</summary>
        void DisposeObjectLogs()
        {
            lock (objectLogLock)
            {
                CloseRecoverySnapshotObjectLogCore();
                if (upgradeObjectLog is not null && !ReferenceEquals(upgradeObjectLog, mainObjectLog))
                    upgradeObjectLog.Dispose();
                upgradeObjectLog = null;
                mainObjectLog?.Dispose();
                mainObjectLog = null;
                objectLogEpoch?.Dispose();
                objectLogEpoch = null;
            }
        }

        /// <summary>Whether this allocator writes its objects to a persistent object log: it has an object-log device and a persisted main log.
        /// Without one, flushes write inline images only, as for the read cache and the in-memory stores of iterators and compaction.</summary>
        bool HasPersistentObjectLog => objectLogDevice is not null and not NullDevice && device is not NullDevice;

        /// <summary>Create an object log over <paramref name="logDevice"/> with this allocator's geometry and shared epoch.</summary>
        /// <remarks>The caller holds <see cref="objectLogLock"/>.</remarks>
        ObjectLog CreateObjectLogCore(IDevice logDevice)
        {
            objectLogEpoch ??= new LightEpoch();
            return new ObjectLog(new ObjectLogSettings
            {
                LogDevice = logDevice,
                PageSizeBits = objectLogPageSizeBits,
                PageCount = objectLogPageCount,
                SegmentSizeBits = objectLogSegmentSizeBits,
                Epoch = objectLogEpoch,
                Logger = logger,
            });
        }

        /// <summary>The main object log, created on first use.</summary>
        ObjectLog EnsureMainObjectLog()
        {
            var log = Volatile.Read(ref mainObjectLog);
            if (log is not null)
                return log;
            lock (objectLogLock)
            {
                if (mainObjectLog is null)
                    Volatile.Write(ref mainObjectLog, CreateObjectLogCore(objectLogDevice));
                return mainObjectLog;
            }
        }

        /// <summary>The object log that ReadOnly and recovery flushes write: the up-conversion's destination while one runs, otherwise the main
        /// object log. Null when this allocator has no persistent object log.</summary>
        ObjectLog FlushObjectLog => isUpgradingObjectLog ? upgradeObjectLog : HasPersistentObjectLog ? EnsureMainObjectLog() : null;

        /// <inheritdoc/>
        internal override ObjectLog RecoveryFlushObjectLog => FlushObjectLog;

        /// <inheritdoc/>
        internal override ObjectLog GetRecoverySnapshotObjectLog(IDevice snapshotObjectLogDevice)
            => ReferenceEquals(snapshotObjectLogDevice, recoverySnapshotObjectLogDevice) ? recoverySnapshotObjectLog : null;

        /// <summary>The object log a current-format read resolves against: the main object log when <paramref name="logDevice"/> is null, otherwise
        /// the recovery snapshot object log over that device.</summary>
        ObjectLog GetReadObjectLog(IDevice logDevice)
        {
            if (logDevice is null)
                return Volatile.Read(ref mainObjectLog) ?? throw new TsavoriteException("A record references the object log, but this store has none open");
            if (ReferenceEquals(logDevice, recoverySnapshotObjectLogDevice))
                return recoverySnapshotObjectLog;
            throw new TsavoriteException("No object log is open over the device a recovery read requested");
        }

        /// <inheritdoc/>
        internal override ObjectLog CreateSnapshotObjectLog(IDevice snapshotObjectLogDevice)
        {
            if (snapshotObjectLogDevice is null or NullDevice)
                return null;
            lock (objectLogLock)
                return CreateObjectLogCore(snapshotObjectLogDevice);
        }

        /// <inheritdoc/>
        internal override SnapshotObjectLogState BeginSnapshotObjectLog(ObjectLog snapshotObjectLog)
        {
            SnapshotObjectLogState captures = null;
            if (snapshotObjectLog is not null)
            {
                // Tags wrap at MaxCheckpointTag. Two checkpoints could be confused only if an object kept a stamp or capture through that
                // many later checkpoints without its record being flushed or evicted.
                var tag = Interlocked.Increment(ref lastSnapshotCheckpointTag) & HeapObjectBase.MaxCheckpointTag;
                captures = new SnapshotObjectLogState(snapshotObjectLog, tag);
            }
            Volatile.Write(ref snapshotObjectLogState, captures);
            return captures;
        }

        /// <inheritdoc/>
        internal override SnapshotObjectLogState ActiveSnapshotObjectLog => Volatile.Read(ref snapshotObjectLogState);

        /// <inheritdoc/>
        internal override void EndSnapshotObjectLog(SnapshotObjectLogState captures)
        {
            if (captures is not null)
                _ = Interlocked.CompareExchange(ref snapshotObjectLogState, null, captures);
        }

        /// <inheritdoc/>
        /// <remarks>
        /// <para>The capture claims the source object, which waits out a flush that is writing it, and then acts on what was published:</para>
        /// <list type="bullet">
        /// <item>The ReadOnly flush wrote the record: its main-log image holds the <c>(v)</c> value, and its device write may still be reading
        /// the record. Capture nothing. If the record is at or above the main-log recovery end, recovery reads its snapshot image instead,
        /// which the Snapshot flush wrote before the ReadOnly flush could.</item>
        /// <item>This checkpoint's Snapshot flush wrote the record: its stamp is the <c>(v)</c> value in this checkpoint's object log, and its
        /// device write may still be reading the record. Only set the marker, so the ReadOnly flush keeps that stamp. The marker is one
        /// <see cref="RecordInfo"/> word, so a device sees it set or clear.</item>
        /// <item>Otherwise no flush of this checkpoint has published the record, so none is reading its fields: write the value to this
        /// checkpoint's object log, stamp the record, then set the marker.</item>
        /// </list>
        /// <para>So no capture rewrites a record's position or hints under a device write, and the flushes can write the live page.</para>
        /// </remarks>
        internal override bool TryCaptureCopyUpdateSource(SnapshotObjectLogState captures, ref LogRecord sourceRecord, long logicalAddress)
        {
            if (!captures.TryBeginCapture())
                return false;
            HeapObjectBase sourceObject = null;
            var claimed = false;
            var finalWord = HeapObjectBase.SupersededWord;
            var captured = true;
            try
            {
                // The source is a (v) record held by this CopyUpdate, so its components cannot change until PostCopyUpdater runs.
                if (!sourceRecord.TryGetOutOfLineComponents(out var keyOverflow, out _, out var valueObject))
                    throw new TsavoriteException($"Could not capture the out-of-line components of the CopyUpdate source at {logicalAddress}");

                sourceObject = valueObject as HeapObjectBase
                    ?? throw new TsavoriteException($"The CopyUpdate source at {logicalAddress} does not support Snapshot capture");
                claimed = sourceObject.TryClaim(out var priorWord);
                if (!claimed)
                {
                    // A capture follows a successful CAS, so the source cannot have been superseded before it.
                    Debug.Fail($"The CopyUpdate source at {logicalAddress} was already superseded");
                    return HeapObjectBase.IsCapturedBy(priorWord, captures.Tag);
                }

                if (HeapObjectBase.IsReadOnlyStamped(priorWord))
                    captured = false;
                else if (HeapObjectBase.IsSnapshotStampedBy(priorWord, captures.Tag))
                {
                    sourceRecord.InfoRef.HasSnapshotObjectLogReference = true;
                    finalWord = HeapObjectBase.CapturedWord(captures.Tag);
                }
                else
                {
                    ObjectLogWriteResult result;
                    HeapObjectBase.BeginClaimedWrite(sourceObject);
                    try
                    {
                        using var writer = new ObjectLogWriter<TStoreFunctions>(captures.Log, storeFunctions, objectLogWriterSettings);
                        result = writer.WriteRecordObjects(in keyOverflow, default, valueIsObject: true, valueObject);
                    }
                    finally
                    {
                        HeapObjectBase.EndClaimedWrite();
                    }

                    // Stamp before marking: a flush that sees the marker keeps the stamp.
                    sourceRecord.SetObjectLogPositionAndHints(in result);
                    sourceRecord.InfoRef.HasSnapshotObjectLogReference = true;
                    finalWord = HeapObjectBase.CapturedWord(captures.Tag);
                }
            }
            catch (Exception ex)
            {
                // The checkpoint cannot recover this record's (v) value, so it fails; the operation that superseded the record proceeds.
                captures.RecordFailure(ex);
            }
            finally
            {
                if (claimed)
                    sourceObject.ReleaseClaim(finalWord);
                captures.EndCapture();
            }
            return captured;
        }

        /// <inheritdoc/>
        /// <remarks>
        /// <para>The new record shares the source's array A, and its PostCopyUpdater changes A next. The capture claims A, which waits out a flush
        /// that is writing it, and then acts on what was published for the source, as <see cref="TryCaptureCopyUpdateSource"/> does for an
        /// object:</para>
        /// <list type="bullet">
        /// <item>The ReadOnly flush wrote the source: capture nothing. A stays shared, as it is outside a checkpoint.</item>
        /// <item>This checkpoint's Snapshot flush wrote the source: only set the marker.</item>
        /// <item>Otherwise: write the key and A to this checkpoint's object log, stamp the source, then set the marker.</item>
        /// </list>
        /// <para>A's phase describes only the record at its owner address: another record that shares A may have published it, and then the
        /// source is unpublished. In the last two cases the capture keeps the claim through PostCopyUpdater, which may read the source's value;
        /// <see cref="CompleteOverflowCapture"/> then detaches the source. A flush that read the source's slot before that waits for the claim
        /// and finds the sentinel when it checks the slot again.</para>
        /// </remarks>
        internal override bool TryCaptureCopyUpdateOverflowSource(SnapshotObjectLogState captures, ref LogRecord sourceRecord, long logicalAddress)
        {
            if (!captures.TryBeginCapture())
                return false;
            OverflowByteArray valueOverflow = default;
            var claimed = false;
            var holdClaim = false;
            int priorWord = 0;
            long priorOwner = 0;
            try
            {
                // The source is a (v) record held by this CopyUpdate, so its components cannot change until PostCopyUpdater runs.
                if (!sourceRecord.TryGetOutOfLineComponents(out var keyOverflow, out valueOverflow, out _) || valueOverflow.IsEmpty)
                    throw new TsavoriteException($"Could not capture the out-of-line components of the CopyUpdate source at {logicalAddress}");

                valueOverflow.ClaimForPublication(out priorWord, out priorOwner);
                claimed = true;
                var publishedSource = priorOwner == logicalAddress;
                if (publishedSource && HeapObjectBase.IsReadOnlyStamped(priorWord))
                    return false;

                if (publishedSource && HeapObjectBase.IsSnapshotStampedBy(priorWord, captures.Tag))
                    sourceRecord.InfoRef.HasSnapshotObjectLogReference = true;
                else
                {
                    using var writer = new ObjectLogWriter<TStoreFunctions>(captures.Log, storeFunctions, objectLogWriterSettings);
                    var result = writer.WriteRecordObjects(in keyOverflow, in valueOverflow, valueIsObject: false, valueObject: null);

                    // Stamp before marking: a flush that sees the marker keeps the stamp.
                    sourceRecord.SetObjectLogPositionAndHints(in result);
                    sourceRecord.InfoRef.HasSnapshotObjectLogReference = true;
                }
                holdClaim = true;
                return true;
            }
            catch (Exception ex)
            {
                // The checkpoint cannot recover this record's (v) value, so it fails; the operation that superseded the record proceeds.
                captures.RecordFailure(ex);
                return false;
            }
            finally
            {
                if (claimed && !holdClaim)
                    valueOverflow.ReleasePublicationClaim(priorWord, priorOwner);
                captures.EndCapture();
            }
        }

        /// <inheritdoc/>
        /// <remarks>Points the source's value slot at the checkpoint's sentinel and credits the source's share of the array's heap, which the new
        /// record also counts; eviction then subtracts the sentinel's share. The array is released at REST: it now belongs to the new record alone,
        /// which no flush has published. The source's bytes are unchanged, so this is safe while a device write of its page is in flight.</remarks>
        internal override void CompleteOverflowCapture(SnapshotObjectLogState captures, ref LogRecord sourceRecord)
        {
            // The capture holds the claim, so the slot still holds the array the new record shares.
            var valueOverflow = sourceRecord.ValueOverflow;
            sourceRecord.ReplaceValueOverflowSlot(captures.OverflowSentinel);
            var credit = valueOverflow.HeapMemorySize - captures.OverflowSentinel.HeapMemorySize;
            if (credit != 0)
                logSizeTracker?.IncrementSize(-credit);
            valueOverflow.ReleasePublicationClaim((int)SerializationPhase.REST, owner: 0);
        }

        /// <inheritdoc/>
        internal override void InspectRecoveryObjectLogReference(in LogRecord record)
        {
            if (!record.DataHeader.RecordHasObjects || record.Info.HasSnapshotObjectLogReference)
                return;
            var reference = record.GetObjectLogRecordRef();
            var address = reference.PositionWord & ObjectLogPositionInfo.AddressMask;
            var highAddress = recoveryHighWaterReference.PositionWord & ObjectLogPositionInfo.AddressMask;
            if (!reference.IsUnstamped && (address > highAddress
                || (address == highAddress && recoveryHighWaterReference.ExtentIsEmpty && !reference.ExtentIsEmpty)))
                recoveryHighWaterReference = reference;
        }

        /// <inheritdoc/>
        internal override void BeginMainObjectLogRecovery(in HybridLogRecoveryInfo info)
        {
            var referenced = info.useSnapshotFile == 0 || info.mainLogRecoveryEndAddress > info.beginAddress;
            RestoreObjectLog(referenced ? info.objectLogBeginAddress : info.hlogEndObjectLogTail, info.hlogEndObjectLogTail);
            recoveryHighWaterReference = default;
            if (mainObjectLog is not null)
                mainObjectLog.RecoveryReadOnly = true;
        }

        /// <inheritdoc/>
        internal override void CompleteMainObjectLogRecovery(in HybridLogRecoveryInfo info, long residentHeadAddress)
        {
            if (mainObjectLog is null)
                return;
            var tail = Math.Max(info.hlogEndObjectLogTail, mainObjectLog.TailAddress);
            if ((recoveryHighWaterReference.PositionWord & ObjectLogPositionInfo.AddressMask) != 0)
                tail = Math.Max(tail, mainObjectLog.GetReferencedExtentEnd(in recoveryHighWaterReference));
            mainObjectLog.RecoveryReadOnly = false;
            mainObjectLog.RestoreTail(mainObjectLog.BeginAddress, tail);
            if (info.useSnapshotFile == 0 || info.snapshotObjectLogStartAddress >= info.mainLogRecoveryEndAddress)
                return;
            if (recoverySnapshotObjectLog is null)
                throw new TsavoriteException("Snapshot-referenced main-log records require the selected snapshot object log");

            using var reader = new ObjectLogReader<TStoreFunctions>(recoverySnapshotObjectLog, storeFunctions);
            using var writer = new ObjectLogWriter<TStoreFunctions>(mainObjectLog, storeFunctions, objectLogWriterSettings);
            var firstAddress = Math.Max(info.beginAddress, info.snapshotObjectLogStartAddress);
            var lastAddress = info.mainLogRecoveryEndAddress;
            for (var page = GetPage(firstAddress); GetLogicalAddressOfStartOfPage(page) < lastAddress; page++)
            {
                var pageStart = GetLogicalAddressOfStartOfPage(page);
                var pageEnd = Math.Min(GetLogicalAddressOfStartOfPage(page + 1), lastAddress);
                ComputeAlignedReadExtent(pageStart, (int)(pageEnd - pageStart), out var deviceAddress, out var readLength, out var validOffset);
                var buffer = bufferPool.Get((int)readLength, clearOnReturn: false);
                try
                {
                    if (!TryReadDevice(device, deviceAddress, (IntPtr)buffer.aligned_pointer, readLength, "Snapshot reference repair",
                        out var errorCode, out var bytesRead, out var ioException) || bytesRead < validOffset + pageEnd - pageStart)
                        throw new TsavoriteException($"Could not read main-log page {page} for Snapshot reference repair: error {errorCode}", ioException);
                    var dirty = false;
                    var address = Math.Max(firstAddress, GetFirstValidLogicalAddressOnPage(page));
                    while (address < pageEnd)
                    {
                        var record = new LogRecord(buffer.aligned_pointer + validOffset + (address - pageStart));
                        if (record.Info.IsNull)
                        {
                            address += RecordInfo.Size;
                            continue;
                        }
                        var size = record.AllocatedSize;
                        if (size <= 0 || address + size > pageEnd)
                            throw new TsavoriteException($"Invalid record extent at {address} during Snapshot reference repair");
                        if (record.Info.Valid && record.Info.HasSnapshotObjectLogReference)
                        {
                            var reference = record.GetObjectLogRecordRef();
                            reader.BeginRecord(in reference);
                            var result = reader.CopyRecordTo(writer);
                            reader.EndRecord();
                            record.SetObjectLogPositionAndHints(in result);
                            record.InfoRef.HasSnapshotObjectLogReference = false;
                            if (address >= residentHeadAddress && IsAllocated(GetPageIndexForPage(page)))
                            {
                                var resident = new LogRecord(GetPhysicalAddress(address));
                                resident.SetObjectLogPositionAndHints(in result);
                                resident.InfoRef.HasSnapshotObjectLogReference = false;
                            }
                            dirty = true;
                        }
                        address += size;
                    }
                    if (dirty)
                    {
                        _ = mainObjectLog.FlushTail(wait: true);
                        if (!TryWriteDevice(device, (IntPtr)buffer.aligned_pointer, deviceAddress, readLength, out errorCode, out ioException))
                            throw new TsavoriteException($"Could not persist Snapshot reference repair for main-log page {page}: error {errorCode}", ioException);
                    }
                }
                finally
                {
                    buffer.Return();
                }
            }
        }

        /// <inheritdoc/>
        internal override void RestoreObjectLog(long beginAddress, long tailAddress)
        {
            if (!HasPersistentObjectLog)
                return;

            // A fresh object log, so nothing appended before this recovery survives, positioned at the durable end the checkpoint recorded. A
            // checkpoint taken before the store wrote its object log records no end, and the log starts empty; one taken while the object log was
            // still uncreated at PREPARE records no begin, which is the log's start.
            lock (objectLogLock)
            {
                mainObjectLog?.Dispose();
                mainObjectLog = null;
                var log = CreateObjectLogCore(objectLogDevice);
                try
                {
                    if (tailAddress > 0)
                        log.RestoreTail(Math.Max(beginAddress, LogAddress.FirstValidAddress), tailAddress);
                }
                catch
                {
                    log.Dispose();
                    throw;
                }
                Volatile.Write(ref mainObjectLog, log);
            }
        }

        /// <inheritdoc/>
        internal override void OpenRecoverySnapshotObjectLog(IDevice snapshotObjectLogDevice, long tailAddress)
        {
            lock (objectLogLock)
            {
                CloseRecoverySnapshotObjectLogCore();
                if (snapshotObjectLogDevice is null || tailAddress <= 0)
                    return;
                var log = CreateObjectLogCore(snapshotObjectLogDevice);
                try
                {
                    log.RestoreTail(LogAddress.FirstValidAddress, tailAddress);
                }
                catch
                {
                    log.Dispose();
                    throw;
                }
                recoverySnapshotObjectLog = log;
                recoverySnapshotObjectLogDevice = snapshotObjectLogDevice;
            }
        }

        /// <inheritdoc/>
        internal override void CloseRecoverySnapshotObjectLog()
        {
            lock (objectLogLock)
                CloseRecoverySnapshotObjectLogCore();
        }

        void CloseRecoverySnapshotObjectLogCore()
        {
            recoverySnapshotObjectLog?.Dispose();
            recoverySnapshotObjectLog = null;
            recoverySnapshotObjectLogDevice = null;
        }

        /// <inheritdoc/>
        internal override long ObjectLogBeginAddress => Volatile.Read(ref mainObjectLog)?.BeginAddress ?? 0;

        /// <inheritdoc/>
        internal override long ObjectLogFlushedUntilAddress => Volatile.Read(ref mainObjectLog)?.FlushedUntilAddress ?? 0;

        /// <inheritdoc/>
        internal override long ObjectLogPageSize => 1L << objectLogPageSizeBits;

        protected override void TruncateUntilAddressBlocking(long toAddress)
        {
            // A page header holds a floor below which neither that page nor any later one has object-log bytes, because the main object log is
            // written in page order. If toAddress is in the middle of a page, that page's retained records bound the truncation, so use its floor;
            // if toAddress is not past the PageHeader, the header may not have been written, so use the previous page's.
            var objectLogFloor = -1L;
            var objectLog = Volatile.Read(ref mainObjectLog);
            if (objectLog is not null)
            {
                var addressOfStartOfMainLogPage = GetAddressOfStartOfPageOfAddress(toAddress);
                if (GetOffsetOnPage(toAddress) <= PageHeader.Size && addressOfStartOfMainLogPage >= PageSize)
                    addressOfStartOfMainLogPage -= PageSize;
                objectLogFloor = ReadObjectLogFloor(addressOfStartOfMainLogPage);
            }

            // Now do the actual truncations.
            base.TruncateUntilAddressBlocking(toAddress);
            if (objectLog is not null && objectLogFloor > objectLog.BeginAddress)
                objectLog.Truncate(objectLogFloor);
        }

        protected override void RemoveSegment(int segment)
        {
            // If segment is not the last segment (which should be the case), the PageHeader at its start has been written. Otherwise use the
            // previous page's.
            var objectLogFloor = -1L;
            var objectLog = Volatile.Read(ref mainObjectLog);
            if (objectLog is not null)
            {
                var addressOfStartOfMainLogPage = GetStartLogicalAddressOfSegment(segment);
                if (segment >= device.EndSegment)
                    addressOfStartOfMainLogPage -= PageSize;
                objectLogFloor = ReadObjectLogFloor(addressOfStartOfMainLogPage);
            }

            base.RemoveSegment(segment);
            if (objectLog is not null && objectLogFloor > objectLog.BeginAddress)
                objectLog.Truncate(objectLogFloor);
        }

        /// <summary>Read the object-log floor from the durable header of the main-log page starting at <paramref name="addressOfStartOfMainLogPage"/>.</summary>
        /// <returns>The floor, or -1 if there is none to use.</returns>
        private long ReadObjectLogFloor(long addressOfStartOfMainLogPage)
        {
            // If we're on the first main-log page, we can't remove anything; if we're not past the PageHeader of the second page, it probably
            // hasn't been written, so we can't read it.
            if (addressOfStartOfMainLogPage <= PageSize + PageHeader.Size)
                return -1;

            var buffer = bufferPool.Get(sectorSize);
            try
            {
                if (!TryReadDevice(device, (ulong)addressOfStartOfMainLogPage, (IntPtr)buffer.aligned_pointer, (uint)sectorSize,
                    nameof(ReadObjectLogFloor), out var errorCode, out var numBytesRead, out var ioException))
                {
                    throw CreateReadException(nameof(ReadObjectLogFloor), (ulong)addressOfStartOfMainLogPage, (uint)sectorSize,
                        errorCode, numBytesRead, ioException);
                }

                return ((PageHeader*)buffer.aligned_pointer)->GetLowestObjectLogAddress();
            }
            finally
            {
                bufferPool.Return(buffer);
            }
        }

        /// <summary>Create read buffers for the downlevel (cv7) object-log reader over <paramref name="legacyObjectLogDevice"/>, or over
        /// <see cref="objectLogDevice"/> when it is null.</summary>
        CircularDiskReadBuffer CreateLegacyReadBuffers(IDevice legacyObjectLogDevice)
            => new(bufferPool, IStreamBuffer.BufferSize, numberOfDeserializationBuffers, legacyObjectLogDevice ?? objectLogDevice, logger);

        /// <summary>The exclusive bound for a downlevel read: <paramref name="legacyReadEnd"/>, the snapshot object log's end, or no bound for the
        /// downlevel main object log, which recovery reads before any of its tails are known.</summary>
        LegacyObjectLogPosition GetLegacyReadHardEnd(long legacyReadEnd)
            => new(legacyReadEnd > 0 ? (ulong)legacyReadEnd : ObjectLogPositionInfo.NotSet, objectLogSegmentSizeBits);

        /// <inheritdoc/>
        internal override void BeginObjectLogUpgrade()
        {
            Debug.Assert(!isUpgradingObjectLog, $"{nameof(BeginObjectLogUpgrade)} should be called only once");

            // The destination starts empty: a new object log over the upgrade device, or the main object log of a self-contained snapshot, whose
            // downlevel bytes all live in the checkpoint's snapshot object log. Downlevel bytes on objectLogDevice stay there for the downlevel
            // reader until the conversion completes.
            lock (objectLogLock)
            {
                if (upgradeObjectLogDevice is not null)
                    upgradeObjectLog = CreateObjectLogCore(upgradeObjectLogDevice);
                else
                {
                    mainObjectLog?.Dispose();
                    Volatile.Write(ref mainObjectLog, CreateObjectLogCore(objectLogDevice));
                    upgradeObjectLog = mainObjectLog;
                }
            }
            isUpgradingObjectLog = true;
        }

        /// <inheritdoc/>
        internal override bool IsUpgradingObjectLog => isUpgradingObjectLog;

        /// <inheritdoc/>
        internal override bool ObjectLogWasUpgraded => objectLogWasUpgraded;

        /// <inheritdoc/>
        internal override void CompleteObjectLogUpgrade()
        {
            Debug.Assert(isUpgradingObjectLog, $"{nameof(CompleteObjectLogUpgrade)} requires {nameof(BeginObjectLogUpgrade)}");

            // Every page has been converted and points into the destination, so it becomes the live object log.
            lock (objectLogLock)
            {
                if (!ReferenceEquals(upgradeObjectLog, mainObjectLog))
                {
                    mainObjectLog?.Dispose();
                    Volatile.Write(ref mainObjectLog, upgradeObjectLog);
                    objectLogDevice = upgradeObjectLogDevice;
                }
                upgradeObjectLog = null;
            }
            objectLogWasUpgraded = true;
            isUpgradingObjectLog = false;
        }

        /// <inheritdoc/>
        internal override long ComputeRecoveryOverflowKeyHash(in LogRecord logRecord, ref CircularDiskReadBuffer legacyReadBuffers, IDevice objectLogDevice,
            int checkpointVersion, long legacyReadEnd = 0)
        {
            // The transient objectIdMap is not populated during recovery Pass 1 (index build), so LogRecord.Key cannot resolve an overflow key. Read
            // just this record's key from the object log -- the main one for FoldOver and hybrid-log pages, or the snapshot one (passed as
            // objectLogDevice) for snapshot pages -- and hash it. Overflow keys are rare and recovery is a startup operation, so the extra read per
            // record is acceptable.
            if (!HybridLogRecoveryInfo.UsesDownlevelObjectLog(checkpointVersion))
            {
                var source = logRecord.Info.HasSnapshotObjectLogReference
                    ? recoverySnapshotObjectLog ?? throw new TsavoriteException("Missing snapshot object log for a marked recovery key")
                    : GetReadObjectLog(objectLogDevice);
                using var reader = new ObjectLogReader<TStoreFunctions>(source, storeFunctions);
                var recordRef = logRecord.GetObjectLogRecordRef();
                reader.BeginRecord(in recordRef);
                var key = reader.ReadKey();
                fixed (byte* keyPtr = key.Span)
                    return storeFunctions.GetKeyHashCode64(ConditionallyHoistedKey.CreatePinned(keyPtr, key.Length));
            }

            // A downlevel key is read through the streaming ring rather than as one direct read: the record bounds the key's extent, but the
            // downlevel object log is a dense file with no page structure, and the ring tracks the bytes actually available at its end. The ring is
            // owned by the caller and reused across every key read, so its pooled buffer is rented once per recovery. A ring is bound to one device,
            // so switching between the main and snapshot object logs replaces it.
            var startPosition = new LegacyObjectLogPosition(logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out _, checkpointVersion), objectLogSegmentSizeBits);
            var legacyDevice = objectLogDevice ?? this.objectLogDevice;
            if (legacyReadBuffers is not null && !legacyReadBuffers.UsesDevice(legacyDevice))
            {
                legacyReadBuffers.Dispose();
                legacyReadBuffers = null;
            }
            legacyReadBuffers ??= CreateLegacyReadBuffers(legacyDevice);

            legacyReadBuffers.nextFileReadPosition = startPosition;
            var logReader = new LegacyObjectLogReader<TStoreFunctions>(legacyReadBuffers, storeFunctions);
            logReader.OnBeginReadRecords(startPosition, (ulong)keyLength, GetLegacyReadHardEnd(objectLogDevice is null ? 0 : legacyReadEnd));
            try
            {
                return logReader.ReadOverflowKeyHashCodeForRecovery(in logRecord, objectLogSegmentSizeBits, checkpointVersion);
            }
            finally
            {
                logReader.OnEndReadRecords();
            }
        }

        /// <summary>Object log segment size</summary>
        public override long GetObjectLogSegmentSize() => ObjectLogSegmentSize;

        /// <inheritdoc/>
        internal override bool HasObjectLogDevice => objectLogDevice is not null;

        /// <inheritdoc/>
        internal override bool HasUpgradeObjectLogDevice => upgradeObjectLogDevice is not null;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.NoInlining)]
        protected internal override void RecoveryReset(long tailAddress, long headAddress, long beginAddress, long readonlyAddress)
        {
            base.RecoveryReset(tailAddress, headAddress, beginAddress, readonlyAddress);
            LastIssuedFlushedUntilAddress = readonlyAddress;
            OngoingFlushedUntilAddress = 0;
            NoFlushUntilAddress = 0;

            // Recovery's object load has rewritten the resident records below readonlyAddress in memory, so the first flush must read the
            // durable bytes of the sector it starts in back from the device.
            lastPartialSector = null;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal override void AsyncFlushPagesForReadOnly(long fromAddress, long untilAddress, bool noFlush = false)
        {
            // We do not need to ensure page alignment of the ReadOnlyAddres for correctness, and in fact that is impossible since we support setting it to whatever
            // the current TailAddress is, but for normal flush operations we do set it to page alignment to eliminate concerns about rewriting partial sectors.
            GetFlushPageRange(fromAddress, untilAddress, out var startPage, out var numPages);

            // The object log each page's out-of-line components go to; null if this allocator has none, in which case pages are written inline.
            var objectLog = FlushObjectLog;
            var snapshotCoordination = GetSnapshotFlushCoordinationForReadOnlyRange(untilAddress);

            // Write each page (or partial page) in the range.
            for (var flushPage = startPage; flushPage < (startPage + numPages); flushPage++)
            {
                WaitForSnapshotPage(snapshotCoordination, flushPage);
                if (!PrepareFlushAsyncResult(fromAddress, untilAddress, noFlush, flushPage, out var asyncResult))
                {
                    NoteSkippedFlushEnd(asyncResult.untilAddress);
                    continue;
                }

                asyncResult.objectLog = objectLog;

                // ObjectAllocator does not use PendingFlush chaining; every page in this range is issued here.
                Debug.Assert(PendingFlush[GetPageIndexForAddress(asyncResult.fromAddress)].list.Count == 0,
                    $"Expected PendingFlush count {PendingFlush[GetPageIndexForAddress(asyncResult.fromAddress)].list.Count} to be 0 for ObjectAllocator");
                WriteAsync(flushPage, AsyncFlushPageCallback, asyncResult);
            }
        }

        /// <summary>
        /// Whether <paramref name="logRecord"/> has no object-log position that can be read from, so its components must be
        /// materialized rather than fetched.
        /// </summary>
        /// <remarks>
        /// Two distinct situations produce an unstamped position, and the reader treats them identically:
        /// <list type="number">
        /// <item>The flush wrote no object-log bytes for the record, so it never stamped a position.</item>
        /// <item>An earlier walk of this same in-memory page already deserialized the record. <c>SetDeserializedValueObject</c>
        /// unstamps in the live buffer, so every record on a re-walked page reads as unstamped even though its bytes were written
        /// normally. Garnet's object-log upgrade flow does exactly this.</item>
        /// </list>
        /// Because of (2) an unstamped position is NOT evidence of a flush defect, and this condition cannot be asserted against.
        /// <para>Reading from an unstamped position would consume bytes belonging to a different record, so the walk must not read there.</para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool RecordHasNoReadablePosition(in LogRecord logRecord)
            => ObjectLogPositionInfo.WordIsUnstamped(logRecord.RawObjectLogPositionWord);

        protected override void WriteAsync<TContext>(int flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
            => WriteAsync(flushPage, (ulong)GetFileOffsetOfPage(flushPage), (uint)PageSize, callback, asyncResult, device);

        protected override void WriteAsyncToDeviceForSnapshot<TContext>(int startPage, int flushPage, int pageFlushSize, DeviceIOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, long fuzzyStartLogicalAddress)
        {
            VerifyCompatibleSectorSize(device);

            // ReadOnly cannot publish FlushedUntilAddress into this page until Snapshot advances its watermark past
            // the page. HeadAddress is capped by FlushedUntilAddress for real devices and directly by the Snapshot
            // watermark for NullDevice, so this range remains resident without an epoch hold.
            Debug.Assert(HeadAddress <= asyncResult.fromAddress,
                $"Snapshot page {flushPage} starts at {asyncResult.fromAddress}, below HeadAddress {HeadAddress}");

            // We are writing to a separate device which starts at startPage. Eventually, startPage becomes the basis of
            // HybridLogRecoveryInfo.snapshotFileLogicalStartAddress, which is the page starting at offset 0 of the snapshot file.
            WriteAsync(flushPage, (ulong)GetFileOffsetOfPage(flushPage - startPage), (uint)pageFlushSize,
                        callback, asyncResult, device, fuzzyStartLogicalAddress);
        }

        private void WriteAsync<TContext>(int flushPage, ulong alignedMainLogFlushPageAddress, uint numBytesToWrite,
                        DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                        IDevice device, long fuzzyStartLogicalAddress = long.MaxValue)
        {
            // Short circuit if we are using a null device
            if (device is NullDevice)
            {
                device.WriteAsync(IntPtr.Zero, 0, 0, numBytesToWrite, callback, asyncResult);
                asyncResult.snapshotDeviceWriteIssued = true;
                return;
            }

            var pageStart = GetLogicalAddressOfStartOfPage(asyncResult.page);
            var logPagePointer = (byte*)pagePointers[flushPage % BufferSize];

            // The object log this page's out-of-line components go to, or null if the page is written inline only; and, for a recovery flush of
            // snapshot-region pages, the snapshot object log their bytes are copied from.
            var objectLog = asyncResult.objectLog;
            var snapshotSourceLog = asyncResult.recoverySnapshotObjectLog;
            var formerFlushedUntilAddress = asyncResult.recoveryFormerFlushedUntilAddress;
            var isRecovery = asyncResult.flushRequestState == FlushRequestState.Recovery;

            // asyncResult.fromAddress is either start of page or start of a record past the page header
            Debug.Assert(asyncResult.fromAddress - pageStart is >= PageHeader.Size or 0, $"fromAddress ({asyncResult.fromAddress}, offset {asyncResult.fromAddress - pageStart}) must be 0 or after the PageHeader");
            int startOffset = (int)(asyncResult.fromAddress - pageStart), endOffset = startOffset + (int)numBytesToWrite;
            var isFirstRecordOnPage = startOffset <= PageHeader.Size;

            // An up-conversion rewrites the page's floor even when it starts mid-page, as it may on the first page it converts: the records below its
            // start are below BeginAddress, and the floor it would otherwise leave is a position in the downlevel object log.
            var writesPageHeader = isFirstRecordOnPage || (isRecovery && isUpgradingObjectLog);

            // Stamp the page's object-log floor: the destination's tail before this flush writes any of the page's records. A page header's floor bounds
            // truncation of the main object log, so it must be a main object-log address no greater than any position a record on this page or a later
            // page references; the main object log is written in page order, which makes its tail here such a bound. A ReadOnly flush keeps a floor an
            // earlier fragment of the page stamped, and stamps an object-free page too, so truncation can advance past it. A Snapshot flush stamps
            // nothing: its destination is the checkpoint's object log, whose addresses bound nothing in the main one, and the live header it would
            // stamp is the one a later ReadOnly flush writes to the main log, which keeps an existing floor. A recovery flush stamps only when it writes
            // the page's objects into a new address space: an up-conversion, or the copy of a page that starts in the snapshot region, whose header
            // came from the snapshot file. A recovery flush that writes no objects leaves the header it read from the main log alone.
            if (writesPageHeader && objectLog is not null)
            {
                if (asyncResult.flushRequestState == FlushRequestState.ReadOnly)
                    ((PageHeader*)logPagePointer)->SetLowestObjectLogAddress(objectLog.TailAddress);
                else if (isRecovery && (isUpgradingObjectLog || (snapshotSourceLog is not null && pageStart >= formerFlushedUntilAddress)))
                    ((PageHeader*)logPagePointer)->SetLowestObjectLogAddress(objectLog.TailAddress, force: true);
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
            if (writesPageHeader)
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

            var deviceSectorSize = (int)device.SectorSize;
            var alignedStartOffset = RoundDown(startOffset, deviceSectorSize);

            // A page whose records are entirely inline (no Overflow keys/values and no Object values, i.e. the page's objectIdMap is empty) has
            // nothing to serialize to the object log, so write its inline span directly and skip walking every record. This also covers an
            // ObjectAllocator with no object log, which is used for string-only purposes. The span and destination are the ones the serializing path
            // computes below, so a Snapshot lands at its own device's page offset and stops at the checkpoint boundary; Snapshot page ordering and
            // fuzzy rules still apply through its coordination callbacks.
            //
            // The test is objectIdMap.Count, which is page-scoped, so a sub-range flush of a page that has objects outside that range still
            // serializes. Narrowing it to the range would mean either deferring the writer until the walk below reaches its first out-of-line
            // record, or pre-walking the range to look for one. Neither is worth it: normal ReadOnly shifts are page-aligned
            // (CalculateReadOnlyAddress rounds down to PageSize), so sub-range flushes are boundary events -- the trailing page of an explicit
            // Flush, a checkpoint-driven shift, or a truncation -- at most one page each. A pre-walk is the worse of the two, because its
            // skip rules (Invalid, and fuzzy v+1 for Snapshot) would have to stay in agreement with the walk below forever: if it ever became
            // the more restrictive of the two it would take this inline path for a record the walk would have serialized, writing an unstamped
            // objectId slot and no object bytes.
            var objectIdMap = objectPages[flushPage % BufferSize].objectIdMap;
            var pageHasNoObjectsToFlush = objectIdMap.Count == 0
                && asyncResult.flushRequestState is FlushRequestState.ReadOnly or FlushRequestState.Snapshot;
            var writeInlineOnly = false;
            if (objectLog is null || pageHasNoObjectsToFlush)
            {
                // Recovery never takes the inline path, for two independent reasons. Its objectIdMap is empty because records read from disk
                // still carry object-log positions in their objectId slots rather than map indices, so Count is not a test for "no out-of-line
                // data" here and would report zero for a page full of overflow keys and objects. And a recovery flush must examine every record:
                // snapshot-region records copy their object bytes into the main object log and are repointed, legacy records are up-converted,
                // and a downlevel record that reaches a plain recovery flush must fail rather than be persisted.
                if (!isRecovery)
                    writeInlineOnly = true;
            }

            // All object-log flush modes stamp and write the live page. Recovery has exclusive access. Snapshot writes one page at a time
            // and keeps ReadOnly one page behind its completion watermark; ReadOnly subsequently resolves objects through objectIdMap and
            // replaces Snapshot positions with main-object-log positions. Fuzzy v+1 records remain unchanged and recovery rejects them
            // before interpreting their payload. A Snapshot checkpoint's capture of a CopyUpdate source rewrites the record's position and
            // hints only while no flush of that checkpoint has published the record; afterwards it sets at most the record's marker bit,
            // which is one RecordInfo word. Each publisher holds the value object's claim from its object-log write through its stamps, so
            // a flush and a capture never interleave their stamps. See TryCaptureCopyUpdateSource and PublishFlushedObject.
            //
            // A front-partial ReadOnly flush starts after bytes that this flush sequence has already made durable, and its device write
            // rounds down to a sector boundary, so it writes those bytes again. It takes them from the PartialSectorImage the previous
            // flush kept, or reads them back from the device, never from the live page: the records there may have changed in memory since
            // they became durable, and the durable bytes may be a completed checkpoint's image. Recovery and Snapshot write the live page:
            // recovery's resident bytes below the start are the ones it read from the device, and a snapshot file is never read below its
            // start. The write likewise rounds UP at the logical end, carrying live bytes above the endpoint to disk verbatim; see the
            // write span comment below for why those bytes are never read back.
            //
            // Correctness also relies on the OnDispose contract that a record stays READABLE (byte-consistent) throughout a flush --
            // OnDispose implementations copy off whatever they need for cleanup rather than tearing the record's flush-critical bytes
            // -- so the async device write always observes a consistent record even if a concurrent op supersedes it. The stamping is
            // non-destructive to in-memory readers (the ValueLength property masks the raw field to ObjectIdSize; the objectId slot keeps
            // its index bits), and the page stays resident throughout the flush (HeadAddress <= FlushedUntilAddress until it completes).
            //
            // Recovery and Snapshot enter without epoch protection. Snapshot ordering prevents FlushedUntilAddress, and
            // therefore HeadAddress, from reaching the active page. ReadOnly enters protected from the epoch callback that
            // marked the range immutable; release that hold during serialization and IO, then restore it before returning.
            //
            // Suspending is necessary because object serialization can block on object-log IO, and an epoch held across IO stalls
            // reclamation process-wide: queued drain-list actions cannot fire, and every thread waiting on a BumpCurrentEpoch barrier
            // waits behind this one.
            //
            // Suspending is safe because this page's residency does not depend on holding the epoch. Eviction is capped at
            // FlushedUntilAddress, which cannot advance past this page until this flush's own completion callback records the
            // page as flushed, so HeadAddress cannot reach the page while the write is in flight. The out-of-line heap the
            // records point to is kept alive by the per-record capture below, which roots the byte[]/object independently of the
            // epoch. TrySuspend reports whether it actually released a hold, so only ReadOnly Resumes in the finally.
            var protectEpochWhenDone = epoch.TrySuspend();

            // Overflow keys and values are written to, and object values are serialized to, this writer, if the page has an object log.
            ObjectLogWriter<TStoreFunctions> logWriter = null;

            // For a snapshot-region recovery flush, the reader over the snapshot object log from which each record's object bytes are copied into
            // the main object log through logWriter.
            ObjectLogReader<TStoreFunctions> snapshotReader = null;

            // Do everything below here in the try{} to be sure the epoch is Resumed()d if we Suspended it.
            try
            {
                // A ReadOnly flush that starts mid-sector composes that sector separately; get the durable bytes below its start first.
                var startsMidSector = asyncResult.flushRequestState == FlushRequestState.ReadOnly && alignedStartOffset != startOffset;
                Debug.Assert(!startsMidSector || deviceSectorSize == sectorSize, "A ReadOnly flush writes the main log device");
                SectorAlignedMemory firstSector = null;
                if (startsMidSector && !TryGetDurableSectorPrefix(pageStart + alignedStartOffset, startOffset - alignedStartOffset,
                        alignedMainLogFlushPageAddress + (ulong)alignedStartOffset, device, callback, asyncResult, out firstSector))
                    return;

                // Record traversal and metadata stamping are page-relative. The page remains resident until this flush's callback completes.
                var recordsBasePtr = (byte*)logPagePointer;

                if (objectLog is not null && !writeInlineOnly)
                    logWriter = new(objectLog, storeFunctions, objectLogWriterSettings);

                // A Snapshot flush publishes for its own checkpoint, which stays published until the flush completes: an abort waits for the
                // flush before unpublishing it. A ReadOnly flush reads the active checkpoint per record instead; see PublishFlushedObject.
                var snapshotFlushState = asyncResult.flushRequestState == FlushRequestState.Snapshot && ActiveSnapshotObjectLog is { } activeState
                    && ReferenceEquals(activeState.Log, objectLog) ? activeState : null;

                // Whether this flush appended to objectLog, so the page must wait for those bytes to be durable.
                var wroteObjects = false;

                // Include page header when calculating end address. Using page-relative addressing makes both paths look identical.
                var endPhysicalAddress = (long)recordsBasePtr + startOffset + numBytesToWrite;
                var physicalAddress = (long)recordsBasePtr + firstRecordOffset;

                // A page with nothing to serialize skips the record walk and writes its span as it is.
                if (writeInlineOnly)
                    physicalAddress = endPhysicalAddress;

                while (physicalAddress < endPhysicalAddress)
                {
                    // Increment for next iteration; use allocatedSize because that is what LogicalAddress is based on.
                    var logRecord = new LogRecord(physicalAddress, objectIdMap);
                    var logRecordSize = logRecord.AllocatedSize;

                    // Skip object serialization for Invalid records, including IsNull; the page write below still carries their inline
                    // image. ReadOnlyAddress has advanced, so a record's version and inline data are stable here, but elision can still
                    // clear Valid concurrently -- the capture below re-reads Info to catch that.
                    if (logRecord.Info.Valid)
                    {
                        // Skip object serialization for v+1 records (e.g. during a checkpoint); the page write below still carries their
                        // inline image verbatim. For non-Snapshot flushes fuzzyStartLogicalAddress is long.MaxValue, so this is always
                        // true and the else branch is unreachable.
                        if (logicalAddress < fuzzyStartLogicalAddress || !logRecord.Info.IsInNewVersion)
                        {
                            // Fully-inline records have nothing to serialize. This is always false when there is no logWriter, i.e. an
                            // ObjectAllocator used for inline string records only.
                            if (logRecord.DataHeader.RecordHasObjects)
                            {
                                if (!isRecovery)
                                {
                                    // Capture the out-of-line components BEFORE consulting the live record's state. The capture roots the
                                    // byte[]/object while it is still reachable, so the bytes written to the object log stay valid even if a
                                    // concurrent operation frees the record's objectIdMap slots; and because it does not throw on an
                                    // already-freed or already-cleared slot, losing the race is observable rather than fatal to the flush thread.
                                    var captureIsComplete = logRecord.TryGetOutOfLineComponents(out var keyOverflow, out var valueOverflow, out var valueObject);

                                    // Test-only interleave point: the window between capturing the components and re-reading the live
                                    // RecordInfo is exactly where a concurrent elision can land, so a test runs its racing operation here.
                                    ObjectFlushInjection.At(ObjectFlushPhase.AfterCapture, logicalAddress);

                                    // Re-read the LIVE RecordInfo after the capture. Elision CASes the record out of its tag chain, then
                                    // SealAndInvalidate()s it, and only then frees its heap through OnDispose(Elided) -- so a capture that lost
                                    // its heap surfaces here as an Invalid live record. Skipping such a record is equivalent to it having been
                                    // Invalid at the Info.Valid test above: the live page already carries the Invalid bit that this flush
                                    // persists, recovery skips the image, and the superseding record (already CAS'd into the chain) wins.
                                    if (captureIsComplete && logRecord.Info.Valid)
                                    {
                                        // WriteRecordObjects can wait on object-log IO and must not hold the epoch. Stamping writes the position
                                        // word and the key/value read-size hints into the record image being flushed. In-memory readers mask
                                        // via ObjectIdMap.GetIndex, so stamping a live page is non-destructive. RDH lengths remain exact.
                                        if (valueObject is HeapObjectBase heapObject)
                                            wroteObjects |= PublishFlushedObject(heapObject, ref logRecord, logicalAddress, in keyOverflow, logWriter,
                                                asyncResult.flushRequestState, snapshotFlushState);
                                        else if (!valueOverflow.IsEmpty)
                                            wroteObjects |= PublishFlushedOverflow(ref logRecord, logicalAddress, in keyOverflow, valueOverflow, logWriter,
                                                asyncResult.flushRequestState, snapshotFlushState);
                                        else
                                        {
                                            var result = logWriter.WriteRecordObjects(in keyOverflow, in valueOverflow, logRecord.DataHeader.ValueIsObject, valueObject);
                                            StampFlushedRecord(ref logRecord, in result);
                                            wroteObjects = true;
                                        }

                                        // Test-only interleave point: the record's bytes are now in the object log and its position is stamped,
                                        // so a test can assert the freeze invariant or race a dispose against the in-flight write.
                                        ObjectFlushInjection.At(ObjectFlushPhase.AfterRecordWritten, logicalAddress);
                                    }
                                    else
                                    {
                                        // A still-Valid record must keep its heap. The paths that free an overflow key or value
                                        // (OnDispose with Elided or RevivificationFreeList) are preceded by SealAndInvalidate, and both the Deleted
                                        // path and a CopyUpdate's release of its source value are declined for this flush range by IsFrozenForFlush,
                                        // which defers the release to the drain or eviction. So a lost capture means the live record is Invalid (elided).
                                        Debug.Assert(!logRecord.Info.Valid,
                                            "A Valid record lost its captured heap during object-log flush; expected the live record to be Invalid (elided).");
                                    }
                                }
                                else if (isUpgradingObjectLog)
                                {
                                    // Up-converting a downlevel object log. The ascending conversion pass has already deserialized this
                                    // page's objects -- from the main object log for hybrid-log-region pages, from the snapshot object log
                                    // for snapshot-region pages -- so re-serialize them in the current format, appending to the conversion's
                                    // destination. Only the object-log extent changes; the record's inline image keeps its size and is
                                    // rewritten in place.
                                    if (!logRecord.TryGetOutOfLineComponents(out var keyOverflow, out var valueOverflow, out var valueObject))
                                        throw new TsavoriteException($"Object-log upgrade could not capture the out-of-line components of the record at {logicalAddress}");

                                    var result = logWriter.WriteRecordObjects(in keyOverflow, in valueOverflow, logRecord.DataHeader.ValueIsObject, valueObject);
                                    logRecord.SetObjectLogPositionAndHints(in result);
                                    logRecord.InfoRef.HasSnapshotObjectLogReference = false;
                                    wroteObjects = true;

                                    // In-place main-log rewrite is only sound while the inline image is size-stable. Fail loudly rather
                                    // than write a page whose records no longer line up with the addresses recovery assigned them.
                                    if (logRecord.AllocatedSize != logRecordSize)
                                        throw new TsavoriteException($"Object-log upgrade changed the inline size of the record at {logicalAddress} from {logRecordSize} to {logRecord.AllocatedSize}");
                                }
                                else if (snapshotSourceLog is not null && logicalAddress >= formerFlushedUntilAddress)
                                {
                                    // Snapshot-region recovery flush: the record's objects live only in the snapshot object log. Copy them into the
                                    // main object log so the page becomes durable and can be evicted, then repoint the record there. The objects
                                    // are not deserialized; the copy re-chunks them for the destination, so the record takes the new position and
                                    // hints, while its inline image keeps its size.
                                    var recordRef = logRecord.GetObjectLogRecordRef();
                                    snapshotReader ??= new(snapshotSourceLog, storeFunctions);
                                    snapshotReader.BeginRecord(in recordRef);
                                    var result = snapshotReader.CopyRecordTo(logWriter);
                                    snapshotReader.EndRecord();
                                    logRecord.SetObjectLogPositionAndHints(in result);
                                    logRecord.InfoRef.HasSnapshotObjectLogReference = false;
                                    wroteObjects = true;
                                }
                                else
                                {
                                    // A downlevel record reaching here was not up-converted. Recovery begins the up-conversion whenever a
                                    // downlevel checkpoint has an object log, and converts every page before it can be evicted or flushed,
                                    // so this cannot happen; fail loudly rather than persist a page in a format later releases cannot decode.
                                    if (HybridLogRecoveryInfo.UsesDownlevelObjectLog(asyncResult.checkpointVersion))
                                        throw new TsavoriteException($"Recovery flush reached the downlevel record at {logicalAddress} without up-converting it");

                                    // Current-format hybrid-log-region record. Its object bytes are already durable in the main object log and its
                                    // position and hints are already correct; the only recovery mutation for this page -- SetInvalid on undone v+1
                                    // records -- was applied to the live page by RecoverFromPage. So persist the record VERBATIM.
                                }
                            }
                        }
                        else
                        {
                            // Snapshot writes the live page, so leave v+1 records unchanged. Recovery rejects them from the persisted fuzzy
                            // boundary before hashing an overflow key or interpreting payload bytes.
                            Debug.Assert(asyncResult.flushRequestState == FlushRequestState.Snapshot);
                        }
                    } // endif record id Valid

                    logicalAddress += logRecordSize;    // advance in main log
                    physicalAddress += logRecordSize;   // advance in source buffer
                }

                // Object serialization and metadata stamping are complete. Ask the object log to flush what this page appended; the page completes
                // only once those bytes are durable as well as its own, since its records reference them.
                var objectLogFlushUntilAddress = wroteObjects ? objectLog.FlushTail(wait: false) : 0;

                // Write the page span.
                {
                    // Write whole sectors: [alignedStartOffset, RoundUp(endOffset)). Bytes above the logical endpoint go to disk verbatim
                    // from the live page, because they are never read back:
                    //   - the next ReadOnly flush starts at the endpoint and writes this sector again;
                    //   - recovery bounds its record walk by maxAddressOffsetOnPage, which is the UNROUNDED endpoint, so records above
                    //     the endpoint are never parsed and their object-log position words are never interpreted; and
                    //   - the hybrid-log/snapshot boundary is applied at exact address granularity (RecoveryLoadObjectsPass2 splits the
                    //     boundary page at snapshotScanFromAddress), so a snapshot object-log position cannot be read as a main-log one.
                    // PageSize >= sectorSize and AlignedPageSizeBytes == RoundUp(PageSize, sectorSize), so the span stays within the page.
                    var writeAddress = alignedMainLogFlushPageAddress + (uint)alignedStartOffset;
                    var writePtr = (byte*)logPagePointer + alignedStartOffset;
                    var writeLength = RoundUp(endOffset, deviceSectorSize) - alignedStartOffset;
                    Debug.Assert(writeLength > 0, $"Flush of page {flushPage} produced an empty write span");

                    // A ReadOnly flush that starts mid-sector writes that sector from firstSector, which holds the durable bytes below its
                    // start; complete it with the live bytes from the start. The rest of the span is written from the live page.
                    byte* firstSectorPtr = null;
                    var firstSectorLength = 0;
                    if (firstSector is not null)
                    {
                        firstSectorPtr = firstSector.aligned_pointer;
                        firstSectorLength = deviceSectorSize;
                        var prefixLength = startOffset - alignedStartOffset;
                        Buffer.MemoryCopy(writePtr + prefixLength, firstSectorPtr + prefixLength, deviceSectorSize - prefixLength, deviceSectorSize - prefixLength);
                        writePtr += deviceSectorSize;
                        writeLength -= deviceSectorSize;
                    }

                    if (asyncResult.flushRequestState == FlushRequestState.ReadOnly)
                    {
                        RecordPartialSectorImage(pageStart, alignedStartOffset, endOffset, (byte*)logPagePointer, firstSector);

                        // The previous flush also wrote the sector this one starts in. Devices do not order overlapping writes, so let that
                        // write complete first, or it could land after this one and replace the records stamped here with their image from
                        // before this flush.
                        if (startsMidSector)
                            WaitForWritesBelow(pageStart + startOffset);
                    }

                    // The page's callback runs once, after its device writes and the object-log flush have all completed. The callback owns
                    // releasing this page's native snapshot-IO unit and buffers from here on, including when a submission below throws: that
                    // part completes with the error, so the join still runs the callback, and the throw propagates to the issuer.
                    asyncResult.snapshotDeviceWriteIssued = true;
                    var join = new PageFlushJoin(callback, asyncResult);
                    try
                    {
                        if (firstSectorLength > 0)
                            SubmitWrite(join, device, firstSectorPtr, writeAddress, firstSectorLength);
                        if (writeLength > 0)
                            SubmitWrite(join, device, writePtr, writeAddress + (uint)firstSectorLength, writeLength);
                        if (objectLogFlushUntilAddress > 0)
                        {
                            join.Add();
                            objectLog.OnFlushed(objectLogFlushUntilAddress, PageFlushJoin.PartCallback, join);
                        }
                    }
                    finally
                    {
                        // The issuer's sentinel; held until every part is submitted, so an early completion cannot finish the join.
                        join.Complete(errorCode: 0, ioException: null);
                    }
                }
            }
            finally
            {
                if (protectEpochWhenDone)
                    epoch.Resume();
                logWriter?.Dispose();
                snapshotReader?.Dispose();
            }

            static void SubmitWrite(PageFlushJoin join, IDevice device, byte* source, ulong destinationAddress, int length)
            {
                join.Add();
                try
                {
                    device.WriteAsync((IntPtr)source, destinationAddress, (uint)length, PageFlushJoin.PartCallback, join);
                }
                catch (Exception ex)
                {
                    join.Complete(uint.MaxValue, ex);
                    throw;
                }
            }
        }

        /// <summary>
        /// Stamp a flushed record with the position and hints of what the flush wrote for it, and clear its marker: the stamp is now a
        /// position in the flush's own object log, not a Snapshot checkpoint's capture.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void StampFlushedRecord(ref LogRecord logRecord, in ObjectLogWriteResult result)
        {
            logRecord.SetObjectLogPositionAndHints(in result);
            if (logRecord.Info.HasSnapshotObjectLogReference)
                logRecord.InfoRef.HasSnapshotObjectLogReference = false;
        }

        /// <summary>
        /// Publish a flushed record whose value is a <see cref="HeapObjectBase"/>: write its out-of-line components to the flush's object log
        /// and stamp it, or keep its reference to the active Snapshot checkpoint's capture.
        /// </summary>
        /// <param name="valueObject">The record's value object</param>
        /// <param name="logRecord">The record, on the live page</param>
        /// <param name="logicalAddress">The record's address</param>
        /// <param name="keyOverflow">The record's overflow key, or default</param>
        /// <param name="logWriter">The writer for the flush's object log</param>
        /// <param name="flushRequestState">Whether this is a Snapshot or a ReadOnly flush</param>
        /// <param name="snapshotFlushState">For a Snapshot flush, its checkpoint; otherwise null</param>
        /// <returns>True if this wrote to the flush's object log.</returns>
        /// <remarks>
        /// <para>The flush claims the object, which waits out a capture or a serializer that holds it, then writes the object, stamps the
        /// record, and leaves the object's phase saying which flush published it: <see cref="SerializationPhase.SNAPSHOT_STAMPED"/> with
        /// this checkpoint's tag, or <see cref="SerializationPhase.READONLY_STAMPED"/>. A capture reads that phase to decide what it may still
        /// change in the record; see <see cref="TryCaptureCopyUpdateSource"/>.</para>
        /// <para>A superseded object is never claimed: the CopyUpdate's new record shares its collections and may have changed them. If the
        /// active checkpoint captured it, the record's stamp and marker are that capture, which the flush keeps; a ReadOnly flush also
        /// records the lowest main-log image that keeps one, which bounds what recovery repoints. Otherwise the flush writes what a
        /// superseded object serializes as: a null value, or the bytes CacheSerializedObjectData cached.</para>
        /// </remarks>
        private bool PublishFlushedObject(HeapObjectBase valueObject, ref LogRecord logRecord, long logicalAddress, in OverflowByteArray keyOverflow,
            ObjectLogWriter<TStoreFunctions> logWriter, FlushRequestState flushRequestState, SnapshotObjectLogState snapshotFlushState)
        {
            ObjectLogWriteResult result;
            if (!valueObject.TryClaim(out var priorWord))
            {
                if (logRecord.Info.HasSnapshotObjectLogReference)
                {
                    // A ReadOnly flush reads the active checkpoint only after finding the capture, because a checkpoint that started during
                    // this walk may be the one that captured the record. If the capturing checkpoint has since ended, it no longer needs this
                    // image: this flush had not completed when it sampled its main-log recovery end, so it recovers the record from its
                    // snapshot file. The record is superseded, so no later checkpoint needs its value either.
                    var state = flushRequestState == FlushRequestState.Snapshot ? snapshotFlushState : ActiveSnapshotObjectLog;
                    if (state is not null && HeapObjectBase.IsCapturedBy(priorWord, state.Tag))
                    {
                        if (flushRequestState == FlushRequestState.ReadOnly)
                            state.NoteMainLogReference(logicalAddress);
                        return false;
                    }
                }
                result = logWriter.WriteRecordObjects(in keyOverflow, default, valueIsObject: true, valueObject);
                StampFlushedRecord(ref logRecord, in result);
                return true;
            }

            // On failure, restore the phase this flush found: it published nothing.
            var finalWord = priorWord;
            HeapObjectBase.BeginClaimedWrite(valueObject);
            try
            {
                result = logWriter.WriteRecordObjects(in keyOverflow, default, valueIsObject: true, valueObject);

                // Test-only interleave point: a capture of this record must wait here for the claim.
                ObjectFlushInjection.At(ObjectFlushPhase.ClaimHeld, logicalAddress);
                StampFlushedRecord(ref logRecord, in result);
                if (flushRequestState == FlushRequestState.ReadOnly)
                    finalWord = HeapObjectBase.ReadOnlyStampedWord;
                else if (snapshotFlushState is not null)
                {
                    // A ReadOnly flush writes a page in the Snapshot flush's range only after the Snapshot flush has written it, and below that
                    // range the Snapshot flush writes nothing.
                    Debug.Assert(!HeapObjectBase.IsReadOnlyStamped(priorWord), $"The Snapshot flush found the record at {logicalAddress} already published by a ReadOnly flush");
                    finalWord = HeapObjectBase.SnapshotStampedWord(snapshotFlushState.Tag);
                }
            }
            finally
            {
                HeapObjectBase.EndClaimedWrite();
                valueObject.ReleaseClaim(finalWord);
            }
            return true;
        }

        /// <summary>
        /// Publish a flushed record whose value is overflow: write its out-of-line components to the flush's object log and stamp it, or keep its
        /// reference to the active Snapshot checkpoint's capture.
        /// </summary>
        /// <param name="logRecord">The record, on the live page</param>
        /// <param name="logicalAddress">The record's address</param>
        /// <param name="keyOverflow">The record's overflow key, or default</param>
        /// <param name="valueOverflow">The record's value array as the walk found it</param>
        /// <param name="logWriter">The writer for the flush's object log</param>
        /// <param name="flushRequestState">Whether this is a Snapshot or a ReadOnly flush</param>
        /// <param name="snapshotFlushState">For a Snapshot flush, its checkpoint; otherwise null</param>
        /// <returns>True if this wrote to the flush's object log.</returns>
        /// <remarks>
        /// <para>A capture detaches its source by pointing the source's slot at the checkpoint's sentinel, because the CopyUpdate's new record
        /// shares the value's array and changes it. A marked record whose slot holds the active checkpoint's sentinel keeps its reference, as a
        /// captured object does in <see cref="PublishFlushedObject"/>; any other sentinel is stale and the record, which is superseded, is
        /// written with an empty value.</para>
        /// <para>Otherwise the flush claims the array, which waits out a capture. It then checks the slot again: a capture may have detached the
        /// record after the walk read the slot, and the array may already hold the new record's changes. The array's phase describes only the
        /// record at its owner address, so the flush replaces it unless a newer record that shares the array owns it: the records sharing an
        /// array are versions of one key, and an older version is superseded.</para>
        /// </remarks>
        private bool PublishFlushedOverflow(ref LogRecord logRecord, long logicalAddress, in OverflowByteArray keyOverflow, OverflowByteArray valueOverflow,
            ObjectLogWriter<TStoreFunctions> logWriter, FlushRequestState flushRequestState, SnapshotObjectLogState snapshotFlushState)
        {
            ObjectLogWriteResult result;
            while (true)
            {
                if (valueOverflow.IsSentinel)
                {
                    if (logRecord.Info.HasSnapshotObjectLogReference)
                    {
                        // As for an object, a ReadOnly flush reads the active checkpoint only after finding the capture.
                        var state = flushRequestState == FlushRequestState.Snapshot ? snapshotFlushState : ActiveSnapshotObjectLog;
                        if (state is not null && ReferenceEquals(valueOverflow.Array, state.OverflowSentinel.Array))
                        {
                            if (flushRequestState == FlushRequestState.ReadOnly)
                                state.NoteMainLogReference(logicalAddress);
                            return false;
                        }
                    }
                    result = logWriter.WriteRecordObjects(in keyOverflow, in valueOverflow, valueIsObject: false, valueObject: null);
                    StampFlushedRecord(ref logRecord, in result);
                    return true;
                }

                valueOverflow.ClaimForPublication(out var priorWord, out var priorOwner);
                if (!logRecord.TryGetOutOfLineComponents(out _, out var currentValue, out _) || !ReferenceEquals(currentValue.Array, valueOverflow.Array))
                {
                    valueOverflow.ReleasePublicationClaim(priorWord, priorOwner);

                    // A slot that no longer resolves was freed by an elision, which invalidated the record first; the page carries that.
                    if (currentValue.IsEmpty)
                        return false;
                    valueOverflow = currentValue;
                    continue;
                }

                // On failure, restore the phase this flush found: it published nothing.
                var finalWord = priorWord;
                var finalOwner = priorOwner;
                try
                {
                    result = logWriter.WriteRecordObjects(in keyOverflow, in valueOverflow, valueIsObject: false, valueObject: null);

                    // Test-only interleave point: a capture of this record must wait here for the claim.
                    ObjectFlushInjection.At(ObjectFlushPhase.ClaimHeld, logicalAddress);
                    StampFlushedRecord(ref logRecord, in result);
                    if (priorOwner <= logicalAddress)
                    {
                        if (flushRequestState == FlushRequestState.ReadOnly)
                            (finalWord, finalOwner) = (HeapObjectBase.ReadOnlyStampedWord, logicalAddress);
                        else if (snapshotFlushState is not null)
                            (finalWord, finalOwner) = (HeapObjectBase.SnapshotStampedWord(snapshotFlushState.Tag), logicalAddress);
                    }
                }
                finally
                {
                    valueOverflow.ReleasePublicationClaim(finalWord, finalOwner);
                }
                return true;
            }
        }

        /// <summary>
        /// Records that a ReadOnly flush range ending at <paramref name="untilAddress"/> was skipped rather than written. Nothing reads a
        /// skipped range's bytes, so a flush that starts right after it may write the start of its sector from the live page.
        /// </summary>
        private void NoteSkippedFlushEnd(long untilAddress)
        {
            var sectorStart = RoundDown(untilAddress, sectorSize);
            lastPartialSector = untilAddress == sectorStart ? null : new PartialSectorImage(sectorStart, (int)(untilAddress - sectorStart), bytes: null);
        }

        /// <summary>
        /// Gets the bytes that a ReadOnly flush starting mid-sector must write again below its start: the durable part of that sector, from
        /// <see cref="lastPartialSector"/>, or read back from the device when that does not describe this sector.
        /// </summary>
        /// <param name="sectorStartAddress">Logical address of the sector the flush starts in</param>
        /// <param name="prefixLength">Number of bytes from the start of the sector to the start of the flush</param>
        /// <param name="deviceAddress">Device address of the sector</param>
        /// <param name="device">The main log device</param>
        /// <param name="callback">The flush's completion callback, which receives a read-back failure</param>
        /// <param name="asyncResult">The flush's result, which returns <paramref name="firstSector"/> to the pool when the flush completes</param>
        /// <param name="firstSector">A sector buffer starting with the durable bytes; null if the range below the flush was skipped, so the
        /// bytes may come from the live page</param>
        /// <returns>False if the read-back failed, in which case the failure has been reported to <paramref name="callback"/></returns>
        private bool TryGetDurableSectorPrefix<TContext>(long sectorStartAddress, int prefixLength, ulong deviceAddress, IDevice device,
            DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult, out SectorAlignedMemory firstSector)
        {
            var image = lastPartialSector;
            if (image is not null && image.SectorStart == sectorStartAddress && image.Length == prefixLength)
            {
                if (image.Bytes is null)
                {
                    firstSector = null;
                    return true;
                }
                firstSector = bufferPool.Get(sectorSize);
                asyncResult.freeBuffer1 = firstSector;
                image.Bytes.AsSpan().CopyTo(new Span<byte>(firstSector.aligned_pointer, prefixLength));
                return true;
            }

            // No image of this sector, as for the first flush after recovery: read it back once its earlier write has completed.
            WaitForWritesBelow(sectorStartAddress + prefixLength);
            firstSector = bufferPool.Get(sectorSize);
            asyncResult.freeBuffer1 = firstSector;
            if (TryReadDevice(device, deviceAddress, (IntPtr)firstSector.aligned_pointer, (uint)sectorSize, "Partial-sector flush read-back",
                    out var errorCode, out var numBytesRead, out var ioException))
                return true;
            callback(errorCode, numBytesRead, asyncResult, ioException);
            return false;
        }

        /// <summary>
        /// Keeps what a ReadOnly flush writes for the durable part of the sector it ends in, which the next flush writes again.
        /// </summary>
        /// <param name="pageStart">Logical address of the start of the flushed page</param>
        /// <param name="alignedStartOffset">Page offset of the sector the flush starts in</param>
        /// <param name="endOffset">Page offset where the flush ends</param>
        /// <param name="logPagePointer">The live page</param>
        /// <param name="firstSector">The flush's composed first sector, or null</param>
        private void RecordPartialSectorImage(long pageStart, int alignedStartOffset, int endOffset, byte* logPagePointer, SectorAlignedMemory firstSector)
        {
            var endSectorOffset = RoundDown(endOffset, sectorSize);
            var length = endOffset - endSectorOffset;
            if (length == 0)
            {
                lastPartialSector = null;
                return;
            }

            // A flush that ends in the sector it composed writes that sector from firstSector, not the live page.
            var source = firstSector is not null && endSectorOffset == alignedStartOffset ? firstSector.aligned_pointer : logPagePointer + endSectorOffset;
            lastPartialSector = new PartialSectorImage(pageStart + endSectorOffset, length, new ReadOnlySpan<byte>(source, length).ToArray());
        }

        /// <summary>
        /// Blocks until every flush write below <paramref name="address"/> has completed, so that a write of the sector holding it cannot be
        /// overtaken by an earlier write of the same sector that is still in flight.
        /// </summary>
        /// <remarks>The caller must not hold the epoch. Returns early if the allocator is disposed, or if a failed flush below
        /// <paramref name="address"/> means FlushedUntilAddress can never reach it.</remarks>
        private void WaitForWritesBelow(long address)
        {
            Debug.Assert(!epoch.ThisInstanceProtected(), "WaitForWritesBelow must not hold the epoch");
            var spins = 0;
            var localFlushEvent = flushEvent;
            while (FlushedUntilAddress < address)
            {
                if (IsDisposed || HasFlushFailedBelow(address))
                    return;
                if (++spins < Constants.kFlushSpinCount)
                    _ = Thread.Yield();
                else
                    localFlushEvent.Wait();
                localFlushEvent = flushEvent;
            }
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

                var updatedLIFUA = MonotonicUpdate(ref LastIssuedFlushedUntilAddress, flushEndAddress, out var oldLastIssuedFlushedUntilAddress);
                Debug.Assert(updatedLIFUA, $"Failed to update LIFUA");
                Debug.Assert(oldLastIssuedFlushedUntilAddress == flushStartAddress, $"Expected LastIssuedFlushedUntilAddress to be {flushStartAddress} but was {oldLastIssuedFlushedUntilAddress}");

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

                // End if we have exhausted co-operative work. This includes the case where OngoingFUA and flushEndAddress are already 0.
                if (Interlocked.CompareExchange(ref OngoingFlushedUntilAddress, 0, flushEndAddress) == flushEndAddress)
                    break;
                _ = Thread.Yield();
            }
        }

        private void AsyncReadPageCallback(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            var result = (PageAsyncReadResult<Empty>)context;
            result.errorCode = errorCode;
            result.numBytesRead = numBytes;
            result.ioException = ioException;
            _ = result.handle.Signal();
        }

        private bool TryReadDevice(IDevice readDevice, ulong sourceAddress, IntPtr destinationAddress, uint readLength, string operation,
            out uint errorCode, out uint numBytesRead, out Exception ioException)
        {
            PageAsyncReadResult<Empty> result = new() { handle = new CountdownEvent(1) };
            try
            {
                readDevice.ReadAsync(sourceAddress, destinationAddress, readLength, AsyncReadPageCallback, result);
                result.handle.Wait();

                errorCode = result.errorCode;
                numBytesRead = result.numBytesRead;
                ioException = result.ioException;

                if (errorCode == 0 && numBytesRead == readLength)
                    return true;

                if (errorCode == 0)
                {
                    errorCode = uint.MaxValue;
                    ioException = new EndOfStreamException($"{operation} read {numBytesRead} of {readLength} bytes at device offset {sourceAddress}");
                }

                if (ioException is null)
                {
                    logger?.LogError("{operation} failed reading {readLength} bytes at device offset {sourceAddress}: error {errorCode}; transferred {numBytesRead} bytes",
                        operation, readLength, sourceAddress, errorCode, numBytesRead);
                }
                else
                {
                    logger?.LogError("{operation} failed reading {readLength} bytes at device offset {sourceAddress}: {exception}; transferred {numBytesRead} bytes",
                        operation, readLength, sourceAddress, Utility.GetCallbackExceptionDetail(ioException), numBytesRead);
                }

                return false;
            }
            finally
            {
                result.DisposeHandle();
            }
        }

        /// <summary>Write <paramref name="writeLength"/> sector-aligned bytes to <paramref name="writeDevice"/> and wait for the write to complete.</summary>
        /// <returns>False if the write failed, with the device's error code and exception.</returns>
        private bool TryWriteDevice(IDevice writeDevice, IntPtr sourceAddress, ulong destinationAddress, uint writeLength,
            out uint errorCode, out Exception ioException)
        {
            // The read result and callback record any device completion.
            PageAsyncReadResult<Empty> result = new() { handle = new CountdownEvent(1) };
            try
            {
                writeDevice.WriteAsync(sourceAddress, destinationAddress, writeLength, AsyncReadPageCallback, result);
                result.handle.Wait();
                errorCode = result.errorCode;
                ioException = result.ioException;
                return errorCode == 0;
            }
            finally
            {
                result.DisposeHandle();
            }
        }

        private static TsavoriteIOException CreateReadException(string operation, ulong sourceAddress, uint readLength,
            uint errorCode, uint numBytesRead, Exception ioException)
            => new($"{operation} failed reading {readLength} bytes at device offset {sourceAddress}: error {errorCode}; transferred {numBytesRead} bytes", ioException);

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
            if (diskLogRecord.Info.HasSnapshotObjectLogReference)
            {
                prevAddressToRead = diskLogRecord.Info.PreviousAddress;
                return false;
            }
            if (diskLogRecord.DataHeader.RecordIsInline)
                return true;

            // A record read back from disk was necessarily written by a flush, so it must carry a position. The in-memory unstamping
            // that RecordHasNoReadablePosition describes cannot apply here: this is a dedicated single-record IO buffer, not a shared
            // page that an earlier walk could have deserialized in place. So an unstamped record means a Valid record reached disk
            // without its object bytes, which is a flush defect rather than something to recover from.
            if (RecordHasNoReadablePosition(in diskLogRecord.logRecord))
                throw new TsavoriteException($"Record read from disk at {ctx.logicalAddress} has out-of-line components but an unstamped object-log position");

            // This is a runtime pending read of the live log, which is written in the current object-log format.
            using var reader = new ObjectLogReader<TStoreFunctions>(GetReadObjectLog(logDevice: null), storeFunctions);
            if (ReadRecordObjects(reader, ref diskLogRecord.logRecord, ctx.requestKey))
            {
                // Success. The deserialized heap object's Dispose() will be invoked when the DiskLogRecord
                // is disposed (ObjectIdMap.Free → IHeapObject.Dispose), unless the object is transferred out
                // (e.g. via CopyToTail) in which case the transient ObjectIdMap slot is cleared without disposing.
                prevAddressToRead = 0;
                return true;
            }

            // The overflow key did not match, so get the previous record. The base call transferred ctx.record to diskLogRecord.
            prevAddressToRead = diskLogRecord.Info.PreviousAddress;
            return false;
        }

        /// <summary>
        /// Read <paramref name="logRecord"/>'s out-of-line components through <paramref name="reader"/> and materialize them in the record.
        /// </summary>
        /// <param name="reader">The reader over the object log the record's position belongs to.</param>
        /// <param name="logRecord">A record read from disk, still carrying its object-log position and hints.</param>
        /// <param name="requestedKey">The requested key, if not ReadAtAddress; a record whose overflow key does not match it is not read further.
        /// Without one this is ReadAtAddress (an implicit match), a scan, or recovery.</param>
        /// <returns>False if <paramref name="requestedKey"/> is set and the record's overflow key does not match it; otherwise true.</returns>
        private bool ReadRecordObjects<TKey>(ObjectLogReader<TStoreFunctions> reader, ref LogRecord logRecord, TKey requestedKey)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            // Capture the position and hints before materializing anything: assigning a component unstamps the record.
            var recordRef = logRecord.GetObjectLogRecordRef();
            var keyWasSet = false;
            try
            {
                reader.BeginRecord(in recordRef);
                if (recordRef.KeyIsOverflow)
                {
                    // This assignment also allocates the slot in ObjectIdMap, replacing the size hint the flush stamped in the objectId slot.
                    logRecord.KeyOverflow = reader.ReadKey();
                    keyWasSet = true;
                    if (!requestedKey.IsEmpty && !storeFunctions.KeysEqual(requestedKey, logRecord))
                    {
                        // Release the key, and clear the value field, which still holds the flush's slot id and size hint rather than a
                        // transient slot, so the caller's dispose of the record frees nothing it does not own.
                        logRecord.OnDeserializationError(keyWasSet);
                        return false;
                    }
                }

                if (recordRef.ValueIsOverflow)
                    logRecord.ValueOverflow = reader.ReadValueOverflow();
                else if (recordRef.ValueIsObject)
                {
                    // An object that serialized to no bytes carries no type information, so the reader records it as absent (null) rather than
                    // invoking the deserializer on an empty stream.
                    logRecord.SetDeserializedValueObject(reader.ReadValueObject());
                }

                // Restore non-inline length fields to ObjectIdSize for in-memory record length correctness.
                logRecord.OnObjectReadComplete();
                return true;
            }
            catch
            {
                logRecord.OnDeserializationError(keyWasSet);
                throw;
            }
            finally
            {
                reader.EndRecord();
            }
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
            // read end, so the fields read near its tail -- the ObjectLogPosition word and the raw RDH length bits -- fall past the bytes
            // actually transferred and are read from the un-read buffer tail. That tail is only incidentally zero (the read buffer is
            // cleared before the read); a different boundary alignment could bisect a field and yield arbitrary bytes. Either way the
            // resulting position would be bogus. Only fall back to the read length when the caller left the extent unset.
            if (asyncResult.maxAddressOffsetOnPage == 0)
                asyncResult.maxAddressOffsetOnPage = aligned_read_length;

            device.ReadAsync(alignedSourceAddress, destinationPtr, aligned_read_length, AsyncReadPageWithObjectsCallback<TContext>, asyncResult);
        }

        private void AsyncReadPageWithObjectsCallback<TContext>(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            var result = (PageAsyncReadResult<TContext>)context;

            if (errorCode != 0)
            {
                // The page read failed or was cancelled. The destination buffer may contain partial or
                // garbage data, so do not attempt to parse record headers or deserialize objects from it
                // (doing so can compute bogus lengths and throw OutOfMemoryException/AccessViolation).
                // Surface the error to the real page-read callback, which handles the failure.
                if (ioException is null)
                    logger?.LogError($"{nameof(AsyncReadPageWithObjectsCallback)} error: {{errorCode}}", errorCode);
                else
                    logger?.LogError($"{nameof(AsyncReadPageWithObjectsCallback)} error: {{exception}}", Utility.GetCallbackExceptionDetail(ioException));
                result.callback(errorCode, numBytes, context, ioException: ioException);
                return;
            }

            // Recovery Pass 1 skips object deserialization; those objects are loaded afterwards by LoadObjectsForRecoveryPass2.
            if (result.recoveryPhase != RecoveryPhase.Pass1)
            {
                // The remaining callers are runtime reads and the post-recovery head read (RecoveryPhase.None), which read the live
                // current-format log into a transient map, so decode as current.
                DeserializeObjectsOnPage((long)result.destinationPtr, result.maxAddressOffsetOnPage, transientObjectIdMap, objectLogDevice: null,
                    HybridLogRecoveryInfo.CheckpointVersion);
            }

            // Call the "real" page read callback
            result.callback(errorCode, numBytes, context, ioException: ioException);
            result.Free();
        }

        /// <summary>
        /// Deserialize objects on a page that has already been loaded into memory (physical addresses).
        /// </summary>
        /// <param name="pageStartPhysicalAddress">Physical start address of the page in memory</param>
        /// <param name="maxAddressOffsetOnPage">Maximum offset on the page (PageSize or less for partial pages)</param>
        /// <param name="objectIdMap">The ObjectIdMap to use for deserialized objects</param>
        /// <param name="objectLogDevice">The object log to read: null for the main object log, otherwise the snapshot object log being recovered</param>
        /// <param name="checkpointVersion">The checkpoint metadata version whose object-log encoding is being decoded; current-format for a runtime read.</param>
        /// <param name="legacyReadEnd">For a downlevel snapshot object log, its exclusive durable end; unused otherwise</param>
        /// <param name="minAddressOffsetOnPage">First record offset to scan; callers use this to split a page at an object-log address-space boundary</param>
        private void DeserializeObjectsOnPage(long pageStartPhysicalAddress, long maxAddressOffsetOnPage, ObjectIdMap objectIdMap, IDevice objectLogDevice,
            int checkpointVersion, long legacyReadEnd = 0, long minAddressOffsetOnPage = PageHeader.Size)
        {
            if (HybridLogRecoveryInfo.UsesDownlevelObjectLog(checkpointVersion))
            {
                using var legacyReadBuffers = CreateLegacyReadBuffers(objectLogDevice);
                DeserializeLegacyObjectsOnPage(pageStartPhysicalAddress, maxAddressOffsetOnPage, objectIdMap, legacyReadBuffers, checkpointVersion,
                    GetLegacyReadHardEnd(objectLogDevice is null ? 0 : legacyReadEnd), minAddressOffsetOnPage);
                return;
            }

            ObjectLogReader<TStoreFunctions> reader = null;
            try
            {
                var recordAddress = pageStartPhysicalAddress + minAddressOffsetOnPage;
                var endAddress = pageStartPhysicalAddress + maxAddressOffsetOnPage;
                while (recordAddress < endAddress)
                {
                    var logRecord = new LogRecord(recordAddress, objectIdMap);

                    // PAGE-EXTENT GUARD -- not a record-validity or stamping test. The device read is sector-aligned, so endAddress
                    // over-estimates the page's valid extent by up to a sector and this walk can reach bytes no flush ever wrote.
                    // A zeroed RecordDataHeader marks that boundary: every real record sets at least one inline flag, and bit 0
                    // (KeyIsInline) is set even for a zero-length key, so word == 0 means "no record here". Parsing those bytes
                    // would report KeyIsOverflow/ValueIsOverflow set -- out-of-line is the absence of the inline flags -- and yield
                    // a bogus minimum AllocatedSize, stepping the walk off the record boundary.
                    var allocatedSize = logRecord.AllocatedSize;
                    if (logRecord.DataHeader.word == 0 || allocatedSize <= 0 || recordAddress + allocatedSize > endAddress)
                        break;
                    recordAddress += allocatedSize;

                    if (!logRecord.DataHeader.RecordHasObjects)
                        continue;

                    // A main-log image that references a checkpoint's object log has no components in the main object log.
                    if (objectLogDevice is null && logRecord.Info.Valid && logRecord.Info.HasSnapshotObjectLogReference)
                        logRecord.InfoRef.SetInvalid();

                    // A record whose components are not read gets empty ones, so that every slot on the page indexes objectIdMap: an image's
                    // slots index the map of the page it was flushed from, and a scan that includes closed records resolves the slots of invalid
                    // records too. This covers invalid records, which are never read, and a record with no readable position, whose read would
                    // consume another record's bytes (see RecordHasNoReadablePosition). Neither added object-log bytes, so the recovered-object
                    // accounting is unchanged, and eviction skips invalid records.
                    if (!logRecord.Info.Valid || RecordHasNoReadablePosition(in logRecord))
                    {
                        logRecord.MaterializeObjectsWithoutReading();
                        continue;
                    }

                    reader ??= new(GetReadObjectLog(objectLogDevice), storeFunctions);
                    _ = ReadRecordObjects(reader, ref logRecord, default(EmptyKey));
                    TrackRecoveredObjectRecord(in logRecord);
                }
            }
            finally
            {
                reader?.Dispose();
            }
        }

        /// <summary>
        /// Deserialize objects on a page of a downlevel (cv7) checkpoint, whose object log is a dense file read through the streaming ring.
        /// </summary>
        private void DeserializeLegacyObjectsOnPage(long pageStartPhysicalAddress, long maxAddressOffsetOnPage, ObjectIdMap objectIdMap,
            CircularDiskReadBuffer readBuffers, int checkpointVersion, LegacyObjectLogPosition hardReadEndPosition, long minAddressOffsetOnPage)
        {
            LegacyObjectLogPosition startPosition = new(), initialEndPosition = new();
            var recordAddress = pageStartPhysicalAddress + minAddressOffsetOnPage;
            var endAddress = pageStartPhysicalAddress + maxAddressOffsetOnPage;

            // Establish an initial read range from record positions and the final record's first-component length. A later record
            // position is a safe successor bound for the preceding record because callers split mixed snapshot/main pages at the exact
            // address-space boundary. Stop scanning once one full ring fill is required; component lengths extend demand as the ring is
            // consumed.
            while (recordAddress < endAddress)
            {
                var logRecord = new LogRecord(recordAddress);

                // Must match the walk in DeserializeObjectsOnPage; see the page-extent guard there.
                var allocatedSize = logRecord.AllocatedSize;
                if (logRecord.DataHeader.word == 0 || allocatedSize <= 0 || recordAddress + allocatedSize > endAddress)
                    break;
                recordAddress += allocatedSize;

                if (logRecord.DataHeader.RecordHasObjects && logRecord.Info.Valid)
                {
                    if (RecordHasNoReadablePosition(in logRecord))
                        continue;

                    var position = new LegacyObjectLogPosition(
                        logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength, checkpointVersion),
                        objectLogSegmentSizeBits);

                    if (!startPosition.IsSet)
                        startPosition = position;
                    else if (position - startPosition >= readBuffers.Capacity)
                    {
                        initialEndPosition = position;
                        break;
                    }

                    initialEndPosition = position;
                    initialEndPosition.Advance(keyLength > 0 ? (ulong)keyLength : valueLength);
                    if (initialEndPosition - startPosition >= readBuffers.Capacity)
                        break;
                }
            }

            // The page may not have contained any records with objects
            if (!startPosition.IsSet)
                return;

            // Every out-of-line component on this page may be zero length, leaving nothing to read; materialize instead of opening a
            // zero-length read range.
            var totalBytesToRead = initialEndPosition - startPosition;
            var hasBytesToRead = totalBytesToRead > 0;

            // Second pass: deserialize objects
            readBuffers.nextFileReadPosition = startPosition;
            recordAddress = pageStartPhysicalAddress + minAddressOffsetOnPage;
            var logReader = new LegacyObjectLogReader<TStoreFunctions>(readBuffers, storeFunctions);
            if (hasBytesToRead)
                logReader.OnBeginReadRecords(startPosition, totalBytesToRead, hardReadEndPosition);

            try
            {
                while (recordAddress < endAddress)
                {
                    var logRecord = new LogRecord(recordAddress, objectIdMap);

                    // Must match the first pass exactly; see the bounds check there.
                    var allocatedSize = logRecord.AllocatedSize;
                    if (logRecord.DataHeader.word == 0 || allocatedSize <= 0 || recordAddress + allocatedSize > endAddress)
                        break;
                    recordAddress += allocatedSize;

                    if (logRecord.DataHeader.RecordHasObjects && logRecord.Info.Valid)
                    {
                        if (!hasBytesToRead || RecordHasNoReadablePosition(in logRecord))
                        {
                            logRecord.MaterializeObjectsWithoutReading();
                            continue;
                        }

                        _ = logReader.ReadRecordObjects(ref logRecord, default(EmptyKey), objectLogSegmentSizeBits, checkpointVersion);
                        TrackRecoveredObjectRecord(in logRecord);
                    }
                }
            }
            finally
            {
                if (hasBytesToRead)
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

        /// <summary>
        /// A disk-image record's object-log position and a best-guess size for its object-log bytes, from its stamped hints.
        /// </summary>
        /// <remarks>
        /// For a current-format record the size is what its first read covers: exact for a single small component, a page-count bound for a
        /// larger one, and the top directory record's size for a record with a directory. For a downlevel record it is exact. Budget estimates
        /// tolerate the error.
        /// </remarks>
        private static long EstimateRecordObjectLogExtent(in LogRecord logRecord, int checkpointVersion, out long position)
        {
            if (HybridLogRecoveryInfo.UsesDownlevelObjectLog(checkpointVersion))
            {
                position = (long)logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength, checkpointVersion);
                return keyLength + (long)valueLength;
            }

            var recordRef = logRecord.GetObjectLogRecordRef();
            position = (long)(recordRef.PositionWord & ObjectLogPositionInfo.AddressMask);
            return recordRef.KeyIsOverflow
                ? ObjectLogExtent.KeyInitialRead(recordRef.KeyRdhLengthBits, recordRef.KeySizeHint, (recordRef.PositionWord & ObjectLogPositionInfo.kKeyIsExactSizeMask) != 0)
                : ObjectLogExtent.ValueInitialRead(recordRef.ValueSizeHint, (recordRef.PositionWord & ObjectLogPositionInfo.kValueIsExactSizeMask) != 0);
        }

        /// <inheritdoc/>
        internal override long CalculatePageObjectSizes(int page, long startAddress, long untilAddress, int checkpointVersion)
        {
            var recordAddress = Math.Max(startAddress, GetFirstValidLogicalAddressOnPage(page));
            var endAddress = Math.Min(untilAddress, GetLogicalAddressOfStartOfPage(page + 1));

            // These are disk-image records (recovery, before object deserialization), so their positions and hints hold the on-disk values.
            // A page's records are written to the object log in order, so estimate the page's object-log bytes as the span from the first
            // object record's position to the last object record's position plus that record's estimated extent. This is a best guess (the
            // eviction/budget logic tolerates an over-estimate).
            long startPosition = -1, endPosition = -1, endExtent = 0;

            while (recordAddress < endAddress)
            {
                var logRecord = new LogRecord(GetPhysicalAddress(recordAddress));
                var allocatedSize = logRecord.AllocatedSize;
                if (allocatedSize <= 0)
                    ThrowTsavoriteException($"LogRecord size should be > 0; encountered {allocatedSize}");

                recordAddress += allocatedSize;
                if (recordAddress > endAddress)
                    ThrowTsavoriteException($"Unaligned end of page; record exceeded page by {recordAddress - endAddress} bytes");

                if (logRecord.Info.Valid && logRecord.DataHeader.RecordHasObjects && !RecordHasNoReadablePosition(in logRecord))
                {
                    endExtent = EstimateRecordObjectLogExtent(in logRecord, checkpointVersion, out endPosition);
                    if (startPosition < 0)
                        startPosition = endPosition;
                }
            }

            return startPosition < 0 ? 0 : Math.Max(0, endPosition + endExtent - startPosition);
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
        internal override void ResetRecoveredResidentPageHeader(int page)
        {
            var pageIndex = GetPageIndexForPage(page);
            if (!IsAllocated(pageIndex))
                return;
            ((PageHeader*)pagePointers[pageIndex])->objectLogLowestPositionWord = ObjectLogPositionInfo.NotSet;
        }

        /// <inheritdoc/>
        internal override void LoadObjectsForRecoveryPass2(int page, long fromAddress, long untilAddress, IDevice objectLogDevice, int checkpointVersion,
            long legacyReadEnd = 0)
        {
            var pageStartAddress = GetFirstValidLogicalAddressOnPage(page);
            var address = Math.Max(fromAddress, pageStartAddress);
            var endAddress = Math.Min(untilAddress, GetLogicalAddressOfStartOfPage(page + 1));
            if (address >= endAddress)
                return;

            var pagePhysicalAddress = GetPhysicalAddress(GetLogicalAddressOfStartOfPage(page));
            var maxOffset = endAddress - GetLogicalAddressOfStartOfPage(page);
            var minOffset = address - GetLogicalAddressOfStartOfPage(page);
            var objectIdMapToUse = objectPages[page % BufferSize].objectIdMap;
            DeserializeObjectsOnPage(pagePhysicalAddress, maxOffset, objectIdMapToUse, objectLogDevice, checkpointVersion, legacyReadEnd, minOffset);
        }

        /// <inheritdoc/>
        internal override long FindHeadAddressCutoffOnPage(int page, long untilAddress, long totalPageObjectSize, int numPagesBelowCurrentPage, long remainingBudget, int checkpointVersion, out int numPagesBelowToEvict)
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

                if (logRecord.Info.Valid && logRecord.DataHeader.RecordHasObjects && !RecordHasNoReadablePosition(in logRecord))
                {
                    // Disk-image record (recovery, before object deserialization): a best-guess per-record object size from its hints.
                    overBudgetAmount -= EstimateRecordObjectLogExtent(in logRecord, checkpointVersion, out _);
                    if (overBudgetAmount <= 0)
                        return recordAddress;
                }
            }

            return stopAddress;
        }

        /// <summary>
        /// Clear checkpoint serialization cached by heap objects without copying records or remapping overflow keys.
        /// </summary>
        internal override void ClearSerializedObjectData(long beginAddress, long endAddress)
        {
            // Consume the cached extent and reset it, so the next sweep bounds itself by what is cached after this one. A
            // cache landing during the sweep re-establishes the pair from the empty sentinel, so it is covered by the next
            // sweep rather than lost.
            var cachedMin = Interlocked.Exchange(ref serializedObjectCacheMinAddress, long.MaxValue);
            var cachedMax = Interlocked.Exchange(ref serializedObjectCacheMaxAddress, 0);

            // Both sentinels mean nothing was cached since the last sweep, so there is nothing in range to clear and the
            // per-record epoch acquire/release below would be pure overhead. Testing both matters: a cache landing between
            // the two exchanges can leave one of them behind, and that residue must still force a sweep.
            if (cachedMin == long.MaxValue && cachedMax == 0)
                return;

            // Narrow to the addresses actually cached. Both are record starts, so clamping cannot land mid-record; the end
            // is exclusive, hence the +1 to keep the record beginning at cachedMax in range. A torn capture yields an
            // incoherent pair, so narrow only when the pair is coherent and otherwise fall back to the caller's range
            // rather than risk skipping a cached record.
            if (cachedMin <= cachedMax)
            {
                if (cachedMin > beginAddress)
                    beginAddress = cachedMin;
                if (cachedMax < endAddress - 1)
                    endAddress = cachedMax + 1;
            }

            ClearSerializedObjectDataInRange(beginAddress, endAddress);
        }

        void ClearSerializedObjectDataInRange(long beginAddress, long endAddress)
        {
            var address = beginAddress;
            while (address < endAddress)
            {
                IHeapObject valueObject = default;
                var epochTaken = epoch.ResumeIfNotProtected();
                try
                {
                    if (Initializing)
                        return;

                    var headAddress = Volatile.Read(ref HeadAddress);
                    var stopAddress = Math.Min(endAddress, GetTailAddress());
                    if (address < headAddress)
                        address = headAddress;
                    if (address >= stopAddress)
                        return;

                    var page = GetPage(address);
                    var firstValidAddress = GetFirstValidLogicalAddressOnPage(page);
                    if (address < firstValidAddress)
                        address = firstValidAddress;
                    if (address >= stopAddress)
                        return;

                    var offset = GetOffsetOnPage(address);
                    if (offset == 0)
                    {
                        address = GetFirstValidLogicalAddressOnPage(page + 1);
                        continue;
                    }

                    var physicalAddress = GetPhysicalAddress(address);
                    var objectIdMap = objectPages[GetPageIndexForAddress(address)].objectIdMap;
                    var logRecord = new LogRecord(physicalAddress, objectIdMap);
                    var allocatedSize = logRecord.AllocatedSize;
                    if (allocatedSize <= 0 || offset + allocatedSize > PageSize)
                    {
                        address = GetFirstValidLogicalAddressOnPage(page + 1);
                        continue;
                    }
                    address += allocatedSize;

                    var dataHeader = logRecord.DataHeader;
                    if (logRecord.Info.IsNull || !dataHeader.ValueIsObject)
                        continue;

                    var (_, valueAddress) = dataHeader.GetValueFieldInfo(physicalAddress);
                    _ = objectIdMap.TryGetHeapObject(*(int*)valueAddress, out valueObject);
                }
                finally
                {
                    if (epochTaken)
                        epoch.Suspend();
                }

                // The local reference remains valid if the record clears or reuses its object-map slot.
                valueObject?.ClearSerializedObjectData();
            }
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
            using ObjectScanIterator<TStoreFunctions, ObjectAllocator<TStoreFunctions>> iter = new(store, this, cursor, endAddress, epoch, GetScanCursorBufferingMode(count),
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