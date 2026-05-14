// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Identifies which log a record eviction originated from. Main log and read cache are
    /// separate <see cref="AllocatorBase{TStoreFunctions, TAllocator}"/> instances but share
    /// a single <see cref="IStoreFunctions"/>; this enum lets an <see cref="IRecordTriggers"/>
    /// implementation tell the two apart when reacting to <see cref="IRecordTriggers.OnEvict"/>.
    /// </summary>
    public enum EvictionSource
    {
        /// <summary>The record is being evicted from the main hybrid log.</summary>
        MainLog,

        /// <summary>The record is being evicted from the read cache.</summary>
        ReadCache,
    }

    /// <summary>
    /// Per-record lifecycle callbacks invoked by the store at key events:
    /// flush to disk, eviction, disposal, and disk read.
    /// </summary>
    public interface IRecordTriggers
    {
        /// <summary>
        /// If true, <see cref="OnFlush(ref LogRecord, long)"/> is called per valid record on the
        /// original in-memory page before it is flushed to disk.
        /// </summary>
        bool CallOnFlush => false;

        /// <summary>
        /// If true, <see cref="OnEvict(ref LogRecord, EvictionSource)"/> is called per non-tombstoned
        /// record when a page is evicted past HeadAddress. Returning false lets the allocator skip the
        /// per-record OnEvict callback when the application has no work to do.
        /// Note: Tsavorite's internal heap-size accounting runs regardless of this flag.
        /// </summary>
        bool CallOnEvict => false;

        /// <summary>
        /// If true, <see cref="OnDiskRead(ref LogRecord)"/> is called per record loaded from
        /// disk into memory (recovery, delta log apply, pending reads, push scans).
        /// </summary>
        bool CallOnDiskRead => false;

        /// <summary>
        /// If true, <see cref="PostCopyToTail{TSourceLogRecord}(in TSourceLogRecord, long, ref LogRecord, long)"/>
        /// is called after a successful CAS into the hash chain by <c>TryCopyToTail</c> (compaction lookup/scan,
        /// CopyReadsToTail, ConditionalCopyToTail). Allows the application to perform per-record post-copy work
        /// while the destination record is still sealed (concurrent readers see <see cref="RecordInfo.SkipOnScan"/>
        /// and retry).
        /// </summary>
        bool CallPostCopyToTail => false;

        /// <summary>
        /// If true, <see cref="OnTruncate(long)"/> is called from
        /// <see cref="AllocatorBase{TStoreFunctions, TAllocator}.TruncateUntilAddressBlocking"/>
        /// after the underlying device has been truncated to the new BeginAddress.
        /// </summary>
        bool CallOnTruncate => false;

        /// <summary>
        /// Called when a record is disposed due to delete, expiration, CAS failure, elision,
        /// revivification, or other store-internal reasons. Use <paramref name="reason"/> to
        /// distinguish the event type (e.g. <see cref="DisposeReason.Deleted"/> for tombstoning).
        /// Heap-size accounting is handled internally by the store — implementations only need
        /// this callback for app-level resource cleanup (e.g. releasing native handles).
        /// NOT called for page eviction (use <see cref="OnEvict"/> instead).
        /// NOT called for transient records materialized from disk (use <see cref="OnDisposeDiskRecord"/>).
        /// Default implementation is a no-op.
        /// </summary>
        void OnDispose(ref LogRecord logRecord, DisposeReason reason) { }

        /// <summary>
        /// Called when a transient <see cref="DiskLogRecord"/> is about to be disposed — e.g. a record
        /// deserialized from disk for a pending Read/RMW, delivered via scan iteration, or streamed
        /// during cluster migration/replication. If the value object implements <see cref="IDisposable"/>
        /// and holds resources that this DiskLogRecord owns, the application should invoke
        /// <see cref="IDisposable.Dispose"/> from this callback.
        /// <para>
        /// Caveat: scan iterators may briefly wrap an in-memory log record as a DiskLogRecord that
        /// <em>shares</em> its value-object reference with the live on-log record. Implementations that
        /// hold external resources should either gate disposal on <paramref name="reason"/> or avoid
        /// disposing the value object from this callback; uncritical disposal there would corrupt the
        /// still-alive on-log record.
        /// </para>
        /// Default implementation is a no-op.
        /// </summary>
        void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason) { }

        /// <summary>
        /// Called per valid record on the original in-memory page before flush to disk.
        /// Allows the application to snapshot external resources (using <paramref name="logicalAddress"/>
        /// to disambiguate per-flush snapshot identity) and set flags on the live record.
        /// Only called when <see cref="CallOnFlush"/> is true. Default implementation is a no-op.
        /// </summary>
        /// <param name="logRecord">The record being flushed (in-memory copy).</param>
        /// <param name="logicalAddress">Logical address of the record being flushed; useful for naming
        /// per-flush snapshot files immutably.</param>
        void OnFlush(ref LogRecord logRecord, long logicalAddress) { }

        /// <summary>
        /// Called per non-tombstoned record when a page is evicted past HeadAddress.
        /// Allows the application to free external resources (e.g. native memory).
        /// Only called when <see cref="CallOnEvict"/> is true.
        /// </summary>
        /// <param name="logRecord">The record being evicted.</param>
        /// <param name="source">Which log (main or read cache) the record is being evicted from.</param>
        void OnEvict(ref LogRecord logRecord, EvictionSource source) { }

        /// <summary>
        /// Called per record loaded from disk into memory. Allows the application to invalidate
        /// stale external resource handles (e.g. native pointers from a previous process).
        /// Only called when <see cref="CallOnDiskRead"/> is true. Default implementation is a no-op.
        /// </summary>
        void OnDiskRead(ref LogRecord logRecord) { }

        /// <summary>
        /// Called once before recovering records from a checkpoint snapshot file.
        /// Default implementation is a no-op.
        /// </summary>
        void OnRecovery(System.Guid checkpointToken) { }

        /// <summary>
        /// Called per record recovered from a checkpoint snapshot file (above FlushedUntilAddress).
        /// Only called when <see cref="CallOnDiskRead"/> is true. Default implementation is a no-op.
        /// </summary>
        void OnRecoverySnapshotRead(ref LogRecord logRecord) { }

        /// <summary>
        /// Called at checkpoint lifecycle points identified by <paramref name="trigger"/>.
        /// Default implementation is a no-op.
        /// </summary>
        void OnCheckpoint(CheckpointTrigger trigger, System.Guid checkpointToken) { }

        /// <summary>
        /// Called after <c>TryCopyToTail</c> has CAS'd a destination record into the hash chain
        /// but before unsealing the destination. While this callback runs, the destination record
        /// is sealed (concurrent readers see <see cref="RecordInfo.SkipOnScan"/> and retry), so
        /// implementations may safely mutate <paramref name="dstLogRecord"/>'s value bytes (e.g.
        /// adjust embedded native handles). The source record is still in the chain with its old
        /// state; implementations may also clear handles on the source if appropriate (analogous
        /// to RIPROMOTE's PostCopyUpdater for live transfers).
        ///
        /// <para>Only called when <see cref="CallPostCopyToTail"/> is true. Default implementation is a no-op.</para>
        /// </summary>
        /// <typeparam name="TSourceLogRecord">Source record type (in-memory log record or DiskLogRecord).</typeparam>
        /// <param name="srcLogRecord">The source record that was copied from.</param>
        /// <param name="srcLogicalAddress">Logical address of the source record (or
        /// <see cref="LogAddress.kInvalidAddress"/> if not available, e.g. read-cache source).</param>
        /// <param name="dstLogRecord">The destination record at the tail (sealed).</param>
        /// <param name="dstLogicalAddress">Logical address of the destination record at the tail.</param>
        void PostCopyToTail<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, long srcLogicalAddress,
                                               ref LogRecord dstLogRecord, long dstLogicalAddress)
            where TSourceLogRecord : ISourceLogRecord
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        { }

        /// <summary>
        /// Called from <see cref="AllocatorBase{TStoreFunctions, TAllocator}.TruncateUntilAddressBlocking"/>
        /// AFTER the device has been truncated to <paramref name="newBeginAddress"/>. Allows the application
        /// to clean up external state (e.g. per-flush snapshot files) tied to log addresses below the new
        /// BeginAddress.
        /// Only called when <see cref="CallOnTruncate"/> is true. Default implementation is a no-op.
        /// </summary>
        /// <param name="newBeginAddress">The new BeginAddress. Application state tied to addresses
        /// strictly less than this value may be reclaimed.</param>
        void OnTruncate(long newBeginAddress) { }
    }

    /// <summary>
    /// Default no-op implementation of <see cref="IRecordTriggers"/>.
    /// </summary>
    /// <remarks>It is appropriate to call methods on this instance as a no-op.</remarks>
    public readonly struct DefaultRecordTriggers : IRecordTriggers
    {
        /// <summary>Default instance.</summary>
        public static readonly DefaultRecordTriggers Instance = new();

        /// <inheritdoc/>
        public bool CallOnFlush => false;

        /// <inheritdoc/>
        public bool CallOnEvict => false;

        /// <inheritdoc/>
        public bool CallOnDiskRead => false;

        /// <inheritdoc/>
        public bool CallPostCopyToTail => false;

        /// <inheritdoc/>
        public bool CallOnTruncate => false;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason) { }
    }

    /// <summary>
    /// No-op implementation of <see cref="IRecordTriggers"/> for SpanByte.
    /// </summary>
    public readonly struct SpanByteRecordTriggers : IRecordTriggers    // TODO remove for dual
    {
        /// <summary>Default instance.</summary>
        public static readonly SpanByteRecordTriggers Instance = new();

        /// <inheritdoc/>
        public bool CallOnFlush => false;

        /// <inheritdoc/>
        public bool CallOnEvict => false;

        /// <inheritdoc/>
        public bool CallOnDiskRead => false;

        /// <inheritdoc/>
        public bool CallPostCopyToTail => false;

        /// <inheritdoc/>
        public bool CallOnTruncate => false;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason) { }
    }
}