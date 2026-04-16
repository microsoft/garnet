// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Per-record and store-level lifecycle callbacks invoked by the store at key events:
    /// flush to disk, eviction, disposal, disk read, and checkpoint.
    /// </summary>
    public interface IRecordTriggers
    {
        /// <summary>
        /// If true, <see cref="OnFlush(ref LogRecord)"/> is called per valid record on the
        /// original in-memory page before it is flushed to disk.
        /// </summary>
        bool CallOnFlush { get; }

        /// <summary>
        /// If true, <see cref="OnEvict(ref LogRecord)"/> is called per non-tombstoned record
        /// when a page is evicted past HeadAddress.
        /// </summary>
        bool CallOnEvict { get; }

        /// <summary>
        /// If true, <see cref="OnDiskRead(ref LogRecord)"/> is called per record loaded from
        /// disk into memory (recovery, delta log apply, pending reads, push scans).
        /// </summary>
        bool CallOnDiskRead { get; }

        /// <summary>
        /// Called when a heap value object needs to be disposed (e.g. during ClearHeapFields).
        /// </summary>
        void OnDisposeValueObject(IHeapObject valueObject, DisposeReason reason);

        /// <summary>
        /// Called when a record is disposed due to delete, CAS failure, or other store-internal reasons.
        /// NOT called for page eviction (use <see cref="OnEvict"/> instead).
        /// Default implementation is a no-op.
        /// </summary>
        void OnDispose(ref LogRecord logRecord, DisposeReason reason) { }

        /// <summary>
        /// Called per valid record on the original in-memory page before flush to disk.
        /// Allows the application to snapshot external resources and set flags on the live record.
        /// Only called when <see cref="CallOnFlush"/> is true. Default implementation is a no-op.
        /// </summary>
        void OnFlush(ref LogRecord logRecord) { }

        /// <summary>
        /// Called per non-tombstoned record when a page is evicted past HeadAddress.
        /// Allows the application to free external resources (e.g. native memory).
        /// Only called when <see cref="CallOnEvict"/> is true. Default implementation is a no-op.
        /// </summary>
        void OnEvict(ref LogRecord logRecord) { }

        /// <summary>
        /// Called per record loaded from disk into memory. Allows the application to invalidate
        /// stale external resource handles (e.g. native pointers from a previous process).
        /// Only called when <see cref="CallOnDiskRead"/> is true. Default implementation is a no-op.
        /// </summary>
        void OnDiskRead(ref LogRecord logRecord) { }

        /// <summary>
        /// Called once before recovering records from a checkpoint snapshot file.
        /// Provides the checkpoint token so the application knows which snapshot files to use.
        /// Default implementation is a no-op.
        /// </summary>
        void OnRecovery(System.Guid checkpointToken) { }

        /// <summary>
        /// Called per record recovered from a checkpoint snapshot file (above FlushedUntilAddress).
        /// Allows the application to mark stubs so the restore path uses the checkpoint snapshot.
        /// Only called when <see cref="CallOnDiskRead"/> is true. Default implementation is a no-op.
        /// </summary>
        void OnRecoverySnapshotRead(ref LogRecord logRecord) { }

        /// <summary>
        /// Called at checkpoint lifecycle points identified by <paramref name="trigger"/>.
        /// <list type="bullet">
        /// <item><see cref="CheckpointTrigger.VersionShift"/>: PREPARE → IN_PROGRESS transition.
        /// Set a barrier to block v+1 operations on external resources.</item>
        /// <item><see cref="CheckpointTrigger.FlushBegin"/>: WAIT_FLUSH, after all v threads
        /// completed. Snapshot external resources and clear the barrier.</item>
        /// </list>
        /// Default implementation is a no-op.
        /// </summary>
        void OnCheckpoint(CheckpointTrigger trigger, System.Guid checkpointToken) { }
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
        public void OnDisposeValueObject(IHeapObject valueObject, DisposeReason reason) { }
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnDisposeValueObject(IHeapObject valueObject, DisposeReason reason) { }
    }
}