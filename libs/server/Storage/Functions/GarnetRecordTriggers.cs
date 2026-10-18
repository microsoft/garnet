// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Record lifecycle triggers for Garnet's unified store. Implements <see cref="IRecordTriggers"/>
    /// to handle per-record cleanup on delete, eviction, flush, copy-to-tail, log truncation,
    /// and disk read for BfTree stubs (RangeIndex).
    /// </summary>
    public readonly struct GarnetRecordTriggers : IRecordTriggers, ICompactionFunctions
    {
        /// <summary>
        /// Cache size tracker for heap size accounting.
        /// Created before the store and initialized after via <c>CacheSizeTracker.Initialize</c>.
        /// May be <c>null</c> if memory tracking is disabled.
        /// </summary>
        internal readonly CacheSizeTracker cacheSizeTracker;

        /// <summary>
        /// Reference to the RangeIndexManager for BfTree lifecycle management.
        /// May be <c>null</c> if RangeIndex is not enabled.
        /// </summary>
        internal readonly RangeIndexManager rangeIndexManager;

        /// <summary>
        /// Reference to the VectorManager for Vector Set lifecycle management.
        /// May be <c>null</c> if Vector Sets are not enabled.
        /// </summary>
        internal readonly VectorManager vectorManager;

        /// <summary>
        /// Creates a GarnetRecordTriggers with a cache size tracker and optional RangeIndexManager.
        /// </summary>
        public GarnetRecordTriggers(CacheSizeTracker cacheSizeTracker, RangeIndexManager rangeIndexManager, VectorManager vectorManager)
        {
            this.cacheSizeTracker = cacheSizeTracker;
            this.rangeIndexManager = rangeIndexManager;
            this.vectorManager = vectorManager;
        }

        // Trigger gates: when EnableRangeIndexPreview=false (the default), GarnetServer
        // passes null in place of a manager, so these gates return false → Tsavorite skips
        // ALL trigger invocations entirely → zero per-operation cost on non-RangeIndex
        // workloads. Without this, every flush/evict/disk-read/CTT/truncate would invoke
        // our callback only to do an early-return on the RecordType check.

        /// <inheritdoc/>
        public bool CallOnFlush => rangeIndexManager != null;

        /// <inheritdoc/>
        public bool CallOnEvict => rangeIndexManager != null || vectorManager != null;

        /// <inheritdoc/>
        public bool CallOnDiskRead => rangeIndexManager != null || vectorManager != null;

        /// <inheritdoc/>
        public bool CallPostCopyToTail => rangeIndexManager != null;

        /// <inheritdoc/>
        public bool CallOnTruncate => rangeIndexManager != null;

        /// <inheritdoc/>
        public void OnDispose(ref LogRecord logRecord, DisposeReason reason)
        {
            if (!logRecord.DataHeader.ValueIsObject)
            {
                // Free BfTree and delete data files on key deletion.
                if (reason is DisposeReason.Deleted or DisposeReason.Expired && logRecord.DataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
                {
                    rangeIndexManager?.DisposeTreeUnderLock(logRecord.Key, logRecord.ValueSpan, deleteFiles: true);
                }

                // Request Vector Set cleanup when the index key is deleted.
                if (reason is DisposeReason.Deleted or DisposeReason.Expired && logRecord.DataHeader.RecordType == VectorManager.RecordType)
                {
                    vectorManager?.RequestDeletion(logRecord.KeyBytes, logRecord.ValueSpan);
                }
            }
        }

        /// <inheritdoc/>
        // Explicit no-op (rather than relying on the IRecordTriggers default) so that the call
        // through the TRecordTriggers generic constraint in StoreFunctions resolves here instead of
        // boxing this struct to dispatch the default interface method on disk-record disposal.
        public readonly void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason) { }

        /// <inheritdoc/>
        public readonly void OnFlush(ref LogRecord logRecord, long logicalAddress)
        {
            if (rangeIndexManager is null
                || logRecord.DataHeader.ValueIsObject
                || logRecord.DataHeader.RecordType != RangeIndexManager.RangeIndexRecordType)
                return;

            rangeIndexManager.SnapshotTreeForFlush(logRecord.Key, logRecord.ValueSpan, logicalAddress);
        }

        /// <inheritdoc/>
        public readonly void OnEvict(ref LogRecord logRecord, EvictionSource source)
        {
            if (!logRecord.DataHeader.ValueIsObject)
            {
                // Free BfTree on page eviction under exclusive lock.
                if (logRecord.DataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
                {
                    rangeIndexManager?.DisposeTreeUnderLock(logRecord.Key, logRecord.ValueSpan, deleteFiles: false);
                }

                // Drop DiskANN side of index
                if (logRecord.DataHeader.RecordType == VectorManager.RecordType)
                {
                    vectorManager?.RequestDropInMemoryIndex(logRecord.KeyBytes, logRecord.ValueSpan);
                }
            }
        }

        /// <inheritdoc/>
        public readonly void OnDiskRead(ref LogRecord logRecord)
        {
            if (!logRecord.DataHeader.ValueIsObject)
            {
                // Invalidate stale TreeHandle bytes on records loaded from disk.
                // The promote (RIPROMOTE) of this stub later stages data.bftree from its flush snapshot.
                if (logRecord.DataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
                {
                    RangeIndexManager.InvalidateStub(logRecord.ValueSpan);
                }

                // Clear DiskANN index pointer so we'll recreate it on first touch
                if (logRecord.DataHeader.RecordType == VectorManager.RecordType)
                {
                    VectorManager.ClearIndexPointer(logRecord.ValueSpan);
                }
            }
        }

        /// <inheritdoc/>
        public readonly void OnRecovery(Guid checkpointToken)
        {
            rangeIndexManager?.SetRecoveredCheckpointToken(checkpointToken);
        }

        /// <inheritdoc/>
        public readonly void OnRecoverySnapshotRead(ref LogRecord logRecord)
        {
            if (!logRecord.DataHeader.ValueIsObject)
            {
                // Above-FUA-at-checkpoint stubs: pre-stage data.bftree from the checkpoint snapshot
                // file DURING recovery (snapshot files may be deleted post-recovery). Below-FUA
                // stubs are staged lazily by their promote (RIPROMOTE) on first access.
                if (rangeIndexManager is not null && logRecord.DataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
                {
                    RangeIndexManager.MarkRecoveredFromCheckpoint(logRecord.ValueSpan);
                    rangeIndexManager.RebuildFromSnapshotIfPending(logRecord.Key);
                }


                if (vectorManager is not null && !logRecord.Info.Tombstone)
                {
                    vectorManager.SanitizeAndTrackIngestedRecordIfApplicable(ref logRecord);
                }
            }
        }

        /// <inheritdoc/>
        public readonly void OnCheckpoint(CheckpointTrigger trigger, Guid checkpointToken)
        {
            if (rangeIndexManager == null && vectorManager == null)
                return;

            switch (trigger)
            {
                case CheckpointTrigger.VersionShift:
                    rangeIndexManager?.SetCheckpointBarrier(checkpointToken);
                    break;
                case CheckpointTrigger.FlushBegin:
                    rangeIndexManager?.SnapshotAllTreesForCheckpoint(checkpointToken);
                    rangeIndexManager?.ClearCheckpointBarrier();
                    ExceptionInjectionHelper.ResetAndWait(ExceptionInjectionType.Checkpoint_Pause_At_Flush_Begin);
                    break;
                case CheckpointTrigger.CheckpointCompleted:
                    vectorManager?.CheckpointCompleted();
                    break;
                case CheckpointTrigger.CheckpointFailed:
                    // Release the barrier set at VersionShift, which FlushBegin would have cleared - range index
                    // operations spin-wait on it, so an aborted checkpoint would block them indefinitely. Idempotent,
                    // so it is safe when the abort happened after FlushBegin or before the barrier was ever set.
                    //
                    // Deliberately does not call vectorManager.CheckpointCompleted(): that reclaims deletions, which
                    // is only safe once a checkpoint has made them recoverable. A failed checkpoint has not, so they
                    // must stay queued for the next successful one.
                    rangeIndexManager?.ClearCheckpointBarrier();
                    break;
            }
        }

        /// <inheritdoc/>
        public readonly void PostCopyToTail<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, long srcLogicalAddress,
                                                              ref LogRecord dstLogRecord, long dstLogicalAddress)
            where TSourceLogRecord : ISourceLogRecord, allows ref struct
        {
            // Only act on RangeIndex records.
            // Check the SOURCE's RecordType, not the destination's: Tsavorite's
            // CopyReadsToTail / compaction CTT path allocates dst at tail and copies the
            // value bytes, but does NOT carry the RecordDataHeader.RecordType through. After
            // CTT, dst.RecordType == 0 by default. We must inspect src to recognize this is
            // a RangeIndex record, and explicitly propagate the RecordType to dst below
            // before the next Read-with-RangeIndex-cmd can pass the type-mismatch guard in
            // ReadMethods.CheckRecordTypeMismatch.
            if (rangeIndexManager is null
                || dstLogRecord.DataHeader.ValueIsObject
                || srcLogRecord.RecordType != RangeIndexManager.RangeIndexRecordType)
                return;

            // Propagate RecordType from src onto dst (CTT does not do this for us).
            // RecordDataHeader is a wrapper struct over the header pointer, so the setter
            // writes through to the underlying memory even on this struct value.
            var dstHeader = dstLogRecord.DataHeader;
            dstHeader.RecordType = RangeIndexManager.RangeIndexRecordType;

            var srcSpan = srcLogRecord.ValueSpan;
            var dstSpan = dstLogRecord.ValueSpan;
            ref readonly var srcStub = ref RangeIndexManager.ReadIndex(srcSpan);
            var srcHandle = srcStub.TreeHandle;

            if (srcHandle != nint.Zero)
            {
                // Live transfer: src had an active tree; dst inherited TreeHandle via byte-copy.
                // liveIndexes entry already exists for this key. Clear src.TreeHandle so a later
                // OnEvict on src does not free the tree the dst now owns.
                // NOTE: srcLogRecord.ValueSpan is the source's value span; the source record is
                //       still in the chain (TryCopyToTail does not unlink/seal the source). Mutating
                //       it in place is safe because the source is logically superseded by dst.
                RangeIndexManager.ClearTreeHandle(srcSpan);

                // Dst owns the live tree, so subsequent reads need not promote it again.
                RangeIndexManager.ClearFlushedFlag(dstSpan);
            }
            else if (srcStub.IsFlushed)
            {
                // Tree evicted (disk source, post-eviction or post-OnDiskRead-invalidate): the tree is only in src's
                // flush snapshot. Staging it into data.bftree needs the per-key exclusive lock, which must not be waited
                // for here: this runs under the bucket latch, while a data operation can hold the lock for as long as it
                // runs. Keep dst flushed, with its own copy of the snapshot, so the next data operation promotes it and
                // stages data.bftree outside any store callback. Without a copy (no source address, or src's snapshot was
                // deferred and is not taken yet), dst is left unflushed, relying on data.bftree like any stub whose tree
                // is not restored.
                if (srcLogicalAddress == Tsavorite.core.LogAddress.kInvalidAddress
                    || !rangeIndexManager.CopyFlushSnapshot(dstLogRecord.Key, srcLogicalAddress, dstLogicalAddress))
                    RangeIndexManager.ClearFlushedFlag(dstSpan);
            }

            // Otherwise data.bftree already holds src's tree, with a pending entry, and both stay with the key.

            // Mark src as transferred-out so a later OnEvict/OnFlush on the stale source does not
            // remove the liveIndexes entry now owned by dst (live case) or by the pending
            // registration (cold case), and does not snapshot a stale view.
            RangeIndexManager.SetTransferredFlag(srcSpan);
        }

        /// <inheritdoc/>
        public readonly void OnTruncate(long newBeginAddress)
        {
            rangeIndexManager?.OnTruncateImpl(newBeginAddress);
        }

        /// <inheritdoc/>
        public readonly bool IsDeleted<TSourceLogRecord>(in TSourceLogRecord logRecord)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (vectorManager is not null && logRecord.RecordType == VectorManager.RecordType)
            {
                vectorManager.VectorSetPotentiallyDeleted(logRecord.KeyBytes, logRecord.ValueSpan);
            }

            return false;
        }
    }
}