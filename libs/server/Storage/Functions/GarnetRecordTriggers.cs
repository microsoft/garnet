// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Record lifecycle triggers for Garnet's unified store. Implements <see cref="IRecordTriggers"/>
    /// to handle per-record cleanup on delete, eviction, flush, copy-to-tail, log truncation,
    /// and disk read for BfTree stubs (RangeIndex).
    /// </summary>
    public readonly struct GarnetRecordTriggers : IRecordTriggers
    {
        /// <summary>
        /// Cache size tracker for heap size accounting.
        /// Created before the store and initialized after via <see cref="CacheSizeTracker.Initialize"/>.
        /// May be <c>null</c> if memory tracking is disabled.
        /// </summary>
        internal readonly CacheSizeTracker cacheSizeTracker;

        /// <summary>
        /// Reference to the RangeIndexManager for BfTree lifecycle management.
        /// May be <c>null</c> if RangeIndex is not enabled.
        /// </summary>
        internal readonly RangeIndexManager rangeIndexManager;

        /// <summary>
        /// Creates a GarnetRecordTriggers with a cache size tracker and optional RangeIndexManager.
        /// </summary>
        public GarnetRecordTriggers(CacheSizeTracker cacheSizeTracker, RangeIndexManager rangeIndexManager = null)
        {
            this.cacheSizeTracker = cacheSizeTracker;
            this.rangeIndexManager = rangeIndexManager;
        }

        // Trigger gates: when EnableRangeIndexPreview=false (the default), GarnetServer
        // passes null in place of a manager, so these gates return false → Tsavorite skips
        // ALL trigger invocations entirely → zero per-operation cost on non-RangeIndex
        // workloads. Without this, every flush/evict/disk-read/CTT/truncate would invoke
        // our callback only to do an early-return on the RecordType check.

        /// <inheritdoc/>
        public bool CallOnFlush => rangeIndexManager != null;

        /// <inheritdoc/>
        public bool CallOnEvict => rangeIndexManager != null;

        /// <inheritdoc/>
        public bool CallOnDiskRead => rangeIndexManager != null;

        /// <inheritdoc/>
        public bool CallPostCopyToTail => rangeIndexManager != null;

        /// <inheritdoc/>
        public bool CallOnTruncate => rangeIndexManager != null;

        /// <inheritdoc/>
        public void OnDispose(ref LogRecord logRecord, DisposeReason reason)
        {
            // Free BfTree and delete data files on key deletion.
            if (!logRecord.Info.ValueIsObject
                && reason == DisposeReason.Deleted
                && logRecord.RecordDataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
            {
                rangeIndexManager?.DisposeTreeUnderLock(logRecord.Key, logRecord.ValueSpan, deleteFiles: true);
            }
        }

        /// <inheritdoc/>
        public readonly void OnFlush(ref LogRecord logRecord, long logicalAddress)
        {
            if (rangeIndexManager is null
                || logRecord.Info.ValueIsObject
                || logRecord.RecordDataHeader.RecordType != RangeIndexManager.RangeIndexRecordType)
                return;

            rangeIndexManager.SnapshotTreeForFlush(logRecord.Key, logRecord.ValueSpan, logicalAddress);
        }

        /// <inheritdoc/>
        public readonly void OnEvict(ref LogRecord logRecord, EvictionSource source)
        {
            // Free BfTree on page eviction under exclusive lock.
            if (!logRecord.Info.ValueIsObject
                && logRecord.RecordDataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
            {
                rangeIndexManager?.DisposeTreeUnderLock(logRecord.Key, logRecord.ValueSpan, deleteFiles: false);
            }
        }

        /// <inheritdoc/>
        public readonly void OnDiskRead(ref LogRecord logRecord)
        {
            // Invalidate stale TreeHandle bytes on records loaded from disk.
            // RIPROMOTE PostCopyUpdater handles file pre-staging when this stub is later promoted.
            if (!logRecord.Info.ValueIsObject
                && logRecord.RecordDataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
            {
                RangeIndexManager.InvalidateStub(logRecord.ValueSpan);
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
            // Above-FUA-at-checkpoint stubs: pre-stage data.bftree from the checkpoint snapshot
            // file DURING recovery (snapshot files may be deleted post-recovery). Below-FUA
            // stubs are handled lazily by RIPROMOTE PostCopyUpdater on first access.
            if (rangeIndexManager is null
                || logRecord.Info.ValueIsObject
                || logRecord.RecordDataHeader.RecordType != RangeIndexManager.RangeIndexRecordType)
                return;

            RangeIndexManager.MarkRecoveredFromCheckpoint(logRecord.ValueSpan);
            rangeIndexManager.RebuildFromSnapshotIfPending(logRecord.Key);
        }

        /// <inheritdoc/>
        public readonly void OnCheckpoint(CheckpointTrigger trigger, Guid checkpointToken)
        {
            if (rangeIndexManager == null)
                return;

            switch (trigger)
            {
                case CheckpointTrigger.VersionShift:
                    rangeIndexManager.SetCheckpointBarrier(checkpointToken);
                    break;
                case CheckpointTrigger.FlushBegin:
                    rangeIndexManager.SnapshotAllTreesForCheckpoint(checkpointToken);
                    rangeIndexManager.ClearCheckpointBarrier();
                    break;
                case CheckpointTrigger.CheckpointCompleted:
                    // No action — Tsavorite's checkpoint manager removes per-token snapshot dirs
                    // when removeOutdated is true; per-flush snapshots are cleaned by OnTruncate.
                    break;
            }
        }

        /// <inheritdoc/>
        public readonly void PostCopyToTail<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, long srcLogicalAddress,
                                                              ref LogRecord dstLogRecord, long dstLogicalAddress)
            where TSourceLogRecord : ISourceLogRecord
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
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
                || dstLogRecord.Info.ValueIsObject
                || srcLogRecord.RecordType != RangeIndexManager.RangeIndexRecordType)
                return;

            // Propagate RecordType from src onto dst (CTT does not do this for us).
            // RecordDataHeader is a wrapper struct over the header pointer, so the setter
            // writes through to the underlying memory even on this struct value.
            var dstHeader = dstLogRecord.RecordDataHeader;
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
            }
            else
            {
                // Disk source (post-eviction or post-OnDiskRead-invalidate): pre-stage data.bftree
                // from <srcAddr:x16>.flush.bftree, and register a pending entry so the next
                // checkpoint captures dst's content.
                if (srcLogicalAddress != Tsavorite.core.LogAddress.kInvalidAddress)
                    rangeIndexManager.PreStageAndRegisterPending(dstLogRecord.Key, srcLogicalAddress);
            }

            // Mark src as transferred-out so a later OnEvict/OnFlush on the stale source does not
            // remove the liveIndexes entry now owned by dst (live case) or by the pending
            // registration (cold case), and does not snapshot a stale view.
            RangeIndexManager.SetTransferredFlag(srcSpan);

            // Dst is a freshly copied record at the tail. Clear IsFlushed so subsequent reads
            // don't loop through PromoteToTail again.
            RangeIndexManager.ClearFlushedFlag(dstSpan);
        }

        /// <inheritdoc/>
        public readonly void OnTruncate(long newBeginAddress)
        {
            rangeIndexManager?.OnTruncateImpl(newBeginAddress);
        }
    }
}