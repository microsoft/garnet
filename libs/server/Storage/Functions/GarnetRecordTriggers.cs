// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Record lifecycle triggers for Garnet's unified store. Implements <see cref="IRecordTriggers"/>
    /// to handle per-record cleanup on delete, eviction, flush, and disk read for both
    /// heap objects (e.g., Hash, List, Set) and BfTree stubs (RangeIndex).
    ///
    /// <para>Trigger call sites in the store lifecycle:</para>
    /// <list type="bullet">
    /// <item><see cref="OnDispose"/>: Record deleted (mutable or read-only region). Frees BfTree
    /// under exclusive lock; updates heap size tracker for objects.</item>
    /// <item><see cref="OnFlush"/>: Page moves to read-only. Snapshots BfTree data file and
    /// sets FlagFlushed so the next RI operation promotes the stub to the mutable tail.</item>
    /// <item><see cref="OnEvict"/>: Page evicted past HeadAddress. Frees BfTree under exclusive lock.</item>
    /// <item><see cref="OnDiskRead"/>: Record loaded from disk. Zeroes stale TreeHandle so lazy
    /// restore is triggered on next access.</item>
    /// <item><see cref="OnRecovery"/>/<see cref="OnRecoverySnapshotRead"/>: Checkpoint recovery.
    /// Sets <c>recoveredCheckpointToken</c> and marks stubs with FlagRecovered.</item>
    /// <item><see cref="OnCheckpoint"/>: Checkpoint lifecycle (version shift → snapshot → cleanup).</item>
    /// </list>
    /// </summary>
    public readonly struct GarnetRecordTriggers : IRecordTriggers
    {
        /// <summary>
        /// Cache size tracker for heap size accounting on delete.
        /// Created before the store and initialized after via <see cref="CacheSizeTracker.Initialize"/>.
        /// May be <c>null</c> if memory tracking is disabled.
        /// </summary>
        internal readonly CacheSizeTracker cacheSizeTracker;

        /// <summary>
        /// Reference to the RangeIndexManager for BfTree lifecycle management.
        /// May be <c>null</c> if RangeIndex is not enabled
        /// (<see cref="GarnetServerOptions.EnableRangeIndexPreview"/> is <c>false</c>).
        /// </summary>
        internal readonly RangeIndexManager rangeIndexManager;

        /// <summary>
        /// Creates a GarnetRecordTriggers with a cache size tracker and optional RangeIndexManager.
        /// </summary>
        /// <param name="cacheSizeTracker">Tracker for heap object memory accounting. May be <c>null</c>.</param>
        /// <param name="rangeIndexManager">Manager for BfTree lifecycle. <c>null</c> disables all RI trigger callbacks.</param>
        public GarnetRecordTriggers(CacheSizeTracker cacheSizeTracker, RangeIndexManager rangeIndexManager = null)
        {
            this.cacheSizeTracker = cacheSizeTracker;
            this.rangeIndexManager = rangeIndexManager;
        }

        /// <inheritdoc/>
        public bool CallOnFlush => rangeIndexManager != null;

        /// <inheritdoc/>
        public bool CallOnEvict => rangeIndexManager != null;

        /// <inheritdoc/>
        public bool CallOnDiskRead => rangeIndexManager != null;

        /// <inheritdoc/>
        public void OnDisposeValueObject(IHeapObject valueObject, DisposeReason reason)
        {
            // Heap object disposal is handled by ClearHeapFields in ObjectAllocatorImpl
        }

        /// <inheritdoc/>
        public void OnDispose(ref LogRecord logRecord, DisposeReason reason)
        {
            // Handle inline BfTree records: free on delete under exclusive lock.
            if (!logRecord.Info.ValueIsObject)
            {
                if (reason == DisposeReason.Deleted)
                {
                    var recordType = logRecord.RecordDataHeader.RecordType;
                    if (recordType == RangeIndexManager.RangeIndexRecordType)
                        rangeIndexManager?.DisposeTreeUnderLock(logRecord.Key, logRecord.ValueSpan);
                }
                return;
            }

            // Handle heap objects: update cache size tracker on delete
            if (reason == DisposeReason.Deleted)
            {
                cacheSizeTracker?.AddHeapSize(-logRecord.ValueObject.HeapMemorySize);
            }
        }

        /// <inheritdoc/>
        public readonly void OnFlush(ref LogRecord logRecord)
        {
            // When a page moves to read-only, snapshot the BfTree data file
            // and set FlagFlushed so the next RI operation promotes the stub to tail.
            // Snapshot failure is fatal — propagates to the state machine driver.
            if (!logRecord.Info.ValueIsObject
                && logRecord.RecordDataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
            {
                rangeIndexManager?.SnapshotTreeForFlush(logRecord.Key, logRecord.ValueSpan);
                RangeIndexManager.SetFlushedFlag(logRecord.ValueSpan);
            }
        }

        /// <inheritdoc/>
        public readonly void OnEvict(ref LogRecord logRecord)
        {
            // Free BfTree on page eviction under exclusive lock.
            if (!logRecord.Info.ValueIsObject
                && logRecord.RecordDataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
            {
                rangeIndexManager?.DisposeTreeUnderLock(logRecord.Key, logRecord.ValueSpan);
            }
        }

        /// <inheritdoc/>
        public readonly void OnDiskRead(ref LogRecord logRecord)
        {
            // Invalidate stale BfTree handles on records loaded from disk.
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
            // Mark RangeIndex stubs recovered from checkpoint snapshot so the restore
            // path uses the checkpoint snapshot file instead of the flush snapshot.
            if (!logRecord.Info.ValueIsObject
                && logRecord.RecordDataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
            {
                RangeIndexManager.MarkRecoveredFromCheckpoint(logRecord.ValueSpan);
            }
        }

        /// <inheritdoc/>
        public readonly void OnCheckpoint(CheckpointTrigger trigger, Guid checkpointToken)
        {
            if (rangeIndexManager == null)
                return;

            switch (trigger)
            {
                case CheckpointTrigger.VersionShift:
                    // Block v+1 RI operations until snapshot completes.
                    rangeIndexManager.SetCheckpointBarrier(checkpointToken);
                    break;
                case CheckpointTrigger.FlushBegin:
                    // All v threads drained. Snapshot all live BfTrees, then release barrier.
                    rangeIndexManager.SnapshotAllTreesForCheckpoint(checkpointToken);
                    rangeIndexManager.ClearCheckpointBarrier();
                    break;
                case CheckpointTrigger.CheckpointCompleted:
                    // Checkpoint fully persisted. Purge old BfTree checkpoint snapshots.
                    rangeIndexManager.PurgeOldCheckpointSnapshots(checkpointToken);
                    break;
            }
        }
    }
}