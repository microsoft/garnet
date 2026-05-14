// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Record lifecycle triggers for Garnet's unified store. Implements <see cref="IRecordTriggers"/>
    /// to handle per-record cleanup on delete, eviction, flush, and disk read for
    /// BfTree stubs (RangeIndex).
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

        /// <inheritdoc/>
        public bool CallOnFlush => rangeIndexManager != null;

        /// <inheritdoc/>
        public bool CallOnEvict => rangeIndexManager != null;

        /// <inheritdoc/>
        public bool CallOnDiskRead => rangeIndexManager != null;

        /// <inheritdoc/>
        public void OnDispose(ref LogRecord logRecord, DisposeReason reason)
        {
            if (!logRecord.Info.ValueIsObject)
            {
                // Free BfTree and delete data files on key deletion.
                if (reason == DisposeReason.Deleted && logRecord.RecordDataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
                {
                    rangeIndexManager?.DisposeTreeUnderLock(logRecord.Key, logRecord.ValueSpan, deleteFiles: true);
                }

                // Request Vector Set cleanup when the index key is deleted.
                if (reason is DisposeReason.Deleted or DisposeReason.Expired && logRecord.RecordDataHeader.RecordType == VectorManager.RecordType)
                {
                    vectorManager?.RequestDeletion(logRecord.ValueSpan);
                }
            }
        }

        /// <inheritdoc/>
        public readonly void OnFlush(ref LogRecord logRecord)
        {
            if (!logRecord.Info.ValueIsObject
                && logRecord.RecordDataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
            {
                rangeIndexManager?.SnapshotTreeForFlush(logRecord.Key, logRecord.ValueSpan);
                RangeIndexManager.SetFlushedFlag(logRecord.ValueSpan);
            }
        }

        /// <inheritdoc/>
        public readonly void OnEvict(ref LogRecord logRecord, EvictionSource source)
        {
            if (!logRecord.Info.ValueIsObject)
            {
                // Free BfTree on page eviction under exclusive lock.
                if (logRecord.RecordDataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
                {
                    rangeIndexManager?.DisposeTreeUnderLock(logRecord.Key, logRecord.ValueSpan, deleteFiles: false);
                }

                // Drop DiskANN side of index
                if (logRecord.RecordDataHeader.RecordType == VectorManager.RecordType)
                {
                    vectorManager?.DropInMemoryIndex(logRecord.ValueSpan);
                }
            }
        }

        /// <inheritdoc/>
        public readonly void OnDiskRead(ref LogRecord logRecord)
        {
            if (!logRecord.Info.ValueIsObject)
            {
                if (logRecord.RecordDataHeader.RecordType == RangeIndexManager.RangeIndexRecordType)
                {
                    RangeIndexManager.InvalidateStub(logRecord.ValueSpan);
                }

                // Clear DiskANN index pointer so we'll recreate it on first touch
                if (logRecord.RecordDataHeader.RecordType == VectorManager.RecordType)
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
                    rangeIndexManager.SetCheckpointBarrier(checkpointToken);
                    break;
                case CheckpointTrigger.FlushBegin:
                    rangeIndexManager.SnapshotAllTreesForCheckpoint(checkpointToken);
                    rangeIndexManager.ClearCheckpointBarrier();
                    break;
                case CheckpointTrigger.CheckpointCompleted:
                    rangeIndexManager.PurgeOldCheckpointSnapshots(checkpointToken);
                    break;
            }
        }
    }
}