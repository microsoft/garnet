// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// This task is the base class for a checkpoint "backend", which decides how a captured version is
    /// persisted on disk.
    /// </summary>
    internal abstract class HybridLogCheckpointSMTask<TStoreFunctions, TAllocator> : IStateMachineTask
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        protected readonly TsavoriteKV<TStoreFunctions, TAllocator> store;
        protected long lastVersion;
        protected readonly Guid guid;
        protected bool isStreaming;

        public HybridLogCheckpointSMTask(TsavoriteKV<TStoreFunctions, TAllocator> store, Guid guid)
        {
            this.store = store;
            this.guid = guid;
            this.isStreaming = false;
        }

        /// <inheritdoc />
        public virtual void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    // Capture state before checkpoint starts
                    lastVersion = store._hybridLogCheckpoint.info.version = next.Version;
                    store._hybridLogCheckpoint.info.fuzzyRegionStartAddress = store.hlogBase.GetTailAddress();
                    store._hybridLogCheckpoint.info.beginAddress = store.hlogBase.BeginAddress;
                    store._hybridLogCheckpoint.info.pageSize = store.hlogBase.PageSize;
                    store._hybridLogCheckpoint.info.segmentSize = store.hlogBase.GetMainLogSegmentSize();

                    // Allocators without an object log return -1; record 0 there, matching "not applicable" in the metadata.
                    var objectLogSegmentSize = store.hlogBase.GetObjectLogSegmentSize();
                    store._hybridLogCheckpoint.info.objectLogSegmentSize = objectLogSegmentSize > 0 ? objectLogSegmentSize : 0;
                    break;

                case Phase.IN_PROGRESS:
                    store.CheckpointVersionShiftStart(lastVersion, next.Version, isStreaming);
                    store.storeFunctions.OnCheckpoint(CheckpointTrigger.VersionShift, guid);
                    break;

                case Phase.WAIT_FLUSH:
                    store.CheckpointVersionShiftEnd(lastVersion, next.Version, isStreaming);
                    store.storeFunctions.OnCheckpoint(CheckpointTrigger.FlushBegin, guid);

                    Debug.Assert(stateMachineDriver.GetNumActiveTransactions(lastVersion) == 0, $"Active transactions in last version: {stateMachineDriver.GetNumActiveTransactions(lastVersion)}");
                    stateMachineDriver.ResetLastVersion();
                    // Grab final logical address (end of fuzzy region)
                    store._hybridLogCheckpoint.info.recoveredTailAddress = store.hlogBase.GetTailAddress();

                    // Grab other metadata for the checkpoint
                    store._hybridLogCheckpoint.info.headAddress = store.hlogBase.HeadAddress;
                    store._hybridLogCheckpoint.info.nextVersion = next.Version;
                    break;

                case Phase.PERSISTENCE_CALLBACK:
                    store.WriteHybridLogMetaInfo();
                    store.lastVersion = lastVersion;
                    break;

                case Phase.REST:
                    store.CleanupLogCheckpoint();
                    store.storeFunctions.OnCheckpoint(CheckpointTrigger.CheckpointCompleted, guid);
                    store._hybridLogCheckpoint.Dispose();
                    var nextTcs = new TaskCompletionSource<LinkedCheckpointInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
                    store.checkpointTcs.SetResult(new LinkedCheckpointInfo { NextTask = nextTcs.Task });
                    store.checkpointTcs = nextTcs;
                    break;
            }
        }

        protected void ObjectLog_OnPrepare()
        {
            // This will be zero unless Truncate() has removed enough main-log segments to allow freeing one or more object-log segments.
            store._hybridLogCheckpoint.info.beginAddressObjectLogSegment = store.hlogBase.LowestObjectLogSegmentInUse;
        }

        protected CircularDiskWriteBuffer ObjectLog_OnWaitFlush()
        {
            if (store._hybridLogCheckpoint.info.useSnapshotFile != 0)
            {
                // GetObjectTail().HasData may be false if we have not flushed the main log (ReadOnlyAddress has not advanced).
                store._hybridLogCheckpoint.info.snapshotStartObjectLogTail = store.hlogBase.GetObjectLogTail();

                // Flush buffers are only used for Snapshot checkpoints.
                store._hybridLogCheckpoint.objectLogFlushBuffers = store.hlogBase.CreateCircularFlushBuffers(store._hybridLogCheckpoint.snapshotFileObjectLogDevice, store.hlogBase.logger);
                store._hybridLogCheckpoint.objectLogFlushBuffers?.InitializeOwnObjectLogFilePosition(store._hybridLogCheckpoint.snapshotFileObjectLogDevice.SegmentSize);
            }
            return store._hybridLogCheckpoint.objectLogFlushBuffers;
        }

        protected void ObjectLog_OnPersistenceCallback()
        {
            // GetObjectTail().HasData may be false if we have not flushed the main log (ReadOnlyAddress has not advanced).
            store._hybridLogCheckpoint.info.hlogEndObjectLogTail = store.hlogBase.GetObjectLogTail();

            if (store._hybridLogCheckpoint.info.useSnapshotFile != 0)
                store._hybridLogCheckpoint.info.snapshotEndObjectLogTail = store._hybridLogCheckpoint.objectLogFlushBuffers?.filePosition ?? new();
        }

        /// <inheritdoc />
        public virtual void GlobalAfterEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.IN_PROGRESS:
                    // State machine should wait for active transactions in the last version to complete (drain out).
                    // Note that we allow new transactions to process in parallel.
                    stateMachineDriver.TrackLastVersion(lastVersion);
                    break;
            }
        }

        /// <inheritdoc />
        public virtual void OnAbort(StateMachineDriver stateMachineDriver, Exception exception)
        {
            // Mirrors the Phase.REST handling above, which an aborted state machine never reaches.

            // Lets the application release the barrier it set at VersionShift. Deliberately a distinct trigger from
            // CheckpointCompleted: work that is only safe once a checkpoint has made its data recoverable - such as
            // reclaiming deletions - must stay pending for the next successful checkpoint.
            store.storeFunctions.OnCheckpoint(CheckpointTrigger.CheckpointFailed, guid);

            // The snapshot flush issued at WAIT_FLUSH writes through the devices and flush buffers that Dispose
            // releases below, and the driver only awaits it when it reaches the end of that phase. Aborting in
            // between leaves the flush in flight, so it has to be awaited here: releasing what it is writing to
            // fails it, and its completion would then be counted against the next checkpoint's flush state.
            TsavoriteBase.WaitForCheckpointFlush(store._hybridLogCheckpoint.flushedTask);

            // Releases any snapshot devices and flush buffers already created, and clears the checkpoint so the next
            // one can run. Matches the cleanup CompleteCheckpointAsync performs when it observes a failed checkpoint.
            store._hybridLogCheckpoint.Dispose();

            // Waiters such as ClientSession.WaitForCommitAsync park on store.CheckpointTask, which REST would have
            // completed; leaving it pending hangs them forever. Publish the next checkpoint's source before faulting
            // the old one, so a continuation that immediately re-reads store.checkpointTcs picks up the source for
            // the next checkpoint rather than the one it just watched fail.
            var previousTcs = store.checkpointTcs;
            store.checkpointTcs = new TaskCompletionSource<LinkedCheckpointInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
            _ = previousTcs.TrySetException(exception ?? new TsavoriteException("Checkpoint state machine aborted"));
        }
    }
}