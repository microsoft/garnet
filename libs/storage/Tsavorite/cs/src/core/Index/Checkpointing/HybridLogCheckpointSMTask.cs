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
                    store._hybridLogCheckpoint.info.startLogicalAddress = store.hlogBase.GetTailAddress();
                    store._hybridLogCheckpoint.info.beginAddress = store.hlogBase.BeginAddress;
                    break;

                case Phase.IN_PROGRESS:
                    store.CheckpointVersionShiftStart(lastVersion, next.Version, isStreaming);
                    break;

                case Phase.WAIT_FLUSH:
                    store.CheckpointVersionShiftEnd(lastVersion, next.Version, isStreaming);

                    Debug.Assert(stateMachineDriver.GetNumActiveTransactions(lastVersion) == 0, $"Active transactions in last version: {stateMachineDriver.GetNumActiveTransactions(lastVersion)}");
                    stateMachineDriver.lastVersionTransactionsDone = null;
                    stateMachineDriver.lastVersion = 0;
                    // Grab final logical address (end of fuzzy region)
                    store._hybridLogCheckpoint.info.finalLogicalAddress = store.hlogBase.GetTailAddress();

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
            // GetObjectTail().HasData may be false if we have not flushed the main log (ReadOnlyAddress has not advanced).
            store._hybridLogCheckpoint.info.snapshotStartObjectLogTail = store.hlogBase.GetObjectLogTail();

            store._hybridLogCheckpoint.objectLogFlushBuffers = store.hlogBase.CreateCircularFlushBuffers(store._hybridLogCheckpoint.snapshotFileObjectLogDevice, store.hlogBase.logger);
            store._hybridLogCheckpoint.objectLogFlushBuffers?.InitializeOwnObjectLogFilePosition(store._hybridLogCheckpoint.snapshotFileObjectLogDevice.SegmentSize);
            return store._hybridLogCheckpoint.objectLogFlushBuffers;
        }

        protected void ObjectLog_OnPersistenceCallback()
        {
            // GetObjectTail().HasData may be false if we have not flushed the main log (ReadOnlyAddress has not advanced).
            store._hybridLogCheckpoint.info.hlogEndObjectLogTail = store.hlogBase.GetObjectLogTail();
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
                    if (stateMachineDriver.GetNumActiveTransactions(lastVersion) > 0)
                    {
                        stateMachineDriver.lastVersion = lastVersion;
                        stateMachineDriver.lastVersionTransactionsDone = new(0);
                    }
                    if (stateMachineDriver.GetNumActiveTransactions(lastVersion) > 0)
                        stateMachineDriver.AddToWaitingList(stateMachineDriver.lastVersionTransactionsDone);
                    break;
            }
        }
    }
}