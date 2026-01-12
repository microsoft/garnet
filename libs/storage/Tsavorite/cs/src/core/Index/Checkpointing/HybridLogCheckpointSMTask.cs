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
    internal abstract class HybridLogCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator> : IStateMachineTask
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        protected readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store;
        protected long lastVersion;
        protected readonly Guid guid;
        protected bool isStreaming;

        public HybridLogCheckpointSMTask(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, Guid guid)
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
                    CollectMetadata(next, store);
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

        protected static void CollectMetadata(SystemState next, TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
            // Collect object log offsets only after flushes
            // are completed
            var seg = store.hlog.GetSegmentOffsets();
            if (seg != null)
            {
                store._hybridLogCheckpoint.info.objectLogSegmentOffsets = new long[seg.Length];
                Array.Copy(seg, store._hybridLogCheckpoint.info.objectLogSegmentOffsets, seg.Length);
            }
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

                    // We have to re-check the number of active transactions after assigning lastVersion and lastVersionTransactionsDone
                    if (stateMachineDriver.GetNumActiveTransactions(lastVersion) > 0)
                        stateMachineDriver.AddToWaitingList(stateMachineDriver.lastVersionTransactionsDone);
                    break;
            }
        }
    }
}