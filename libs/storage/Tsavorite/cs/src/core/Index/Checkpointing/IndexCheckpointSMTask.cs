// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Tsavorite.core
{
    /// <summary>
    /// This task performs an index checkpoint.
    /// </summary>
    internal sealed class IndexCheckpointSMTask<TStoreFunctions, TAllocator> : IStateMachineTask
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        readonly TsavoriteKV<TStoreFunctions, TAllocator> store;
        readonly Guid guid;

        public IndexCheckpointSMTask(TsavoriteKV<TStoreFunctions, TAllocator> store, Guid guid)
        {
            this.store = store;
            this.guid = guid;
        }

        /// <inheritdoc />
        public void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    Debug.Assert(store._indexCheckpoint.IsDefault);
                    store._indexCheckpointToken = guid;
                    store.InitializeIndexCheckpoint(store._indexCheckpointToken);
                    store._indexCheckpoint.info.startLogicalAddress = store.hlogBase.GetTailAddress();
                    store.TakeIndexFuzzyCheckpoint();
                    break;

                case Phase.WAIT_INDEX_CHECKPOINT:
                    store.AddIndexCheckpointWaitingList(stateMachineDriver);
                    break;

                case Phase.WAIT_FLUSH:
                    store._indexCheckpoint.info.num_buckets = store.overflowBucketsAllocator.GetMaxValidAddress();
                    store._indexCheckpoint.info.finalLogicalAddress = store.hlogBase.GetTailAddress();
                    break;

                case Phase.PERSISTENCE_CALLBACK:
                    store.WriteIndexMetaInfo();
                    break;

                case Phase.REST:
                    store.CleanupIndexCheckpoint();
                    store._indexCheckpoint.Reset();
                    break;

                default:
                    break;
            }
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
        }

        /// <inheritdoc />
        public void OnAbort(StateMachineDriver stateMachineDriver, Exception exception)
        {
            // Mirrors the Phase.REST handling above, which an aborted state machine never reaches. Leaving
            // _indexCheckpoint set would make the PREPARE phase of every later checkpoint fail its IsDefault check,
            // so one failed checkpoint would stop the store from ever checkpointing again.

            // The flush issued at PREPARE writes to _indexCheckpoint.main_ht_device, and Reset disposes it. Aborting
            // before WAIT_INDEX_CHECKPOINT means the driver never awaited that flush, so it has to be awaited here:
            // disposing the device under an in-flight write fails it, and the completion would then be counted
            // against the next checkpoint's flush state rather than this one's.
            store.WaitForIndexCheckpointFlushCompletion();
            store._indexCheckpoint.Reset();
        }
    }
}