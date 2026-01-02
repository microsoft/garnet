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
    }
}