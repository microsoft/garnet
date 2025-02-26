// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// This task performs an index checkpoint.
    /// </summary>
    internal sealed class IndexCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator> : IStateMachineTask
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store;
        readonly Guid guid;

        public IndexCheckpointSMTask(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, Guid guid)
        {
            this.store = store;
            this.guid = guid;
        }

        /// <inheritdoc />
        public void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    Debug.Assert(store._indexCheckpoint.IsDefault());
                    store._indexCheckpointToken = guid;
                    store.InitializeIndexCheckpoint(store._indexCheckpointToken);
                    store._indexCheckpoint.info.startLogicalAddress = store.hlogBase.GetTailAddress();
                    store.TakeIndexFuzzyCheckpoint();
                    break;

                case Phase.WAIT_INDEX_CHECKPOINT:
                case Phase.WAIT_INDEX_ONLY_CHECKPOINT:
                    store.AddIndexCheckpointWaitingList(stateMachineDriver);
                    break;

                case Phase.REST:
                    // If the tail address has already been obtained, because another task on the state machine
                    // has done so earlier (e.g. FullCheckpoint captures log tail at WAIT_FLUSH), don't update
                    // the tail address.
                    if (store.ObtainCurrentTailAddress(ref store._indexCheckpoint.info.finalLogicalAddress))
                        store._indexCheckpoint.info.num_buckets = store.overflowBucketsAllocator.GetMaxValidAddress();
                    if (!store._indexCheckpoint.IsDefault())
                    {
                        store.WriteIndexMetaInfo();
                        store._indexCheckpoint.Reset();
                    }
                    break;
            }
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
        }
    }
}