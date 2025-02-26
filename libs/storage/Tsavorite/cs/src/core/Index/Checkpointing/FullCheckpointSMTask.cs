// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Tsavorite.core
{
    /// <summary>
    /// This task contains logic to orchestrate the index and hybrid log checkpoint in parallel
    /// </summary>
    internal sealed class FullCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator> : IStateMachineTask
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store;
        readonly Guid guid;
        public FullCheckpointSMTask(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, Guid guid)
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
                    Debug.Assert(store._hybridLogCheckpoint.IsDefault());
                    store._hybridLogCheckpointToken = guid;
                    store.InitializeHybridLogCheckpoint(store._hybridLogCheckpointToken, next.Version);
                    break;
                case Phase.WAIT_FLUSH:
                    store._indexCheckpoint.info.num_buckets = store.overflowBucketsAllocator.GetMaxValidAddress();
                    store._indexCheckpoint.info.finalLogicalAddress = store.hlogBase.GetTailAddress();
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    store.WriteIndexMetaInfo();
                    store._indexCheckpoint.Reset();
                    break;
            }
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
        }
    }
}