﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// This task performs an index checkpoint.
    /// </summary>
    internal sealed class IndexSnapshotSMTask<TKey, TValue, TStoreFunctions, TAllocator> : IStateMachineTask
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store;

        public IndexSnapshotSMTask(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
            this.store = store;
        }

        /// <inheritdoc />
        public void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    if (store._indexCheckpoint.IsDefault())
                    {
                        store._indexCheckpointToken = Guid.NewGuid();
                        store.InitializeIndexCheckpoint(store._indexCheckpointToken);
                    }

                    store._indexCheckpoint.info.startLogicalAddress = store.hlogBase.GetTailAddress();
                    store.TakeIndexFuzzyCheckpoint();
                    break;

                case Phase.WAIT_INDEX_CHECKPOINT:
                case Phase.WAIT_INDEX_ONLY_CHECKPOINT:
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

        /// <inheritdoc />
        public void OnThreadState<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
            SystemState current,
            SystemState prev,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.TsavoriteExecutionContext<TInput, TOutput, TContext> ctx,
            TSessionFunctionsWrapper sessionFunctions,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionEpochControl
        {
            switch (current.Phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    store.GlobalStateMachineStep(current);
                    break;
                case Phase.WAIT_INDEX_CHECKPOINT:
                case Phase.WAIT_INDEX_ONLY_CHECKPOINT:
                    var notify = store.IsIndexFuzzyCheckpointCompleted();
                    notify = notify || !store.SameCycle(ctx, current);

                    if (valueTasks != null && !notify)
                    {
                        var t = store.IsIndexFuzzyCheckpointCompletedAsync(token);
                        if (!store.SameCycle(ctx, current))
                            notify = true;
                        else
                            valueTasks.Add(t);
                    }

                    if (!notify) return;
                    store.GlobalStateMachineStep(current);
                    break;
            }
        }
    }
}