// Copyright (c) Microsoft Corporation.
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
    internal class IndexSnapshotTask : ISynchronizationTask
    {
        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value>(
            SystemState next,
            TsavoriteKV<Key, Value> store)
        {
            switch (next.Phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    if (store._indexCheckpoint.IsDefault())
                    {
                        store._indexCheckpointToken = Guid.NewGuid();
                        store.InitializeIndexCheckpoint(store._indexCheckpointToken);
                    }

                    store._indexCheckpoint.info.startLogicalAddress = store.hlog.GetTailAddress();
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
        public void GlobalAfterEnteringState<Key, Value>(
            SystemState next,
            TsavoriteKV<Key, Value> store)
        {
        }

        /// <inheritdoc />
        public void OnThreadState<Key, Value, Input, Output, Context, TsavoriteSession>(
            SystemState current,
            SystemState prev,
            TsavoriteKV<Key, Value> store,
            TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TsavoriteSession storeSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where TsavoriteSession : ITsavoriteSession
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

    /// <summary>
    /// This state machine performs an index checkpoint
    /// </summary>
    internal sealed class IndexSnapshotStateMachine : SynchronizationStateMachineBase
    {
        /// <summary>
        /// Create a new IndexSnapshotStateMachine
        /// </summary>
        public IndexSnapshotStateMachine() : base(new IndexSnapshotTask())
        {
        }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start)
        {
            var result = SystemState.Copy(ref start);
            switch (start.Phase)
            {
                case Phase.REST:
                    result.Phase = Phase.PREP_INDEX_CHECKPOINT;
                    break;
                case Phase.PREP_INDEX_CHECKPOINT:
                    result.Phase = Phase.WAIT_INDEX_ONLY_CHECKPOINT;
                    break;
                case Phase.WAIT_INDEX_ONLY_CHECKPOINT:
                    result.Phase = Phase.REST;
                    break;
                default:
                    throw new TsavoriteException("Invalid Enum Argument");
            }

            return result;
        }
    }
}