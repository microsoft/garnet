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
    /// This task contains logic to orchestrate the index and hybrid log checkpoint in parallel
    /// </summary>
    internal sealed class FullCheckpointOrchestrationTask : ISynchronizationTask
    {
        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value>(
            SystemState next,
            TsavoriteKV<Key, Value> store)
        {
            switch (next.Phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    Debug.Assert(store._indexCheckpoint.IsDefault() &&
                                 store._hybridLogCheckpoint.IsDefault());
                    var fullCheckpointToken = Guid.NewGuid();
                    store._indexCheckpointToken = fullCheckpointToken;
                    store._hybridLogCheckpointToken = fullCheckpointToken;
                    store.InitializeIndexCheckpoint(store._indexCheckpointToken);
                    store.InitializeHybridLogCheckpoint(store._hybridLogCheckpointToken, next.Version);
                    break;
                case Phase.WAIT_FLUSH:
                    store._indexCheckpoint.info.num_buckets = store.overflowBucketsAllocator.GetMaxValidAddress();
                    store._indexCheckpoint.info.finalLogicalAddress = store.hlog.GetTailAddress();
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    store.WriteIndexMetaInfo();
                    store._indexCheckpoint.Reset();
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
        }
    }

    /// <summary>
    /// The state machine orchestrates a full checkpoint
    /// </summary>
    internal sealed class FullCheckpointStateMachine : HybridLogCheckpointStateMachine
    {
        /// <summary>
        /// Construct a new FullCheckpointStateMachine to use the given checkpoint backend (either fold-over or snapshot),
        /// drawing boundary at targetVersion.
        /// </summary>
        /// <param name="checkpointBackend">A task that encapsulates the logic to persist the checkpoint</param>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        public FullCheckpointStateMachine(ISynchronizationTask checkpointBackend, long targetVersion = -1) : base(
            targetVersion, new VersionChangeTask(), new FullCheckpointOrchestrationTask(),
            new IndexSnapshotTask(), checkpointBackend)
        { }

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
                    result.Phase = Phase.PREPARE;
                    break;
                case Phase.IN_PROGRESS:
                    result.Phase = Phase.WAIT_INDEX_CHECKPOINT;
                    break;
                case Phase.WAIT_INDEX_CHECKPOINT:
                    result.Phase = Phase.WAIT_FLUSH;
                    break;
                default:
                    result = base.NextState(start);
                    break;
            }

            return result;
        }
    }
}