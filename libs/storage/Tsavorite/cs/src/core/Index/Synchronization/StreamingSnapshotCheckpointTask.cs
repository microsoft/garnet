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
    /// A Streaming Snapshot persists a version by yielding a stream of key-value pairs that correspond to
    /// a consistent snapshot of the database, for the old version (v). Unlike Snapshot, StreamingSnapshot
    /// is designed to not require tail growth even during the WAIT_FLUSH phase of checkpointing. Further,
    /// it does not require a snapshot of the index. Recovery is achieved by replaying the yielded log 
    /// of key-value pairs and inserting each record into an empty database.
    /// </summary>
    sealed class StreamingSnapshotCheckpointTask<TKey, TValue, TStoreFunctions, TAllocator> : HybridLogCheckpointOrchestrationTask<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        readonly long targetVersion;

        public StreamingSnapshotCheckpointTask(long targetVersion)
        {
            this.targetVersion = targetVersion;
        }

        /// <inheritdoc />
        public override void GlobalBeforeEnteringState(SystemState next, TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
            switch (next.Phase)
            {
                case Phase.PREP_STREAMING_SNAPSHOT_CHECKPOINT:
                    base.GlobalBeforeEnteringState(next, store);
                    store._hybridLogCheckpointToken = Guid.NewGuid();
                    store._hybridLogCheckpoint.info.version = next.Version;
                    store._hybridLogCheckpoint.info.nextVersion = targetVersion == -1 ? next.Version + 1 : targetVersion;
                    store._lastSnapshotCheckpoint.Dispose();
                    _ = Task.Run(store.StreamingSnapshotScanPhase1);
                    break;
                case Phase.PREPARE:
                    store.InitializeHybridLogCheckpoint(store._hybridLogCheckpointToken, next.Version);
                    base.GlobalBeforeEnteringState(next, store);
                    break;
                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, store);
                    store._hybridLogCheckpoint.flushedSemaphore = new SemaphoreSlim(0);
                    var finalLogicalAddress = store.hlogBase.GetTailAddress();
                    Task.Run(() => store.StreamingSnapshotScanPhase2(finalLogicalAddress));
                    break;
                default:
                    base.GlobalBeforeEnteringState(next, store);
                    break;
            }
        }

        /// <inheritdoc />
        public override void OnThreadState<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
            SystemState current,
            SystemState prev, TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.TsavoriteExecutionContext<TInput, TOutput, TContext> ctx,
            TSessionFunctionsWrapper sessionFunctions,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
        {
            base.OnThreadState(current, prev, store, ctx, sessionFunctions, valueTasks, token);

            if (current.Phase != Phase.WAIT_FLUSH) return;

            if (ctx is null)
            {
                var s = store._hybridLogCheckpoint.flushedSemaphore;

                var notify = s != null && s.CurrentCount > 0;
                notify = notify || !store.SameCycle(ctx, current) || s == null;

                if (valueTasks != null && !notify)
                {
                    Debug.Assert(s != null);
                    valueTasks.Add(new ValueTask(s.WaitAsync(token).ContinueWith(t => s.Release())));
                }
            }
        }
    }
}