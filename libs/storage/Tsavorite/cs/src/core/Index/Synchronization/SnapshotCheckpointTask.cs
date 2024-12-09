// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// A Snapshot persists a version by making a copy for every entry of that version separate from the log. It is
    /// slower and more complex than a foldover, but more space-efficient on the log, and retains in-place
    /// update performance as it does not advance the readonly marker unnecessarily.
    /// </summary>
    internal sealed class SnapshotCheckpointTask<TKey, TValue, TStoreFunctions, TAllocator> : HybridLogCheckpointOrchestrationTask<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <inheritdoc />
        public override void GlobalBeforeEnteringState(SystemState next, TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    store._lastSnapshotCheckpoint.Dispose();
                    base.GlobalBeforeEnteringState(next, store);
                    store._hybridLogCheckpoint.info.useSnapshotFile = 1;
                    break;
                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, store);
                    store._hybridLogCheckpoint.info.finalLogicalAddress = store.hlogBase.GetTailAddress();
                    store._hybridLogCheckpoint.info.snapshotFinalLogicalAddress = store._hybridLogCheckpoint.info.finalLogicalAddress;

                    store._hybridLogCheckpoint.snapshotFileDevice =
                        store.checkpointManager.GetSnapshotLogDevice(store._hybridLogCheckpointToken);
                    store._hybridLogCheckpoint.snapshotFileObjectLogDevice =
                        store.checkpointManager.GetSnapshotObjectLogDevice(store._hybridLogCheckpointToken);
                    store._hybridLogCheckpoint.snapshotFileDevice.Initialize(store.hlogBase.GetSegmentSize());
                    store._hybridLogCheckpoint.snapshotFileObjectLogDevice.Initialize(-1);

                    // If we are using a NullDevice then storage tier is not enabled and FlushedUntilAddress may be ReadOnlyAddress; get all records in memory.
                    store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress = store.hlogBase.IsNullDevice ? store.hlogBase.HeadAddress : store.hlogBase.FlushedUntilAddress;

                    long startPage = store.hlogBase.GetPage(store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress);
                    long endPage = store.hlogBase.GetPage(store._hybridLogCheckpoint.info.finalLogicalAddress);
                    if (store._hybridLogCheckpoint.info.finalLogicalAddress >
                        store.hlog.GetStartLogicalAddress(endPage))
                    {
                        endPage++;
                    }

                    // We are writing pages outside epoch protection, so callee should be able to
                    // handle corrupted or unexpected concurrent page changes during the flush, e.g., by
                    // resuming epoch protection if necessary. Correctness is not affected as we will
                    // only read safe pages during recovery.
                    store.hlogBase.AsyncFlushPagesToDevice(
                        startPage,
                        endPage,
                        store._hybridLogCheckpoint.info.finalLogicalAddress,
                        store._hybridLogCheckpoint.info.startLogicalAddress,
                        store._hybridLogCheckpoint.snapshotFileDevice,
                        store._hybridLogCheckpoint.snapshotFileObjectLogDevice,
                        out store._hybridLogCheckpoint.flushedSemaphore,
                        store.ThrottleCheckpointFlushDelayMs);
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    // Set actual FlushedUntil to the latest possible data in main log that is on disk
                    // If we are using a NullDevice then storage tier is not enabled and FlushedUntilAddress may be ReadOnlyAddress; get all records in memory.
                    store._hybridLogCheckpoint.info.flushedLogicalAddress = store.hlogBase.IsNullDevice ? store.hlogBase.HeadAddress : store.hlogBase.FlushedUntilAddress;
                    base.GlobalBeforeEnteringState(next, store);
                    store._lastSnapshotCheckpoint = store._hybridLogCheckpoint.Transfer();
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

            if (ctx is null || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
            {
                var s = store._hybridLogCheckpoint.flushedSemaphore;

                var notify = s != null && s.CurrentCount > 0;
                notify = notify || !store.SameCycle(ctx, current) || s == null;

                if (valueTasks != null && !notify)
                {
                    Debug.Assert(s != null);
                    valueTasks.Add(new ValueTask(s.WaitAsync(token).ContinueWith(t => s.Release())));
                }

                if (!notify) return;

                if (ctx is not null)
                    ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
            }

            store.epoch.Mark(EpochPhaseIdx.WaitFlush, current.Version);
            if (store.epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, current.Version))
                store.GlobalStateMachineStep(current);
        }
    }
}