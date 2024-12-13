// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// A Incremental Snapshot makes a copy of only changes that have happened since the last full Snapshot. It is
    /// slower and more complex than a foldover, but more space-efficient on the log, and retains in-place
    /// update performance as it does not advance the readonly marker unnecessarily.
    /// </summary>
    internal sealed class IncrementalSnapshotCheckpointTask<TKey, TValue, TStoreFunctions, TAllocator> : HybridLogCheckpointOrchestrationTask<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <inheritdoc />
        public override void GlobalBeforeEnteringState(SystemState next, TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    store._hybridLogCheckpoint = store._lastSnapshotCheckpoint;
                    base.GlobalBeforeEnteringState(next, store);
                    store._hybridLogCheckpoint.prevVersion = next.Version;
                    break;
                case Phase.IN_PROGRESS:
                    base.GlobalBeforeEnteringState(next, store);
                    break;
                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, store);
                    store._hybridLogCheckpoint.info.finalLogicalAddress = store.hlogBase.GetTailAddress();

                    if (store._hybridLogCheckpoint.deltaLog == null)
                    {
                        store._hybridLogCheckpoint.deltaFileDevice = store.checkpointManager.GetDeltaLogDevice(store._hybridLogCheckpointToken);
                        store._hybridLogCheckpoint.deltaFileDevice.Initialize(-1);
                        store._hybridLogCheckpoint.deltaLog = new DeltaLog(store._hybridLogCheckpoint.deltaFileDevice, store.hlogBase.LogPageSizeBits, -1);
                        store._hybridLogCheckpoint.deltaLog.InitializeForWrites(store.hlogBase.bufferPool);
                    }

                    // We are writing delta records outside epoch protection, so callee should be able to
                    // handle corrupted or unexpected concurrent page changes during the flush, e.g., by
                    // resuming epoch protection if necessary. Correctness is not affected as we will
                    // only read safe pages during recovery.
                    store.hlogBase.AsyncFlushDeltaToDevice(
                        store.hlogBase.FlushedUntilAddress,
                        store._hybridLogCheckpoint.info.finalLogicalAddress,
                        store._lastSnapshotCheckpoint.info.finalLogicalAddress,
                        store._hybridLogCheckpoint.prevVersion,
                        store._hybridLogCheckpoint.deltaLog,
                        out store._hybridLogCheckpoint.flushedSemaphore,
                        store.ThrottleCheckpointFlushDelayMs);
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    CollectMetadata(next, store);
                    store._hybridLogCheckpoint.info.deltaTailAddress = store._hybridLogCheckpoint.deltaLog.TailAddress;
                    store.WriteHybridLogIncrementalMetaInfo(store._hybridLogCheckpoint.deltaLog);
                    store._hybridLogCheckpoint.info.deltaTailAddress = store._hybridLogCheckpoint.deltaLog.TailAddress;
                    store._lastSnapshotCheckpoint = store._hybridLogCheckpoint.Transfer();
                    store._hybridLogCheckpoint.Dispose();
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