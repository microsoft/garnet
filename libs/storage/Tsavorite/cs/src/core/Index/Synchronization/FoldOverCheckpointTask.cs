// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// A FoldOver checkpoint persists a version by setting the read-only marker past the last entry of that
    /// version on the log and waiting until it is flushed to disk. It is simple and fast, but can result
    /// in garbage entries on the log, and a slower recovery of performance.
    /// </summary>
    internal sealed class FoldOverCheckpointTask<TKey, TValue, TStoreFunctions, TAllocator> : HybridLogCheckpointOrchestrationTask<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <inheritdoc />
        public override void GlobalBeforeEnteringState(SystemState next,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
            base.GlobalBeforeEnteringState(next, store);

            if (next.Phase == Phase.PREPARE)
            {
                store._lastSnapshotCheckpoint.Dispose();
            }

            if (next.Phase == Phase.IN_PROGRESS)
                base.GlobalBeforeEnteringState(next, store);

            if (next.Phase != Phase.WAIT_FLUSH) return;

            _ = store.hlogBase.ShiftReadOnlyToTail(out var tailAddress, out store._hybridLogCheckpoint.flushedSemaphore);
            store._hybridLogCheckpoint.info.finalLogicalAddress = tailAddress;
        }

        /// <inheritdoc />
        public override void OnThreadState<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
            SystemState current,
            SystemState prev,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store,
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

                var notify = store.hlogBase.FlushedUntilAddress >= store._hybridLogCheckpoint.info.finalLogicalAddress;
                notify = notify || !store.SameCycle(ctx, current) || s == null;

                if (valueTasks != null && !notify)
                {
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