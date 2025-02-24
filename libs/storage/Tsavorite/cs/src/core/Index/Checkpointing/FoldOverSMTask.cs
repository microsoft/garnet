// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// A FoldOver checkpoint persists a version by setting the read-only marker past the last entry of that
    /// version on the log and waiting until it is flushed to disk. It is simple and fast, but can result
    /// in garbage entries on the log, and a slower recovery of performance.
    /// </summary>
    internal sealed class FoldOverSMTask<TKey, TValue, TStoreFunctions, TAllocator> : HybridLogCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        public FoldOverSMTask(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
            : base(store)
        {
        }

        /// <inheritdoc />
        public override void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            base.GlobalBeforeEnteringState(next, stateMachineDriver);

            if (next.Phase == Phase.PREPARE)
            {
                store._lastSnapshotCheckpoint.Dispose();
            }

            if (next.Phase == Phase.IN_PROGRESS)
                base.GlobalBeforeEnteringState(next, stateMachineDriver);

            if (next.Phase != Phase.WAIT_FLUSH) return;

            _ = store.hlogBase.ShiftReadOnlyToTail(out var tailAddress, out store._hybridLogCheckpoint.flushedSemaphore);
            store._hybridLogCheckpoint.info.finalLogicalAddress = tailAddress;
        }
    }
}