// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

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
        public FoldOverSMTask(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, Guid guid)
            : base(store, guid)
        {
        }

        /// <inheritdoc />
        public override void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    store._lastSnapshotCheckpoint.Dispose();
                    store._hybridLogCheckpointToken = guid;
                    store.InitializeHybridLogCheckpoint(store._hybridLogCheckpointToken, next.Version);
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;

                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    try
                    {
                        store.epoch.Resume();
                        _ = store.hlogBase.ShiftReadOnlyToTail(out var tailAddress, out store._hybridLogCheckpoint.flushedSemaphore);
                        if (store._hybridLogCheckpoint.flushedSemaphore != null)
                            stateMachineDriver.AddToWaitingList(store._hybridLogCheckpoint.flushedSemaphore);

                        // Update final logical address to the flushed tail - this may not be necessary
                        store._hybridLogCheckpoint.info.finalLogicalAddress = tailAddress;
                    }
                    finally
                    {
                        store.epoch.Suspend();
                    }
                    break;

                default:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;
            }
        }
    }
}