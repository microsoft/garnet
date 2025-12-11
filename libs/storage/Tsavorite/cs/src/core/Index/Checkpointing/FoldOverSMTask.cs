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
    internal sealed class FoldOverSMTask<TStoreFunctions, TAllocator> : HybridLogCheckpointSMTask<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        public FoldOverSMTask(TsavoriteKV<TStoreFunctions, TAllocator> store, Guid guid)
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
                    ObjectLog_OnPrepare();
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
                        _ = ObjectLog_OnWaitFlush();
                    }
                    finally
                    {
                        store.epoch.Suspend();
                    }
                    break;

                case Phase.PERSISTENCE_CALLBACK:
                    // Set actual FlushedUntil to the latest possible data in main log that is on disk
                    // If we are using a NullDevice then storage tier is not enabled and FlushedUntilAddress may be ReadOnlyAddress; get all records in memory.
                    ObjectLog_OnPersistenceCallback();
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;

                default:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;
            }
        }
    }
}