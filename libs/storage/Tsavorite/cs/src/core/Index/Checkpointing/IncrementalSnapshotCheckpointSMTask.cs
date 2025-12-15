// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// A Incremental Snapshot makes a copy of only changes that have happened since the last full Snapshot. It is
    /// slower and more complex than a foldover, but more space-efficient on the log, and retains in-place
    /// update performance as it does not advance the readonly marker unnecessarily.
    /// </summary>
    internal sealed class IncrementalSnapshotCheckpointSMTask<TStoreFunctions, TAllocator> : HybridLogCheckpointSMTask<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        public IncrementalSnapshotCheckpointSMTask(TsavoriteKV<TStoreFunctions, TAllocator> store, Guid guid)
            : base(store, guid)
        {
        }

        /// <inheritdoc />
        public override void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    // Capture state before checkpoint starts
                    store._hybridLogCheckpoint = store._lastSnapshotCheckpoint;
                    ObjectLog_OnPrepare();
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    store._hybridLogCheckpoint.prevVersion = next.Version;
                    break;

                case Phase.IN_PROGRESS:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;

                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
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
                    store.hlogBase.AsyncFlushDeltaToDevice(ObjectLog_OnWaitFlush(),
                        store.hlogBase.FlushedUntilAddress,
                        store._hybridLogCheckpoint.info.finalLogicalAddress,
                        store._lastSnapshotCheckpoint.info.finalLogicalAddress,
                        store._hybridLogCheckpoint.prevVersion,
                        store._hybridLogCheckpoint.deltaLog,
                        out store._hybridLogCheckpoint.flushedSemaphore,
                        store.ThrottleCheckpointFlushDelayMs);
                    if (store._hybridLogCheckpoint.flushedSemaphore != null)
                        stateMachineDriver.AddToWaitingList(store._hybridLogCheckpoint.flushedSemaphore);
                    break;

                case Phase.PERSISTENCE_CALLBACK:
                    ObjectLog_OnPersistenceCallback();
                    store._hybridLogCheckpoint.info.deltaTailAddress = store._hybridLogCheckpoint.deltaLog.TailAddress;
                    store.WriteHybridLogIncrementalMetaInfo(store._hybridLogCheckpoint.deltaLog);
                    store._hybridLogCheckpoint.info.deltaTailAddress = store._hybridLogCheckpoint.deltaLog.TailAddress;
                    store._lastSnapshotCheckpoint = store._hybridLogCheckpoint.Transfer();
                    break;

                case Phase.REST:
                    store.CleanupLogIncrementalCheckpoint();
                    store._hybridLogCheckpoint.Dispose();
                    break;
            }
        }
    }
}