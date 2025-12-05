// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// A Snapshot persists a version by making a copy for every entry of that version separate from the log. It is
    /// slower and more complex than a foldover, but more space-efficient on the log, and retains in-place
    /// update performance as it does not advance the readonly marker unnecessarily.
    /// </summary>
    internal sealed class SnapshotCheckpointSMTask<TStoreFunctions, TAllocator> : HybridLogCheckpointSMTask<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        public SnapshotCheckpointSMTask(TsavoriteKV<TStoreFunctions, TAllocator> store, Guid guid)
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
                    store._hybridLogCheckpoint.info.useSnapshotFile = 1;
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;

                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);

                    store._hybridLogCheckpoint.info.snapshotFinalLogicalAddress = store._hybridLogCheckpoint.info.finalLogicalAddress;
                    store._hybridLogCheckpoint.snapshotFileDevice = store.checkpointManager.GetSnapshotLogDevice(store._hybridLogCheckpointToken);
                    store._hybridLogCheckpoint.snapshotFileObjectLogDevice = store.checkpointManager.GetSnapshotObjectLogDevice(store._hybridLogCheckpointToken);
                    store._hybridLogCheckpoint.snapshotFileDevice.Initialize(store.hlogBase.GetMainLogSegmentSize());
                    store._hybridLogCheckpoint.snapshotFileObjectLogDevice.Initialize(store.hlogBase.GetObjectLogSegmentSize());

                    // If we are using a NullDevice then storage tier is not enabled and FlushedUntilAddress may be ReadOnlyAddress; get all records in memory.
                    store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress = store.hlogBase.IsNullDevice ? store.hlogBase.HeadAddress : store.hlogBase.FlushedUntilAddress;

                    if (store._hybridLogCheckpoint.info.finalLogicalAddress <= store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress)
                    {
                        // Nothing to flush because the flushed region already contains everything up to finalLogicalAddress.
                        break;
                    }

                    long startPage = store.hlogBase.GetPage(store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress);
                    long endPage = store.hlogBase.GetPage(store._hybridLogCheckpoint.info.finalLogicalAddress);
                    if (store._hybridLogCheckpoint.info.finalLogicalAddress > store.hlogBase.GetLogicalAddressOfStartOfPage(endPage))
                        endPage++;

                    // We are writing pages outside epoch protection, so callee should be able to
                    // handle corrupted or unexpected concurrent page changes during the flush, e.g., by
                    // resuming epoch protection if necessary. Correctness is not affected as we will
                    // only read safe pages during recovery.
                    store.hlogBase.AsyncFlushPagesForSnapshot(PrepareObjectLogSnapshotBuffers(),
                        startPage, endPage,
                        store._hybridLogCheckpoint.info.finalLogicalAddress,
                        store._hybridLogCheckpoint.info.startLogicalAddress,
                        store._hybridLogCheckpoint.snapshotFileDevice,
                        store._hybridLogCheckpoint.snapshotFileObjectLogDevice,
                        out store._hybridLogCheckpoint.flushedSemaphore,
                        store.ThrottleCheckpointFlushDelayMs);
                    if (store._hybridLogCheckpoint.flushedSemaphore != null)
                        stateMachineDriver.AddToWaitingList(store._hybridLogCheckpoint.flushedSemaphore);
                    break;

                case Phase.PERSISTENCE_CALLBACK:
                    // Set actual FlushedUntil to the latest possible data in main log that is on disk
                    // If we are using a NullDevice then storage tier is not enabled and FlushedUntilAddress may be ReadOnlyAddress; get all records in memory.
                    CompleteObjectLogSnapshotBuffers();
                    store._hybridLogCheckpoint.info.flushedLogicalAddress = store.hlogBase.IsNullDevice ? store.hlogBase.HeadAddress : store.hlogBase.FlushedUntilAddress;
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    store._lastSnapshotCheckpoint = store._hybridLogCheckpoint.Transfer();
                    break;

                default:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;
            }
        }
    }
}