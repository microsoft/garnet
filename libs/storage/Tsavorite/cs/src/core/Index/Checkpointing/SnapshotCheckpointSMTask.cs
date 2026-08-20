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
                    store._hybridLogCheckpointToken = guid;
                    store.InitializeHybridLogCheckpoint(store._hybridLogCheckpointToken, next.Version);
                    store._hybridLogCheckpoint.info.useSnapshotFile = 1;
                    ObjectLog_OnPrepare();
                    var prepareSnapshotStart = store.hlogBase.IsNullDevice ? store.hlogBase.HeadAddress : store.hlogBase.FlushedUntilAddress;
                    store._hybridLogCheckpoint.snapshotFlushCoordination =
                        new SnapshotFlushCoordination(store.hlogBase.GetPage(prepareSnapshotStart));
                    store.hlogBase.PrepareSnapshotFlushCoordination(store._hybridLogCheckpoint.snapshotFlushCoordination);
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;

                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);

                    store._hybridLogCheckpoint.info.snapshotFinalLogicalAddress = store._hybridLogCheckpoint.info.finalLogicalAddress;

                    store._hybridLogCheckpoint.snapshotFileDevice = store.checkpointManager.GetSnapshotLogDevice(store._hybridLogCheckpointToken);
                    store._hybridLogCheckpoint.snapshotFileObjectLogDevice = store.checkpointManager.GetSnapshotObjectLogDevice(store._hybridLogCheckpointToken);
                    store._hybridLogCheckpoint.snapshotFileDevice.Initialize(store.hlogBase.GetMainLogSegmentSize());
                    store._hybridLogCheckpoint.snapshotFileObjectLogDevice.Initialize(store.hlogBase.GetObjectLogSegmentSize());

                    // If we are using a NullDevice then storage tier is not enabled and FlushedUntilAddress may be ReadOnlyAddress but no records
                    // have actually been written; get all records in memory for the (non-NullDevice) Snapshot to write.
                    // Install a conservative gate before capturing snapshotStartFlushedLogicalAddress. Installation
                    // waits until every ReadOnly page flush already in flight has published its FlushedUntilAddress
                    // advancement; new flushes at or above the provisional page block on the coordination watermark.
                    var provisionalSnapshotStart = store.hlogBase.IsNullDevice ? store.hlogBase.HeadAddress : store.hlogBase.FlushedUntilAddress;
                    if (store._hybridLogCheckpoint.info.finalLogicalAddress <= provisionalSnapshotStart)
                    {
                        // Nothing to flush because the flushed region already contains everything up to finalLogicalAddress.
                        store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress = provisionalSnapshotStart;
                        store._hybridLogCheckpoint.snapshotFlushCoordination.Dispose();
                        store.hlogBase.ClearSnapshotFlushCoordination(store._hybridLogCheckpoint.snapshotFlushCoordination);
                        break;
                    }

                    try
                    {
                        store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress =
                            store.hlogBase.InstallSnapshotFlushCoordination(store._hybridLogCheckpoint.snapshotFlushCoordination);
                    }
                    catch (Exception ex)
                    {
                        store._hybridLogCheckpoint.snapshotFlushCoordination.Fail(ex);
                        store.hlogBase.ClearSnapshotFlushCoordination(store._hybridLogCheckpoint.snapshotFlushCoordination);
                        throw;
                    }

                    if (store._hybridLogCheckpoint.info.finalLogicalAddress <= store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress)
                    {
                        // Existing ReadOnly writes completed the range while installation drained. Release threads that
                        // sampled the coordination and remove the now-unneeded gate.
                        store._hybridLogCheckpoint.snapshotFlushCoordination.Dispose();
                        store.hlogBase.ClearSnapshotFlushCoordination(store._hybridLogCheckpoint.snapshotFlushCoordination);
                        break;
                    }

                    var startPage = store.hlogBase.GetPage(store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress);
                    var endPage = store.hlogBase.GetPage(store._hybridLogCheckpoint.info.finalLogicalAddress);
                    if (store._hybridLogCheckpoint.info.finalLogicalAddress > store.hlogBase.GetLogicalAddressOfStartOfPage(endPage))
                        endPage++;

                    // ReadOnly can advance only through pages strictly below Snapshot's completion watermark. Because HeadAddress
                    // is capped by FlushedUntilAddress, the active Snapshot page remains resident without epoch protection.
                    // Because we are writing pages outside epoch protection, the callee must be able to handle concurrent page
                    // changes during the flush; correctness is not affected as we will only read safe pages during recovery.
                    store.hlogBase.AsyncFlushPagesForSnapshot(ObjectLog_OnWaitFlush(),
                        startPage, endPage,
                        startLogicalAddress: store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress,
                        endLogicalAddress: store._hybridLogCheckpoint.info.finalLogicalAddress,
                        fuzzyStartLogicalAddress: store._hybridLogCheckpoint.info.startLogicalAddress,
                        logDevice: store._hybridLogCheckpoint.snapshotFileDevice,
                        objectLogDevice: store._hybridLogCheckpoint.snapshotFileObjectLogDevice,
                        coordination: store._hybridLogCheckpoint.snapshotFlushCoordination,
                        out store._hybridLogCheckpoint.flushedTask,
                        store.ThrottleCheckpointFlushDelayMs);
                    if (store._hybridLogCheckpoint.flushedTask != null)
                        stateMachineDriver.AddToWaitingList(store._hybridLogCheckpoint.flushedTask, StateMachineTaskType.SnapshotCheckpointSMTaskHybridLogFlushed);
                    break;

                case Phase.PERSISTENCE_CALLBACK:
                    // Set actual FlushedUntil to the latest possible main-log data on disk. NullDevice has no durable
                    // main-log prefix; Head may advance behind Snapshot for eviction, so recovery must still begin the
                    // snapshot overlay at the captured Snapshot start.
                    ObjectLog_OnPersistenceCallback();
                    store._hybridLogCheckpoint.info.flushedLogicalAddress = store.hlogBase.IsNullDevice
                        ? store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress
                        : store.hlogBase.FlushedUntilAddress;
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    store._hybridLogCheckpoint.snapshotFileDevice?.Dispose();
                    store._hybridLogCheckpoint.snapshotFileDevice = null;
                    store._hybridLogCheckpoint.snapshotFileObjectLogDevice?.Dispose();
                    store._hybridLogCheckpoint.snapshotFileObjectLogDevice = null;
                    store.hlogBase.ClearSnapshotFlushCoordination(store._hybridLogCheckpoint.snapshotFlushCoordination);
                    break;

                default:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;
            }
        }

    }
}