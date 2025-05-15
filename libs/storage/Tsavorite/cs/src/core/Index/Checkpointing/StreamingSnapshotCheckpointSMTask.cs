// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{

    /// <summary>
    /// A Streaming Snapshot persists a version by yielding a stream of key-value pairs that correspond to
    /// a consistent snapshot of the database, for the old version (v). Unlike Snapshot, StreamingSnapshot
    /// is designed to not require tail growth even during the WAIT_FLUSH phase of checkpointing. Further,
    /// it does not require a snapshot of the index. Recovery is achieved by replaying the yielded log 
    /// of key-value pairs and inserting each record into an empty database.
    /// </summary>
    sealed class StreamingSnapshotCheckpointSMTask<TStoreFunctions, TAllocator> : HybridLogCheckpointSMTask<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        public StreamingSnapshotCheckpointSMTask(TsavoriteKV<TStoreFunctions, TAllocator> store, Guid guid)
            : base(store, guid)
        {
            isStreaming = true;
        }

        /// <inheritdoc />
        public override void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    store._lastSnapshotCheckpoint.Dispose();
                    store._hybridLogCheckpointToken = guid;
                    store.InitializeHybridLogCheckpoint(store._hybridLogCheckpointToken, next.Version);
                    store._hybridLogCheckpoint.info.nextVersion = next.Version + 1;
                    store.StreamingSnapshotScanPhase1();
                    break;

                case Phase.IN_PROGRESS:
                    if (store._hybridLogCheckpoint.info.nextVersion != next.Version)
                        throw new TsavoriteException($"StreamingSnapshotCheckpointSMTask: IN_PROGRESS phase with incorrect version {next.Version}, expected {store._hybridLogCheckpoint.info.nextVersion}");
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;

                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    var finalLogicalAddress = store.hlogBase.GetTailAddress();
                    store.StreamingSnapshotScanPhase2(finalLogicalAddress);
                    break;

                default:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;
            }
        }
    }
}