﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{

    /// <summary>
    /// A Streaming Snapshot persists a version by yielding a stream of key-value pairs that correspond to
    /// a consistent snapshot of the database, for the old version (v). Unlike Snapshot, StreamingSnapshot
    /// is designed to not require tail growth even during the WAIT_FLUSH phase of checkpointing. Further,
    /// it does not require a snapshot of the index. Recovery is achieved by replaying the yielded log 
    /// of key-value pairs and inserting each record into an empty database.
    /// </summary>
    sealed class StreamingSnapshotCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator> : HybridLogCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        readonly long targetVersion;

        public StreamingSnapshotCheckpointSMTask(long targetVersion, TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
            : base(store)
        {
            this.targetVersion = targetVersion;
        }

        /// <inheritdoc />
        public override void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.PREP_STREAMING_SNAPSHOT_CHECKPOINT:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    store._hybridLogCheckpointToken = Guid.NewGuid();
                    store._hybridLogCheckpoint.info.version = next.Version;
                    store._hybridLogCheckpoint.info.nextVersion = targetVersion == -1 ? next.Version + 1 : targetVersion;
                    store._lastSnapshotCheckpoint.Dispose();
                    _ = Task.Run(store.StreamingSnapshotScanPhase1);
                    break;
                case Phase.PREPARE:
                    store.InitializeHybridLogCheckpoint(store._hybridLogCheckpointToken, next.Version);
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;
                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    store._hybridLogCheckpoint.flushedSemaphore = new SemaphoreSlim(0);
                    var finalLogicalAddress = store.hlogBase.GetTailAddress();
                    Task.Run(() => store.StreamingSnapshotScanPhase2(finalLogicalAddress));
                    break;
                default:
                    base.GlobalBeforeEnteringState(next, stateMachineDriver);
                    break;
            }
        }
    }
}