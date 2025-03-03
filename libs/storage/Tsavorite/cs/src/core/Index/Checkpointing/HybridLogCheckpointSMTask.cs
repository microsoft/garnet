// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// This task is the base class for a checkpoint "backend", which decides how a captured version is
    /// persisted on disk.
    /// </summary>
    internal abstract class HybridLogCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator> : IStateMachineTask
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        protected readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store;
        long lastVersion;
        readonly Guid guid;

        public HybridLogCheckpointSMTask(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, Guid guid)
        {
            this.store = store;
            this.guid = guid;
        }

        /// <inheritdoc />
        public virtual void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    lastVersion = next.Version;
                    if (store._hybridLogCheckpoint.IsDefault())
                    {
                        store._hybridLogCheckpointToken = guid;
                        store.InitializeHybridLogCheckpoint(store._hybridLogCheckpointToken, next.Version);
                    }
                    store._hybridLogCheckpoint.info.version = next.Version;
                    store._hybridLogCheckpoint.info.startLogicalAddress = store.hlogBase.GetTailAddress();
                    // Capture begin address before checkpoint starts
                    store._hybridLogCheckpoint.info.beginAddress = store.hlogBase.BeginAddress;
                    break;

                case Phase.IN_PROGRESS:
                    // Wait for PREPARE threads to finish active transactions and enter barrier
                    while (store.hlogBase.NumActiveLockingSessions > 0)
                    {
                        _ = Thread.Yield();
                    }
                    store.CheckpointVersionShift(lastVersion, next.Version);
                    break;

                case Phase.WAIT_FLUSH:
                    store._hybridLogCheckpoint.info.headAddress = store.hlogBase.HeadAddress;
                    store._hybridLogCheckpoint.info.nextVersion = next.Version;
                    break;

                case Phase.PERSISTENCE_CALLBACK:
                    CollectMetadata(next, store);
                    store.WriteHybridLogMetaInfo();
                    store.lastVersion = lastVersion;
                    break;

                case Phase.REST:
                    store._hybridLogCheckpoint.Dispose();
                    var nextTcs = new TaskCompletionSource<LinkedCheckpointInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
                    store.checkpointTcs.SetResult(new LinkedCheckpointInfo { NextTask = nextTcs.Task });
                    store.checkpointTcs = nextTcs;
                    break;
            }
        }

        protected static void CollectMetadata(SystemState next, TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
            // Collect object log offsets only after flushes
            // are completed
            var seg = store.hlog.GetSegmentOffsets();
            if (seg != null)
            {
                store._hybridLogCheckpoint.info.objectLogSegmentOffsets = new long[seg.Length];
                Array.Copy(seg, store._hybridLogCheckpoint.info.objectLogSegmentOffsets, seg.Length);
            }
        }

        /// <inheritdoc />
        public virtual void GlobalAfterEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
        }
    }
}