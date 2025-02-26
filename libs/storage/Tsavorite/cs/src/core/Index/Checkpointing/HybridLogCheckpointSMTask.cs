﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
                    lastVersion = store.systemState.Version;
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

            // Temporarily block new sessions from starting, which may add an entry to the table and resize the
            // dictionary. There should be minimal contention here.
            lock (store._activeSessions)
            {
                List<int> toDelete = null;

                // write dormant sessions to checkpoint
                foreach (var kvp in store._activeSessions)
                {
                    kvp.Value.session.AtomicSwitch(next.Version - 1);
                    if (!kvp.Value.isActive)
                    {
                        toDelete ??= new();
                        toDelete.Add(kvp.Key);
                    }
                }

                // delete any sessions that ended during checkpoint cycle
                if (toDelete != null)
                {
                    foreach (var key in toDelete)
                        _ = store._activeSessions.Remove(key);
                }
            }
        }

        /// <inheritdoc />
        public virtual void GlobalAfterEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
        }
    }
}