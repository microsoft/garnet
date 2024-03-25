// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// This task is the base class for a checkpoint "backend", which decides how a captured version is
    /// persisted on disk.
    /// </summary>
    internal abstract class HybridLogCheckpointOrchestrationTask : ISynchronizationTask
    {
        private long lastVersion;
        /// <inheritdoc />
        public virtual void GlobalBeforeEnteringState<Key, Value>(SystemState next,
            TsavoriteKV<Key, Value> store)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    lastVersion = store.systemState.Version;
                    if (store._hybridLogCheckpoint.IsDefault())
                    {
                        store._hybridLogCheckpointToken = Guid.NewGuid();
                        store.InitializeHybridLogCheckpoint(store._hybridLogCheckpointToken, next.Version);
                    }
                    store._hybridLogCheckpoint.info.version = next.Version;
                    store._hybridLogCheckpoint.info.startLogicalAddress = store.hlog.GetTailAddress();
                    // Capture begin address before checkpoint starts
                    store._hybridLogCheckpoint.info.beginAddress = store.hlog.BeginAddress;
                    break;
                case Phase.IN_PROGRESS:
                    store.CheckpointVersionShift(lastVersion, next.Version);
                    break;
                case Phase.WAIT_FLUSH:
                    store._hybridLogCheckpoint.info.headAddress = store.hlog.HeadAddress;
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

        protected static void CollectMetadata<Key, Value>(SystemState next, TsavoriteKV<Key, Value> store)
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
                        store._activeSessions.Remove(key);
                }
            }

            // Make sure previous recoverable sessions are re-checkpointed
            foreach (var item in store.RecoverableSessions)
            {
                store._hybridLogCheckpoint.info.checkpointTokens.TryAdd(item.Item1, (item.Item2, item.Item3));
            }
        }

        /// <inheritdoc />
        public virtual void GlobalAfterEnteringState<Key, Value>(SystemState next,
            TsavoriteKV<Key, Value> store)
        {
        }

        /// <inheritdoc />
        public virtual void OnThreadState<Key, Value, Input, Output, Context, TsavoriteSession>(
            SystemState current,
            SystemState prev, TsavoriteKV<Key, Value> store,
            TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TsavoriteSession storeSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where TsavoriteSession : ITsavoriteSession
        {
            if (current.Phase != Phase.PERSISTENCE_CALLBACK) return;

            if (ctx is not null)
            {
                if (!ctx.prevCtx.markers[EpochPhaseIdx.CheckpointCompletionCallback])
                {
                    store.IssueCompletionCallback(ctx, storeSession);
                    ctx.prevCtx.markers[EpochPhaseIdx.CheckpointCompletionCallback] = true;
                }
            }

            store.epoch.Mark(EpochPhaseIdx.CheckpointCompletionCallback, current.Version);
            if (store.epoch.CheckIsComplete(EpochPhaseIdx.CheckpointCompletionCallback, current.Version))
                store.GlobalStateMachineStep(current);
        }
    }

    /// <summary>
    /// A FoldOver checkpoint persists a version by setting the read-only marker past the last entry of that
    /// version on the log and waiting until it is flushed to disk. It is simple and fast, but can result
    /// in garbage entries on the log, and a slower recovery of performance.
    /// </summary>
    internal sealed class FoldOverCheckpointTask : HybridLogCheckpointOrchestrationTask
    {
        /// <inheritdoc />
        public override void GlobalBeforeEnteringState<Key, Value>(SystemState next,
            TsavoriteKV<Key, Value> store)
        {
            base.GlobalBeforeEnteringState(next, store);

            if (next.Phase == Phase.PREPARE)
            {
                store._lastSnapshotCheckpoint.Dispose();
            }

            if (next.Phase == Phase.IN_PROGRESS)
                base.GlobalBeforeEnteringState(next, store);

            if (next.Phase != Phase.WAIT_FLUSH) return;

            store.hlog.ShiftReadOnlyToTail(out var tailAddress,
                out store._hybridLogCheckpoint.flushedSemaphore);
            store._hybridLogCheckpoint.info.finalLogicalAddress = tailAddress;
        }

        /// <inheritdoc />
        public override void OnThreadState<Key, Value, Input, Output, Context, TsavoriteSession>(
            SystemState current,
            SystemState prev,
            TsavoriteKV<Key, Value> store,
            TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TsavoriteSession storeSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
        {
            base.OnThreadState(current, prev, store, ctx, storeSession, valueTasks, token);

            if (current.Phase != Phase.WAIT_FLUSH) return;

            if (ctx is null || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
            {
                var s = store._hybridLogCheckpoint.flushedSemaphore;

                var notify = store.hlog.FlushedUntilAddress >= store._hybridLogCheckpoint.info.finalLogicalAddress;
                notify = notify || !store.SameCycle(ctx, current) || s == null;

                if (valueTasks != null && !notify)
                {
                    valueTasks.Add(new ValueTask(s.WaitAsync(token).ContinueWith(t => s.Release())));
                }

                if (!notify) return;

                if (ctx is not null)
                    ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
            }

            store.epoch.Mark(EpochPhaseIdx.WaitFlush, current.Version);
            if (store.epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, current.Version))
                store.GlobalStateMachineStep(current);
        }
    }

    /// <summary>
    /// A Snapshot persists a version by making a copy for every entry of that version separate from the log. It is
    /// slower and more complex than a foldover, but more space-efficient on the log, and retains in-place
    /// update performance as it does not advance the readonly marker unnecessarily.
    /// </summary>
    internal sealed class SnapshotCheckpointTask : HybridLogCheckpointOrchestrationTask
    {
        /// <inheritdoc />
        public override void GlobalBeforeEnteringState<Key, Value>(SystemState next, TsavoriteKV<Key, Value> store)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    store._lastSnapshotCheckpoint.Dispose();
                    base.GlobalBeforeEnteringState(next, store);
                    store._hybridLogCheckpoint.info.useSnapshotFile = 1;
                    break;
                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, store);
                    store._hybridLogCheckpoint.info.finalLogicalAddress = store.hlog.GetTailAddress();
                    store._hybridLogCheckpoint.info.snapshotFinalLogicalAddress = store._hybridLogCheckpoint.info.finalLogicalAddress;

                    store._hybridLogCheckpoint.snapshotFileDevice =
                        store.checkpointManager.GetSnapshotLogDevice(store._hybridLogCheckpointToken);
                    store._hybridLogCheckpoint.snapshotFileObjectLogDevice =
                        store.checkpointManager.GetSnapshotObjectLogDevice(store._hybridLogCheckpointToken);
                    store._hybridLogCheckpoint.snapshotFileDevice.Initialize(store.hlog.GetSegmentSize());
                    store._hybridLogCheckpoint.snapshotFileObjectLogDevice.Initialize(-1);

                    // If we are using a NullDevice then storage tier is not enabled and FlushedUntilAddress may be ReadOnlyAddress; get all records in memory.
                    store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress = store.hlog.IsNullDevice ? store.hlog.HeadAddress : store.hlog.FlushedUntilAddress;

                    long startPage = store.hlog.GetPage(store._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress);
                    long endPage = store.hlog.GetPage(store._hybridLogCheckpoint.info.finalLogicalAddress);
                    if (store._hybridLogCheckpoint.info.finalLogicalAddress >
                        store.hlog.GetStartLogicalAddress(endPage))
                    {
                        endPage++;
                    }

                    // We are writing pages outside epoch protection, so callee should be able to
                    // handle corrupted or unexpected concurrent page changes during the flush, e.g., by
                    // resuming epoch protection if necessary. Correctness is not affected as we will
                    // only read safe pages during recovery.
                    store.hlog.AsyncFlushPagesToDevice(
                        startPage,
                        endPage,
                        store._hybridLogCheckpoint.info.finalLogicalAddress,
                        store._hybridLogCheckpoint.info.startLogicalAddress,
                        store._hybridLogCheckpoint.snapshotFileDevice,
                        store._hybridLogCheckpoint.snapshotFileObjectLogDevice,
                        out store._hybridLogCheckpoint.flushedSemaphore,
                        store.ThrottleCheckpointFlushDelayMs);
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    // Set actual FlushedUntil to the latest possible data in main log that is on disk
                    // If we are using a NullDevice then storage tier is not enabled and FlushedUntilAddress may be ReadOnlyAddress; get all records in memory.
                    store._hybridLogCheckpoint.info.flushedLogicalAddress = store.hlog.IsNullDevice ? store.hlog.HeadAddress : store.hlog.FlushedUntilAddress;
                    base.GlobalBeforeEnteringState(next, store);
                    store._lastSnapshotCheckpoint = store._hybridLogCheckpoint.Transfer();
                    break;
                default:
                    base.GlobalBeforeEnteringState(next, store);
                    break;
            }
        }

        /// <inheritdoc />
        public override void OnThreadState<Key, Value, Input, Output, Context, TsavoriteSession>(
            SystemState current,
            SystemState prev, TsavoriteKV<Key, Value> store,
            TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TsavoriteSession storeSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
        {
            base.OnThreadState(current, prev, store, ctx, storeSession, valueTasks, token);

            if (current.Phase != Phase.WAIT_FLUSH) return;

            if (ctx is null || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
            {
                var s = store._hybridLogCheckpoint.flushedSemaphore;

                var notify = s != null && s.CurrentCount > 0;
                notify = notify || !store.SameCycle(ctx, current) || s == null;

                if (valueTasks != null && !notify)
                {
                    Debug.Assert(s != null);
                    valueTasks.Add(new ValueTask(s.WaitAsync(token).ContinueWith(t => s.Release())));
                }

                if (!notify) return;

                if (ctx is not null)
                    ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
            }

            store.epoch.Mark(EpochPhaseIdx.WaitFlush, current.Version);
            if (store.epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, current.Version))
                store.GlobalStateMachineStep(current);
        }
    }

    /// <summary>
    /// A Incremental Snapshot makes a copy of only changes that have happened since the last full Snapshot. It is
    /// slower and more complex than a foldover, but more space-efficient on the log, and retains in-place
    /// update performance as it does not advance the readonly marker unnecessarily.
    /// </summary>
    internal sealed class IncrementalSnapshotCheckpointTask : HybridLogCheckpointOrchestrationTask
    {
        /// <inheritdoc />
        public override void GlobalBeforeEnteringState<Key, Value>(SystemState next, TsavoriteKV<Key, Value> store)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    store._hybridLogCheckpoint = store._lastSnapshotCheckpoint;
                    base.GlobalBeforeEnteringState(next, store);
                    store._hybridLogCheckpoint.prevVersion = next.Version;
                    break;
                case Phase.IN_PROGRESS:
                    base.GlobalBeforeEnteringState(next, store);
                    break;
                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, store);
                    store._hybridLogCheckpoint.info.finalLogicalAddress = store.hlog.GetTailAddress();

                    if (store._hybridLogCheckpoint.deltaLog == null)
                    {
                        store._hybridLogCheckpoint.deltaFileDevice = store.checkpointManager.GetDeltaLogDevice(store._hybridLogCheckpointToken);
                        store._hybridLogCheckpoint.deltaFileDevice.Initialize(-1);
                        store._hybridLogCheckpoint.deltaLog = new DeltaLog(store._hybridLogCheckpoint.deltaFileDevice, store.hlog.LogPageSizeBits, -1);
                        store._hybridLogCheckpoint.deltaLog.InitializeForWrites(store.hlog.bufferPool);
                    }

                    // We are writing delta records outside epoch protection, so callee should be able to
                    // handle corrupted or unexpected concurrent page changes during the flush, e.g., by
                    // resuming epoch protection if necessary. Correctness is not affected as we will
                    // only read safe pages during recovery.
                    store.hlog.AsyncFlushDeltaToDevice(
                        store.hlog.FlushedUntilAddress,
                        store._hybridLogCheckpoint.info.finalLogicalAddress,
                        store._lastSnapshotCheckpoint.info.finalLogicalAddress,
                        store._hybridLogCheckpoint.prevVersion,
                        store._hybridLogCheckpoint.deltaLog,
                        out store._hybridLogCheckpoint.flushedSemaphore,
                        store.ThrottleCheckpointFlushDelayMs);
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    CollectMetadata(next, store);
                    store._hybridLogCheckpoint.info.deltaTailAddress = store._hybridLogCheckpoint.deltaLog.TailAddress;
                    store.WriteHybridLogIncrementalMetaInfo(store._hybridLogCheckpoint.deltaLog);
                    store._hybridLogCheckpoint.info.deltaTailAddress = store._hybridLogCheckpoint.deltaLog.TailAddress;
                    store._lastSnapshotCheckpoint = store._hybridLogCheckpoint.Transfer();
                    store._hybridLogCheckpoint.Dispose();
                    break;
            }
        }

        /// <inheritdoc />
        public override void OnThreadState<Key, Value, Input, Output, Context, TsavoriteSession>(
            SystemState current,
            SystemState prev, TsavoriteKV<Key, Value> store,
            TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TsavoriteSession storeSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
        {
            base.OnThreadState(current, prev, store, ctx, storeSession, valueTasks, token);

            if (current.Phase != Phase.WAIT_FLUSH) return;

            if (ctx is null || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
            {
                var s = store._hybridLogCheckpoint.flushedSemaphore;

                var notify = s != null && s.CurrentCount > 0;
                notify = notify || !store.SameCycle(ctx, current) || s == null;

                if (valueTasks != null && !notify)
                {
                    Debug.Assert(s != null);
                    valueTasks.Add(new ValueTask(s.WaitAsync(token).ContinueWith(t => s.Release())));
                }

                if (!notify) return;

                if (ctx is not null)
                    ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
            }

            store.epoch.Mark(EpochPhaseIdx.WaitFlush, current.Version);
            if (store.epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, current.Version))
                store.GlobalStateMachineStep(current);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    internal class HybridLogCheckpointStateMachine : VersionChangeStateMachine
    {
        /// <summary>
        /// Construct a new HybridLogCheckpointStateMachine to use the given checkpoint backend (either fold-over or
        /// snapshot), drawing boundary at targetVersion.
        /// </summary>
        /// <param name="checkpointBackend">A task that encapsulates the logic to persist the checkpoint</param>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        public HybridLogCheckpointStateMachine(ISynchronizationTask checkpointBackend, long targetVersion = -1)
            : base(targetVersion, new VersionChangeTask(), checkpointBackend) { }

        /// <summary>
        /// Construct a new HybridLogCheckpointStateMachine with the given tasks. Does not load any tasks by default.
        /// </summary>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        /// <param name="tasks">The tasks to load onto the state machine</param>
        protected HybridLogCheckpointStateMachine(long targetVersion, params ISynchronizationTask[] tasks)
            : base(targetVersion, tasks) { }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start)
        {
            var result = SystemState.Copy(ref start);
            switch (start.Phase)
            {
                case Phase.IN_PROGRESS:
                    result.Phase = Phase.WAIT_FLUSH;
                    break;
                case Phase.WAIT_FLUSH:
                    result.Phase = Phase.PERSISTENCE_CALLBACK;
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    result.Phase = Phase.REST;
                    break;
                default:
                    result = base.NextState(start);
                    break;
            }

            return result;
        }
    }
}