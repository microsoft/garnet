// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// A Version change captures a version on the log by forcing all threads to coordinate a move to the next
    /// version. It is used as the basis of many other tasks, which decides what they do with the captured
    /// version.
    /// </summary>
    internal sealed class VersionChangeTask : ISynchronizationTask
    {
        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value>(
            SystemState next,
            TsavoriteKV<Key, Value> store)
        {
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value>(
            SystemState start,
            TsavoriteKV<Key, Value> store)
        {
        }

        /// <inheritdoc />
        public void OnThreadState<Key, Value, Input, Output, Context, TsavoriteSession>(
            SystemState current, SystemState prev,
            TsavoriteKV<Key, Value> store,
            TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TsavoriteSession storeSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where TsavoriteSession : ITsavoriteSession
        {
            switch (current.Phase)
            {
                case Phase.PREPARE:
                    if (ctx is not null)
                        ctx.markers[EpochPhaseIdx.Prepare] = true;

                    store.epoch.Mark(EpochPhaseIdx.Prepare, current.Version);

                    // Using bumpEpoch: true allows us to guarantee that when system state proceeds, all threads in prior state
                    // will see that hlog.NumActiveLockingSessions == 0, ensuring that they can potentially block for the next state.
                    if (store.epoch.CheckIsComplete(EpochPhaseIdx.Prepare, current.Version) && store.hlog.NumActiveLockingSessions == 0)
                        store.GlobalStateMachineStep(current, bumpEpoch: store.CheckpointVersionSwitchBarrier);
                    break;
                case Phase.IN_PROGRESS:
                    if (ctx != null)
                    {
                        // Need to be very careful here as threadCtx is changing
                        var _ctx = prev.Phase == Phase.IN_PROGRESS ? ctx.prevCtx : ctx;
                        var tokens = store._hybridLogCheckpoint.info.checkpointTokens;
                        if (!store.SameCycle(ctx, current) || tokens == null)
                            return;

                        if (!_ctx.markers[EpochPhaseIdx.InProgress])
                        {
                            store.AtomicSwitch(ctx, ctx.prevCtx, _ctx.version, tokens);
                            TsavoriteKV<Key, Value>.InitContext(ctx, ctx.prevCtx.sessionID, ctx.prevCtx.sessionName, ctx.prevCtx.serialNum);

                            // Has to be prevCtx, not ctx
                            ctx.prevCtx.markers[EpochPhaseIdx.InProgress] = true;
                        }
                    }

                    store.epoch.Mark(EpochPhaseIdx.InProgress, current.Version);
                    if (store.epoch.CheckIsComplete(EpochPhaseIdx.InProgress, current.Version))
                        store.GlobalStateMachineStep(current);
                    break;
                case Phase.REST:
                    break;
            }
        }
    }

    /// <summary>
    /// The FoldOver task simply sets the read only offset to the current end of the log, so a captured version
    /// is immutable and will eventually be flushed to disk.
    /// </summary>
    internal sealed class FoldOverTask : ISynchronizationTask
    {
        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value>(
            SystemState next,
            TsavoriteKV<Key, Value> store)
        {
            if (next.Phase == Phase.REST)
                // Before leaving the checkpoint, make sure all previous versions are read-only.
                store.hlog.ShiftReadOnlyToTail(out _, out _);
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value>(
            SystemState next,
            TsavoriteKV<Key, Value> store)
        { }

        /// <inheritdoc />
        public void OnThreadState<Key, Value, Input, Output, Context, TsavoriteSession>(
            SystemState current,
            SystemState prev,
            TsavoriteKV<Key, Value> store,
            TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TsavoriteSession storeSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where TsavoriteSession : ITsavoriteSession
        {
        }
    }

    /// <summary>
    /// A VersionChangeStateMachine orchestrates to capture a version, but does not flush to disk.
    /// </summary>
    internal class VersionChangeStateMachine : SynchronizationStateMachineBase
    {
        private readonly long targetVersion;

        /// <summary>
        /// Construct a new VersionChangeStateMachine with the given tasks. Does not load any tasks by default.
        /// </summary>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        /// <param name="tasks">The tasks to load onto the state machine</param>
        protected VersionChangeStateMachine(long targetVersion = -1, params ISynchronizationTask[] tasks) : base(tasks)
        {
            this.targetVersion = targetVersion;
        }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start)
        {
            var nextState = SystemState.Copy(ref start);
            switch (start.Phase)
            {
                case Phase.REST:
                    nextState.Phase = Phase.PREPARE;
                    break;
                case Phase.PREPARE:
                    nextState.Phase = Phase.IN_PROGRESS;
                    SetToVersion(targetVersion == -1 ? start.Version + 1 : targetVersion);
                    nextState.Version = ToVersion();
                    break;
                case Phase.IN_PROGRESS:
                    nextState.Phase = Phase.REST;
                    break;
                default:
                    throw new TsavoriteException("Invalid Enum Argument");
            }

            return nextState;
        }
    }
}