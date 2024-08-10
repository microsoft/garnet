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
    internal sealed class VersionChangeTask<TKey, TValue, TStoreFunctions, TAllocator> : ISynchronizationTask<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <inheritdoc />
        public void GlobalBeforeEnteringState(
            SystemState next,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState(
            SystemState start,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
        }

        /// <inheritdoc />
        public void OnThreadState<Input, Output, Context, TSessionFunctionsWrapper>(
            SystemState current, SystemState prev,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TSessionFunctionsWrapper sessionFunctions,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionEpochControl
        {
            switch (current.Phase)
            {
                case Phase.PREPARE:
                    if (ctx is not null)
                        ctx.markers[EpochPhaseIdx.Prepare] = true;

                    store.kernel.epoch.Mark(EpochPhaseIdx.Prepare, current.Version);

                    // Using bumpEpoch: true allows us to guarantee that when system state proceeds, all threads in prior state
                    // will see that hlog.NumActiveLockingSessions == 0, ensuring that they can potentially block for the next state.
                    if (store.kernel.epoch.CheckIsComplete(EpochPhaseIdx.Prepare, current.Version) && store.hlogBase.NumActiveLockingSessions == 0)
                        store.GlobalStateMachineStep(current, bumpEpoch: store.CheckpointVersionSwitchBarrier);
                    break;
                case Phase.IN_PROGRESS:
                    if (ctx != null)
                    {
                        // Need to be very careful here as threadCtx is changing
                        var _ctx = prev.Phase == Phase.IN_PROGRESS ? ctx.prevCtx : ctx;

                        if (!_ctx.markers[EpochPhaseIdx.InProgress])
                        {
                            _ = TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.AtomicSwitch(ctx, ctx.prevCtx, _ctx.version);
                            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.InitContext(ctx, ctx.prevCtx.sessionID, ctx.prevCtx.sessionName);

                            // Has to be prevCtx, not ctx
                            ctx.prevCtx.markers[EpochPhaseIdx.InProgress] = true;
                        }
                    }

                    store.kernel.epoch.Mark(EpochPhaseIdx.InProgress, current.Version);
                    if (store.kernel.epoch.CheckIsComplete(EpochPhaseIdx.InProgress, current.Version))
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
    internal sealed class FoldOverTask<Key, Value, TStoreFunctions, TAllocator> : ISynchronizationTask<Key, Value, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<Key, Value>
        where TAllocator : IAllocator<Key, Value, TStoreFunctions>
    {
        /// <inheritdoc />
        public void GlobalBeforeEnteringState(
            SystemState next,
            TsavoriteKV<Key, Value, TStoreFunctions, TAllocator> store)
        {
            if (next.Phase == Phase.REST)
                // Before leaving the checkpoint, make sure all previous versions are read-only.
                store.hlogBase.ShiftReadOnlyToTail(out _, out _);
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState(
            SystemState next,
            TsavoriteKV<Key, Value, TStoreFunctions, TAllocator> store)
        { }

        /// <inheritdoc />
        public void OnThreadState<Input, Output, Context, TSessionFunctionsWrapper>(
            SystemState current,
            SystemState prev,
            TsavoriteKV<Key, Value, TStoreFunctions, TAllocator> store,
            TsavoriteKV<Key, Value, TStoreFunctions, TAllocator>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TSessionFunctionsWrapper sessionFunctions,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionEpochControl
        {
        }
    }

    /// <summary>
    /// A VersionChangeStateMachine orchestrates to capture a version, but does not flush to disk.
    /// </summary>
    internal class VersionChangeStateMachine<Key, Value, TStoreFunctions, TAllocator> : SynchronizationStateMachineBase<Key, Value, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<Key, Value>
        where TAllocator : IAllocator<Key, Value, TStoreFunctions>
    {
        private readonly long targetVersion;

        /// <summary>
        /// Construct a new VersionChangeStateMachine with the given tasks. Does not load any tasks by default.
        /// </summary>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        /// <param name="tasks">The tasks to load onto the state machine</param>
        protected VersionChangeStateMachine(long targetVersion = -1, params ISynchronizationTask<Key, Value, TStoreFunctions, TAllocator>[] tasks) : base(tasks)
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