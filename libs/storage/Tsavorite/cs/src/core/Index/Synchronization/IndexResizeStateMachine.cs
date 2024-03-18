// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Resizes an index
    /// </summary>
    internal sealed class IndexResizeTask : ISynchronizationTask
    {
        bool allThreadsInPrepareGrow;

        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value>(
            SystemState next,
            TsavoriteKV<Key, Value> store)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE_GROW:
                    allThreadsInPrepareGrow = false;
                    break;
                case Phase.IN_PROGRESS_GROW:
                    // Set up the transition to new version of HT
                    var numChunks = (int)(store.state[store.resizeInfo.version].size / Constants.kSizeofChunk);
                    if (numChunks == 0) numChunks = 1; // at least one chunk

                    store.numPendingChunksToBeSplit = numChunks;
                    store.splitStatus = new long[numChunks];
                    store.overflowBucketsAllocatorResize = store.overflowBucketsAllocator;
                    store.overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>();

                    // Because version is 0 or 1, indexing by [1 - resizeInfo.version] references to the "new version".
                    // Once growth initialization is complete, the state versions are swapped by setting resizeInfo.version = 1 - resizeInfo.version.
                    // Initialize the new version to twice the size of the old version.
                    store.Initialize(1 - store.resizeInfo.version, store.state[store.resizeInfo.version].size * 2, store.sectorSize);

                    store.resizeInfo.version = 1 - store.resizeInfo.version;
                    break;
                case Phase.REST:
                    // nothing to do
                    break;
                default:
                    throw new TsavoriteException("Invalid Enum Argument");
            }
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value>(
            SystemState next,
            TsavoriteKV<Key, Value> store)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE_GROW:
                    bool isProtected = store.epoch.ThisInstanceProtected();
                    if (!isProtected)
                        store.epoch.Resume();
                    try
                    {
                        store.epoch.BumpCurrentEpoch(() => allThreadsInPrepareGrow = true);
                    }
                    finally
                    {
                        if (!isProtected)
                            store.epoch.Suspend();
                    }
                    break;
                case Phase.IN_PROGRESS_GROW:
                case Phase.REST:
                    // nothing to do
                    break;
                default:
                    throw new TsavoriteException("Invalid Enum Argument");
            }
        }

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
            switch (current.Phase)
            {
                case Phase.PREPARE_GROW:
                    // Using bumpEpoch: true allows us to guarantee that when system state proceeds, all threads in prior state
                    // will see that hlog.NumActiveLockingSessions == 0, ensuring that they can potentially block for the next state.
                    if (allThreadsInPrepareGrow && store.hlog.NumActiveLockingSessions == 0)
                        store.GlobalStateMachineStep(current, bumpEpoch: true);
                    break;

                case Phase.IN_PROGRESS_GROW:
                case Phase.REST:
                    return;
                default:
                    throw new TsavoriteException("Invalid Enum Argument");
            }
        }
    }

    /// <summary>
    /// Resizes the index
    /// </summary>
    internal sealed class IndexResizeStateMachine : SynchronizationStateMachineBase
    {
        /// <summary>
        /// Constructs a new IndexResizeStateMachine
        /// </summary>
        public IndexResizeStateMachine() : base(new IndexResizeTask()) { }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start)
        {
            var nextState = SystemState.Copy(ref start);
            switch (start.Phase)
            {
                case Phase.REST:
                    nextState.Phase = Phase.PREPARE_GROW;
                    break;
                case Phase.PREPARE_GROW:
                    nextState.Phase = Phase.IN_PROGRESS_GROW;
                    break;
                case Phase.IN_PROGRESS_GROW:
                    nextState.Phase = Phase.REST;
                    break;
                default:
                    throw new TsavoriteException("Invalid Enum Argument");
            }

            return nextState;
        }
    }
}