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
    internal sealed class IndexResizeTask<TKey, TValue, TStoreFunctions, TAllocator> : ISynchronizationTask<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        bool allThreadsInPrepareGrow;

        /// <inheritdoc />
        public void GlobalBeforeEnteringState(
            SystemState next,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE_GROW:
                    allThreadsInPrepareGrow = false;
                    break;
                case Phase.IN_PROGRESS_GROW:
                    // Set up the transition to new version of HT
                    var numChunks = (int)(store.Kernel.hashTable.spine.state[store.Kernel.hashTable.spine.resizeInfo.version].size / Constants.kSizeofChunk);
                    if (numChunks == 0) numChunks = 1; // at least one chunk

                    store.Kernel.hashTable.numPendingChunksToBeSplit = numChunks;
                    store.Kernel.hashTable.splitStatus = new long[numChunks];
                    store.Kernel.hashTable.overflowBucketsAllocatorResize = store.Kernel.hashTable.overflowBucketsAllocator;
                    store.Kernel.hashTable.overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>();

                    // Because version is 0 or 1, indexing by [1 - resizeInfo.version] references to the "new version".
                    // Once growth initialization is complete, the state versions are swapped by setting resizeInfo.version = 1 - resizeInfo.version.
                    // Initialize the new version to twice the size of the old version.
                    store.Kernel.hashTable.Reinitialize(1 - store.Kernel.hashTable.spine.resizeInfo.version, store.Kernel.hashTable.spine.state[store.Kernel.hashTable.spine.resizeInfo.version].size * 2, store.sectorSize, store.Kernel.Logger);

                    store.Kernel.hashTable.spine.resizeInfo.version = 1 - store.Kernel.hashTable.spine.resizeInfo.version;
                    break;
                case Phase.REST:
                    // nothing to do
                    break;
                default:
                    throw new TsavoriteException("Invalid Enum Argument");
            }
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState(
            SystemState next,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE_GROW:
                    bool isProtected = store.Kernel.Epoch.ThisInstanceProtected();
                    if (!isProtected)
                        store.Kernel.Epoch.Resume();
                    try
                    {
                        store.Kernel.Epoch.BumpCurrentEpoch(() => allThreadsInPrepareGrow = true);
                    }
                    finally
                    {
                        if (!isProtected)
                            store.Kernel.Epoch.Suspend();
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
        public void OnThreadState<TInput, TOutput, TContext>(
            SystemState current,
            SystemState prev,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.ExecutionContext<TInput, TOutput, TContext> ctx,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
        {
            switch (current.Phase)
            {
                case Phase.PREPARE_GROW:
                    // Using bumpEpoch: true allows us to guarantee that when system state proceeds, all threads in prior state
                    // will see that hlog.NumActiveLockingSessions == 0, ensuring that they can potentially block for the next state.
                    if (allThreadsInPrepareGrow && store.hlogBase.NumActiveTxnSessions == 0)
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
    internal sealed class IndexResizeStateMachine<TKey, TValue, TStoreFunctions, TAllocator> : SynchronizationStateMachineBase<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// Constructs a new IndexResizeStateMachine
        /// </summary>
        public IndexResizeStateMachine() : base(new IndexResizeTask<TKey, TValue, TStoreFunctions, TAllocator>()) { }

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