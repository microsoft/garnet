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
                    var numChunks = (int)(store.kernel.hashTable.spine.state[store.kernel.hashTable.spine.resizeInfo.version].size / Constants.kSizeofChunk);
                    if (numChunks == 0) numChunks = 1; // at least one chunk

                    store.kernel.hashTable.numPendingChunksToBeSplit = numChunks;
                    store.kernel.hashTable.splitStatus = new long[numChunks];
                    store.kernel.hashTable.overflowBucketsAllocatorResize = store.kernel.hashTable.overflowBucketsAllocator;
                    store.kernel.hashTable.overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>();

                    // Because version is 0 or 1, indexing by [1 - resizeInfo.version] references to the "new version".
                    // Once growth initialization is complete, the state versions are swapped by setting resizeInfo.version = 1 - resizeInfo.version.
                    // Initialize the new version to twice the size of the old version.
                    store.kernel.hashTable.Reinitialize(1 - store.kernel.hashTable.spine.resizeInfo.version, store.kernel.hashTable.spine.state[store.kernel.hashTable.spine.resizeInfo.version].size * 2, store.sectorSize);

                    store.kernel.hashTable.spine.resizeInfo.version = 1 - store.kernel.hashTable.spine.resizeInfo.version;
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
                    bool isProtected = store.kernel.epoch.ThisInstanceProtected();
                    if (!isProtected)
                        store.kernel.epoch.Resume();
                    try
                    {
                        store.kernel.epoch.BumpCurrentEpoch(() => allThreadsInPrepareGrow = true);
                    }
                    finally
                    {
                        if (!isProtected)
                            store.kernel.epoch.Suspend();
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
        public void OnThreadState<Input, Output, Context, TSessionFunctionsWrapper>(
            SystemState current,
            SystemState prev,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TSessionFunctionsWrapper sessionFunctions,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionEpochControl
        {
            switch (current.Phase)
            {
                case Phase.PREPARE_GROW:
                    // Using bumpEpoch: true allows us to guarantee that when system state proceeds, all threads in prior state
                    // will see that hlog.NumActiveLockingSessions == 0, ensuring that they can potentially block for the next state.
                    if (allThreadsInPrepareGrow && store.hlogBase.NumActiveLockingSessions == 0)
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
    internal sealed class IndexResizeStateMachine<Key, Value, TStoreFunctions, TAllocator> : SynchronizationStateMachineBase<Key, Value, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<Key, Value>
        where TAllocator : IAllocator<Key, Value, TStoreFunctions>
    {
        /// <summary>
        /// Constructs a new IndexResizeStateMachine
        /// </summary>
        public IndexResizeStateMachine() : base(new IndexResizeTask<Key, Value, TStoreFunctions, TAllocator>()) { }

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