// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Resizes an index
    /// </summary>
    internal sealed class IndexResizeSMTask<TKey, TValue, TStoreFunctions, TAllocator> : IStateMachineTask
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store;

        public IndexResizeSMTask(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
            this.store = store;
        }

        /// <inheritdoc />
        public void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE_GROW:
                    break;

                case Phase.IN_PROGRESS_GROW:
                    // Wait for PREPARE_GROW threads to finish active transactions and enter barrier
                    while (store.hlogBase.NumActiveLockingSessions > 0)
                    {
                        _ = Thread.Yield();
                    }

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
        public void GlobalAfterEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.IN_PROGRESS_GROW:
                    store.SplitAllBuckets();
                    break;
            }
        }
    }
}