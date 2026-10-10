// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

namespace Tsavorite.core
{
    /// <summary>
    /// Resizes an index
    /// </summary>
    internal sealed class IndexResizeSMTask<TStoreFunctions, TAllocator> : IStateMachineTask
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        readonly TsavoriteKV<TStoreFunctions, TAllocator> store;
        long lastVersion;

        public IndexResizeSMTask(TsavoriteKV<TStoreFunctions, TAllocator> store)
        {
            this.store = store;
        }

        /// <inheritdoc />
        public void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE_GROW:
                    lastVersion = next.Version;
                    break;

                case Phase.IN_PROGRESS_GROW:
                    // Verify full transaction barrier
                    Debug.Assert(stateMachineDriver.GetNumActiveTransactions(lastVersion) == 0);
                    Debug.Assert(stateMachineDriver.GetNumActiveTransactions(next.Version) == 0);
                    stateMachineDriver.ResetLastVersion();

                    // Set up the transition to new version of HT
                    var oldSize = store.state[store.resizeInfo.version].size;

                    // Build the new generation before mutating any resize state, so that a failure to size it refuses
                    // the grow rather than leaving it half-applied. Its ceiling is twice the old generation's, keeping
                    // the allowed chain length constant as the index grows, and is floored at what the split can need.
                    // That capacity is then reserved, so writes arriving during the grow cannot consume what the split
                    // was sized for; SplitAllBuckets releases the reservation once every chunk has been split.
                    var splitRequirement = store.GetSplitOverflowBucketRequirement();
                    var newOverflowBucketsAllocator = store.NewOverflowBucketsAllocator(oldSize * 2, splitRequirement);
                    newOverflowBucketsAllocator.Reserve(splitRequirement);

                    var numChunks = (int)(oldSize / Constants.kSizeofChunk);
                    if (numChunks == 0) numChunks = 1; // at least one chunk

                    store.numPendingChunksToBeSplit = numChunks;
                    store.splitStatus = new long[numChunks];
                    store.overflowBucketsAllocatorResize = store.overflowBucketsAllocator;
                    store.overflowBucketsAllocator = newOverflowBucketsAllocator;

                    // Because version is 0 or 1, indexing by [1 - resizeInfo.version] references to the "new version".
                    // Once growth initialization is complete, the state versions are swapped by setting resizeInfo.version = 1 - resizeInfo.version.
                    // Initialize the new version to twice the size of the old version.
                    store.Initialize(1 - store.resizeInfo.version, oldSize * 2, store.sectorSize);

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
                case Phase.PREPARE_GROW:
                    // State machine should wait for active transactions in the last version to complete (drain out).
                    // Note that we DO NOT allow new transactions to start in PREPARE_GROW (i.e., this is a full barrier)
                    stateMachineDriver.TrackLastVersion(lastVersion);
                    break;

                case Phase.IN_PROGRESS_GROW:
                    store.SplitAllBuckets();
                    break;
            }
        }
    }
}