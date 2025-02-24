// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// The state machine orchestrates a full checkpoint
    /// </summary>
    internal sealed class FullCheckpointSM<TKey, TValue, TStoreFunctions, TAllocator> : HybridLogCheckpointSM
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// Construct a new FullCheckpointStateMachine to use the given checkpoint backend (either fold-over or snapshot),
        /// drawing boundary at targetVersion.
        /// </summary>
        /// <param name="checkpointBackend">A task that encapsulates the logic to persist the checkpoint</param>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        public FullCheckpointSM(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, IStateMachineTask checkpointBackend, long targetVersion = -1) : base(
            targetVersion,
            new FullCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store),
            new IndexSnapshotSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store),
            checkpointBackend)
        { }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start)
        {
            var result = SystemState.Copy(ref start);
            switch (start.Phase)
            {
                case Phase.REST:
                    result.Phase = Phase.PREP_INDEX_CHECKPOINT;
                    break;
                case Phase.PREP_INDEX_CHECKPOINT:
                    result.Phase = Phase.PREPARE;
                    break;
                case Phase.IN_PROGRESS:
                    result.Phase = Phase.WAIT_INDEX_CHECKPOINT;
                    break;
                case Phase.WAIT_INDEX_CHECKPOINT:
                    result.Phase = Phase.WAIT_FLUSH;
                    break;
                default:
                    result = base.NextState(start);
                    break;
            }

            return result;
        }
    }
}