// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// This state machine performs an index checkpoint
    /// </summary>
    internal sealed class IndexSnapshotSM<TKey, TValue, TStoreFunctions, TAllocator> : StateMachineBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// Create a new IndexSnapshotStateMachine
        /// </summary>
        public IndexSnapshotSM(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
            : base(-1, new IndexSnapshotSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store))
        {
        }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start, out bool barrier)
        {
            barrier = false;
            var result = SystemState.Copy(ref start);
            switch (start.Phase)
            {
                case Phase.REST:
                    result.Phase = Phase.PREP_INDEX_CHECKPOINT;
                    break;
                case Phase.PREP_INDEX_CHECKPOINT:
                    result.Phase = Phase.WAIT_INDEX_ONLY_CHECKPOINT;
                    break;
                case Phase.WAIT_INDEX_ONLY_CHECKPOINT:
                    result.Phase = Phase.REST;
                    break;
                default:
                    throw new TsavoriteException("Invalid Enum Argument");
            }
            return result;
        }
    }
}