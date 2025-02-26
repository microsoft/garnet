// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// This state machine performs an index checkpoint
    /// </summary>
    internal sealed class IndexCheckpointSM : StateMachineBase
    {
        /// <summary>
        /// Create a new IndexSnapshotStateMachine
        /// </summary>
        public IndexCheckpointSM(long targetVersion = -1, params IStateMachineTask[] tasks)
            : base(targetVersion, tasks)
        {
        }

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