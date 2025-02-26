// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// The state machine orchestrates a full checkpoint
    /// </summary>
    internal sealed class FullCheckpointSM : HybridLogCheckpointSM
    {
        /// <summary>
        /// Construct a new FullCheckpointStateMachine to use the given set of checkpoint tasks,
        /// drawing boundary at targetVersion.
        /// </summary>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        /// <param name="tasks">Tasks</param>
        public FullCheckpointSM(long targetVersion = -1, params IStateMachineTask[] tasks)
            : base(targetVersion, tasks)
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