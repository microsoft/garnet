// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Resizes the index
    /// </summary>
    internal sealed class IndexResizeSM : StateMachineBase
    {
        /// <summary>
        /// Constructs a new IndexResizeStateMachine
        /// </summary>
        public IndexResizeSM(params IStateMachineTask[] tasks) : base(tasks)
        {
        }

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