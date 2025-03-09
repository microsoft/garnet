// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// StreamingSnapshot checkpoint state machine.
    /// </summary>
    class StreamingSnapshotCheckpointSM : VersionChangeSM
    {
        /// <summary>
        /// Construct a new StreamingSnapshotCheckpointStateMachine.
        /// </summary>
        public StreamingSnapshotCheckpointSM(params IStateMachineTask[] tasks)
            : base(tasks)
        { }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start)
        {
            var result = SystemState.Copy(ref start);
            switch (start.Phase)
            {
                case Phase.REST:
                    result.Phase = Phase.PREPARE;
                    break;
                case Phase.IN_PROGRESS:
                    result.Phase = Phase.WAIT_FLUSH;
                    break;
                case Phase.WAIT_FLUSH:
                    result.Phase = Phase.REST;
                    break;
                default:
                    result = base.NextState(start);
                    break;
            }

            return result;
        }
    }
}