// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// A VersionChangeStateMachine orchestrates to capture a version, but does not flush to disk.
    /// </summary>
    internal class VersionChangeSM : StateMachineBase
    {
        /// <summary>
        /// Construct a new VersionChangeStateMachine with the given tasks. Does not load any tasks by default.
        /// </summary>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        /// <param name="tasks">The tasks to load onto the state machine</param>
        protected VersionChangeSM(long targetVersion = -1, params IStateMachineTask[] tasks) : base(targetVersion, tasks)
        {
        }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start, out bool barrier)
        {
            barrier = false;
            var nextState = SystemState.Copy(ref start);
            switch (start.Phase)
            {
                case Phase.REST:
                    nextState.Phase = Phase.PREPARE;
                    break;
                case Phase.PREPARE:
                    barrier = true;
                    nextState.Phase = Phase.IN_PROGRESS;
                    if (toVersion == -1) toVersion = start.Version + 1;
                    nextState.Version = toVersion;
                    break;
                case Phase.IN_PROGRESS:
                    nextState.Phase = Phase.REST;
                    break;
                default:
                    throw new TsavoriteException("Invalid Enum Argument");
            }
            return nextState;
        }
    }
}