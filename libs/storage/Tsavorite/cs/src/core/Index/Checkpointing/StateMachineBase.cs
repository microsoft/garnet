// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Abstract base class for IStateMachine that implements that state machine logic
    /// with IStateMachineTasks
    /// </summary>
    internal abstract class StateMachineBase : IStateMachine
    {
        readonly IStateMachineTask[] tasks;

        /// <summary>
        /// Construct a new SynchronizationStateMachine with the given tasks. The order of tasks given is the
        /// order they are executed on each state machine.
        /// </summary>
        /// <param name="tasks">The ISynchronizationTasks to run on the state machine</param>
        protected StateMachineBase(params IStateMachineTask[] tasks)
        {
            this.tasks = tasks;
        }

        /// <inheritdoc />
        public abstract SystemState NextState(SystemState start);

        /// <inheritdoc />
        public void GlobalBeforeEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            foreach (var task in tasks)
                task.GlobalBeforeEnteringState(next, stateMachineDriver);
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState(SystemState next, StateMachineDriver stateMachineDriver)
        {
            foreach (var task in tasks)
                task.GlobalAfterEnteringState(next, stateMachineDriver);
        }
    }
}