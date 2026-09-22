// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for tasks that are executed as part of the state machine
    /// </summary>
    public interface IStateMachineTask
    {
        /// <summary>
        /// Called before we move to nextState. All participant threads will be in previousState.
        /// </summary>
        /// <param name="nextState"></param>
        /// <param name="stateMachineDriver"></param>
        public void GlobalBeforeEnteringState(SystemState nextState, StateMachineDriver stateMachineDriver);

        /// <summary>
        /// Called after we move to nextState. All participant threads will be in nextState.
        /// </summary>
        /// <param name="nextState"></param>
        /// <param name="stateMachineDriver"></param>
        public void GlobalAfterEnteringState(SystemState nextState, StateMachineDriver stateMachineDriver);

        /// <summary>
        /// Called when the state machine aborts before reaching <see cref="Phase.REST"/>, so that the task can
        /// release whatever it set up in the phases it did enter.
        /// </summary>
        /// <param name="stateMachineDriver"></param>
        /// <param name="exception">The exception that aborted the state machine.</param>
        /// <remarks>
        /// The REST phase is where a task normally releases its resources and completes whatever the rest of the
        /// system is waiting on, and an aborted state machine never enters it. A task that leaves state behind here
        /// makes every subsequent run of the same state machine fail, and a task that leaves a waiter uncompleted
        /// hangs it, so the failure of one operation becomes permanent. Defaults to doing nothing for tasks that hold
        /// no such state.
        /// </remarks>
        public void OnAbort(StateMachineDriver stateMachineDriver, Exception exception) { }
    }
}