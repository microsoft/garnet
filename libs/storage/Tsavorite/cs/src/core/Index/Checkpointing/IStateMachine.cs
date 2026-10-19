// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// State machine API
    /// </summary>
    public interface IStateMachine : IStateMachineTask
    {
        /// <summary>
        /// Returns the next state given the current state
        /// </summary>
        /// <param name="currentState">Current state</param>
        /// <returns>Next state</returns>
        public SystemState NextState(SystemState currentState);

        /// <summary>
        /// Whether a step may block the thread it runs on, for example in callbacks that wait for a consumer to take what
        /// they produce. The driver runs each step of such a state machine on a dedicated thread and awaits it, so the
        /// step holds no thread-pool thread.
        /// </summary>
        public bool StepsMayBlock => false;
    }
}