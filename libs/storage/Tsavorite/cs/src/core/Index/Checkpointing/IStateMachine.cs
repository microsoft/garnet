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
    }
}