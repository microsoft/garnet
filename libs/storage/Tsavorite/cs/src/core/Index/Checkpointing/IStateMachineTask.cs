// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public interface IStateMachineTask
    {
        /// <summary>
        /// Called before we move to nextState. All participant threads will be in previousState.
        /// </summary>
        /// <param name="nextState"></param>
        /// <param name="stateMachineDriver"></param>
        public void GlobalBeforeEnteringState(SystemState nextState, StateMachineDriver stateMachineDriver);

        /// <summary>
        /// Called after we move to nextState. Participant threads may be in previousState or nextState.
        /// </summary>
        /// <param name="nextState"></param>
        /// <param name="stateMachineDriver"></param>
        public void GlobalAfterEnteringState(SystemState nextState, StateMachineDriver stateMachineDriver);
    }
}