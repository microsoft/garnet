// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public interface IStateMachineTask
    {
        public void GlobalBeforeEnteringState(SystemState nextState, StateMachineDriver stateMachineDriver);
        public void GlobalAfterEnteringState(SystemState nextState, StateMachineDriver stateMachineDriver);
    }
}
