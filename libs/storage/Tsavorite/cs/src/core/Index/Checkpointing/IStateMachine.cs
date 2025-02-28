// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public interface IStateMachine : IStateMachineTask
    {
        public SystemState NextState(SystemState currentState);
    }
}