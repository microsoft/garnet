// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Encapsulates custom logic to be executed as part of Tsavorite's state machine logic
    /// </summary>
    public interface IStateMachineCallback<TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        /// <summary>
        /// Invoked immediately before every state transition.
        /// </summary>
        void BeforeEnteringState(SystemState next, TsavoriteKV<TValue, TStoreFunctions, TAllocator> store);
    }
}