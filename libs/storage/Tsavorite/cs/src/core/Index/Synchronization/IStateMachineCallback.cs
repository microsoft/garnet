// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Encapsulates custom logic to be executed as part of Tsavorite's state machine logic
    /// </summary>
    public interface IStateMachineCallback<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// Invoked immediately before every state transition.
        /// </summary>
        void BeforeEnteringState(SystemState next, TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> tsavorite);
    }
}