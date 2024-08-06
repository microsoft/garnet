// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Encapsulates custom logic to be executed as part of Tsavorite's state machine logic
    /// </summary>
    public interface IStateMachineCallback<Key, Value, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<Key, Value>
        where TAllocator : IAllocator<Key, Value, TStoreFunctions>
    {
        /// <summary>
        /// Invoked immediately before every state transition.
        /// </summary>
        void BeforeEnteringState(SystemState next, TsavoriteKV<Key, Value, TStoreFunctions, TAllocator> tsavorite);
    }
}