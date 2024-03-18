// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Encapsulates custom logic to be executed as part of Tsavorite's state machine logic
    /// </summary>
    public interface IStateMachineCallback
    {
        /// <summary>
        /// Invoked immediately before every state transition.
        /// </summary>
        /// <param name="next"> next system state </param>
        /// <param name="tsavorite"> reference to Tsavorite KV </param>
        /// <typeparam name="Key">Key Type</typeparam>
        /// <typeparam name="Value">Value Type</typeparam>
        void BeforeEnteringState<Key, Value>(SystemState next, TsavoriteKV<Key, Value> tsavorite);
    }
}