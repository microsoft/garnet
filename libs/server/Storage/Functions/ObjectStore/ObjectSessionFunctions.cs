// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, GarnetObjectStoreOutput, long>
    {
        readonly FunctionsState functionsState;

        /// <summary>
        /// Constructor
        /// </summary>
        internal ObjectSessionFunctions(FunctionsState functionsState)
        {
            this.functionsState = functionsState;
        }

        /// <inheritdoc />
        public void ConvertOutputToHeap(ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            // TODO: Inspect input to determine whether we're in a context requiring ConvertToHeap.
            //output.ConvertToHeap();
        }

        /// <inheritdoc />
        public ConsistentReadContextCallbacks GetContextCallbacks()
            => functionsState.consistentReadContextCallbacks;
    }
}