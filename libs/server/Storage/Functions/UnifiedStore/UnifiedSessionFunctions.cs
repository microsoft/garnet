// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedInput, UnifiedOutput, long>
    {
        readonly FunctionsState functionsState;

        /// <summary>
        /// Constructor
        /// </summary>
        internal UnifiedSessionFunctions(FunctionsState functionsState)
        {
            this.functionsState = functionsState;
        }

        public void ConvertOutputToHeap(ref UnifiedInput input, ref UnifiedOutput output)
        {
            // TODO: Inspect input to determine whether we're in a context requiring ConvertToHeap.
            //output.ConvertToHeap();
        }

        /// <inheritdoc />
        public ConsistentReadContextCallbacks GetContextCallbacks()
            => functionsState.consistentReadContextCallbacks;
    }
}