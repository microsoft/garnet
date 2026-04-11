// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly partial struct MainSessionFunctions : ISessionFunctions<StringInput, StringOutput, long>
    {
        const byte NeedAofLog = 0x1;
        readonly FunctionsState functionsState;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="functionsState"></param>
        internal MainSessionFunctions(FunctionsState functionsState)
        {
            this.functionsState = functionsState;
        }

        /// <inheritdoc />
        public void ConvertOutputToHeap(ref StringInput input, ref StringOutput output)
        {
            // TODO: Inspect input to determine whether we're in a context requiring ConvertToHeap.
            //output.ConvertToHeap();
        }
    }
}