﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainStoreFunctions : ISessionFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
    {
        readonly FunctionsState functionsState;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="functionsState"></param>
        internal MainStoreFunctions(FunctionsState functionsState)
        {
            this.functionsState = functionsState;
        }

        /// <inheritdoc />
        public void ConvertOutputToHeap(ref SpanByte input, ref SpanByteAndMemory output)
        {
            // TODO: Inspect input to determine whether we're in a context requiring ConvertToHeap.
            //output.ConvertToHeap();
        }
    }
}