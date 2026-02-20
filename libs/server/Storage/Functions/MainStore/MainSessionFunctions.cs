// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        readonly ReadSessionState readSessionState;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="functionsState"></param>
        /// <param name="readSessionState"></param>
        internal MainSessionFunctions(FunctionsState functionsState, ReadSessionState readSessionState = null)
        {
            this.functionsState = functionsState;
            this.readSessionState = readSessionState;
        }

        /// <inheritdoc />
        public void ConvertOutputToHeap(ref StringInput input, ref StringOutput output)
        {
            // TODO: Inspect input to determine whether we're in a context requiring ConvertToHeap.
            //output.ConvertToHeap();
        }

        /// <inheritdoc />
        public void BeforeConsistentReadCallback(PinnedSpanByte key)
            => readSessionState.BeforeConsistentReadKeyCallback(key);

        /// <inheritdoc />
        public void AfterConsistentReadKeyCallback()
            => readSessionState.AfterConsistentReadKeyCallback();

        /// <inheritdoc />
        public void BeforeConsistentReadKeyBatchCallback<TBatch>(ref TBatch batch)
            where TBatch : IReadArgBatch<StringInput, StringOutput>
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public void AfterConsistentReadKeyBatchCallback()
        {
            throw new NotImplementedException();
        }
    }
}