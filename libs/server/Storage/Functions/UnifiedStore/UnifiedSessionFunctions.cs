// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    public readonly partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedInput, UnifiedOutput, long>
    {
        const byte NeedAofLog = 0x1;
        readonly FunctionsState functionsState;
        readonly ReadSessionState readSessionState;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="functionsState"></param>
        /// <param name="readSessionState"></param>
        internal UnifiedSessionFunctions(FunctionsState functionsState, ReadSessionState readSessionState = null)
        {
            this.functionsState = functionsState;
            this.readSessionState = readSessionState;
        }

        public void ConvertOutputToHeap(ref UnifiedInput input, ref UnifiedOutput output)
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
            where TBatch : IReadArgBatch<UnifiedInput, UnifiedOutput>
        {
            for(var i = 0; i < batch.Count; i++)
            {
                batch.GetKey(i, out var key);
            }
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public void AfterConsistentReadKeyBatchCallback()
        {
            throw new NotImplementedException();
        }
    }
}