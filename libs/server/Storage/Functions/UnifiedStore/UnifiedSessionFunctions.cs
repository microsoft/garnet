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
        public void BeforeConsistentReadCallback(long hash)
            => readSessionState?.BeforeConsistentReadKeyCallback(hash);

        /// <inheritdoc />
        public void AfterConsistentReadKeyCallback()
            => readSessionState?.AfterConsistentReadKeyCallback();

        /// <inheritdoc />
        public void BeforeConsistentReadKeyBatchCallback(ReadOnlySpan<PinnedSpanByte> parameters)
            => readSessionState?.BeforeConsistentReadKeyBatch(parameters);

        /// <inheritdoc />
        public bool AfterConsistentReadKeyBatchCallback(int keyCount)
            => readSessionState != null && readSessionState.AfterConsistentReadKeyBatch(keyCount);
    }
}