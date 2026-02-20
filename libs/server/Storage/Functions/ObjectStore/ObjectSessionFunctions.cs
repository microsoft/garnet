// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, ObjectOutput, long>
    {
        const byte NeedAofLog = 0x1;
        readonly FunctionsState functionsState;
        readonly ReadSessionState readSessionState;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="functionsState"></param>
        /// <param name="readSessionState"></param>
        internal ObjectSessionFunctions(FunctionsState functionsState, ReadSessionState readSessionState = null)
        {
            this.functionsState = functionsState;
            this.readSessionState = readSessionState;
        }

        /// <inheritdoc />
        public void ConvertOutputToHeap(ref ObjectInput input, ref ObjectOutput output)
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
            where TBatch : IReadArgBatch<ObjectInput, ObjectOutput>
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