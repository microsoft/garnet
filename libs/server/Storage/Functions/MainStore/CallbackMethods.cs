// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public void ReadCompletionCallback(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        /// <inheritdoc />
        public void RMWCompletionCallback(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }
    }
}