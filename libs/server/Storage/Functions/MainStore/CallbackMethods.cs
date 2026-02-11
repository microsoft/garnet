// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly partial struct MainSessionFunctions : ISessionFunctions<StringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref StringInput input, ref StringOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        /// <inheritdoc />
        public void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref StringInput input, ref StringOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }
    }
}