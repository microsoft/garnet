// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedStoreInput, UnifiedStoreOutput, long>
    {
        public void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref UnifiedStoreInput input,
            ref UnifiedStoreOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        public void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref UnifiedStoreInput input,
            ref UnifiedStoreOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }
    }
}