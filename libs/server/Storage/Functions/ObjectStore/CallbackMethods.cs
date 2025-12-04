// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, ObjectOutput, long>
    {
        /// <inheritdoc />
        public void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref ObjectInput input, ref ObjectOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        /// <inheritdoc />
        public void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref ObjectInput input, ref ObjectOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }
    }
}