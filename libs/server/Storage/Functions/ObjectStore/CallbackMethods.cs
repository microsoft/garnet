// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectStoreFunctions : IFunctions<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public void ReadCompletionCallback(ref byte[] key, ref SpanByte input, ref GarnetObjectStoreOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        /// <inheritdoc />
        public void RMWCompletionCallback(ref byte[] key, ref SpanByte input, ref GarnetObjectStoreOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        /// <inheritdoc />
        public void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
        {
        }
    }
}