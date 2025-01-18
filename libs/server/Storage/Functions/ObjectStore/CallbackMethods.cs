﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public void ReadCompletionCallback(ref LogRecord<IGarnetObject> logRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        /// <inheritdoc />
        public void RMWCompletionCallback(ref LogRecord<IGarnetObject> logRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }
    }
}