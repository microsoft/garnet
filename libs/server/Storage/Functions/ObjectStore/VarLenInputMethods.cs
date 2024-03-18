// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectStoreFunctions : IFunctions<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc/>
        public int GetRMWModifiedValueLength(ref IGarnetObject value, ref SpanByte input)
        {
            throw new GarnetException("GetRMWModifiedValueLength is not available on the object store");
        }

        /// <inheritdoc/>
        public int GetRMWInitialValueLength(ref SpanByte input)
        {
            throw new GarnetException("GetRMWInitialValueLength is not available on the object store");
        }
    }
}