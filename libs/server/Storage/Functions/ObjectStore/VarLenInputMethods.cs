// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc/>
        public int GetRMWModifiedValueLength(ref IGarnetObject value, ref ObjectInput input, bool hasEtag)
        {
            throw new GarnetException("GetRMWModifiedValueLength is not available on the object store");
        }

        /// <inheritdoc/>
        public int GetRMWInitialValueLength(ref ObjectInput input)
        {
            throw new GarnetException("GetRMWInitialValueLength is not available on the object store");
        }
    }
}