// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Output type used by Garnet object store.
    /// Does not convert to heap when going pending, because we immediately complete pending operations for object store.
    /// </summary>
    public struct GarnetObjectStoreOutput
    {
        /// <summary>
        /// span byte and memory
        /// </summary>
        public SpanByteAndMemory spanByteAndMemory;

        /// <summary>
        /// Garnet object
        /// </summary>
        public IGarnetObject garnetObject;
    }
}