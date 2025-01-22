// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Flags for object store outputs.
    /// </summary>
    [Flags]
    public enum ObjectStoreOutputFlags : byte
    {
        /// <summary>
        /// No flags set
        /// </summary>
        None = 0,

        /// <summary>
        /// Remove key
        /// </summary>
        RemoveKey = 1,

        /// <summary>
        /// Wrong type of object
        /// </summary>
        WrongType = 1 << 1,
    }

    /// <summary>
    /// Output type used by Garnet object store.
    /// </summary>
    public struct GarnetObjectStoreOutput
    {
        /// <summary>
        /// Span byte and memory
        /// </summary>
        public SpanByteAndMemory SpanByteAndMemory;

        /// <summary>
        /// Garnet object
        /// </summary>
        public IGarnetObject GarnetObject;

        /// <summary>
        /// Output flags
        /// </summary>
        public ObjectStoreOutputFlags OutputFlags;

        public void ConvertToHeap()
        {
            // Does not convert to heap when going pending, because we immediately complete pending operations for object store.
        }
    }
}