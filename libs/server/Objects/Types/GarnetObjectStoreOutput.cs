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
    /// Any field / property added to this struct must be set in the back-end (IFunctions) and used in the front-end (GarnetApi caller).
    /// That is in order to justify transferring data in this struct through the Tsavorite storage layer.
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
        /// Object header
        /// </summary>
        public ObjectOutputHeader Header;

        /// <summary>
        /// Output flags
        /// </summary>
        public ObjectStoreOutputFlags OutputFlags;

        /// <summary>
        /// True if output flag WrongType is set
        /// </summary>
        public bool HasWrongType => (OutputFlags & ObjectStoreOutputFlags.WrongType) == ObjectStoreOutputFlags.WrongType;

        /// <summary>
        /// True if output flag RemoveKey is set
        /// </summary>
        public bool HasRemoveKey => (OutputFlags & ObjectStoreOutputFlags.RemoveKey) == ObjectStoreOutputFlags.RemoveKey;

        public GarnetObjectStoreOutput()
        {
            SpanByteAndMemory = new(null);
        }

        public GarnetObjectStoreOutput(SpanByteAndMemory spam)
        {
            SpanByteAndMemory = spam;
        }

        public void ConvertToHeap()
        {
            // Does not convert to heap when going pending, because we immediately complete pending operations for object store.
        }
    }
}