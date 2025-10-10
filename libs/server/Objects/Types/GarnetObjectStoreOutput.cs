﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
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
        /// Output header
        /// </summary>
        public OutputHeader Header;

        /// <summary>
        /// Output flags
        /// </summary>
        public OutputFlags OutputFlags;

        /// <summary>
        /// True if output flag WrongType is set
        /// </summary>
        public readonly bool HasWrongType =>
            (OutputFlags & OutputFlags.WrongType) == OutputFlags.WrongType;

        /// <summary>
        /// True if output flag RemoveKey is set
        /// </summary>
        public readonly bool HasRemoveKey =>
            (OutputFlags & OutputFlags.RemoveKey) == OutputFlags.RemoveKey;

        /// <summary>
        /// True if output flag ValueUpdated is set
        /// </summary>
        public readonly bool HasValueUpdated =>
            (OutputFlags & OutputFlags.ValueUpdated) == OutputFlags.ValueUpdated;

        public GarnetObjectStoreOutput() => SpanByteAndMemory = new(null);

        public GarnetObjectStoreOutput(SpanByteAndMemory span) => SpanByteAndMemory = span;

        public static unsafe GarnetObjectStoreOutput FromPinnedPointer(byte* pointer, int length)
            => new(new SpanByteAndMemory() { SpanByte = PinnedSpanByte.FromPinnedPointer(pointer, length) });

        public void ConvertToHeap()
        {
            // Does not convert to heap when going pending, because we immediately complete pending operations for object store.
        }
    }
}