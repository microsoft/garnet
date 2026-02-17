// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Flags for store outputs.
    /// </summary>
    [Flags]
    public enum ObjectOutputFlags : byte
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
        /// Wrong type of value
        /// </summary>
        WrongType = 1 << 1,

        /// <summary>
        /// Indicates that the value has not changed
        /// </summary>
        ValueUnchanged = 1 << 2,
    }

    /// <summary>
    /// Output type used by Garnet object store.
    /// Any field / property added to this struct must be set in the back-end (IFunctions) and used in the front-end (GarnetApi caller).
    /// That is in order to justify transferring data in this struct through the Tsavorite storage layer.
    /// </summary>
    public struct ObjectOutput
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
        /// Some result of operation (e.g., number of items added successfully)
        /// </summary>
        public int result1;

        /// <summary>
        /// The updated etag of the key operated on (if single key, not set: -1, no etag: 0)
        /// </summary>
        public long etag;

        /// <summary>
        /// Output flags
        /// </summary>
        public ObjectOutputFlags OutputFlags;

        /// <summary>
        /// True if output flag WrongType is set
        /// </summary>
        public readonly bool HasWrongType =>
            (OutputFlags & ObjectOutputFlags.WrongType) == ObjectOutputFlags.WrongType;

        /// <summary>
        /// True if output flag RemoveKey is set
        /// </summary>
        public readonly bool HasRemoveKey =>
            (OutputFlags & ObjectOutputFlags.RemoveKey) == ObjectOutputFlags.RemoveKey;

        public ObjectOutput() => SpanByteAndMemory = new(null);

        public ObjectOutput(SpanByteAndMemory span) => SpanByteAndMemory = span;

        public static unsafe ObjectOutput FromPinnedPointer(byte* pointer, int length)
            => new(SpanByteAndMemory.FromPinnedPointer(pointer, length));

        public void ConvertToHeap()
        {
            // Does not convert to heap when going pending, because we complete all pending operations before releasing the pinned source bytes.
        }

        public void Dispose()
        {
            SpanByteAndMemory.Dispose();
        }
    }
}