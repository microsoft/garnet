// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Output type used by Garnet main store.
    /// Any field / property added to this struct must be set in the back-end (IFunctions) and used in the front-end (GarnetApi caller).
    /// That is in order to justify transferring data in this struct through the Tsavorite storage layer.
    /// </summary>
    public struct StringOutput
    {
        /// <summary>
        /// Span byte and memory
        /// </summary>
        public SpanByteAndMemory SpanByteAndMemory;

        /// <summary>
        /// Output header
        /// </summary>
        public OutputHeader Header;

        /// <summary>
        /// Output flags
        /// </summary>
        public OutputFlags OutputFlags;

        public StringOutput() => SpanByteAndMemory = new(null);

        public StringOutput(SpanByteAndMemory span) => SpanByteAndMemory = span;

        public static unsafe StringOutput FromPinnedPointer(byte* pointer, int length)
            => new(SpanByteAndMemory.FromPinnedPointer(pointer, length));

        public static unsafe StringOutput FromPinnedSpan(ReadOnlySpan<byte> span)
            => new(SpanByteAndMemory.FromPinnedSpan(span));

        public void ConvertToHeap()
        {
            // Does not convert to heap when going pending, because we immediately complete pending operations for main store.
        }

        public void Dispose()
        {
            SpanByteAndMemory.Dispose();
        }
    }
}