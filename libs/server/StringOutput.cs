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
        /// Output flags
        /// </summary>
        public StringOutputFlags OutputFlags;

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

    /// <summary>
    /// Output flags for <see cref="StringOutput"/>."/>
    /// </summary>
    [Flags]
    public enum StringOutputFlags : byte
    {
        // Non-error flags
        None = 0,

        // Error marker
        Error = 1 << 7,

        // Error flags (Error bit always set)
        InvalidTypeError = Error | (1 << 0),
        NaNOrInfinityError = Error | (1 << 1),
    }

    public static class StringOutputExtensions
    {
        /// <summary>
        /// Check if <see cref="StringOutputFlags"/> has error bit set.
        /// </summary>
        /// <param name="flags">Output flags</param>
        /// <returns>True if <see cref="StringOutputFlags"/> has error bit set.</returns>
        public static bool HasError(this StringOutputFlags flags)
            => (flags & StringOutputFlags.Error) != 0;
    }
}