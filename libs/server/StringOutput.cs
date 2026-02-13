// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
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

        public readonly bool HasError => (OutputFlags & StringOutputFlags.Error) != 0;

        public StringOutput() => SpanByteAndMemory = new(null);

        public StringOutput(SpanByteAndMemory span) => SpanByteAndMemory = span;

        public static unsafe StringOutput FromPinnedPointer(byte* pointer, int length)
            => new(SpanByteAndMemory.FromPinnedPointer(pointer, length));

        public static StringOutput FromPinnedSpan(ReadOnlySpan<byte> span)
            => new(SpanByteAndMemory.FromPinnedSpan(span));

        /// <summary>
        /// Reinterprets the output's underlying <see cref="SpanByteAndMemory.SpanByte"/> as a reference to an unmanaged value of type <typeparamref name="T"/>.
        /// The span length must exactly match the size of <typeparamref name="T"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref T AsRef<T>() where T : unmanaged
            => ref SpanByteAndMemory.SpanByte.AsRef<T>();

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
}