﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Represents contiguous region of arbitrary _pinned_ memory.
    /// </summary>
    /// <remarks>
    /// SAFETY: This type is used to represent arguments that are assumed to point to pinned memory.
    /// </remarks>
    [StructLayout(LayoutKind.Explicit, Size = 12)]
    public unsafe struct ArgSlice
    {
        [FieldOffset(0)]
        internal byte* ptr;

        [FieldOffset(8)]
        internal int length;

        /// <summary>
        /// Create new ArgSlice from given pointer and length
        /// </summary>
        public ArgSlice(byte* ptr, int length)
        {
            this.ptr = ptr;
            this.length = length;
        }

        /// <summary>
        /// Create new ArgSlice from given SpanByte (without metadata header)
        /// </summary>
        internal ArgSlice(ref SpanByte input)
        {
            this.ptr = input.ToPointer();
            this.length = input.LengthWithoutMetadata;
        }

        /// <summary>
        /// Get length of ArgSlice
        /// </summary>
        public readonly int Length => length;

        /// <summary>
        /// Get slice as ReadOnlySpan
        /// </summary>
        public readonly ReadOnlySpan<byte> ReadOnlySpan => new(ptr, length);

        /// <summary>
        /// Get slice as Span
        /// </summary>
        public readonly Span<byte> Span => new(ptr, length);

        /// <summary>
        /// Get slice as SpanByte
        /// </summary>
        public readonly SpanByte SpanByte => SpanByte.FromPinnedPointer(ptr, length);

        /// <summary>
        /// Copies the contents of this slice into a new array.
        /// </summary>
        public readonly byte[] ToArray() => ReadOnlySpan.ToArray();

        /// <summary>
        /// Decodes the contents of this slice as ASCII into a new string.
        /// </summary>
        /// <returns>A string ASCII decoded string from the slice.</returns>
        public override readonly string ToString()
            => Encoding.ASCII.GetString(ReadOnlySpan);

        /// <summary>
        /// Create a <see cref="ArgSlice"/> from the given <paramref name="span"/>.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="span"/> MUST point to pinned memory.
        /// </remarks>
        internal static ArgSlice FromPinnedSpan(ReadOnlySpan<byte> span)
        {
            return new ArgSlice((byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(span)), span.Length);
        }
    }
}