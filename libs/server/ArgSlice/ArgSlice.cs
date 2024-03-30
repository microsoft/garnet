// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Slice of Key
    /// </summary>
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
        public readonly ReadOnlySpan<byte> ReadOnlySpan => new(ptr, Length);

        /// <summary>
        /// Get slice as Span
        /// </summary>
        public readonly Span<byte> Span => new(ptr, Length);

        /// <summary>
        /// Get slice as SpanByte
        /// </summary>
        public readonly SpanByte SpanByte => SpanByte.FromPointer(ptr, Length);

        /// <summary>
        /// Get slice as byte array
        /// </summary>
        public readonly byte[] ToArray() => ReadOnlySpan.ToArray();

        /// <summary>
        /// Interpret ArgSlice as a long number expressed in (decimal) digits
        /// </summary>
        public readonly long AsLongDigits => NumUtils.BytesToLong(Length, ptr);

        /// <inheritdoc />
        public override readonly string ToString()
            => Encoding.ASCII.GetString(ReadOnlySpan);
    }
}