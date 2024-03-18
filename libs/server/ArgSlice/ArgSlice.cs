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
        public int Length => length;

        /// <summary>
        /// Get slice as ReadOnlySpan
        /// </summary>
        public ReadOnlySpan<byte> ReadOnlySpan => new(ptr, length);

        /// <summary>
        /// Get slice as Span
        /// </summary>
        public Span<byte> Span => new(ptr, length);

        /// <summary>
        /// Get slice as SpanByte
        /// </summary>
        public SpanByte SpanByte => SpanByte.FromPointer(ptr, length);

        /// <summary>
        /// Get slice as byte array
        /// </summary>
        public byte[] Bytes => ReadOnlySpan.ToArray();

        /// <summary>
        /// Interpret ArgSlice as a long number expressed in (decimal) digits
        /// </summary>
        public long AsLongDigits => NumUtils.BytesToLong(length, ptr);

        /// <inheritdoc />
        public override string ToString()
            => Encoding.ASCII.GetString(ReadOnlySpan);
    }
}