// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Equality comparer for <see cref="ReadOnlySpan{_byte_}"/>
    /// </summary>
    public struct SpanByteComparer : IKeyComparer
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly SpanByteComparer Instance = new();

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe long GetHashCode64(ReadOnlySpan<byte> key) => StaticGetHashCode64(key);

        /// <summary>
        /// Get 64-bit hash code
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long StaticGetHashCode64(ReadOnlySpan<byte> key) => Utility.HashBytes(key);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => StaticEquals(k1, k2);

        /// <summary>
        /// Equality comparison
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe bool StaticEquals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => k1.SequenceEqual(k2);
    }
}