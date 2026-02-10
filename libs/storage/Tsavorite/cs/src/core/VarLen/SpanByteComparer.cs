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
        public readonly long GetHashCode64(ReadOnlySpan<byte> key) => StaticGetHashCode64(key);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetHashCode64(ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes) => StaticGetHashCode64(key, namespaceBytes);

        /// <summary>
        /// Get 64-bit hash code
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long StaticGetHashCode64(ReadOnlySpan<byte> key) => Utility.HashBytes(key);

        /// <summary>
        /// Get 64-bit hash code
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long StaticGetHashCode64(ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes) => Utility.HashBytes(key, namespaceBytes);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => StaticEquals(k1, k2);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> ns1, ReadOnlySpan<byte> k2, ReadOnlySpan<byte> ns2) => StaticEquals(k1, ns1, k2, ns2);

        /// <summary>
        /// Equality comparison
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool StaticEquals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => k1.SequenceEqual(k2);

        /// <summary>
        /// Equality comparison
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool StaticEquals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> ns1, ReadOnlySpan<byte> k2, ReadOnlySpan<byte> ns2) 
            => ns1.SequenceEqual(ns2) && k1.SequenceEqual(k2);
    }
}