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
        public readonly long GetHashCode64<TKey>(TKey key)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
             => StaticGetHashCode64(key.KeyBytes);

        /// <summary>
        /// Get 64-bit hash code
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long StaticGetHashCode64(ReadOnlySpan<byte> key) => Utility.HashBytes(key);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool Equals<TFirstKey, TSecondKey>(TFirstKey k1, TSecondKey k2)
            where TFirstKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSecondKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => StaticEquals(k1.KeyBytes, k2.KeyBytes);

        /// <summary>
        /// Equality comparison
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool StaticEquals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => k1.SequenceEqual(k2);
    }
}