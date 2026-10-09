// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Garnet.common
{
    /// <summary>
    /// <see cref="IKeyComparer"/> which is aware of the different key types present in Garnet and special cases accordingly.
    /// </summary>
    public readonly struct GarnetKeyComparer : IKeyComparer
    {
        public static readonly GarnetKeyComparer Instance = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool Equals<TFirstKey, TSecondKey>(TFirstKey k1, TSecondKey k2)
            where TFirstKey : IKey
                , allows ref struct
            where TSecondKey : IKey
                , allows ref struct
            => StaticEquals(k1, k2);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetHashCode64<TKey>(TKey key)
            where TKey : IKey
                , allows ref struct
        => StaticGetHashCode64(key);

        /// <summary>
        /// Equality comparison
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool StaticEquals<TFirstKey, TSecondKey>(TFirstKey k1, TSecondKey k2)
            where TFirstKey : IKey
                , allows ref struct
            where TSecondKey : IKey
                , allows ref struct
        {
            // Special cases for FixedSpanByteKey
            if (typeof(TFirstKey) == typeof(FixedSpanByteKey))
            {
                // Guarantee, irrespective of inlining, that we reduce to this
                if (typeof(TSecondKey) == typeof(FixedSpanByteKey))
                {
                    return SpanByteComparer.StaticEquals(k1.KeyBytes, k2.KeyBytes);
                }

                if (typeof(TSecondKey) == typeof(VectorElementKey))
                {
                    // Vector elements always has namespace, never equal
                    return false;
                }

                if (k2.HasNamespace)
                {
                    return false;
                }

                return SpanByteComparer.StaticEquals(k1.KeyBytes, k2.KeyBytes);
            }
            else if (typeof(TSecondKey) == typeof(FixedSpanByteKey))
            {
                if (k1.HasNamespace)
                {
                    return false;
                }

                return SpanByteComparer.StaticEquals(k1.KeyBytes, k2.KeyBytes);
            }

            // Special cases for VectorElementKey
            if (typeof(TFirstKey) == typeof(VectorElementKey))
            {
                // Guarantee, irrespective of inlining, that we reduce to this
                if (typeof(TSecondKey) == typeof(VectorElementKey))
                {
                    return SpanByteComparer.StaticEquals(k1.NamespaceBytes, k2.NamespaceBytes) && SpanByteComparer.StaticEquals(k1.KeyBytes, k2.KeyBytes);
                }

                if (typeof(TSecondKey) == typeof(FixedSpanByteKey))
                {
                    // FixedSpanByteKey never has namespace, never equal
                    return false;
                }

                if (!k2.HasNamespace)
                {
                    return false;
                }

                return SpanByteComparer.StaticEquals(k1.NamespaceBytes, k2.NamespaceBytes) && SpanByteComparer.StaticEquals(k1.KeyBytes, k2.KeyBytes);
            }
            else if (typeof(TSecondKey) == typeof(VectorElementKey))
            {
                if (!k1.HasNamespace)
                {
                    return false;
                }

                return SpanByteComparer.StaticEquals(k1.NamespaceBytes, k2.NamespaceBytes) && SpanByteComparer.StaticEquals(k1.KeyBytes, k2.KeyBytes);
            }

            // Generic cases
            if (k1.HasNamespace)
            {
                if (!k2.HasNamespace)
                {
                    return false;
                }

                return SpanByteComparer.StaticEquals(k1.NamespaceBytes, k2.NamespaceBytes) && SpanByteComparer.StaticEquals(k1.KeyBytes, k2.KeyBytes);
            }
            else if (k2.HasNamespace)
            {
                // Know that k1 has no namespace, bail
                return false;
            }

            // Known no namespace
            return SpanByteComparer.StaticEquals(k1.KeyBytes, k2.KeyBytes);
        }

        /// <summary>
        /// Get 64-bit hash code
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long StaticGetHashCode64<TKey>(TKey key)
            where TKey : IKey
                , allows ref struct
        {
            // Guarantee, irrespective of inlining decisions, that FixedSpanByteKey is special cased
            if (typeof(TKey) == typeof(FixedSpanByteKey))
            {
                return SpanByteComparer.StaticGetHashCode64(key.KeyBytes);
            }

            // Guarantee, irrespective of inlining decisions, that VectorElementKey is special cased
            if (typeof(TKey) == typeof(VectorElementKey))
            {
                // TODO: Better hash construction?
                return SpanByteComparer.StaticGetHashCode64(key.KeyBytes) ^ SpanByteComparer.StaticGetHashCode64(key.NamespaceBytes);
            }

            // Generic cases
            if (key.HasNamespace)
            {
                // TODO: Better hash construction?
                return SpanByteComparer.StaticGetHashCode64(key.KeyBytes) ^ SpanByteComparer.StaticGetHashCode64(key.NamespaceBytes);
            }
            else
            {
                return SpanByteComparer.StaticGetHashCode64(key.KeyBytes);
            }
        }
    }
}