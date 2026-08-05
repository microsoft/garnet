// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Garnet.server
{
    /// <summary>
    /// ASCII case-insensitive comparer over configuration parameter names held as raw bytes. On .NET 9+
    /// it also serves as the alternate comparer so a name can be looked up directly from the network
    /// buffer as a <see cref="ReadOnlySpan{T}"/>, without allocating.
    /// </summary>
    internal sealed class ConfigNameComparer : IEqualityComparer<byte[]>
#if NET9_0_OR_GREATER
        , IAlternateEqualityComparer<ReadOnlySpan<byte>, byte[]>
#endif
    {
        /// <summary>Shared instance; the comparer is stateless.</summary>
        internal static readonly ConfigNameComparer Instance = new();

        ConfigNameComparer()
        {
        }

        /// <inheritdoc/>
        public bool Equals(byte[] left, byte[] right) => Equals(left.AsSpan(), right);

        /// <inheritdoc/>
        public int GetHashCode(byte[] key) => GetHashCode(key.AsSpan());

        /// <summary>Compare a name held in a span against a stored name.</summary>
        /// <param name="alternate">Name to compare, typically a slice of the network buffer.</param>
        /// <param name="other">Stored name.</param>
        /// <returns><see langword="true"/> if the names match ignoring ASCII case.</returns>
        public bool Equals(ReadOnlySpan<byte> alternate, byte[] other)
        {
            if (alternate.Length != other.Length)
                return false;

            for (var i = 0; i < alternate.Length; i++)
            {
                if (ToUpperAscii(alternate[i]) != ToUpperAscii(other[i]))
                    return false;
            }

            return true;
        }

        /// <summary>Case-insensitive hash of a name held in a span.</summary>
        /// <param name="alternate">Name to hash.</param>
        /// <returns>Hash code matching that of the equivalent <see cref="byte"/>[] key.</returns>
        public int GetHashCode(ReadOnlySpan<byte> alternate)
        {
            var hash = 17;
            foreach (var value in alternate)
                hash = unchecked(hash * 31 + ToUpperAscii(value));
            return hash;
        }

#if NET9_0_OR_GREATER
        /// <inheritdoc/>
        public byte[] Create(ReadOnlySpan<byte> alternate) => alternate.ToArray();
#endif

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static byte ToUpperAscii(byte value)
            => value is >= (byte)'a' and <= (byte)'z' ? (byte)(value - ('a' - 'A')) : value;
    }
}