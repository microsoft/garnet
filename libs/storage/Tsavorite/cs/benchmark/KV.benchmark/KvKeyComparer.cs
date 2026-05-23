// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// Custom key comparer for <see cref="KvKey"/>. The KvKey byte sequence is 11 bytes
    /// (3 bytes leading padding + 8-byte payload at the end) so that the value start in
    /// the Tsavorite record is 8-byte aligned (offset 24 = 13 header + 11 key).
    ///
    /// The padding is always zero, so for hashing and equality we only need to look at
    /// the last 8 bytes — the actual payload. This is dramatically cheaper than the
    /// default <see cref="SpanByteComparer"/> which hashes/compares all 11 bytes via
    /// <c>HashBytes</c> / <c>SequenceEqual</c>.
    /// </summary>
    public struct KvKeyComparer : IKeyComparer
    {
        public static readonly KvKeyComparer Instance = new();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetHashCode64<TKey>(TKey key)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            var bytes = key.KeyBytes;
            // Read the last 8 bytes as a long (the payload portion).
            long payload = MemoryMarshal.Read<long>(bytes.Slice(bytes.Length - sizeof(long)));
            return Utility.GetHashCode(payload);
        }

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
        {
            var b1 = k1.KeyBytes;
            var b2 = k2.KeyBytes;
            // Only the 8-byte payload (trailing bytes) matters; padding is constant zero.
            long p1 = MemoryMarshal.Read<long>(b1.Slice(b1.Length - sizeof(long)));
            long p2 = MemoryMarshal.Read<long>(b2.Slice(b2.Length - sizeof(long)));
            return p1 == p2;
        }
    }
}
