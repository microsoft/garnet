// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// xoshiro256** PRNG with a 256-bit state. The four state words are seeded from
    /// a single 64-bit seed via SplitMix64 (per the xoshiro paper recommendation) so
    /// that adjacent seeds produce well-decorrelated streams.
    /// </summary>
    /// <remarks>
    /// Hot-path: each <c>Next</c>/<c>NextUInt64</c> call is 4 register operations
    /// plus a rotl. Constructor is ~30 ns (4 SplitMix64 calls + zero-state check).
    /// </remarks>
    public struct XoshiroRng
    {
        ulong s0, s1, s2, s3;

        public XoshiroRng(ulong seed)
        {
            ulong basis = seed;
            s0 = SplitMix64(ref basis);
            s1 = SplitMix64(ref basis);
            s2 = SplitMix64(ref basis);
            s3 = SplitMix64(ref basis);
            // Reject the (vanishingly unlikely) all-zero state - it's a fixed point.
            if ((s0 | s1 | s2 | s3) == 0UL)
            {
                s0 = 1UL;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong SplitMix64(ref ulong x)
        {
            x += 0x9E3779B97F4A7C15UL;
            ulong z = x;
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
            return z ^ (z >> 31);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Rotl(ulong x, int k) => (x << k) | (x >> (64 - k));

        /// <summary>
        /// Produce the next 64-bit pseudorandom value.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong NextUInt64()
        {
            ulong result = Rotl(s1 * 5UL, 7) * 9UL;
            ulong t = s1 << 17;
            s2 ^= s0;
            s3 ^= s1;
            s1 ^= s2;
            s0 ^= s3;
            s2 ^= t;
            s3 = Rotl(s3, 45);
            return result;
        }

        /// <summary>
        /// Produce the next 32-bit pseudorandom value.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint NextUInt32() => (uint)(NextUInt64() >> 32);
    }
}