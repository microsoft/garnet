// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Numerics;
using System.Runtime.CompilerServices;

namespace Garnet.common
{
    /// <summary>
    /// Hash utilities
    /// </summary>
    public static class HashUtils
    {
        /// <inheritdoc cref="BitOperations.RotateLeft(ulong, int)"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static unsafe ulong Rotl64(ulong v, int r) => BitOperations.RotateLeft(v, r);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static unsafe ulong fmix64(ulong k)
        {
            k ^= k >> 33;
            k *= (ulong)0xff51afd7ed558ccd;
            k ^= k >> 33;
            k *= (ulong)0xc4ceb9fe1a85ec53;
            k ^= k >> 33;

            return k;
        }

        /// <summary>
        /// MurmurHash3 - 64 bit fast
        /// </summary>
        /// <param name="bString"></param>
        /// <param name="len"></param>
        /// <param name="seed"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe ulong MurmurHash3x64A(byte* bString, int len, uint seed = 0)
        {
            ulong h1 = seed;
            ulong c1 = 0x87c37b91114253d5;
            ulong c2 = 0x4cf5ad432745937f;
            ulong k1 = 0;

            ulong* blockStart = (ulong*)bString;
            ulong* blockEnd = blockStart + (len >> 3);

            while (blockStart < blockEnd)
            {
                k1 = *blockStart++;

                k1 *= c1;
                k1 = Rotl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;
                h1 = Rotl64(h1, 27);
                h1 += k1;
                h1 = h1 * 5 + 0x52dce729;
            }

            int suffixLen = len & 15;
            byte* suffix = (byte*)(blockEnd);
            k1 = 0;

            if (suffixLen >= 7) k1 ^= (ulong)((*(suffix + 6) << 48));
            if (suffixLen >= 6) k1 ^= (ulong)((*(suffix + 5) << 40));
            if (suffixLen >= 5) k1 ^= (ulong)((*(suffix + 4) << 32));
            if (suffixLen >= 4) k1 ^= (ulong)((*(suffix + 3) << 24));
            if (suffixLen >= 3) k1 ^= (ulong)((*(suffix + 2) << 16));
            if (suffixLen >= 2) k1 ^= (ulong)((*(suffix + 1) << 8));
            if (suffixLen >= 1)
            {
                k1 ^= (ulong)((*(suffix + 0) << 0));
                k1 *= c1;
                k1 = Rotl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;
            }

            h1 ^= (ulong)len;
            h1 = fmix64(h1);

            return h1;
        }

        /// <summary>
        /// Murmurhash 3 - 64 bit
        /// </summary>
        /// <param name="bString"></param>
        /// <param name="len"></param>
        /// <param name="seed"></param>
        /// <returns></returns>        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe ulong MurmurHash3x64(byte* bString, int len, uint seed = 0)
        {
            ulong h1, h2;
            (h1, h2) = MurmurHash3x128(bString, len, seed);
            return h1;
        }

        /// <summary>
        /// Murmurhash 3 - 128 bit
        /// </summary>
        /// <param name="bString"></param>
        /// <param name="len"></param>
        /// <param name="seed"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe (ulong, ulong) MurmurHash3x128(byte* bString, int len, uint seed = 0)
        {
            ulong h1 = seed;
            ulong h2 = seed;

            ulong c1 = 0x87c37b91114253d5;
            ulong c2 = 0x4cf5ad432745937f;

            ulong k1 = 0;
            ulong k2 = 0;

            ulong* blockStart = (ulong*)bString;
            ulong* blockEnd = blockStart + (len >> 4);

            while (blockStart < blockEnd)
            {
                k1 = *blockStart++;
                k2 = *blockStart++;

                k1 *= c1;
                k1 = Rotl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;
                h1 = Rotl64(h1, 27);
                h1 += h2;
                h1 = h1 * 5 + 0x52dce729;

                k2 *= c2;
                k2 = Rotl64(k2, 33);
                k2 *= c1;
                h2 ^= k2;
                h2 = Rotl64(h2, 31);
                h2 += h1;
                h2 = h2 * 5 + 0x38495ab5;
            }

            int suffixLen = len & 15;
            byte* suffix = (byte*)(blockEnd);
            k1 = 0;
            k2 = 0;
            if (suffixLen >= 15) k2 ^= (ulong)((*(suffix + 14) << 48));
            if (suffixLen >= 14) k2 ^= (ulong)((*(suffix + 13) << 40));
            if (suffixLen >= 13) k2 ^= (ulong)((*(suffix + 12) << 32));
            if (suffixLen >= 12) k2 ^= (ulong)((*(suffix + 11) << 24));
            if (suffixLen >= 11) k2 ^= (ulong)((*(suffix + 10) << 16));
            if (suffixLen >= 10) k2 ^= (ulong)((*(suffix + 9) << 8));
            if (suffixLen >= 9)
            {
                k2 ^= (ulong)((*(suffix + 8) << 0));
                k2 *= c2;
                k2 = Rotl64(k2, 33);
                k2 *= c1;
                h2 ^= k2;
            }

            if (suffixLen >= 8) k1 ^= (ulong)((*(suffix + 7) << 56));
            if (suffixLen >= 7) k1 ^= (ulong)((*(suffix + 6) << 48));
            if (suffixLen >= 6) k1 ^= (ulong)((*(suffix + 5) << 40));
            if (suffixLen >= 5) k1 ^= (ulong)((*(suffix + 4) << 32));
            if (suffixLen >= 4) k1 ^= (ulong)((*(suffix + 3) << 24));
            if (suffixLen >= 3) k1 ^= (ulong)((*(suffix + 2) << 16));
            if (suffixLen >= 2) k1 ^= (ulong)((*(suffix + 1) << 8));
            if (suffixLen >= 1)
            {
                k1 ^= (ulong)((*(suffix + 0) << 0));
                k1 *= c1;
                k1 = Rotl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;
            }

            h1 ^= (ulong)len;
            h2 ^= (ulong)len;
            h1 += h2;
            h2 += h1;
            h1 = fmix64(h1);
            h2 = fmix64(h2);
            h1 += h2;
            h2 += h1;

            return (h1, h2);
        }

        /// <summary>
        /// MurmurHash2 Get 64-bit hash code for a byte array
        /// </summary>
        /// <param name="bString"></param>
        /// <param name="len"></param>
        /// <param name="seed"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe ulong MurmurHash2x64A(byte* bString, int len, uint seed = 0)
        {
            ulong m = (ulong)0xc6a4a7935bd1e995;
            int r = 47;
            ulong h = seed ^ ((ulong)len * m);
            byte* data = bString;
            byte* end = data + (len - (len & 7));

            while (data != end)
            {
                ulong k;
                k = (ulong)data[0];
                k |= (ulong)data[1] << 8;
                k |= (ulong)data[2] << 16;
                k |= (ulong)data[3] << 24;
                k |= (ulong)data[4] << 32;
                k |= (ulong)data[5] << 40;
                k |= (ulong)data[6] << 48;
                k |= (ulong)data[7] << 56;

                k *= m;
                k ^= k >> r;
                k *= m;
                h ^= k;
                h *= m;

                data += 8;
            }

            int cs = len & 7;

            if (cs >= 7)
                h ^= ((ulong)data[6] << 48);

            if (cs >= 6)
                h ^= ((ulong)data[5] << 40);

            if (cs >= 5)
                h ^= ((ulong)data[4] << 32);

            if (cs >= 4)
                h ^= ((ulong)data[3] << 24);

            if (cs >= 3)
                h ^= ((ulong)data[2] << 16);

            if (cs >= 2) h ^= ((ulong)data[1] << 8);
            if (cs >= 1)
            {
                h ^= (ulong)data[0];
                h *= m;
            }

            h ^= h >> r;
            h *= m;
            h ^= h >> r;
            return h;
        }
    }
}