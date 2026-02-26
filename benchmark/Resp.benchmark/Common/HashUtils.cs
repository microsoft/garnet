// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Resp.benchmark
{
    /// <summary>
    /// Utility class for generating stable hashes
    /// Implementations are copied from Trill: https://github.com/microsoft/Trill/blob/master/Sources/Core/Microsoft.StreamProcessing/Utilities/Utility.cs
    /// </summary>
    internal static class HashUtils
    {
        /// <summary>
        /// Generate a stable hashcode for input string.
        /// </summary>
        /// <param name="stringToHash"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int StableHash(this string stringToHash)
        {
            unsafe
            {
                fixed (char* str = stringToHash)
                {
                    return StableHashUnsafe(str, stringToHash.Length);
                }
            }
        }

        /// <summary>
        /// Stable hash implementations.
        /// </summary>
        /// <param name="stringToHash"></param>
        /// <param name="length"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe int StableHashUnsafe(char* stringToHash, int length)
        {
            const long magicno = 40343L;
            ulong hashState = (ulong)length;
            var stringChars = stringToHash;
            for (int i = 0; i < length; i++, stringChars++)
                hashState = magicno * hashState + *stringChars;

            var rotate = magicno * hashState;
            var rotated = (rotate >> 4) | (rotate << 60);
            return (int)(rotated ^ (rotated >> 32));
        }

        public static unsafe ulong MurmurHash2x64A(Span<byte> bString, uint seed = 0)
        {
            fixed (byte* p = bString)
            {
                return MurmurHash2x64A(p, bString.Length, seed);
            }
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