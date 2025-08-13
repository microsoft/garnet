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
    }
}