// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Garnet.shared
{
    /// <summary>
    /// Hash helpers shared across Garnet and Tsavorite.
    /// </summary>
    public static class HashUtility
    {
        /// <summary>
        /// A 32-bit murmur3 implementation.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Murmur3(int h)
        {
            var a = (uint)h;
            a ^= a >> 16;
            a *= 0x85ebca6b;
            a ^= a >> 13;
            a *= 0xc2b2ae35;
            a ^= a >> 16;
            return (int)a;
        }
    }
}