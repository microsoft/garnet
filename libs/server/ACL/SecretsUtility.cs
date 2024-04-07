// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Garnet.server.ACL
{
    internal static class SecretsUtility
    {
        [MethodImpl(MethodImplOptions.NoOptimization)]
        public static bool ConstantEquals(string a, string b) => ConstantEquals(a.AsSpan(), b.AsSpan());

        [MethodImpl(MethodImplOptions.NoOptimization)]
        public static bool ConstantEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
        {
            var accumulator = 0;

            accumulator |= a.Length ^ b.Length;

            for (var i = 0; i < Math.Min(a.Length, b.Length); i++)
            {
                accumulator |= a[i] ^ b[i];
            }

            return accumulator == 0;
        }

        [MethodImpl(MethodImplOptions.NoOptimization)]
        public static bool ConstantEquals(ReadOnlySpan<char> a, ReadOnlySpan<char> b)
        {
            var accumulator = 0;

            accumulator |= a.Length ^ b.Length;

            for (var i = 0; i < Math.Min(a.Length, b.Length); i++)
            {
                accumulator |= a[i] ^ b[i];
            }

            return accumulator == 0;
        }
    }
}
