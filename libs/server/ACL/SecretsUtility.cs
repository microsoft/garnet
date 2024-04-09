// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace Garnet.server.ACL
{
    internal static class SecretsUtility
    {
        public static bool ConstantEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => CryptographicOperations.FixedTimeEquals(a, b);

        public static bool ConstantEquals(string a, string b) => ConstantEquals(a.AsSpan(), b.AsSpan());

        [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
        public static bool ConstantEquals(ReadOnlySpan<char> a, ReadOnlySpan<char> b)
        {
            // all secrets are ASCII or UTF-8

            return ConstantEquals(MemoryMarshal.Cast<char, byte>(a), MemoryMarshal.Cast<char, byte>(b));
        }
    }
}