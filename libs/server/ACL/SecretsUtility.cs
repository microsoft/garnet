// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Security.Cryptography;

namespace Garnet.server.ACL
{
    /// <summary>
    /// A collection of shared utility methods to operate against secret key material.
    /// </summary>
    internal static class SecretsUtility
    {
        /*
         * Future implementation note: the ConstantEquals => FixedTimeEquals call can accept strings through
         *
         * string a = ...;
         * MemoryMarshal.Cast<char, byte>(a.AsSpan())
         *
         * If both a and b strings are ASCII or UTF-8. UTF-16 or other encoding requires comparing
         * against char directly instead of casting to do it safely.
         */

        /// <summary>
        /// Compare two spans of bytes and evaluate if they are equal in constant O(n) time.
        /// This is intended to reduce the likelihood of leaking the contents of the spans through side-channel timing attacks.
        /// 
        /// Note: This function is explicitly not optimized for performance and must remain O(n) where a.Length == b.Length.
        /// </summary>
        /// <param name="a">The first span of data to compare</param>
        /// <param name="b">The second span of data to compare against</param>
        /// <returns>Returns true if both spans are of equal length and each sequential value matches</returns>
        public static bool ConstantEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => CryptographicOperations.FixedTimeEquals(a, b);
    }
}