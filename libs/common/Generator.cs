// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Security.Cryptography;

namespace Garnet.common
{
    /// <summary>
    /// Collection of methods generating hex ids
    /// </summary>
    public static class Generator
    {
        /// <summary>
        /// Generates a random hex string of specified length
        /// </summary>
        /// <param name="size">The length of the hex identifier string</param>
        /// <returns></returns>
        public static string CreateHexId(int size = 40)
        {
            byte[] byteLease = null;
            Span<byte> nodeIdBuffer = size <= 64 ? stackalloc byte[size / 2] : Lease(size / 2, out byteLease);
            RandomNumberGenerator.Fill(nodeIdBuffer);

            char[] charLease = null;
            var cLength = nodeIdBuffer.Length * 2; // not the same as size (if size is odd)
            Span<char> charBuffer = size <= 64 ? stackalloc char[cLength] : Lease(cLength, out charLease);
            int index = 0;

            const string HexChars = "0123456789abcdef";
            foreach (byte b in nodeIdBuffer)
            {
                charBuffer[index++] = HexChars[b >> 4]; // hi nibble
                charBuffer[index++] = HexChars[b & 0xF]; // lo nibble
            }

            var result = new string(charBuffer);
            if (byteLease is not null) ArrayPool<byte>.Shared.Return(byteLease);
            if (charLease is not null) ArrayPool<char>.Shared.Return(charLease);
            return result;

            static Span<T> Lease<T>(int length, out T[] oversized)
            {
                oversized = ArrayPool<T>.Shared.Rent(length);
                return new(oversized, 0, length);
            }
        }

        /// <summary>
        /// Generates a default hex string of specified length (all zeros)
        /// </summary>
        /// <param name="size">The length of the hex identifier string</param>
        /// <returns></returns>
        public static string DefaultHexId(int size = 40)
            => size == 40 ? DefaultHexId40 : new string('0', size & ~1); // note: trimmed to multiples of 2

        const string DefaultHexId40 = "0000000000000000000000000000000000000000"; // 40 times '0'

    }
}