// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        public static unsafe string CreateHexId(int size = 40)
        {
            Span<byte> nodeIdBuffer = stackalloc byte[size / 2];
            RandomNumberGenerator.Fill(nodeIdBuffer);

            Span<char> charBuffer = stackalloc char[nodeIdBuffer.Length * 2]; // not the same as size (if size is odd)
            int index = 0;
            fixed (char* hexChars = "0123456789abcdef")
            {
                foreach (byte b in nodeIdBuffer)
                {
                    charBuffer[index++] = hexChars[b >> 4]; // hi nibble
                    charBuffer[index++] = hexChars[b & 0xF]; // lo nibble
                }
            }
            return new string(charBuffer);
        }

        /// <summary>
        /// Generates a default hex string of specified length (all zeros)
        /// </summary>
        /// <param name="size">The length of the hex identifier string</param>
        /// <returns></returns>
        public static string DefaultHexId(int size = 40)
            => size == 40 ? DefaultHex40 : new string('0', size &= ~1); // note: trimmed to multiples of 2

        const string DefaultHex40 = "0000000000000000000000000000000000000000"; // '0' x40

    }
}