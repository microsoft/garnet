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
        public static string CreateHexId(int size = 40)
        {
            Span<byte> nodeIdBuffer = stackalloc byte[size / 2];
            RandomNumberGenerator.Fill(nodeIdBuffer);
            return Convert.ToHexString(nodeIdBuffer).ToLowerInvariant();
        }

        /// <summary>
        /// Generates a default hex string of specified length (all zeros)
        /// </summary>
        /// <param name="size">The length of the hex identifier string</param>
        /// <returns></returns>
        public static string DefaultHexId(int size = 40)
        {
            Span<byte> nodeIdBuffer = stackalloc byte[size / 2];
            nodeIdBuffer.Clear();
            return Convert.ToHexString(nodeIdBuffer).ToLowerInvariant();
        }
    }
}