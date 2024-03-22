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
        /// Random hex id
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public static string CreateHexId(int size = 40)
        {
            Span<byte> nodeIdBuffer = stackalloc byte[size];
            RandomNumberGenerator.Fill(nodeIdBuffer);
            return Convert.ToHexString(nodeIdBuffer).ToLowerInvariant();
        }

        /// <summary>
        /// Default hex id
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public static string DefaultHexId(int size = 40)
        {
            Span<byte> nodeIdBuffer = stackalloc byte[size];
            nodeIdBuffer.Clear();
            return Convert.ToHexString(nodeIdBuffer).ToLowerInvariant();
        }
    }
}