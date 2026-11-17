// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.cluster
{
    /// <summary>
    /// Single-writer membership coordinated with readers by migration batch publication and retirement.
    /// </summary>
    internal interface IBloomFilter
    {
        /// <summary>
        /// Hash a key and add its membership.
        /// </summary>
        /// <param name="key">Key bytes.</param>
        /// <param name="seed">Hash seed, including any namespace context.</param>
        void Add(ReadOnlySpan<byte> key, uint seed = 0);

        /// <summary>
        /// Check membership using a precomputed MurmurHash2x64A hash.
        /// </summary>
        /// <param name="hash">The full key hash.</param>
        /// <returns>Whether the key may be present.</returns>
        bool Contains(ulong hash);

        /// <summary>
        /// Clear membership before building the next protected batch.
        /// </summary>
        void Clear();
    }
}