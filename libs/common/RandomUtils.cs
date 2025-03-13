// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.common
{
    /// <summary>
    /// Utility class for Random-related operations
    /// </summary>
    public static class RandomUtils
    {
        // Threshold of k / n for choosing the random picking algorithm
        private static readonly double KOverNThreshold = 0.1;

        /// <summary>
        /// Pick indices from a collection of n items
        /// </summary>
        /// <param name="n">Number of items in the collection</param>
        /// <param name="indices">Span of indices to pick.</param>
        /// <param name="seed">Random seed</param>
        /// <param name="distinct">Whether items returned should be distinct (default: true)</param>
        /// <returns>K indices picked</returns>
        public static void PickRandomIndices(int n, Span<int> indices, int seed, bool distinct = true)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(n);
            if (distinct && indices.Length > n) throw new ArgumentException(
                    $"{nameof(indices)} cannot be larger than {nameof(n)} when indexes should be distinct.");

            if (!distinct || (double)indices.Length / n < KOverNThreshold)
            {
                PickRandomIndicesIteratively(n, indices, seed, distinct);
            }
            else
            {
                PickRandomDistinctIndicesWithShuffle(n, indices, seed);
            }
        }

        /// <summary>
        /// Pick random index from a collection of n items, using the given random integer
        /// </summary>
        /// <param name="n">Number of items in the collection</param>
        /// <param name="rand">Random integer</param>
        /// <returns>Index picked</returns>
        public static int PickRandomIndex(int n, int rand)
            => rand % n;

        private static void PickRandomIndicesIteratively(int n, Span<int> indices, int seed, bool distinct)
        {
            var random = new Random(seed);
            HashSet<int> pickedIndices = default;
            if (distinct) pickedIndices = [];

            var i = 0;
            while (i < indices.Length)
            {
                var nextIdx = random.Next(n);
                if (!distinct || pickedIndices.Add(nextIdx))
                {
                    indices[i] = nextIdx;
                    i++;
                }
            }
        }

        private static void PickRandomDistinctIndicesWithShuffle(int n, Span<int> indices, int seed)
        {
            const int StackallocThreshold = 256;

            var random = new Random(seed);
            var shuffledIndices = n <= StackallocThreshold ? 
                stackalloc int[StackallocThreshold].Slice(0, n) : new int[n];

            for (var i = 0; i < shuffledIndices.Length; i++)
            {
                shuffledIndices[i] = i;
            }

            random.Shuffle(shuffledIndices);
            shuffledIndices.Slice(0, indices.Length).CopyTo(indices);
        }
    }
}