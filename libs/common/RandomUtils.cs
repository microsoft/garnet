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
        /// <summary>
        /// Stack allocation size threshold for integer arrays.
        /// </summary>
        public const int IndexStackallocThreshold = 1024 / sizeof(int);

        // Threshold of k / n for choosing the random picking algorithm
        private const double KOverNThreshold = 0.1;

        /// <summary>
        /// Pick k indexes from a collection of n items into a given span of length k
        /// </summary>
        /// <param name="n">Number of items in the collection</param>
        /// <param name="indexes">Span of indexes to pick.</param>
        /// <param name="seed">Random seed</param>
        /// <param name="distinct">Whether items returned should be distinct (default: true)</param>
        /// <returns>K indexes picked</returns>
        public static void PickKRandomIndexes(int n, Span<int> indexes, int seed, bool distinct = true)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(n);
            if (distinct && indexes.Length > n) throw new ArgumentException(
                    $"{nameof(indexes)} cannot be larger than {nameof(n)} when indexes should be distinct.");

            if (!distinct || (double)indexes.Length / n < KOverNThreshold)
            {
                PickKRandomIndexesIteratively(n, indexes, seed, distinct);
            }
            else
            {
                PickKRandomDistinctIndexesWithShuffle(n, indexes, seed);
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

        private static void PickKRandomIndexesIteratively(int n, Span<int> indexes, int seed, bool distinct)
        {
            var random = new Random(seed);
            HashSet<int> pickedIndexes = default;
            if (distinct) pickedIndexes = [];

            var i = 0;
            while (i < indexes.Length)
            {
                var nextIdx = random.Next(n);
                if (!distinct || pickedIndexes.Add(nextIdx))
                {
                    indexes[i] = nextIdx;
                    i++;
                }
            }
        }

        private static void PickKRandomDistinctIndexesWithShuffle(int n, Span<int> indexes, int seed)
        {
            var random = new Random(seed);
            var shuffledIndexes = n <= IndexStackallocThreshold ?
                stackalloc int[IndexStackallocThreshold].Slice(0, n) : new int[n];

            for (var i = 0; i < shuffledIndexes.Length; i++)
            {
                shuffledIndexes[i] = i;
            }

            random.Shuffle(shuffledIndexes);
            shuffledIndexes.Slice(0, indexes.Length).CopyTo(indexes);
        }
    }
}