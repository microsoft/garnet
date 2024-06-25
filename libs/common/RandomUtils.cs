// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        /// Pick k indexes from a collection of n items
        /// </summary>
        /// <param name="n">Number of items in the collection</param>
        /// <param name="k">Number of items to pick</param>
        /// <param name="seed">Random seed</param>
        /// <param name="distinct">Whether items returned should be distinct (default: true)</param>
        /// <returns>K indexes picked</returns>
        public static int[] PickKRandomIndexes(int n, int k, int seed, bool distinct = true)
        {
            if (k < 0) throw new ArgumentOutOfRangeException(nameof(k));
            if (n < 0) throw new ArgumentOutOfRangeException(nameof(n));
            if (distinct && k > n) throw new ArgumentException(
                    $"{nameof(k)} cannot be larger than {nameof(n)} when indexes should be distinct.");

            return !distinct || (double)k / n < KOverNThreshold
                ? PickKRandomIndexesIteratively(n, k, seed, distinct)
                : PickKRandomDistinctIndexesWithShuffle(n, k, seed);
        }

        /// <summary>
        /// Pick random index from a collection of n items
        /// </summary>
        /// <param name="n">Number of items in the collection</param>
        /// <param name="seed">Random seed</param>
        /// <returns>Index picked</returns>
        public static int PickRandomIndex(int n, int seed)
        {
            var random = new Random(seed);
            return random.Next(n);
        }

        private static int[] PickKRandomIndexesIteratively(int n, int k, int seed, bool distinct)
        {
            var random = new Random(seed);
            var result = new int[k];
            HashSet<int> pickedIndexes = default;
            if (distinct) pickedIndexes = [];

            var i = 0;
            while (i < k)
            {
                var nextIdx = random.Next(n);
                if (!distinct || pickedIndexes.Add(nextIdx))
                {
                    result[i] = nextIdx;
                    i++;
                }
            }

            return result;
        }

        private static int[] PickKRandomDistinctIndexesWithShuffle(int n, int k, int seed)
        {
            var random = new Random(seed);
            var shuffledIndexes = new int[n];
            for (var i = 0; i < n; i++)
            {
                shuffledIndexes[i] = i;
            }

            // Fisher-Yates shuffle
            for (var i = n - 1; i > 0; i--)
            {
                // Get a random index from 0 to i
                var j = random.Next(i + 1);
                // Swap shuffledIndexes[i] and shuffledIndexes[j]
                (shuffledIndexes[i], shuffledIndexes[j]) = (shuffledIndexes[j], shuffledIndexes[i]);
            }

            var result = new int[k];
            Array.Copy(shuffledIndexes, result, k);
            return result;
        }
    }
}