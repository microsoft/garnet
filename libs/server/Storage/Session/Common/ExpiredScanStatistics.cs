// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Storage.Session.Common
{
    /*
     * HK TODO: use this class in Scan functions to store statistics, and then use this in CollectExpiredMainStoreKeys method to decide when to wake up for next round
     * of GC.
     * Crux of the problem is that I need to store key expirations, and be able to answer the best timespan to wakeup in to expire N percent of keys (Percentile).
     * This class attempts to answer percentiles from a stream of data (expiration values).
     * Every incoming expiration value is binned to reduce memory usage.
    */
    internal class ActiveExpirationScanStats
    {
        private readonly int minTimeBeforeCollection;
        private readonly int maxTimeBeforeCollection;
        private readonly int resolution;
        private readonly int targetPercentage;

        // array of range bins which maintains time spans between which most expirations will happen
        // want to find at what time if I wake up will I expire atleast N% of records.
        // such that index:
        // 0 -> Range [0 - minGcDuration)
        // 1 -> Range [minGcDuration - (minGcDuration + resolution))
        // N -> Range [maxGcDuration - Inf) we keep a track of this solely to know if maxTimeBeforeGC is too low
        private int[] bins;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="minTimeBeforeCollection">What is the minimum time between 2 Active expiration scan cycles. 2 cycles cannot occur in time lesser than this.</param>
        /// <param name="maxTimeBeforeCollection">What is the maximum time between 2 Active expiration scan cycles. 2 cycles should not occur in longer than this.</param>
        /// <param name="resolution">Granularity of how precisely to track expiration statistics, inversely proportional to the memory taken by this class</param>
        public ActiveExpirationScanStats(int minTimeBeforeCollection, int maxTimeBeforeCollection, int resolution)
        {
            this.minTimeBeforeCollection = minTimeBeforeCollection;
            this.maxTimeBeforeCollection = maxTimeBeforeCollection;
            this.resolution = resolution;

            // from minGcDuration to maxGcDuration if we use binning range each index of bins represents a range segment
            int numRanges = (maxTimeBeforeCollection - minTimeBeforeCollection) / resolution;
            bins = new int[numRanges];
        }

        // Use downsampling, and binning?
        public void AddExpiration(int expiration)
        { 
            if (expiration < minTimeBeforeCollection)
            {
                bins[0]++;
                return;
            }

            if (expiration > maxTimeBeforeCollection)
            {

            }
        }
    }
}
