// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable IDE1006 // Naming Styles

namespace Tsavorite.core
{
    /// <summary>
    /// Configuration settings for hybrid log
    /// </summary>
    internal class ReadCacheSettings
    {
        /// <summary>
        /// Total size of in-memory part of log, in bytes
        /// </summary>
        public long MemorySize = 1L << 34;

        /// <summary>
        /// Size of a page in bits
        /// </summary>
        public int PageSizeBits = 25;

        /// <summary>
        /// Number of pages in the circular buffer, rounded down to nearest power of 2
        /// </summary>
        /// <remarks>If 0, it is calculated from <see cref="MemorySize"/> and <see cref="PageSizeBits"/>, which is also the max if both this and <see cref="MemorySize"/> are nonzero.</remarks>
        public int PageCount = 25;

        /// <summary>
        /// Fraction of log head (in memory) used for second chance copy to tail. This is (1 - MutableFraction) for the underlying log
        /// </summary>
        public double SecondChanceFraction = 0.1;
    }
}