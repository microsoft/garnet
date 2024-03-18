/*
 * This is a .NET port of the original Java version, which was written by
 * Gil Tene as described in
 * https://github.com/HdrHistogram/HdrHistogram
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

using System;

namespace HdrHistogram.Iteration
{
    /// <summary>
    /// Represents a value point iterated through in a Histogram, with associated stats.
    /// </summary>
    public sealed class HistogramIterationValue
    {
        /// <summary>
        /// The actual value level that was iterated to by the iterator
        /// </summary>
        public long ValueIteratedTo { get; private set; }

        /// <summary>
        /// The actual value level that was iterated from by the iterator
        /// </summary>
        public long ValueIteratedFrom { get; private set; }
        
        /// <summary>
        /// The count of recorded values in the histogram that exactly match this [lowestEquivalentValue(valueIteratedTo)...highestEquivalentValue(valueIteratedTo)] value range.
        /// </summary>
        public long CountAtValueIteratedTo { get; private set; }

        /// <summary>
        /// The count of recorded values in the histogram that were added to the totalCountToThisValue(below) as a result on this iteration step. Since multiple iteration steps may occur with overlapping equivalent value ranges, the count may be lower than the count found at the value (e.g.multiple linear steps or percentile levels can occur within a single equivalent value range)
        /// </summary>
        public long CountAddedInThisIterationStep { get; private set; }

        /// <summary>
        /// The total count of all recorded values in the histogram at values equal or smaller than valueIteratedTo.
        /// </summary>
        public long TotalCountToThisValue { get; private set; }

        /// <summary>
        /// The sum of all recorded values in the histogram at values equal or smaller than valueIteratedTo.
        /// </summary>
        public long TotalValueToThisValue { get; private set; }

        /// <summary>
        /// The percentile of recorded values in the histogram at values equal or smaller than valueIteratedTo.
        /// </summary>
        public double Percentile { get; private set; }

        /// <summary>
        /// The percentile level that the iterator returning this HistogramIterationValue had iterated to.
        /// Generally, percentileLevelIteratedTo will be equal to or smaller than percentile, but the same value point can contain multiple iteration levels for some iterators. 
        /// e.g. a PercentileEnumerator can stop multiple times in the exact same value point (if the count at that value covers a range of multiple percentiles in the requested percentile iteration points).
        /// </summary>
        public double PercentileLevelIteratedTo { get; private set; }

        /// <summary>
        /// Indicates if this item is to be considered the last value in the set.
        /// </summary>
        /// <returns>Returns <c>true</c> if it is the last value, else <c>false</c>.</returns>
        public bool IsLastValue()
        {
            return Math.Abs(PercentileLevelIteratedTo - 100.0D) < double.Epsilon;
        }

        // Set is all-or-nothing to avoid the potential for accidental omission of some values...
        internal void Set(long valueIteratedTo, 
                          long valueIteratedFrom, 
                          long countAtValueIteratedTo,
                          long countInThisIterationStep, 
                          long totalCountToThisValue, 
                          long totalValueToThisValue,
                          double percentile, 
                          double percentileLevelIteratedTo) 
        {
            ValueIteratedTo = valueIteratedTo;
            ValueIteratedFrom = valueIteratedFrom;
            CountAtValueIteratedTo = countAtValueIteratedTo;
            CountAddedInThisIterationStep = countInThisIterationStep;
            TotalCountToThisValue = totalCountToThisValue;
            TotalValueToThisValue = totalValueToThisValue;
            Percentile = percentile;
            PercentileLevelIteratedTo = percentileLevelIteratedTo;
        }
        
        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string  ToString()
        {
            return "ValueIteratedTo:" + ValueIteratedTo +
                    ", ValueIteratedFrom:" + ValueIteratedFrom +
                    ", CountAtValueIteratedTo:" + CountAtValueIteratedTo +
                    ", CountAddedInThisIterationStep:" + CountAddedInThisIterationStep +
                    ", TotalCountToThisValue:" + TotalCountToThisValue +
                    ", TotalValueToThisValue:" + TotalValueToThisValue +
                    ", Percentile:" + Percentile +
                    ", PercentileLevelIteratedTo:" + PercentileLevelIteratedTo;
        }
    }
}
