/*
 * This is a .NET port of the original Java version, which was written by
 * Gil Tene as described in
 * https://github.com/HdrHistogram/HdrHistogram
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

using System;
using System.Buffers;

namespace HdrHistogram
{
    /// <summary>
    /// A High Dynamic Range (HDR) Histogram
    /// </summary>
    /// <remarks>
    /// Histogram supports the recording and analyzing sampled data value counts across a configurable integer value
    /// range with configurable value precision within the range.
    /// Value precision is expressed as the number of significant digits in the value recording, and provides control 
    /// over value quantization behavior across the value range and the subsequent value resolution at any given level.
    /// <para>
    /// For example, a Histogram could be configured to track the counts of observed integer values between 0 and
    /// 36,000,000,000 while maintaining a value precision of 3 significant digits across that range.
    /// Value quantization within the range will thus be no larger than 1/1,000th (or 0.1%) of any value.
    /// This example Histogram could be used to track and analyze the counts of observed response times ranging between
    /// 100 nanoseconds and 1 hour in magnitude, while maintaining a value resolution of 100 nanosecond up to 
    /// 100 microseconds, a resolution of 1 millisecond(or better) up to one second, and a resolution of 1 second 
    /// (or better) up to 1,000 seconds.
    /// At it's maximum tracked value(1 hour), it would still maintain a resolution of 3.6 seconds (or better).
    /// </para>
    /// Histogram tracks value counts in <see cref="long"/> fields.
    /// </remarks>
    public class LongHistogram : HistogramBase
    {
        /// <summary>
        /// Counts array, dropped by <see cref="Release"/> and <see cref="Return"/> on paths that do not
        /// synchronise with recorders, so every reader must load it into a local exactly once.
        /// </summary>
        private long[] _counts;
        private long _totalCount;

        /// <summary>
        /// Construct a Histogram given the highest value to be tracked and a number of significant decimal digits. 
        /// The histogram will be constructed to implicitly track(distinguish from 0) values as low as 1.
        /// </summary>
        /// <param name="highestTrackableValue">The highest value to be tracked by the histogram. Must be a positive integer that is &gt;= 2.</param>
        /// <param name="numberOfSignificantValueDigits">The number of significant decimal digits to which the histogram will maintain value resolution and separation.
        /// Must be a non-negative integer between 0 and 5.
        /// </param>
        public LongHistogram(long highestTrackableValue, int numberOfSignificantValueDigits)
            : this(1, highestTrackableValue, numberOfSignificantValueDigits)
        {
        }

        /// <summary>
        /// Construct a <see cref="LongHistogram"/> given the lowest and highest values to be tracked and a number of significant decimal digits.
        /// Providing a <paramref name="lowestTrackableValue"/> is useful is situations where the units used for the histogram's values are much smaller that the minimal accuracy required. 
        /// For example when tracking time values stated in nanosecond units, where the minimal accuracy required is a microsecond, the proper value for <paramref name="lowestTrackableValue"/> would be 1000.
        /// </summary>
        /// <param name="lowestTrackableValue">
        /// The lowest value that can be tracked (distinguished from 0) by the histogram.
        /// Must be a positive integer that is &gt;= 1. 
        /// May be internally rounded down to nearest power of 2.
        /// </param>
        /// <param name="highestTrackableValue">The highest value to be tracked by the histogram. Must be a positive integer that is &gt;= (2 * <paramref name="lowestTrackableValue"/>).</param>
        /// <param name="numberOfSignificantValueDigits">The number of significant decimal digits to which the histogram will maintain value resolution and separation.
        /// Must be a non-negative integer between 0 and 5.
        /// </param>
        public LongHistogram(long lowestTrackableValue, long highestTrackableValue,
                         int numberOfSignificantValueDigits)
            : base(lowestTrackableValue, highestTrackableValue, numberOfSignificantValueDigits)
        {
            _counts = ArrayPool<long>.Shared.Rent(CountsArrayLength);
            Array.Clear(_counts, 0, CountsArrayLength);
        }

        /// <summary>
        /// Construct a <see cref="LongHistogram"/> given the lowest and highest values to be tracked and a number of significant decimal digits.
        /// </summary>
        /// <param name="instanceId">An identifier for this instance.</param>
        /// <param name="lowestTrackableValue">The lowest value that can be tracked (distinguished from 0) by the histogram.
        /// Must be a positive integer that is &gt;= 1.
        /// May be internally rounded down to nearest power of 2.
        /// </param>
        /// <param name="highestTrackableValue">The highest value to be tracked by the histogram.
        /// Must be a positive integer that is &gt;= (2 * lowestTrackableValue).
        /// </param>
        /// <param name="numberOfSignificantValueDigits">
        /// The number of significant decimal digits to which the histogram will maintain value resolution and separation. 
        /// Must be a non-negative integer between 0 and 5.
        /// </param>
        /// <remarks>
        /// Providing a lowestTrackableValue is useful in situations where the units used for the histogram's values are much 
        /// smaller that the minimal accuracy required.
        /// For example when tracking time values stated in ticks (100 nanoseconds), where the minimal accuracy required is a
        /// microsecond, the proper value for lowestTrackableValue would be 10.
        /// </remarks>
        public LongHistogram(long instanceId, long lowestTrackableValue, long highestTrackableValue,
                         int numberOfSignificantValueDigits)
            : base(instanceId, lowestTrackableValue, highestTrackableValue, numberOfSignificantValueDigits)
        {
            _counts = ArrayPool<long>.Shared.Rent(CountsArrayLength);
            Array.Clear(_counts, 0, CountsArrayLength);
        }

        /// <summary>
        /// Whether the counts array has been handed back to the pool, after which the histogram holds no
        /// data and must not be read or recorded into.
        /// </summary>
        public bool IsReturned => _counts == null;

        /// <summary>
        /// Return the histogram long array to the pool for recycling.
        /// </summary>
        /// <remarks>
        /// Only for a histogram the caller owns outright, such as a copy taken for reporting. Use
        /// <see cref="Release"/> where a concurrent recorder may still hold the array.
        /// </remarks>
        public void Return()
        {
            var counts = _counts;
            if (counts == null)
                return;

            // Dropped before the array goes back to the shared pool, so a record after this point
            // throws here instead of silently writing into another session's histogram.
            _counts = null;
            ArrayPool<long>.Shared.Return(counts);
        }

        /// <summary>
        /// Releases the counts array without handing it back to the shared pool.
        /// </summary>
        /// <remarks>
        /// For a histogram whose recorder is not quiesced at release time, such as a client disposed while
        /// its receive callback is still completing replies. Pooling the array there would let another
        /// consumer rent it and receive that write. A recorder that reads the field after this point drops
        /// its sample; one that read it before writes into an array nothing else can reach.
        /// </remarks>
        public void Release() => _counts = null;

        /// <summary>
        /// Gets the total number of recorded values.
        /// </summary>
        public override long TotalCount { get { return _totalCount; } protected set { _totalCount = value; } }

        /// <summary>
        /// Returns the word size of this implementation
        /// </summary>
        protected override int WordSizeInBytes => 8;

        /// <summary>
        /// The maximum value a count can be for any given bucket.
        /// </summary>
        protected override long MaxAllowableCount => long.MaxValue;

        /// <summary>
        /// Create a copy of this histogram, complete with data and everything.
        /// </summary>
        /// <returns>A distinct copy of this histogram.</returns>
        public override HistogramBase Copy()
        {
            var copy = new LongHistogram(LowestTrackableValue, HighestTrackableValue, NumberOfSignificantValueDigits);
            copy.Add(this);
            return copy;
        }

        /// <summary>
        /// Copies this histogram, reading the counts array exactly once.
        /// </summary>
        /// <param name="copy">The copy, or null once the array has been released.</param>
        /// <returns>True if a copy was taken.</returns>
        /// <remarks>
        /// <see cref="Release"/> runs on a thread that does not synchronise with readers, so testing
        /// <see cref="IsReturned"/> and then calling <see cref="Copy"/> reads the field twice and can
        /// dereference an array dropped in between.
        /// </remarks>
        public bool TryCopy(out LongHistogram copy)
        {
            var counts = _counts;
            if (counts is null)
            {
                copy = null;
                return false;
            }

            copy = new LongHistogram(LowestTrackableValue, HighestTrackableValue, NumberOfSignificantValueDigits);
            Array.Copy(counts, copy._counts, CountsArrayLength);

            // Derive the total from the counts that were copied rather than reading the live one. A recorder
            // that increments between the copy and that read would leave a total the copied counts cannot
            // reach, and GetValueAtPercentile scales its target by the total and then walks the counts, so it
            // would run off the end and throw.
            copy.EstablishInternalTackingValues(CountsArrayLength);
            return true;
        }

        /// <summary>
        /// Gets the number of recorded values at a given index.
        /// </summary>
        /// <param name="index">The index to get the count for</param>
        /// <returns>The number of recorded values at the given index.</returns>
        protected override long GetCountAtIndex(int index)
        {
            return _counts[index];
        }

        /// <summary>
        /// Sets the count at the given index.
        /// </summary>
        /// <param name="index">The index to be set</param>
        /// <param name="value">The value to set</param>
        protected override void SetCountAtIndex(int index, long value)
        {
            _counts[index] = value;
        }

        /// <summary>
        /// Increments the count at the given index. Will also increment the <see cref="HistogramBase.TotalCount"/>.
        /// </summary>
        /// <param name="index">The index to increment the count at.</param>
        protected override void IncrementCountAtIndex(int index)
        {
            var counts = _counts;
            if (counts is null)
                return;

            counts[index]++;
            _totalCount++;
        }

        /// <summary>
        /// Adds the specified amount to the count of the provided index. Also increments the <see cref="HistogramBase.TotalCount"/> by the same amount.
        /// </summary>
        /// <param name="index">The index to increment.</param>
        /// <param name="addend">The amount to increment by.</param>
        protected override void AddToCountAtIndex(int index, long addend)
        {
            var counts = _counts;
            if (counts is null)
                return;

            counts[index] += addend;
            _totalCount += addend;
        }

        /// <summary>
        /// Clears the counts of this implementation.
        /// </summary>
        protected override void ClearCounts()
        {
            var counts = _counts;
            if (counts != null)
                Array.Clear(counts, 0, counts.Length);
            _totalCount = 0;
        }

        /// <summary>
        /// Copies the internal counts array into the supplied array.
        /// </summary>
        /// <param name="target">The array to write each count value into.</param>
        protected override void CopyCountsInto(long[] target)
        {
            var counts = _counts;
            Array.Copy(counts, target, Math.Min(counts.Length, target.Length));
        }
    }
}
