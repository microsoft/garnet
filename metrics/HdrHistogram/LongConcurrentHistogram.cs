using System;
using System.Diagnostics;
using System.Threading;
using HdrHistogram.Utilities;

namespace HdrHistogram
{
    /// <summary>
    /// An integer values High Dynamic Range (HDR) Histogram that supports safe concurrent recording operations.
    /// </summary>
    /// <remarks>
    /// A <see cref="LongConcurrentHistogram"/> guarantees lossless recording of values into the histogram even when the histogram is updated by multiple threads.
    /// <p>
    /// It is important to note that concurrent recording is the only thread-safe behavior provided by <seealso cref="LongConcurrentHistogram"/>.
    /// It provides no implicit synchronization that would prevent the contents of the histogram from changing during other operations.
    /// These non-synchronised operations include queries, iterations, copies, or addition operations on the histogram. 
    /// Concurrent updates that would safely work in the presence of queries, copies, or additions of histogram objects should use the Recorder which is intended for this purpose.
    /// </p>
    /// </remarks>
    public class LongConcurrentHistogram : HistogramBase
    {
        private readonly WriterReaderPhaser _wrp = new WriterReaderPhaser();
        private readonly AtomicLongArray _counts;
        private long _totalCount = 0L;


        /// <summary>
        /// Construct a <see cref="LongConcurrentHistogram"/> given the lowest and highest values to be tracked and a number of significant decimal digits.
        /// Providing a <paramref name="lowestTrackableValue"/> is useful is situations where the units used for the histogram's values are much smaller that the minimal accuracy required. 
        /// For example when tracking time values stated in nanoseconds, where the minimal accuracy required is a microsecond, the proper value for <paramref name="lowestTrackableValue"/> would be 1000.
        /// </summary>
        /// <param name="lowestTrackableValue">
        /// The lowest value that can be tracked (distinguished from 0) by the histogram.
        /// Must be a positive integer that is &gt;= 1. 
        /// May be internally rounded down to nearest power of 2.
        /// </param>
        /// <param name="highestTrackableValue">The highest value to be tracked by the histogram. Must be a positive integer that is &gt;= (2 * <paramref name="lowestTrackableValue"/>).</param>
        /// <param name="numberOfSignificantValueDigits">The number of significant decimal digits to which the histogram will maintain value resolution and separation.
        /// Must be a non-negative integer between 0 and 5.</param>
        public LongConcurrentHistogram(long lowestTrackableValue, long highestTrackableValue, int numberOfSignificantValueDigits)
            : base(lowestTrackableValue, highestTrackableValue, numberOfSignificantValueDigits)
        {
            _counts = new AtomicLongArray(CountsArrayLength);
        }

        /// <summary>
        /// Construct a <see cref="LongConcurrentHistogram"/> given the lowest and highest values to be tracked and a number of significant decimal digits.
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
        public LongConcurrentHistogram(long instanceId, long lowestTrackableValue, long highestTrackableValue, int numberOfSignificantValueDigits)
            : base(instanceId, lowestTrackableValue, highestTrackableValue, numberOfSignificantValueDigits)
        {
            _counts = new AtomicLongArray(CountsArrayLength);
        }

        /// <summary>
        /// Gets the total number of recorded values.
        /// </summary>
        public override long TotalCount
        {
            get {return Interlocked.Read(ref _totalCount);}
            protected set { Interlocked.Exchange(ref _totalCount, value); }
        }

        /// <summary>
        /// Returns the word size of this implementation
        /// </summary>
        protected override int WordSizeInBytes => sizeof(long);

        /// <summary>
        /// The maximum value a count can be for any given bucket.
        /// </summary>
        protected override long MaxAllowableCount => long.MaxValue;

        /// <summary>
        /// Copies the data from this instance to a new instance.
        /// </summary>
        /// <returns>A new copy of this instance.</returns>
        public override HistogramBase Copy()
        {
            var copy = new LongConcurrentHistogram(InstanceId, LowestTrackableValue, HighestTrackableValue, NumberOfSignificantValueDigits);
            copy.Add(this);
            return copy;
        }

        /// <summary>
        /// Gets the number of recorded values at a given index.
        /// </summary>
        /// <param name="index">The index to get the count for</param>
        /// <returns>The number of recorded values at the given index.</returns>
        protected override long GetCountAtIndex(int index)
        {
            try
            {
                _wrp.ReaderLock();

                Debug.Assert(base.CountsArrayLength == _counts.Length);

                return _counts[index];
            }
            finally
            {
                _wrp.ReaderUnlock();
            }
        }

        /// <summary>
        /// Sets the count at the given index.
        /// </summary>
        /// <param name="index">The index to be set</param>
        /// <param name="value">The value to set</param>
        protected override void SetCountAtIndex(int index, long value)
        {
            //At time of writing, this was unused. 
            //  This method is only used in decoding from byte array, in which case the basic histogram implementations would be used.
            //  You can then Add these decoded instances to this Concurrent implementation.

            //Throwing as this code is currently untested.
            throw new NotImplementedException();
            //try
            //{
            //    _wrp.ReaderLock();
            //    Debug.Assert(CountsArrayLength == _activeCounts.Length);
            //    Debug.Assert(CountsArrayLength == _inactiveCounts.Length);
            //    var idx = NormalizeIndex(index, _activeCounts.NormalizingIndexOffset, _activeCounts.Length);
            //    _activeCounts.LazySet(idx, value);
            //    idx = NormalizeIndex(index, _inactiveCounts.NormalizingIndexOffset, _inactiveCounts.Length);
            //    _inactiveCounts.LazySet(idx, 0);
            //}
            //finally
            //{
            //    _wrp.ReaderUnlock();
            //}
        }

        /// <summary>
        /// Increments the count at the given index. Will also increment the <see cref="HistogramBase.TotalCount"/>.
        /// </summary>
        /// <param name="index">The index to increment the count at.</param>
        protected override void IncrementCountAtIndex(int index)
        {
            long criticalValue = _wrp.WriterCriticalSectionEnter();
            try
            {
                _counts.IncrementAndGet(index);
                Interlocked.Increment(ref _totalCount);
            }
            finally
            {
                _wrp.WriterCriticalSectionExit(criticalValue);
            }
        }

        /// <summary>
        /// Adds the specified amount to the count of the provided index. Also increments the <see cref="HistogramBase.TotalCount"/> by the same amount.
        /// </summary>
        /// <param name="index">The index to increment.</param>
        /// <param name="addend">The amount to increment by.</param>
        protected override void AddToCountAtIndex(int index, long addend)
        {
            long criticalValue = _wrp.WriterCriticalSectionEnter();
            try
            {
                _counts.AddAndGet(index, addend);
                Interlocked.Add(ref _totalCount, addend);
            }
            finally
            {
                _wrp.WriterCriticalSectionExit(criticalValue);
            }
        }

        /// <summary>
        /// Clears the counts of this implementation.
        /// </summary>
        protected override void ClearCounts()
        {
            try
            {
                _wrp.ReaderLock();
                Debug.Assert(CountsArrayLength == _counts.Length);
                for (int i = 0; i<_counts.Length; i++)
                {
                    _counts[i] = 0;
                }
                TotalCount = 0;
            }
            finally
            {
                _wrp.ReaderUnlock();
            }
        }

        /// <summary>
        /// Copies the internal counts array into the supplied array.
        /// </summary>
        /// <param name="target">The array to write each count value into.</param>
        protected override void CopyCountsInto(long[] target)
        {
            for (int i = 0; i<target.Length; i++)
            {
                target[i] = _counts[i];
            }
        }
    }
}