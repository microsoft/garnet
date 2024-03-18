/*
 * This is a .NET port of the original Java version, which was written by
 * Gil Tene as described in
 * https://github.com/HdrHistogram/HdrHistogram
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

using System;
using System.Collections;
using System.Collections.Generic;

namespace HdrHistogram.Iteration
{
    /// <summary>
    /// Provide functionality for enumerating over histogram counts.
    /// </summary>
    internal abstract class AbstractHistogramEnumerator : IEnumerator<HistogramIterationValue>
    {
        private readonly long _savedHistogramTotalRawCount;
        private readonly HistogramIterationValue _currentIterationValue;
        private int _nextBucketIndex;
        private int _nextSubBucketIndex;
        private long _prevValueIteratedTo;
        private long _totalCountToPrevIndex;
        private long _totalValueToCurrentIndex;
        private bool _freshSubBucket;
        private long _currentValueAtIndex;
        private long _nextValueAtIndex;
        
        protected HistogramBase SourceHistogram { get; }
        protected long ArrayTotalCount { get; }
        protected int CurrentBucketIndex { get; private set; }
        protected int CurrentSubBucketIndex { get; private set; }
        protected long TotalCountToCurrentIndex { get; private set; }
        protected long CountAtThisValue { get; private set; }

        public HistogramIterationValue Current { get; private set; }

        protected AbstractHistogramEnumerator(HistogramBase histogram)
        {
            SourceHistogram = histogram;
            _savedHistogramTotalRawCount = histogram.TotalCount;
            ArrayTotalCount = histogram.TotalCount;
            CurrentBucketIndex = 0;
            CurrentSubBucketIndex = 0;
            _currentValueAtIndex = 0;
            _nextBucketIndex = 0;
            _nextSubBucketIndex = 1;
            _nextValueAtIndex = 1;
            _prevValueIteratedTo = 0;
            _totalCountToPrevIndex = 0;
            TotalCountToCurrentIndex = 0;
            _totalValueToCurrentIndex = 0;
            CountAtThisValue = 0;
            _freshSubBucket = true;
            _currentIterationValue = new HistogramIterationValue();
        }

        /// <summary>
        ///  Returns <c>true</c> if the iteration has more elements. (In other words, returns true if next would return an element rather than throwing an exception.)
        /// </summary>
        /// <returns><c>true</c> if the iterator has more elements.</returns>
        protected virtual bool HasNext()
        {
            if (SourceHistogram.TotalCount != _savedHistogramTotalRawCount)
            {
                throw new InvalidOperationException("Source has been modified during enumeration.");
            }
            return (TotalCountToCurrentIndex < ArrayTotalCount);
        }

        protected abstract void IncrementIterationLevel();

        protected abstract bool ReachedIterationLevel();

        protected virtual double GetPercentileIteratedTo()
        {
            return (100.0 * TotalCountToCurrentIndex) / ArrayTotalCount;
        }

        protected virtual long GetValueIteratedTo()
        {
            return SourceHistogram.HighestEquivalentValue(_currentValueAtIndex);
        }

        /// <summary>
        /// Returns the next element in the iteration.
        /// </summary>
        /// <returns>the <see cref="HistogramIterationValue"/> associated with the next element in the iteration.</returns>
        private HistogramIterationValue Next()
        {
            // Move through the sub buckets and buckets until we hit the next reporting level:
            while (!ExhaustedSubBuckets())
            {
                CountAtThisValue = SourceHistogram.GetCountAt(CurrentBucketIndex, CurrentSubBucketIndex);
                if (_freshSubBucket)
                {
                    // Don't add unless we've incremented since last bucket...
                    TotalCountToCurrentIndex += CountAtThisValue;
                    _totalValueToCurrentIndex += CountAtThisValue * SourceHistogram.MedianEquivalentValue(_currentValueAtIndex);
                    _freshSubBucket = false;
                }
                if (ReachedIterationLevel())
                {
                    var valueIteratedTo = GetValueIteratedTo();
                    _currentIterationValue.Set(
                        valueIteratedTo,
                        _prevValueIteratedTo,
                        CountAtThisValue,
                        (TotalCountToCurrentIndex - _totalCountToPrevIndex),
                        TotalCountToCurrentIndex,
                        _totalValueToCurrentIndex,
                        ((100.0 * TotalCountToCurrentIndex) / ArrayTotalCount),
                        GetPercentileIteratedTo());
                    _prevValueIteratedTo = valueIteratedTo;
                    _totalCountToPrevIndex = TotalCountToCurrentIndex;
                    // move the next iteration level forward:
                    IncrementIterationLevel();
                    if (SourceHistogram.TotalCount != _savedHistogramTotalRawCount)
                    {
                        throw new InvalidOperationException("Source has been modified during enumeration.");
                    }
                    return _currentIterationValue;
                }
                IncrementSubBucket();
            }
            // Should not reach here. But possible for overflowed histograms under certain conditions
            throw new ArgumentOutOfRangeException();
        }

        private bool ExhaustedSubBuckets()
        {
            return (CurrentBucketIndex >= SourceHistogram.BucketCount);
        }

        private void IncrementSubBucket()
        {
            _freshSubBucket = true;
            // Take on the next index:
            CurrentBucketIndex = _nextBucketIndex;
            CurrentSubBucketIndex = _nextSubBucketIndex;
            _currentValueAtIndex = _nextValueAtIndex;
            // Figure out the next next index:
            _nextSubBucketIndex++;
            if (_nextSubBucketIndex >= SourceHistogram.SubBucketCount)
            {
                _nextSubBucketIndex = SourceHistogram.SubBucketHalfCount;
                _nextBucketIndex++;
            }
            _nextValueAtIndex = SourceHistogram.ValueFromIndex(_nextBucketIndex, _nextSubBucketIndex);
        }

        #region IEnumerator explicit implementation

        object IEnumerator.Current => Current;

        bool IEnumerator.MoveNext()
        {
            var canMove = HasNext();
            if (canMove)
            {
                Current = Next();
            }
            return canMove;
        }

        void IEnumerator.Reset()
        {
            //throw new NotImplementedException();
        }

        void IDisposable.Dispose()
        {
            //throw new NotImplementedException();
        }

        #endregion
    }
}
