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
    /// Used for iterating through histogram values according to percentile levels.The iteration is
    /// performed in steps that start at 0% and reduce their distance to 100% according to the
    /// <i>percentileTicksPerHalfDistance</i> parameter, ultimately reaching 100% when all recorded histogram
    /// values are exhausted.
    /// </summary>
    internal sealed class PercentileEnumerator : AbstractHistogramEnumerator
    {
        private readonly int _percentileTicksPerHalfDistance;
        private double _percentileLevelToIterateTo;
        private bool _reachedLastRecordedValue;
        
        /// <summary>
        /// The constructor for the <see cref="PercentileEnumerator"/>
        /// </summary>
        /// <param name="histogram">The histogram this iterator will operate on</param>
        /// <param name="percentileTicksPerHalfDistance">The number of iteration steps per half-distance to 100%.</param>
        public PercentileEnumerator(HistogramBase histogram, int percentileTicksPerHalfDistance) : base(histogram) 
        {
            _percentileTicksPerHalfDistance = percentileTicksPerHalfDistance;
            _percentileLevelToIterateTo = 0.0;
            _reachedLastRecordedValue = false;
        }

        protected override bool HasNext() 
        {
            if (base.HasNext())
                return true;
            // We want one additional last step to 100%
            if (!_reachedLastRecordedValue && (ArrayTotalCount > 0)) {
                _percentileLevelToIterateTo = 100.0;
                _reachedLastRecordedValue = true;
                return true;
            }
            return false;
        }

        protected override void IncrementIterationLevel() 
        {
            long percentileReportingTicks =
                    _percentileTicksPerHalfDistance *
                            (long) Math.Pow(2,
                                    (long) (Math.Log(100.0 / (100.0 - (_percentileLevelToIterateTo))) / Math.Log(2)) + 1);
            _percentileLevelToIterateTo += 100.0 / percentileReportingTicks;
        }

        protected override bool ReachedIterationLevel() 
        {
            if (CountAtThisValue == 0)
                return false;
            double currentPercentile = (100.0 * TotalCountToCurrentIndex) / ArrayTotalCount;
            return (currentPercentile >= _percentileLevelToIterateTo);
        }

        protected override double GetPercentileIteratedTo() 
        {
            return _percentileLevelToIterateTo;
        }
    }
}
