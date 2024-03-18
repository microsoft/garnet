/*
 * This is a .NET port of the original Java version, which was written by
 * Gil Tene as described in
 * https://github.com/HdrHistogram/HdrHistogram
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

using System.Collections;
using System.Collections.Generic;

namespace HdrHistogram.Iteration
{
    /// <summary>
    /// An enumerator of <see cref="HistogramIterationValue"/> through the histogram using a <see cref="PercentileEnumerator"/>
    /// </summary>
    internal sealed class PercentileEnumerable : IEnumerable<HistogramIterationValue>
    {
        private readonly HistogramBase _histogram;
        private readonly int _percentileTicksPerHalfDistance;

        public PercentileEnumerable(HistogramBase histogram, int percentileTicksPerHalfDistance)
        {
            _histogram = histogram;
            _percentileTicksPerHalfDistance = percentileTicksPerHalfDistance;
        }

        public IEnumerator<HistogramIterationValue> GetEnumerator()
        {
            return new PercentileEnumerator(_histogram, _percentileTicksPerHalfDistance);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}