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
    /// An enumerator of <see cref="HistogramIterationValue"/> through the histogram using a <see cref="RecordedValuesEnumerator"/>
    /// </summary>
    internal sealed class RecordedValuesEnumerable : IEnumerable<HistogramIterationValue>
    {
        private readonly HistogramBase _histogram;

        public RecordedValuesEnumerable(HistogramBase histogram)
        {
            _histogram = histogram;
        }

        public IEnumerator<HistogramIterationValue> GetEnumerator()
        {
            return new RecordedValuesEnumerator(_histogram);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}