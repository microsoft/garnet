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
    /// An enumerator of <see cref="HistogramIterationValue"/> through the histogram using a <see cref="AllValuesEnumerator"/>
    /// </summary>
    internal sealed class AllValueEnumerable : IEnumerable<HistogramIterationValue>
    {
        private readonly HistogramBase _histogram;

        /// <summary>
        /// The constructor for the <see cref="AllValueEnumerable"/>
        /// </summary>
        /// <param name="histogram">The <see cref="HistogramBase"/> to enumerate the values from.</param>
        public AllValueEnumerable(HistogramBase histogram)
        {
            _histogram = histogram;
        }

        public IEnumerator<HistogramIterationValue> GetEnumerator()
        {
            return new AllValuesEnumerator(_histogram);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}