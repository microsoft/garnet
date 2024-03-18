/*
 * This is a .NET port of the original Java version, which was written by
 * Gil Tene as described in
 * https://github.com/HdrHistogram/HdrHistogram
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using HdrHistogram.Iteration;

namespace HdrHistogram
{
    /// <summary>
    /// Extension methods for the Histogram types.
    /// </summary>
    public static class HistogramExtensions
    {

        /// <summary>
        /// Get the highest recorded value level in the histogram
        /// </summary>
        /// <returns>the Max value recorded in the histogram</returns>
        public static long GetMaxValue(this HistogramBase histogram)
        {
            var max = histogram.RecordedValues().Select(hiv => hiv.ValueIteratedTo).LastOrDefault();
            return histogram.HighestEquivalentValue(max);
        }

        /// <summary>
        /// Get the computed mean value of all recorded values in the histogram
        /// </summary>
        /// <returns>the mean value (in value units) of the histogram data</returns>
        public static double GetMean(this HistogramBase histogram)
        {
            var totalValue = histogram.RecordedValues().Select(hiv => hiv.TotalValueToThisValue).LastOrDefault();
            return (totalValue * 1.0) / histogram.TotalCount;
        }

        /// <summary>
        /// Get the computed standard deviation of all recorded values in the histogram
        /// </summary>
        /// <returns>the standard deviation (in value units) of the histogram data</returns>
        public static double GetStdDeviation(this HistogramBase histogram)
        {
            var mean = histogram.GetMean();
            var geometricDeviationTotal = 0.0;
            foreach (var iterationValue in histogram.RecordedValues())
            {
                double deviation = (histogram.MedianEquivalentValue(iterationValue.ValueIteratedTo) * 1.0) - mean;
                geometricDeviationTotal += (deviation * deviation) * iterationValue.CountAddedInThisIterationStep;
            }
            var stdDeviation = Math.Sqrt(geometricDeviationTotal / histogram.TotalCount);
            return stdDeviation;
        }

        /// <summary>
        /// Get the highest value that is equivalent to the given value within the histogram's resolution.
        /// Where "equivalent" means that value samples recorded for any two equivalent values are counted in a common
        /// total count.
        /// </summary>
        /// <param name="histogram">The histogram to operate on</param>
        /// <param name="value">The given value</param>
        /// <returns>The highest value that is equivalent to the given value within the histogram's resolution.</returns>
        public static long HighestEquivalentValue(this HistogramBase histogram, long value)
        {
            return histogram.NextNonEquivalentValue(value) - 1;
        }

        /// <summary>
        /// Copy this histogram into the target histogram, overwriting it's contents.
        /// </summary>
        /// <param name="source">The source histogram</param>
        /// <param name="targetHistogram">the histogram to copy into</param>
        public static void CopyInto(this HistogramBase source, HistogramBase targetHistogram)
        {
            targetHistogram.Reset();
            targetHistogram.Add(source);
            targetHistogram.StartTimeStamp = source.StartTimeStamp;
            targetHistogram.EndTimeStamp = source.EndTimeStamp;
        }

        /// <summary>
        /// Provide a means of iterating through histogram values according to percentile levels. 
        /// The iteration is performed in steps that start at 0% and reduce their distance to 100% according to the
        /// <paramref name="percentileTicksPerHalfDistance"/> parameter, ultimately reaching 100% when all recorded
        /// histogram values are exhausted.
        /// </summary>
        /// <param name="histogram">The histogram to operate on</param>
        /// <param name="percentileTicksPerHalfDistance">
        /// The number of iteration steps per half-distance to 100%.
        /// </param>
        /// <returns>
        /// An enumerator of <see cref="HistogramIterationValue"/> through the histogram using a
        /// <see cref="PercentileEnumerator"/>.
        /// </returns>
        public static IEnumerable<HistogramIterationValue> Percentiles(this HistogramBase histogram, int percentileTicksPerHalfDistance)
        {
            return new PercentileEnumerable(histogram, percentileTicksPerHalfDistance);
        }

        /// <summary>
        /// Executes the action and records the time to complete the action. 
        /// The time is recorded in system clock ticks. 
        /// This time may vary between frameworks and platforms, but is equivalent to <c>(1/Stopwatch.Frequency)</c> seconds.
        /// Note this is a convenience method and can carry a cost.
        /// If the <paramref name="action"/> delegate is not cached, then it may incur an allocation cost for each invocation of <see cref="Record"/>
        /// </summary>
        /// <param name="recorder">The <see cref="IRecorder"/> instance to record the latency in.</param>
        /// <param name="action">The functionality to execute and measure</param>
        /// <remarks>
        /// <para>
        /// The units returned from <code>Stopwatch.GetTimestamp()</code> are used as the unit of 
        /// recording here as they are the smallest unit that .NET can measure and require no 
        /// conversion at time of recording. 
        /// Instead conversion (scaling) can be done at time of output to microseconds, milliseconds,
        /// seconds or other appropriate unit.
        /// These units are sometimes referred to as ticks, but should not not to be confused with 
        /// ticks used in <seealso cref="DateTime"/> or <seealso cref="TimeSpan"/>.
        /// </para>
        /// <para>
        /// If you are able to cache the <paramref name="action"/> delegate, then doing so is encouraged.
        /// <example>
        /// Here are two examples.
        /// The first does not cache the delegate
        /// 
        /// <code>
        /// for (long i = 0; i &lt; loopCount; i++)
        /// {
        ///   histogram.Record(IncrementNumber);
        /// }
        /// </code>
        /// This second example does cache the delegate
        /// <code>
        /// Action incrementNumber = IncrementNumber;
        /// for (long i = 0; i &lt; loopCount; i++)
        /// {
        ///   histogram.Record(incrementNumber);
        /// }
        /// </code>
        /// In the second example, we will not be making allocations each time i.e. an allocation of an <seealso cref="Action"/> from <code>IncrementNumber</code>.
        /// This will reduce memory pressure and therefore garbage collection penalties.
        /// For performance sensitive applications, this method may not be suitable.
        /// As always, you are encouraged to test and measure the impact for your scenario.
        /// </example>
        /// </para>
        /// </remarks>
        public static void Record(this IRecorder recorder, Action action)
        {
            var start = Stopwatch.GetTimestamp();
            action();
            var elapsed = Stopwatch.GetTimestamp() - start;
            recorder.RecordValue(elapsed);
        }

        /// <summary>
        /// Records the elapsed time till the returned token is disposed.
        /// This can be useful to testing large blocks of code, or wrapping around and <c>await</c> clause.
        /// </summary>
        /// <param name="recorder">The <see cref="IRecorder"/> instance to record the latency in.</param>
        /// <returns>Returns a token to be disposed once the scope </returns>
        /// <remarks>
        /// This can be helpful for recording a scope of work.
        /// It also has the benefit of allowing a simple way to record an awaitable method.
        /// <example>
        /// This example shows how an awaitable method can be cleanly instrumented using C# using scope.
        /// <code>
        /// using(recorder.RecordScope())
        /// {
        ///     await SomeExpensiveCall();
        /// }
        /// </code>
        /// </example>
        /// It should be noted that this method returns a token and as such allocates an object.
        /// This should taken into consideration, specifically the cost of the allocation and GC would affect the program.
        /// </remarks>
        public static IDisposable RecordScope(this IRecorder recorder)
        {
            return new Timer(recorder);
        }

        private sealed class Timer : IDisposable
        {
            private readonly IRecorder _recorder;
            private readonly long _start;

            public Timer(IRecorder recorder)
            {
                _recorder = recorder;
                _start = Stopwatch.GetTimestamp();
            }

            public void Dispose()
            {
                var elapsed = Stopwatch.GetTimestamp() - _start;
                _recorder.RecordValue(elapsed);
            }
        }
    }
}