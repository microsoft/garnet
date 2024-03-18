/*
 * This is a .NET port of the original Java version, which was written by
 * Gil Tene as described in
 * https://github.com/HdrHistogram/HdrHistogram
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

using System.Diagnostics;

namespace HdrHistogram
{
    /// <summary>
    /// Provides constants to use in selecting a scaling factor for output of a histograms recordings.
    /// </summary>
    public static class OutputScalingFactor
    {
        /// <summary>
        /// For use when values are recorded and reported in the same unit of measurement.
        /// </summary>
        public const double None = 1.0;
        
        /// <summary>
        /// For use when values are recorded with <seealso cref="Stopwatch.GetTimestamp()"/> and output should be reported in microseconds.
        /// </summary>
        public static readonly double TimeStampToMicroseconds = Stopwatch.Frequency / (1000d * 1000d);

        /// <summary>
        /// For use when values are recorded with <seealso cref="Stopwatch.GetTimestamp()"/> and output should be reported in milliseconds.
        /// </summary>
        public static readonly double TimeStampToMilliseconds = Stopwatch.Frequency / 1000d;

        /// <summary>
        /// For use when values are recorded with <seealso cref="Stopwatch.GetTimestamp()"/> and output should be reported in seconds.
        /// </summary>
        public static readonly double TimeStampToSeconds = Stopwatch.Frequency;
    }
}