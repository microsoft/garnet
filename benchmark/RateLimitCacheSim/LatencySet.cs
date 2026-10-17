// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using HdrHistogram;

namespace RateLimitCacheSim
{
    /// <summary>
    /// The three latencies that have to be separated to interpret a periodic cache benchmark.
    /// </summary>
    /// <remarks>
    /// A harness that only reports <see cref="EndToEnd"/> cannot distinguish a slow server from a
    /// client that could not issue the request on time, because the two are added together.
    /// </remarks>
    public sealed class LatencySet
    {
        const long LowestTrackable = 1;

        /// <summary>100 seconds, in microseconds.</summary>
        const long HighestTrackable = 100L * 1000 * 1000;

        static readonly double TicksPerMicrosecond = Stopwatch.Frequency / 1_000_000.0;

        /// <summary>Time from the moment the operation became due until the client started sending it.</summary>
        public readonly LongHistogram Schedule = new(LowestTrackable, HighestTrackable, 3);

        /// <summary>Time from the first byte sent until the reply was parsed. The server's share.</summary>
        public readonly LongHistogram Service = new(LowestTrackable, HighestTrackable, 3);

        /// <summary>Time from the moment the operation became due until the reply was parsed.</summary>
        public readonly LongHistogram EndToEnd = new(LowestTrackable, HighestTrackable, 3);

        long completed;
        long errors;
        string firstError;

        /// <summary>Number of operations recorded.</summary>
        public long Completed => completed;

        /// <summary>Number of operations that faulted.</summary>
        public long Errors => errors;

        /// <summary>Message of the first fault seen, so that a silent misconfiguration is visible.</summary>
        public string FirstError => firstError;

        /// <summary>Converts a Stopwatch tick delta to microseconds, clamped into the histogram's range.</summary>
        /// <param name="ticks">Elapsed Stopwatch ticks.</param>
        /// <returns>Microseconds.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ToMicros(long ticks)
        {
            var micros = (long)(ticks / TicksPerMicrosecond);
            if (micros < LowestTrackable) return LowestTrackable;
            if (micros > HighestTrackable) return HighestTrackable;
            return micros;
        }

        /// <summary>
        /// Records one completed operation.
        /// </summary>
        /// <param name="dueTicks">Timestamp at which the operation should have been issued.</param>
        /// <param name="sendTicks">Timestamp at which the client actually started sending.</param>
        /// <param name="doneTicks">Timestamp at which the reply was parsed.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Record(long dueTicks, long sendTicks, long doneTicks)
        {
            Schedule.RecordValue(ToMicros(sendTicks - dueTicks));
            Service.RecordValue(ToMicros(doneTicks - sendTicks));
            EndToEnd.RecordValue(ToMicros(doneTicks - dueTicks));
            completed++;
        }

        /// <summary>Records one faulted operation.</summary>
        /// <param name="exception">The fault.</param>
        public void RecordError(Exception exception)
        {
            errors++;
            firstError ??= exception?.Message;
        }

        /// <summary>
        /// Merges another set into this one.
        /// </summary>
        /// <param name="other">Set to merge.</param>
        public void Merge(LatencySet other)
        {
            Schedule.Add(other.Schedule);
            Service.Add(other.Service);
            EndToEnd.Add(other.EndToEnd);
            completed += other.completed;
            errors += other.errors;
            firstError ??= other.firstError;
        }

        /// <summary>Clears all recorded values, used to drop the warmup window.</summary>
        public void Reset()
        {
            Schedule.Reset();
            Service.Reset();
            EndToEnd.Reset();
            completed = 0;
            errors = 0;
            firstError = null;
        }
    }
}