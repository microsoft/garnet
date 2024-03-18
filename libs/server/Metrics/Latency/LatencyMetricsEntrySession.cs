// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using HdrHistogram;

namespace Garnet.server
{
    struct LatencyMetricsEntrySession
    {
        static readonly long HISTOGRAM_LOWER_BOUND = 1;
        static readonly long HISTOGRAM_UPPER_BOUND = TimeStamp.Seconds(100);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool IsValidRange(long value)
            => value < HISTOGRAM_UPPER_BOUND && value >= HISTOGRAM_LOWER_BOUND;

        public long startTimestamp;
        public readonly LongHistogram[] latency;

        public LatencyMetricsEntrySession()
        {
            latency = new LongHistogram[2] { new(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, 2), new(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, 2) };
            startTimestamp = 0;
        }

        public void Start()
        {
            startTimestamp = Stopwatch.GetTimestamp();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordValue(int ver)
        {
            if (startTimestamp == 0) return;

            long elapsed = Stopwatch.GetTimestamp() - startTimestamp;
            if (IsValidRange(elapsed))
                latency[ver].RecordValue(elapsed);
            else
                latency[ver].RecordValue(HISTOGRAM_UPPER_BOUND);
            startTimestamp = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordValue(int ver, long elapsed)
        {
            if (elapsed == 0) return;
            if (IsValidRange(elapsed))
                latency[ver].RecordValue(elapsed);
            else
                latency[ver].RecordValue(HISTOGRAM_UPPER_BOUND);
        }
    }

}