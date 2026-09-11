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

        /// <summary>
        /// Double-buffered histograms, allocated on the first recorded value. A session typically uses
        /// only some of the latency types, and the buffers are large enough that allocating them at
        /// connection time makes the cost scale with connection count rather than with load.
        /// </summary>
        public LongHistogram[] latency;

        readonly int significantDigits;

        public LatencyMetricsEntrySession(int significantDigits)
        {
            this.significantDigits = significantDigits;
            latency = null;
            startTimestamp = 0;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void Allocate()
            => latency =
            [
                new(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, significantDigits),
                new(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, significantDigits)
            ];

        public void Return()
        {
            if (latency == null)
                return;

            latency[0].Return();
            latency[1].Return();
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
            startTimestamp = 0;

            if (latency == null) Allocate();
            latency[ver].RecordValue(IsValidRange(elapsed) ? elapsed : HISTOGRAM_UPPER_BOUND);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordValue(int ver, long elapsed)
        {
            if (elapsed == 0) return;

            if (latency == null) Allocate();
            latency[ver].RecordValue(IsValidRange(elapsed) ? elapsed : HISTOGRAM_UPPER_BOUND);
        }
    }
}