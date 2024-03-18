// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using HdrHistogram;

namespace Garnet.server
{
    struct LatencyMetricsEntry
    {
        static readonly long HISTOGRAM_LOWER_BOUND = 1;
        static readonly long HISTOGRAM_UPPER_BOUND = TimeStamp.Seconds(100);

        public readonly LongHistogram latency;

        public LatencyMetricsEntry()
        {
            latency = new LongHistogram(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, 2);
        }
    }
}