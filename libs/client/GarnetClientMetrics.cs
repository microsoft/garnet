// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.networking;
using HdrHistogram;

namespace Garnet.client
{
    public sealed partial class GarnetClient : IServerHook, IMessageConsumer, IDisposable
    {
        readonly LongHistogram latency;

        /// <summary>
        /// Get client latency histogram
        /// </summary>
        public HistogramBase GetLatencyHistogram => latency?.Copy();

        /// <summary>
        /// Reset internal latency histogram if enabled
        /// </summary>
        public void ResetLatencyHistogram() => latency?.Reset();

        private MetricsItem[] GetPercentiles(LongHistogram longHistogram, double scaling)
        {
            var histogram = GetLatencyHistogram;
            if (histogram == null || histogram.TotalCount == 0)
                return Array.Empty<MetricsItem>();

            var _min = (histogram.GetValueAtPercentile(0) / scaling).ToString("N2");
            var _5 = (histogram.GetValueAtPercentile(5) / scaling).ToString("N2");
            var _50 = (histogram.GetValueAtPercentile(50) / scaling).ToString("N2");
            var _mean = (histogram.GetMean() / scaling).ToString("N2");
            var _95 = (histogram.GetValueAtPercentile(95) / scaling).ToString("N2");
            var _99 = (histogram.GetValueAtPercentile(99) / scaling).ToString("N2");
            var _999 = (histogram.GetValueAtPercentile(99.9) / scaling).ToString("N2");

            MetricsItem[] percentiles = new MetricsItem[]
            {
                new("calls", latency.TotalCount.ToString()),
                new("min", _min),
                new("5th", _5),
                new("50th", _50),
                new("mean", _mean),
                new("95th", _95),
                new("99th", _99),
                new("99.9th", _999)
            };

            return percentiles;
        }

        private MetricsItem[] GetLatencyPercentiles() => GetPercentiles(latency, OutputScalingFactor.TimeStampToMicroseconds);

        /// <summary>
        /// Return request latency histogram.
        /// </summary>
        /// <returns></returns>
        public MetricsItem[] GetLatencyMetrics() => GetLatencyPercentiles();

        /// <summary>
        /// Dump histogram values to console
        /// </summary>
        public void DumpLatencyHistToConsole(bool withHeader)
        {
            if (latency == null || latency.TotalCount == 0) return;
            var percentiles = GetLatencyMetrics();
            if (withHeader)
            {
                for (int i = 1; i < percentiles.Length; i++)
                    Console.Write("{0};", percentiles[i].Name);
                Console.WriteLine("{0};", percentiles[0].Name);
            }
            for (int i = 1; i < percentiles.Length; i++)
                Console.Write("{0};", percentiles[i].Value);
            Console.WriteLine("{0};", percentiles[0].Value);
        }

        /// <summary>
        /// Return histogram of outstanding requests.
        /// </summary>
        /// <returns></returns>
        public MetricsItem[] GetOutstandingRequestsMetrics() => GetLatencyPercentiles();
    }
}