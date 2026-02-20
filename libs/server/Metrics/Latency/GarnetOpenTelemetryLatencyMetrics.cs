// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Garnet.common;

namespace Garnet.server.Metrics.Latency
{
    internal sealed class GarnetOpenTelemetryLatencyMetrics : IDisposable
    {
        /// <summary>
        /// The meter name used by Garnet latency metrics.
        /// </summary>
        public const string MeterName = "Microsoft.Garnet.Server.Latency";

        public static GarnetOpenTelemetryLatencyMetrics Instance { get; private set; }

        private readonly Meter meter;
        private readonly Histogram<double> latencyHistogram;
        private readonly Histogram<int> bytesPerCallHistogram;
        private readonly Histogram<int> operationsPerCallHistogram;

        public static void Initialize(bool trackLatency)
        {
            Instance = trackLatency
                ? new GarnetOpenTelemetryLatencyMetrics()
                : null;
        }

        public static void DisposeInstance()
        {
            Instance?.Dispose();
            Instance = null;
        }

        public void RecordLatency(long startTimestamp, LatencyMetricsType type)
        {
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp, Stopwatch.GetTimestamp());
            latencyHistogram?.Record(Convert.ToInt32(elapsed.TotalSeconds), new KeyValuePair<string, object>("type", type.ToString()));
        }

        public void RecordBytesProcessed(int bytes)
        {
            bytesPerCallHistogram.Record(bytes);
        }

        public void RecordOperationsProcessed(int operations)
        {
            operationsPerCallHistogram.Record(operations);
        }

        private GarnetOpenTelemetryLatencyMetrics()
        {
            this.meter = new Meter(MeterName);
            this.latencyHistogram = meter.CreateHistogram<double>("garnet.server.command.latency", unit: "s", description: "Latency of processing, per network receive call (server side).");
            this.bytesPerCallHistogram = meter.CreateHistogram<int>("garnet.server.bytes.processed", unit: "By", description: "Bytes processed, per network receive call (server side).");
            this.operationsPerCallHistogram = meter.CreateHistogram<int>("garnet.server.operations.processed", unit: "{operations}", description: "Ops processed, per network receive call (server side).");
        }

        public void Dispose()
        {
            this.meter?.Dispose();
        }
    }
}
