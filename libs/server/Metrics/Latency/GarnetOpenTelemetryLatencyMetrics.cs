// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Garnet.common;

namespace Garnet.server.Metrics.Latency
{

    /// <summary>
    /// Provides OpenTelemetry-compatible latency metrics for Garnet server using <see cref="Meter"/>.
    /// Tracks command latency, bytes processed, and operations processed per network receive call.
    /// This class follows a singleton pattern via <see cref="Initialize"/> and <see cref="DisposeInstance"/>.
    /// Consumers can subscribe to these metrics using the OpenTelemetry SDK or any other <see cref="MeterListener"/>.
    /// </summary>
    internal sealed class GarnetOpenTelemetryLatencyMetrics : IDisposable
    {
        /// <summary>
        /// The meter name used by Garnet latency metrics.
        /// </summary>
        public const string MeterName = "Microsoft.Garnet.Server.Latency";

        /// <summary>
        /// Gets the singleton instance of <see cref="GarnetOpenTelemetryLatencyMetrics"/>,
        /// or <c>null</c> if latency tracking is disabled.
        /// </summary>
        public static GarnetOpenTelemetryLatencyMetrics Instance { get; private set; }

        /// <summary>
        /// The <see cref="Meter"/> used to create all latency-related instruments.
        /// </summary>
        private readonly Meter meter;

        /// <summary>
        /// Histogram that records command processing latency (in seconds) per network receive call.
        /// Tagged with the <see cref="LatencyMetricsType"/> of the recorded operation.
        /// </summary>
        private readonly Histogram<double> latencyHistogram;

        /// <summary>
        /// Histogram that records the number of bytes processed per network receive call.
        /// </summary>
        private readonly Histogram<int> bytesPerCallHistogram;

        /// <summary>
        /// Histogram that records the number of operations processed per network receive call.
        /// </summary>
        private readonly Histogram<int> operationsPerCallHistogram;

        /// <summary>
        /// Initializes the singleton <see cref="Instance"/>.
        /// If <paramref name="trackLatency"/> is <c>true</c>, a new instance is created;
        /// otherwise, <see cref="Instance"/> is set to <c>null</c>.
        /// </summary>
        /// <param name="trackLatency">Whether to enable latency tracking.</param>
        public static void Initialize(bool trackLatency)
        {
            Instance = trackLatency
                ? new GarnetOpenTelemetryLatencyMetrics()
                : null;
        }

        /// <summary>
        /// Disposes the current singleton <see cref="Instance"/> and sets it to <c>null</c>.
        /// </summary>
        public static void DisposeInstance()
        {
            Instance?.Dispose();
            Instance = null;
        }

        /// <summary>
        /// Records the elapsed time since <paramref name="startTimestamp"/> as a latency measurement.
        /// </summary>
        /// <param name="startTimestamp">A timestamp obtained from <see cref="Stopwatch.GetTimestamp"/> at the start of the operation.</param>
        /// <param name="type">The <see cref="LatencyMetricsType"/> categorizing this latency measurement.</param>
        public void RecordLatency(long startTimestamp, LatencyMetricsType type)
        {
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp, Stopwatch.GetTimestamp());
            latencyHistogram?.Record(elapsed.TotalSeconds, new KeyValuePair<string, object>("type", type.ToString()));
        }

        /// <summary>
        /// Records the number of bytes processed in a single network receive call.
        /// </summary>
        /// <param name="bytes">The number of bytes processed.</param>
        public void RecordBytesProcessed(long bytes)
        {
            bytesPerCallHistogram.Record(Convert.ToInt32(Math.Clamp(bytes, 0, int.MaxValue)));
        }

        /// <summary>
        /// Records the number of operations processed in a single network receive call.
        /// </summary>
        /// <param name="operations">The number of operations processed.</param>
        public void RecordOperationsProcessed(long operations)
        {
            operationsPerCallHistogram.Record(Convert.ToInt32(Math.Clamp(operations, 0, int.MaxValue)));
        }

        /// <summary>
        /// Initializes a new instance of <see cref="GarnetOpenTelemetryLatencyMetrics"/>,
        /// creating the <see cref="Meter"/> and all histogram instruments.
        /// </summary>
        private GarnetOpenTelemetryLatencyMetrics()
        {
            this.meter = new Meter(MeterName);
            this.latencyHistogram = meter.CreateHistogram<double>("garnet.server.command.latency", unit: "s", description: "Latency of processing, per network receive call (server side).");
            this.bytesPerCallHistogram = meter.CreateHistogram<int>("garnet.server.bytes.processed", unit: "By", description: "Bytes processed, per network receive call (server side).");
            this.operationsPerCallHistogram = meter.CreateHistogram<int>("garnet.server.operations.processed", unit: "{operations}", description: "Ops processed, per network receive call (server side).");
        }

        /// <summary>
        /// Disposes the underlying <see cref="Meter"/> and releases associated resources.
        /// </summary>
        public void Dispose()
        {
            this.meter?.Dispose();
        }
    }
}
