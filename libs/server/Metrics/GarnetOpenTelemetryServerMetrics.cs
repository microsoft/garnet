// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics.Metrics;

namespace Garnet.server.Metrics
{
    /// <summary>
    /// Provides OpenTelemetry-compatible metrics for Garnet server using <see cref="Meter"/>.
    /// Consumers can subscribe to these metrics using the OpenTelemetry SDK or any other <see cref="MeterListener"/>.
    /// The command-rate and network rates are not exposed as metrics as they can be calculated based on the other exposed metrics.
    /// </summary>
    internal sealed class GarnetOpenTelemetryServerMetrics : IDisposable
    {
        /// <summary>
        /// The meter name used by Garnet server metrics.
        /// </summary>
        public const string MeterName = "Microsoft.Garnet.Server";

        private readonly Meter meter;

        /// <summary>
        /// Initializes a new instance of the <see cref="GarnetOpenTelemetryServerMetrics"/> class,
        /// creating observable instruments that expose server connection metrics via a <see cref="Meter"/>.
        /// </summary>
        /// <param name="serverMetrics">
        /// The <see cref="GarnetServerMetrics"/> instance whose connection counters
        /// (active, received, and disposed) are observed by the created instruments.
        /// </param>
        internal GarnetOpenTelemetryServerMetrics(GarnetServerMetrics serverMetrics)
        {
            meter = new Meter(MeterName);

            meter.CreateObservableGauge(
                "garnet.server.connections.active",
                () => serverMetrics.total_connections_active,
                unit: "{connection}",
                description: "Number of currently active client connections.");

            meter.CreateObservableCounter(
                "garnet.server.connections.received",
                () => serverMetrics.total_connections_received,
                unit: "{connection}",
                description: "Total number of client connections received.");

            meter.CreateObservableCounter(
                "garnet.server.connections.disposed",
                () => serverMetrics.total_connections_disposed,
                unit: "{connection}",
                description: "Total number of client connections disposed.");
        }

        /// <inheritdoc />
        public void Dispose()
        {
            meter.Dispose();
        }
    }
}
