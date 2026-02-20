using System;
using System.Diagnostics.Metrics;

namespace Garnet.server.Metrics
{
    /// <summary>
    /// Provides OpenTelemetry-compatible metrics for Garnet server using <see cref="System.Diagnostics.Metrics.Meter"/>.
    /// Consumers can subscribe to these metrics using the OpenTelemetry SDK or any other <see cref="MeterListener"/>.
    /// </summary>
    internal sealed class GarnetOpenTelemetryServerMetrics : IDisposable
    {
        /// <summary>
        /// The meter name used by Garnet server metrics.
        /// </summary>
        public const string MeterName = "Microsoft.Garnet.Server";

        private readonly Meter meter;

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

            meter.CreateObservableGauge(
                "garnet.server.ops_per_sec",
                () => serverMetrics.instantaneous_cmd_per_sec,
                unit: "{operation}/s",
                description: "Instantaneous operations per second.");

            meter.CreateObservableGauge(
                "garnet.server.network.input.rate",
                () => serverMetrics.instantaneous_net_input_tpt,
                unit: "KBy/s",
                description: "Instantaneous network input throughput in KB/s.");

            meter.CreateObservableGauge(
                "garnet.server.network.output.rate",
                () => serverMetrics.instantaneous_net_output_tpt,
                unit: "KBy/s",
                description: "Instantaneous network output throughput in KB/s.");
        }

        /// <inheritdoc />
        public void Dispose()
        {
            meter.Dispose();
        }
    }
}
