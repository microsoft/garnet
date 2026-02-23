// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics.Metrics;

namespace Garnet.server.Metrics
{
    /// <summary>
    /// Exposes Garnet server session metrics as OpenTelemetry instruments using <see cref="Meter"/>.
    /// Registers observable counters and gauges that report command processing, network I/O,
    /// cache lookup, and session exception statistics from a <see cref="GarnetSessionMetrics"/> instance.
    /// </summary>
    internal sealed class GarnetOpenTelemetrySessionMetrics : IDisposable
    {
        /// <summary>
        /// The meter name used by Garnet session metrics.
        /// </summary>
        public const string MeterName = "Microsoft.Garnet.Server.Session";

        /// <summary>
        /// The <see cref="Meter"/> instance used to create and manage OpenTelemetry instruments
        /// for session-level metrics.
        /// </summary>
        private readonly Meter meter;

        /// <summary>
        /// Initializes a new instance of the <see cref="GarnetOpenTelemetrySessionMetrics"/> class,
        /// creating observable counters and gauges that report session-level statistics from the
        /// specified <paramref name="globalSessionMetrics"/> instance.
        /// </summary>
        /// <param name="globalSessionMetrics">
        /// The <see cref="GarnetSessionMetrics"/> instance that supplies the aggregated session statistics.
        /// Must not be <see langword="null"/>.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="globalSessionMetrics"/> is <see langword="null"/>.
        /// </exception>
        internal GarnetOpenTelemetrySessionMetrics(GarnetSessionMetrics globalSessionMetrics)
        {
            if (globalSessionMetrics == null)
            {
                throw new ArgumentNullException(nameof(globalSessionMetrics));
            }

            meter = new Meter(MeterName);

            meter.CreateObservableCounter(
                "garnet.server.commands.processed",
                () => Convert.ToInt64(globalSessionMetrics.get_total_commands_processed()),
                unit: "{command}",
                description: "Total number of commands processed.");

            meter.CreateObservableCounter(
                "garnet.server.transaction.commands.received",
                () => Convert.ToInt64(globalSessionMetrics.get_total_transaction_commands_received()),
                unit: "{command}",
                description: "Total number of transaction commands received.");

            meter.CreateObservableCounter(
                "garnet.server.transaction.commands.failed",
                () => Convert.ToInt64(globalSessionMetrics.get_total_transaction_commands_execution_failed()),
                unit: "{commands}",
                description: "Total number of transaction command executions that failed.");

            meter.CreateObservableCounter(
                "garnet.server.write.commands.processed",
                () => Convert.ToInt64(globalSessionMetrics.get_total_write_commands_processed()),
                unit: "{command}",
                description: "Total number of write commands processed.");

            meter.CreateObservableCounter(
                "garnet.server.read.commands.processed",
                () => Convert.ToInt64(globalSessionMetrics.get_total_read_commands_processed()),
                unit: "{command}",
                description: "Total number of read commands processed.");

            meter.CreateObservableCounter(
                "garnet.server.cluster.commands.processed",
                () => Convert.ToInt64(globalSessionMetrics.get_total_cluster_commands_processed()),
                unit: "{command}",
                description: "Total number of cluster commands processed.");

            meter.CreateObservableCounter(
                "garnet.server.network.bytes.received",
                () => Convert.ToInt64(globalSessionMetrics.get_total_net_input_bytes()),
                unit: "By",
                description: "Total number of bytes received from the network.");

            meter.CreateObservableCounter(
                "garnet.server.network.bytes.sent",
                () => Convert.ToInt64(globalSessionMetrics.get_total_net_output_bytes()),
                unit: "By",
                description: "Total number of bytes sent to the network.");

            meter.CreateObservableCounter(
                "garnet.server.cache.lookups",
                () => Convert.ToInt64(globalSessionMetrics.get_total_found())  + Convert.ToInt64(globalSessionMetrics.get_total_notfound()),
                unit: "{lookup}",
                description: "Total number of cache lookups.");

            meter.CreateObservableCounter(
                "garnet.server.cache.lookups.missed",
                () => Convert.ToInt64(globalSessionMetrics.get_total_notfound()),
                unit: "{miss}",
                description: "Total number of cache misses (unsuccessful key lookups).");

            meter.CreateObservableGauge(
                "garnet.server.operations.pending",
                () => Convert.ToInt64(globalSessionMetrics.get_total_pending()),
                unit: "{operation}",
                description: "Current number of pending operations.");

            meter.CreateObservableCounter(
                "garnet.server.resp.session.exceptions",
                () => Convert.ToInt64(globalSessionMetrics.get_total_number_resp_server_session_exceptions()),
                unit: "{exception}",
                description: "Total number of RESP server session exceptions.");
        }

        /// <inheritdoc />
        public void Dispose()
        {
            meter.Dispose();
        }
    }
}
