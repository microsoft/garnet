// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics.Metrics;

namespace Garnet.server.Metrics
{
    internal sealed class GarnetOpenTelemetrySessionMetrics : IDisposable
    {
        /// <summary>
        /// The meter name used by Garnet session metrics.
        /// </summary>
        public const string MeterName = "Microsoft.Garnet.Session";

        private readonly Meter meter;

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
                "garnet.server.commands.write",
                () => Convert.ToInt64(globalSessionMetrics.get_total_write_commands_processed()),
                unit: "{command}",
                description: "Total number of write commands processed.");

            meter.CreateObservableCounter(
                "garnet.server.commands.read",
                () => Convert.ToInt64(globalSessionMetrics.get_total_read_commands_processed()),
                unit: "{command}",
                description: "Total number of read commands processed.");

            meter.CreateObservableCounter(
                "garnet.server.commands.cluster",
                () => Convert.ToInt64(globalSessionMetrics.get_total_cluster_commands_processed()),
                unit: "{command}",
                description: "Total number of cluster commands processed.");

            meter.CreateObservableCounter(
                "garnet.server.network.input.bytes",
                () => Convert.ToInt64(globalSessionMetrics.get_total_net_input_bytes()),
                unit: "By",
                description: "Total number of bytes received from the network.");

            meter.CreateObservableCounter(
                "garnet.server.network.output.bytes",
                () => Convert.ToInt64(globalSessionMetrics.get_total_net_output_bytes()),
                unit: "By",
                description: "Total number of bytes sent to the network.");

            meter.CreateObservableCounter(
                "garnet.server.cache.hits",
                () => Convert.ToInt64(globalSessionMetrics.get_total_found()),
                unit: "{hit}",
                description: "Total number of cache hits (successful key lookups).");

            meter.CreateObservableCounter(
                "garnet.server.cache.misses",
                () => Convert.ToInt64(globalSessionMetrics.get_total_notfound()),
                unit: "{miss}",
                description: "Total number of cache misses (unsuccessful key lookups).");

            meter.CreateObservableGauge(
                "garnet.server.cache.hit_rate",
                () =>
                {
                    var found = globalSessionMetrics.get_total_found();
                    var notFound = globalSessionMetrics.get_total_notfound();
                    var total = found + notFound;
                    return Convert.ToInt64(total > 0 ? found / total * 100 : 0);
                },
                unit: "%",
                description: "Cache hit rate as a percentage.");

            meter.CreateObservableCounter(
                "garnet.server.pending",
                () => Convert.ToInt64(globalSessionMetrics.get_total_pending()),
                unit: "{operation}",
                description: "Total number of pending operations.");

            meter.CreateObservableCounter(
                "garnet.server.transactions.received",
                () => Convert.ToInt64(globalSessionMetrics.get_total_transaction_commands_received()),
                unit: "{transaction}",
                description: "Total number of transaction commands received.");

            meter.CreateObservableCounter(
                "garnet.server.transactions.failed",
                () => Convert.ToInt64(globalSessionMetrics.get_total_transaction_commands_execution_failed()),
                unit: "{transaction}",
                description: "Total number of transaction command executions that failed.");

            meter.CreateObservableCounter(
                "garnet.server.exceptions",
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
