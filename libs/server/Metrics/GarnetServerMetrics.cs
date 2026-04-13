// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    internal struct GarnetServerMetrics
    {
        /// <summary>
        /// Server metrics
        /// </summary>
        public long total_connections_received;
        public long total_connections_disposed;
        public long total_connections_active;

        /// <summary>
        /// Instantaneous metrics
        /// </summary>
        public static readonly int byteUnit = 1 << 10;
        public double instantaneous_cmd_per_sec;
        public double instantaneous_net_input_tpt;
        public double instantaneous_net_output_tpt;

        /// <summary>
        /// Global session metrics
        /// </summary>
        public GarnetSessionMetrics globalSessionMetrics;

        /// <summary>
        /// History of session metrics.
        /// </summary>
        public GarnetSessionMetrics historySessionMetrics;

        /// <summary>
        /// Global latency metrics per command.
        /// </summary>
        public readonly GarnetLatencyMetrics globalLatencyMetrics;

        /// <summary>
        /// Global per-command usage statistics (calls, failures, rejections, latency).
        /// </summary>
        public CommandStats globalCommandStats;

        /// <summary>
        /// History of per-command usage statistics from disposed sessions.
        /// </summary>
        public CommandStats historyCommandStats;

        public GarnetServerMetrics(bool trackStats, bool trackLatency, bool trackCommandStats, GarnetServerMonitor monitor)
        {
            total_connections_received = 0;
            total_connections_disposed = 0;
            total_connections_active = 0;

            instantaneous_cmd_per_sec = 0;
            instantaneous_net_input_tpt = 0;
            instantaneous_net_output_tpt = 0;

            globalSessionMetrics = trackStats ? new GarnetSessionMetrics() : null;
            historySessionMetrics = trackStats ? new GarnetSessionMetrics() : null;

            globalLatencyMetrics = trackLatency ? new() : null;

            globalCommandStats = trackCommandStats ? new CommandStats() : null;
            historyCommandStats = trackCommandStats ? new CommandStats() : null;
        }

        public void Dispose()
            => globalLatencyMetrics?.Return();
    }
}