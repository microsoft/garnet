// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;
using Garnet.common;

namespace MetricsMonitor
{
    public class Options
    {
        [Option('p', "port", Required = false, Default = 6379, HelpText = "Port to connect to")]
        public int Port { get; set; }

        [Option('h', "host", Required = false, Default = "127.0.0.1", HelpText = "IP address to connect to")]
        public string Address { get; set; }

        [Option("tls", Required = false, Default = false, HelpText = "Enable TLS.")]
        public bool EnableTLS { get; set; }

        [Option("tlshost", Required = false, Default = "GarnetTest", HelpText = "TLS remote host name.")]
        public string TlsHost { get; set; }

        [Option("poll", Required = false, Default = 5, HelpText = "Poll frequency (seconds)")]
        public int Poll { get; set; }

        [Option("latency-metrics-type", Required = false, Default = LatencyMetricsType.NET_RS_LAT, HelpText = "Latency metrics types to track (NET_RS_LAT)")]
        public LatencyMetricsType LatencyEvent { get; set; }

        [Option("info-metrics-type", Required = false, Default = InfoMetricsType.STATS, HelpText = "Info metrics types to track (SERVER, MEMORY, CLUSTER, STATS, STORE, ALL)")]
        public InfoMetricsType infoType { get; set; }

        [Option("metrics", Required = false, Default = Metric.INFO, HelpText = "What type of server side metrics to retrieve (LATENCY, INFO)")]
        public Metric MetricsType { get; set; }

        [Option("cluster", Required = false, Default = false, HelpText = "Cluster mode benchmark enable")]
        public bool Cluster { get; set; }
    }
}