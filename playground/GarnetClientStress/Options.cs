// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace GarnetClientStress
{
    public class Options
    {
        [Option('p', "port", Required = false, Default = 3278, HelpText = "Port to connect to")]
        public int Port { get; set; }

        [Option('h', "host", Required = false, Default = "127.0.0.1", HelpText = "IP address to connect to")]
        public string? Address { get; set; }

        [Option("dbsize", Required = false, Default = 1 << 10, HelpText = "DB size")]
        public int DbSize { get; set; }

        [Option("valuelength", Required = false, Default = 8, HelpText = "Value length (bytes) - 0 indicates use key as value")]
        public int ValueLength { get; set; }

        [Option("runtime", Required = false, Default = 15, HelpText = "Run time (seconds)")]
        public int RunTime { get; set; }

        [Option("tls", Required = false, Default = false, HelpText = "Enable TLS.")]
        public bool EnableTLS { get; set; }

        [Option("readpercent", Required = false, Default = 10, HelpText = "Read percent (online bench only).")]
        public int ReadPercent { get; set; }

        [Option("pc", Required = false, Default = 1, HelpText = "Number of server partitions")]
        public int PartitionCount { get; set; }

        [Option("mxr", Required = false, Default = 60, HelpText = "Max number of outstanding requests generated from operation thread.")]
        public int MaxOutstandingRequests { get; set; }

        [Option("cmr", Required = false, Default = 1 << 10, HelpText = "Max number of outstanding tasks allowed from the client.")]
        public int ClientMaxOutstandingTasks { get; set; }

        [Option("stype", Required = false, Default = StressTestType.TaskScaling, HelpText = "Choose stress test type (TaskScaling, PingDispose)")]
        public StressTestType StressType { get; set; }
    }
}