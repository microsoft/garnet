// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;
using Microsoft.Extensions.Logging;

namespace MigrateBench
{
    public partial class Options
    {
        [Option('s', "source-endpoint", Required = false, Default = "127.0.0.1:7000", HelpText = "SourceEndpoint")]
        public string SourceEndpoint { get; set; }

        [Option('t', "target-endpoint", Required = false, Default = "127.0.0.1:7001", HelpText = "TargetEndpoint")]
        public string TargetEndpoint { get; set; }

        [Option("slots", Separator = ',', Default = null, HelpText = "Slots to migrate")]
        public IEnumerable<int> Slots { get; set; }

        [Option("mtype", Required = false, Default = MigrateRequestType.SLOTSRANGE, HelpText = "Operation type (GET, MGET, INCR, PING, ZADDREM, PFADD, ZADDCARD)")]
        public MigrateRequestType migrateRequestType { get; set; }

        [Option("logger-level", Required = false, Default = LogLevel.Information, HelpText = "Logging level")]
        public LogLevel LogLevel { get; set; }

        [Option("dbsize", Required = false, Default = false, HelpText = "Count keys for between instances")]
        public bool Dbsize { get; set; }

        [Option("timeout", Required = false, Default = 10, HelpText = "Migrate timeout in seconds")]
        public int Timeout { get; set; }
    }
}