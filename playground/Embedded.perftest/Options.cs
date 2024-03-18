// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using CommandLine;
using Microsoft.Extensions.Logging;

namespace Embedded.perftest
{
    public class Options
    {
        [Option("runtime", Required = false, Default = 15, HelpText = "Run time (seconds)")]
        public int RunTime { get; set; }

        [Option('t', "threads", Separator = ',', Default = new[] { 1, 2, 4, 8, 16, 32 }, HelpText = "Number of threads (comma separated)")]
        public IEnumerable<int> NumThreads { get; set; }

        [Option("batch-size", Required = false, Default = 128, HelpText = "Number of operations per batch")]
        public int BatchSize { get; set; }

        [Option("op-workload", Separator = ',', Default = new[] { OperationType.PING, OperationType.DBSIZE, OperationType.CLIENT, OperationType.ECHO }, HelpText = "Commands to include in the workload")]
        public IEnumerable<OperationType> OpWorkload { get; set; }

        [Option("op-percent", Separator = ',', Default = new[] { 25, 25, 25, 25 }, HelpText = "Percent of commands executed from workload")]
        public IEnumerable<int> OpPercent { get; set; }

        [Option("seed", Required = false, Default = 0, HelpText = "Seed to initialize random number generation")]
        public int Seed { get; set; }

        [Option("logger-level", Required = false, Default = LogLevel.Information, HelpText = "Logging level")]
        public LogLevel LogLevel { get; set; }

        [Option("disable-console-logger", Required = false, Default = false, HelpText = "Disable console logger")]
        public bool DisableConsoleLogger { get; set; }

        [Option("file-logger", Required = false, Default = null, HelpText = "Enable file logger and write to the specified path")]
        public string FileLogger { get; set; }
    }
}