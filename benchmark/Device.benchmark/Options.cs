// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using CommandLine;
using Microsoft.Extensions.Logging;

namespace Resp.benchmark
{
    public class Options
    {
        [Option("filesize", Required = false, Default = 1 << 30, HelpText = "File size")]
        public int FileSize { get; set; }

        [Option("sectorsize", Required = false, Default = 512, HelpText = "Sector size")]
        public int SectorSize { get; set; }

        [Option("filename", Required = false, Default = "c:/data/test.dat", HelpText = "File name")]
        public string FileName { get; set; }

        [Option("device", Required = false, Default = DeviceType.LocalStorage, HelpText = "Device type (LocalStorage, ManagedLocalStorage, RandomAccessLocalStorage)")]
        public DeviceType Device { get; set; }

        [Option('b', "batchsize", Separator = ',', Required = false, Default = new[] { 1024 }, HelpText = "Batch size, number of requests (comma separated)")]
        public IEnumerable<int> BatchSize { get; set; }

        [Option("runtime", Required = false, Default = 15, HelpText = "Run time per benchmark (seconds)")]
        public int RunTime { get; set; }

        [Option('t', "threads", Separator = ',', Default = new[] { 1, 2, 4, 8, 16, 32 }, HelpText = "Number of threads (comma separated)")]
        public IEnumerable<int> NumThreads { get; set; }

        [Option("logger-level", Required = false, Default = LogLevel.Information, HelpText = "Logging level")]
        public LogLevel LogLevel { get; set; }

        [Option("disable-console-logger", Required = false, Default = false, HelpText = "Disable console logger.")]
        public bool DisableConsoleLogger { get; set; }

        [Option("file-logger", Required = false, Default = null, HelpText = "Enable file logger and write to the specified path.")]
        public string FileLogger { get; set; }
    }
}