// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;
using Tsavorite.core;

namespace Device.benchmark
{
    public class Options
    {
        [Option("file-size", Required = false, Default = 1 << 30, HelpText = "File size")]
        public int FileSize { get; set; }

        [Option("sector-size", Required = false, Default = 512, HelpText = "Sector size")]
        public int SectorSize { get; set; }

        [Option("file-name", Required = false, Default = "c:/data/test.dat", HelpText = "File name")]
        public string FileName { get; set; }

        [Option("device-type", Required = false, Default = DeviceType.Native, HelpText = "Device type (Native, FileStream, RandomAccess)")]
        public DeviceType DeviceType { get; set; }

        [Option("throttle-limit", Required = false, Default = 0, HelpText = "Throttle limit (0 = no limit)")]
        public int ThrottleLimit { get; set; }

        [Option("completion-threads", Required = false, Default = 0, HelpText = "Completion threads (0 = processor count)")]
        public int CompletionThreads { get; set; }

        [Option("segment-size", Required = false, Default = 1L << 30, HelpText = "Segment size (bytes)")]
        public long SegmentSize { get; set; }

        [Option('b', "batch-size", Separator = ',', Required = false, Default = new[] { 1024 }, HelpText = "Batch size, number of requests (comma separated)")]
        public IEnumerable<int> BatchSize { get; set; }

        [Option("runtime", Required = false, Default = 15, HelpText = "Run time per benchmark (seconds)")]
        public int Runtime { get; set; }

        [Option('t', "threads", Separator = ',', Default = new[] { 1, 2, 4, 8, 16, 32 }, HelpText = "Number of threads (comma separated)")]
        public IEnumerable<int> NumThreads { get; set; }
    }
}