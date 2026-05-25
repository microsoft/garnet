// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using CommandLine;
using Tsavorite.core;

namespace Device.benchmark
{
    public class Options
    {
        [Option("file-size", Required = false, Default = 1L << 30, HelpText = "File size (bytes)")]
        public long FileSize { get; set; }

        [Option("sector-size", Required = false, Default = 512, HelpText = "Sector size")]
        public int SectorSize { get; set; }

        [Option("file-name", Required = false, Default = "c:/data/test.dat", HelpText = "File name")]
        public string FileName { get; set; }

        [Option("device-type", Required = false, Default = DeviceType.Native, HelpText = "Device type (Native, FileStream, RandomAccess)")]
        public DeviceType DeviceType { get; set; }

        [Option("throttle-limit", Required = false, Default = 0, HelpText = "Max device-level in-flight ops (0 = no throttle). Note: for Native libaio the kernel io_context is only 128 slots wide — running with --throttle-limit 0 plus high QD (threads × batch > 128) floods the ring and the kernel returns EAGAIN per request (surfaced as Status::IOError=4). The benchmark reports these as errors; throughput uses successful completions only. Set to 128 (matches both the libaio io_context capacity and the io_uring SQ depth this build uses) to avoid flood.")]
        public int ThrottleLimit { get; set; }

        [Option("completion-threads", Required = false, Default = 0, HelpText = "Number of background drainer threads that wait on IO completions (0 = processor count on Windows, 1 on Linux). On Linux Native, all completion threads drain the same kernel io_context (libaio) / io_uring (uring) — adding more is rarely useful past 1 today.")]
        public int CompletionThreads { get; set; }

        [Option("io-backend", Required = false, Default = "default", HelpText = "Linux Native IO backend: default, libaio, uring. Ignored on other devices/OSes. Unknown values are rejected at startup.")]
        public string IoBackend { get; set; }

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