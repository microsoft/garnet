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

        [Option("device-type", Required = false, Default = DeviceType.Native, HelpText = "Device type (Native, FileStream, RandomAccess, LocalMemory). For LocalMemory, --file-name and --device-io-backend are ignored.")]
        public DeviceType DeviceType { get; set; }

        [Option("device-throttle-limit", Required = false, Default = 0, HelpText = "Aggregate max device-level in-flight ops (software backpressure; 0 = no throttle). Capped at device-io-contexts * device-queue-depth. Note: for Native libaio the kernel io_context is only 128 slots wide — running with --device-throttle-limit 0 plus high QD (threads × batch > 128) floods the ring and the kernel returns EAGAIN per request (surfaced as Status::IOError=4). The benchmark reports these as errors; throughput uses successful completions only.")]
        public int ThrottleLimit { get; set; }

        [Option("device-completion-threads", Required = false, Default = 0, HelpText = "Number of background drainer threads that wait on IO completions (0 = processor count on Windows, 1 on Linux). On Linux Native, each drainer is bound 1:1 to its own kernel io_context (libaio) or io_uring ring; submitters distribute across contexts/rings via per-thread affinity. For DeviceType.LocalMemory, each drainer owns one SPSC ring fed by one submitter via per-thread routing. Throughput scales with this value up to the available submitter concurrency.")]
        public int CompletionThreads { get; set; }

        [Option("device-io-contexts", Required = false, Default = 0, HelpText = "Linux Native only: number of independent kernel io_contexts / io_uring rings, decoupled from --device-completion-threads. Submitters map to rings via per-thread affinity, so setting this >= submitter concurrency makes io_submit contention-free and spreads completions across more rings; the drainers each range-drain a contiguous slice. 0 = 1 ring per drainer (default). Clamped up to --device-completion-threads.")]
        public int IoContexts { get; set; }

        [Option("device-queue-depth", Required = false, Default = 0, HelpText = "Linux Native only: per-ring kernel queue depth (io_uring SQ entries / libaio io_context nr_events) for each --device-io-contexts ring. 0 = default (4096). Cap 32768 (io_uring hard limit). For libaio, device-io-contexts * device-queue-depth is drawn from the global fs.aio-max-nr budget (warned/clamped if exceeded).")]
        public int QueueDepth { get; set; }

        [Option("device-io-backend", Required = false, Default = "default", HelpText = "Linux Native IO backend: default, libaio, uring. Ignored on other devices/OSes. Unknown values are rejected at startup.")]
        public string IoBackend { get; set; }

        [Option("device-uring-sqpoll", Required = false, Default = false, HelpText = "io_uring only: enable IORING_SETUP_SQPOLL so a kernel thread polls the submission queue and submissions are syscall-free. Each ring gets its own poll thread. Ignored for libaio / on Windows.")]
        public bool DeviceUringSqPoll { get; set; }

        [Option("device-uring-sqpoll-idle-ms", Required = false, Default = 0, HelpText = "io_uring SQPOLL poll-thread idle window in milliseconds (sq_thread_idle). 0 = native default (10000). Only meaningful with --device-uring-sqpoll.")]
        public int DeviceUringSqPollIdleMs { get; set; }

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