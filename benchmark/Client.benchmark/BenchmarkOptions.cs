// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace Client.benchmark
{
    /// <summary>
    /// Command-line options for the in-process client micro-benchmark.
    /// </summary>
    internal sealed class BenchmarkOptions
    {
        [Option("tls", Default = false, HelpText = "Enable TLS on the loopback server and benchmark clients using a temporary self-signed certificate.")]
        public bool EnableTLS { get; set; }

        [Option('t', "threads", Default = 1, HelpText = "Number of parallel worker threads sharing the single client.")]
        public int Threads { get; set; }

        [Option('b', "batch", Default = 1, HelpText = "In-flight operations each worker fires before awaiting completion (pipelining depth). Mirrors Resp.benchmark's online --itp. Latency samples become per-batch group latency when > 1.")]
        public int Batch { get; set; }

        [Option("warmup", Default = 2, HelpText = "Warm-up duration in seconds (untimed).")]
        public int WarmupSeconds { get; set; }

        [Option('d', "duration", Default = 5, HelpText = "Measured duration in seconds.")]
        public int DurationSeconds { get; set; }

        [Option("valuelength", Default = 8, HelpText = "Length in bytes of the seeded value read by GET.")]
        public int ValueLength { get; set; }

        [Option("networkbuffer", Default = 1 << 17, HelpText = "Network send/receive buffer size in bytes (networkBufferSizeBytes). GarnetClient's own default is far smaller (8 KiB).")]
        public int NetworkBufferBytes { get; set; }

        [Option("maxsends", Default = 8, HelpText = "Maximum concurrent transport sends (maxConcurrentNetworkSends).")]
        public int MaxConcurrentSends { get; set; }

        [Option("max-requests", Default = 0, HelpText = "GarnetClient request capacity (maxOutstandingRequests): the number of requests, of any size, awaiting local send completion (max 32767). Default 0 auto-sizes it to the offered load (threads*batch) with headroom; the inline request buffer is sized separately in bytes from the offered load.")]
        public int MaxOutstandingRequests { get; set; }

        [Option("max-completions", Default = 0, HelpText = "GarnetClient completion-ring capacity (maxOutstandingCompletions). Default 0 auto-sizes it to the offered load (threads*batch) with headroom so GarnetClient is not completion-throttled.")]
        public int MaxOutstandingCompletions { get; set; }

        [Option("shared-socket", Default = false, HelpText = "Topology: when false (default) each worker thread owns its own client/connection; when true all workers submit concurrently to ONE shared client/connection, exercising the duplex ring's many-producer send coalescing on a single socket.")]
        public bool SharedSocket { get; set; }

        [Option("min-pool-threads", Default = 0, HelpText = "Thread-pool minimum worker threads. Default 0 provisions max(8192, threads*batch+16) so reply continuations never wait on thread injection.")]
        public int MinPoolThreads { get; set; }
    }
}