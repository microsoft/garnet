// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace Client.benchmark
{
    /// <summary>
    /// The client implementation under test.
    /// </summary>
    internal enum ClientArm
    {
        /// <summary>The original <see cref="Garnet.client.GarnetClient"/> (inline send path).</summary>
        GarnetClient,

        /// <summary>The <see cref="Garnet.client.GarnetLightClient"/> (duplex-ring send path).</summary>
        GarnetLightClient,
    }

    /// <summary>
    /// Command-line options for the in-process client micro-benchmark.
    /// </summary>
    internal sealed class BenchmarkOptions
    {
        [Option("client", Default = ClientArm.GarnetLightClient, HelpText = "Client implementation to benchmark: GarnetClient or GarnetLightClient.")]
        public ClientArm Client { get; set; }

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

        [Option("networkbuffer", Default = 1 << 17, HelpText = "Network send/receive buffer size in bytes, applied to BOTH clients (GarnetClient bufferSize and GarnetLightClient networkBufferSizeBytes) so the two arms run with comparable buffers. GarnetLightClient's own default is far smaller (8 KiB).")]
        public int NetworkBufferBytes { get; set; }

        [Option("maxsends", Default = 8, HelpText = "Maximum concurrent transport sends, applied to BOTH clients (GarnetClient networkSendThrottleMax and GarnetLightClient maxConcurrentNetworkSends).")]
        public int MaxConcurrentSends { get; set; }

        [Option("max-requests", Default = 0, HelpText = "GarnetLightClient request capacity (maxOutstandingRequests): the number of requests, of any size, awaiting local send completion (max 32768). Default 0 auto-sizes it to the offered load (threads*batch) with headroom; the inline request buffer is sized separately in bytes from the offered load. GarnetClient has no request-count cap.")]
        public int MaxOutstandingRequests { get; set; }

        [Option("max-completions", Default = 0, HelpText = "GarnetLightClient completion-ring capacity (maxOutstandingCompletions). Default 0 auto-sizes it to the offered load (threads*batch) with headroom so GarnetLightClient is not completion-throttled and matches GarnetClient. GarnetClient's equivalent (maxOutstandingTasks) stays at its large default.")]
        public int MaxOutstandingCompletions { get; set; }

        [Option("shared-socket", Default = false, HelpText = "Topology: when false (default) each worker thread owns its own client/connection; when true all workers submit concurrently to ONE shared client/connection, exercising the duplex ring's many-producer send coalescing on a single socket.")]
        public bool SharedSocket { get; set; }
    }
}