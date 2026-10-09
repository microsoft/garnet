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
    }
}