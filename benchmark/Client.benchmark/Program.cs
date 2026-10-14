// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;
using LightEpoch = Garnet.client.LightEpoch;

namespace Client.benchmark
{
    /// <summary>
    /// Entry point for the in-process client micro-benchmark. A single real Garnet
    /// server is hosted over loopback and exercised by one client type (GarnetClient
    /// or GarnetLightClient) driven by a configurable number of parallel threads, each
    /// owning its own connection and all sharing one epoch.
    /// </summary>
    internal sealed class Program
    {
        const string Key = "benchmark:key";

        static void Main(string[] args)
            => Parser.Default.ParseArguments<BenchmarkOptions>(args).WithParsed(Run);

        static void Run(BenchmarkOptions opts)
        {
            using var server = new ServerWrapper();
            var endpoint = server.EndPoint;
            var value = new string('v', opts.ValueLength);

            Console.WriteLine($"Server {server.Host}:{server.Port} | arm={opts.Client} threads={opts.Threads} batch={opts.Batch} " +
                $"warmup={opts.WarmupSeconds}s duration={opts.DurationSeconds}s valuelen={opts.ValueLength} " +
                $"netbuf={opts.NetworkBufferBytes} maxsends={opts.MaxConcurrentSends} outstanding={opts.Outstanding}");

            var epoch = new LightEpoch();
            try
            {
                var result = opts.Client switch
                {
                    ClientArm.GarnetClient => WorkloadRunner.Run<GarnetClientDriverFactory, GarnetClientDriver>(
                        new GarnetClientDriverFactory(endpoint, Key, value, opts), epoch,
                        opts.Threads, opts.Batch, opts.WarmupSeconds, opts.DurationSeconds),
                    ClientArm.GarnetLightClient => WorkloadRunner.Run<GarnetLightClientDriverFactory, GarnetLightClientDriver>(
                        new GarnetLightClientDriverFactory(endpoint, Key, value, opts), epoch,
                        opts.Threads, opts.Batch, opts.WarmupSeconds, opts.DurationSeconds),
                    _ => throw new ArgumentOutOfRangeException(nameof(opts)),
                };

                PrintResult(opts, result);
            }
            finally
            {
                epoch.Dispose();
            }
        }

        static void PrintResult(BenchmarkOptions opts, RunResult r)
        {
            Console.WriteLine();
            Console.WriteLine("================ Results ================");
            Console.WriteLine($" Arm          : {opts.Client}");
            Console.WriteLine($" Threads      : {opts.Threads}");
            Console.WriteLine($" Batch        : {r.Batch}" + (r.Batch > 1 ? " (in-flight ops/worker; latency is per-batch group)" : ""));
            Console.WriteLine($" Duration (s) : {r.Seconds:F2}");
            Console.WriteLine($" Total ops    : {r.TotalOps:N0}");
            Console.WriteLine($" Throughput   : {r.ThroughputOpsPerSec:N0} ops/sec");
            Console.WriteLine($" Latency (us) : mean={r.MeanMicros:F2} p50={r.P50Micros:F2} " +
                $"p99={r.P99Micros:F2} p99.9={r.P999Micros:F2} max={r.MaxMicros:F2}");
            Console.WriteLine("=========================================");
        }
    }
}
