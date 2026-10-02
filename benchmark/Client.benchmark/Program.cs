// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace Client.benchmark
{
    /// <summary>
    /// Entry point for the in-process client micro-benchmark. A single real Garnet
    /// server is hosted over loopback and exercised by one client instance (GarnetClient
    /// or GarnetLightClient) driven by a configurable number of parallel threads.
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
                $"warmup={opts.WarmupSeconds}s duration={opts.DurationSeconds}s valuelen={opts.ValueLength}");

            var result = opts.Client switch
            {
                ClientArm.GarnetClient => RunArm(new GarnetClientDriver(endpoint, Key, value), opts),
                ClientArm.GarnetLightClient => RunArm(new GarnetLightClientDriver(endpoint, Key, value), opts),
                _ => throw new ArgumentOutOfRangeException(nameof(opts)),
            };

            PrintResult(opts, result);
        }

        static RunResult RunArm<TDriver>(TDriver driver, BenchmarkOptions opts)
            where TDriver : struct, IOpDriver
        {
            try
            {
                driver.Connect();
                driver.SeedAsync().GetAwaiter().GetResult();
                return WorkloadRunner.Run(driver, opts.Threads, opts.Batch, opts.WarmupSeconds, opts.DurationSeconds);
            }
            finally
            {
                driver.Dispose();
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