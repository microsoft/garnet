// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;
using Microsoft.Extensions.Logging;
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
            // Workers are dedicated threads that block on Task.WhenAll(...).GetAwaiter().GetResult(), so every
            // in-flight operation's async continuation and the client's flush continuation resume on the thread
            // pool. With the pool's default slow thread injection those continuations can starve under a
            // CPU-contended launcher, corrupting (or appearing to hang) the measurement. Pre-provision enough
            // pool threads that forward progress never waits on thread-pool hill-climbing.
            var minPoolThreads = opts.MinPoolThreads > 0
                ? opts.MinPoolThreads
                : Math.Max(8192, Math.Max(Environment.ProcessorCount, (opts.Threads * Math.Max(1, opts.Batch)) + 16));
            ThreadPool.GetMinThreads(out _, out var minIo);
            ThreadPool.SetMinThreads(minPoolThreads, minIo);

            using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));
            using var server = new ServerWrapper(loggerFactory: loggerFactory, enableTLS: opts.EnableTLS);
            var endpoint = server.EndPoint;
            var value = new string('v', opts.ValueLength);

            Console.WriteLine($"Server {server.Host}:{server.Port} | arm={opts.Client} tls={opts.EnableTLS} threads={opts.Threads} batch={opts.Batch} " +
                $"warmup={opts.WarmupSeconds}s duration={opts.DurationSeconds}s valuelen={opts.ValueLength} " +
                $"netbuf={opts.NetworkBufferBytes} maxsends={opts.MaxConcurrentSends} " +
                $"max-requests={opts.MaxOutstandingRequests} max-completions={opts.MaxOutstandingCompletions} " +
                $"topology={(opts.SharedSocket ? "shared-socket" : "per-thread-socket")}");

            var epoch = new LightEpoch();
            try
            {
                var result = opts.Client switch
                {
                    ClientArm.GarnetClient => WorkloadRunner.Run<GarnetClientDriverFactory, GarnetClientDriver>(
                        new GarnetClientDriverFactory(endpoint, Key, value, opts, server.TlsClientOptions), epoch,
                        opts.Threads, opts.Batch, opts.WarmupSeconds, opts.DurationSeconds, opts.SharedSocket),
                    ClientArm.GarnetLightClient => WorkloadRunner.Run<GarnetLightClientDriverFactory, GarnetLightClientDriver>(
                        new GarnetLightClientDriverFactory(endpoint, Key, value, opts, server.TlsClientOptions), epoch,
                        opts.Threads, opts.Batch, opts.WarmupSeconds, opts.DurationSeconds, opts.SharedSocket),
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
            Console.WriteLine($" Topology     : {(opts.SharedSocket ? "shared-socket (one connection, all workers)" : "per-thread-socket (one connection per worker)")}");
            Console.WriteLine($" TLS          : {(opts.EnableTLS ? "Enabled" : "Disabled")}");
            Console.WriteLine($" Threads      : {opts.Threads}");
            Console.WriteLine($" Batch        : {r.Batch}" + (r.Batch > 1 ? " (in-flight ops/worker; latency is per-batch group)" : ""));
            Console.WriteLine($" Duration (s) : {r.Seconds:F2}");
            Console.WriteLine($" Total ops    : {r.TotalOps:N0}");
            Console.WriteLine($" Throughput   : {r.ThroughputOpsPerSec:N0} ops/sec");
            Console.WriteLine($" Latency (us) : mean={r.MeanMicros:F2} p50={r.P50Micros:F2} " +
                $"p99={r.P99Micros:F2} p99.9={r.P999Micros:F2} max={r.MaxMicros:F2}");
            Console.WriteLine($" CPU (us/op)  : {r.CpuMicrosPerOp:F3} (client and in-process server)");
            Console.WriteLine("=========================================");
            Console.WriteLine(FormattableString.Invariant(
                $"RESULT,{opts.Client},{(opts.EnableTLS ? "tls" : "plain")},{(opts.SharedSocket ? "shared" : "per-thread")},{opts.Threads},{r.Batch},{r.ThroughputOpsPerSec:F0},{r.CpuMicrosPerOp:F3},{r.P50Micros:F2},{r.P99Micros:F2}"));
        }
    }
}