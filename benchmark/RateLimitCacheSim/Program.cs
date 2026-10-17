// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Globalization;
using CommandLine;
using HdrHistogram;

namespace RateLimitCacheSim
{
    /// <summary>
    /// Entry point for the rate-limiter cache workload simulator.
    /// </summary>
    public static class Program
    {
        /// <summary>
        /// Parses the command line and runs one measurement.
        /// </summary>
        /// <param name="args">Command line arguments.</param>
        /// <returns>Process exit code.</returns>
        public static async Task<int> Main(string[] args)
        {
            var parsed = Parser.Default.ParseArguments<Options>(args);
            if (parsed.Tag == ParserResultType.NotParsed) return 1;

            var opts = parsed.Value;

            if (opts.MinThreads > 0)
            {
                ThreadPool.GetMinThreads(out _, out var io);
                ThreadPool.SetMinThreads(opts.MinThreads, io);
            }

            var keyspace = new RateLimitKeyspace(opts);

            if (!opts.SkipLoad)
            {
                Console.WriteLine($"Loading {opts.Endpoints} endpoint and {opts.Tenants} tenant buckets with {opts.Fields} fields each...");
                await keyspace.LoadAsync(opts).ConfigureAwait(false);
            }

            var clients = new ICacheClient[opts.Instances];
            for (var i = 0; i < clients.Length; i++)
            {
                clients[i] = opts.Client == ClientKind.SERedis
                    ? new SeRedisCacheClient(opts, keyspace.BucketValue)
                    : new GarnetCacheClient(opts, keyspace.BucketValue);
                await clients[i].ConnectAsync().ConfigureAwait(false);
            }

            PrintPlan(opts);

            try
            {
                if (opts.Mode == RunMode.Periodic)
                {
                    var runner = new PeriodicRunner(opts, keyspace, clients);
                    var latencies = await runner.RunAsync().ConfigureAwait(false);
                    Report(opts, new LoadResult(latencies, latencies.Completed / (double)opts.Runtime, 0, 0));
                }
                else
                {
                    var runner = new LoadRunner(opts, keyspace, clients);
                    Report(opts, await runner.RunAsync().ConfigureAwait(false));
                }
            }
            finally
            {
                foreach (var client in clients) client.Dispose();
            }

            return 0;
        }

        static void PrintPlan(Options opts)
        {
            Console.WriteLine();
            Console.WriteLine($"mode={opts.Mode} client={opts.Client} sync={opts.Sync} dedicated-threads={opts.DedicatedThreads}");
            Console.WriteLine($"target={opts.Host}:{opts.Port} tls={opts.Tls} instances={opts.Instances} runtime={opts.Runtime}s warmup={opts.Warmup}s");
            Console.WriteLine($"mix: {opts.EvalRatio:P0} scripted ({opts.ReadWriteRatio:P0} HGET+HSET+EXPIRE, {1 - opts.ReadWriteRatio:P0} HGET), {1 - opts.EvalRatio:P0} plain GET; {(opts.EvalFull ? "EVAL" : "EVALSHA")}");

            if (opts.Mode == RunMode.Periodic)
            {
                var perInstance = 1000.0 / opts.ProbePeriodMs + 1000.0 / opts.EndpointPeriodMs + 1000.0 / opts.TenantPeriodMs;
                Console.WriteLine($"periods: probe={opts.ProbePeriodMs}ms endpoint={opts.EndpointPeriodMs}ms tenant={opts.TenantPeriodMs}ms");
                Console.WriteLine($"offered load: {perInstance:F2} ops/sec/instance x {opts.Instances} = {perInstance * opts.Instances:F1} ops/sec");
            }
            else if (opts.Mode == RunMode.Rate)
            {
                Console.WriteLine($"offered load: {opts.Ops} ops/sec across {opts.Instances} connections ({(double)opts.Ops / opts.Instances:F0} ops/sec/connection)");
            }
            else
            {
                Console.WriteLine($"closed loop: {opts.InFlightPerConnection} in flight x {opts.Instances} connections");
            }

            Console.WriteLine();
        }

        static void Report(Options opts, LoadResult result)
        {
            var latencies = result.Latencies;

            Console.WriteLine($"completed={latencies.Completed} errors={latencies.Errors} throughput={result.Throughput:F0} ops/sec" +
                (result.MaxInFlight > 0 ? $" max-in-flight={result.MaxInFlight}" : string.Empty) +
                (result.Shed > 0 ? $" shed={result.Shed}" : string.Empty));
            if (latencies.Errors > 0) Console.WriteLine($"first error: {latencies.FirstError}");
            Console.WriteLine();

            Console.WriteLine($"{"latency (ms)",-22}{"p50",10}{"p95",10}{"p99",10}{"p99.9",10}{"max",10}");
            Row("service (send->reply)", latencies.Service);
            Row("schedule (due->send)", latencies.Schedule);
            Row("end-to-end (due->reply)", latencies.EndToEnd);
            Console.WriteLine();

            if (latencies.EndToEnd.TotalCount > 0)
            {
                var serviceP99 = Ms(latencies.Service.GetValueAtPercentile(99));
                var e2eP99 = Ms(latencies.EndToEnd.GetValueAtPercentile(99));
                if (e2eP99 > 0)
                {
                    var serverShare = serviceP99 / e2eP99;
                    Console.WriteLine($"At p99 the server accounts for {serverShare:P1} of end-to-end latency; the client's own scheduling accounts for {1 - serverShare:P1}.");
                }
            }

            if (opts.Csv)
            {
                Console.WriteLine();
                Console.WriteLine("csv,label,mode,client,instances,offered_ops,throughput,svc_p50,svc_p95,svc_p99,svc_p999,sched_p50,sched_p99,e2e_p50,e2e_p95,e2e_p99,e2e_p999,errors");
                Console.WriteLine(string.Join(',', [
                    "csv",
                    opts.Label,
                    opts.Mode.ToString(),
                    opts.Client.ToString(),
                    opts.Instances.ToString(CultureInfo.InvariantCulture),
                    opts.Mode == RunMode.Rate ? opts.Ops.ToString(CultureInfo.InvariantCulture) : "0",
                    result.Throughput.ToString("F0", CultureInfo.InvariantCulture),
                    F(latencies.Service, 50), F(latencies.Service, 95), F(latencies.Service, 99), F(latencies.Service, 99.9),
                    F(latencies.Schedule, 50), F(latencies.Schedule, 99),
                    F(latencies.EndToEnd, 50), F(latencies.EndToEnd, 95), F(latencies.EndToEnd, 99), F(latencies.EndToEnd, 99.9),
                    latencies.Errors.ToString(CultureInfo.InvariantCulture),
                ]));
            }
        }

        static void Row(string name, LongHistogram histogram)
        {
            if (histogram.TotalCount == 0)
            {
                Console.WriteLine($"{name,-22}{"-",10}{"-",10}{"-",10}{"-",10}{"-",10}");
                return;
            }

            Console.WriteLine($"{name,-22}{Ms(histogram.GetValueAtPercentile(50)),10:F3}{Ms(histogram.GetValueAtPercentile(95)),10:F3}" +
                $"{Ms(histogram.GetValueAtPercentile(99)),10:F3}{Ms(histogram.GetValueAtPercentile(99.9)),10:F3}{Ms(histogram.GetMaxValue()),10:F3}");
        }

        static string F(LongHistogram histogram, double percentile)
            => histogram.TotalCount == 0
                ? "-"
                : Ms(histogram.GetValueAtPercentile(percentile)).ToString("F3", CultureInfo.InvariantCulture);

        static double Ms(long micros) => micros / 1000.0;
    }
}