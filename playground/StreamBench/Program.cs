// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using StackExchange.Redis;

namespace StreamBench;

// Self-contained benchmark for Redis Stream commands. Designed to be pointed at either
// Garnet or Redis (or any other RESP-compatible server) so the same client + payload
// produces apples-to-apples numbers. Reports throughput and latency percentiles.
//
// Usage:
//   dotnet run -c Release -- --endpoint localhost:6379 --workload xadd           --duration 10
//   dotnet run -c Release -- --endpoint localhost:3278 --workload xrange         --duration 10 --window 100
//   dotnet run -c Release -- --endpoint localhost:6379 --workload xrevrange      --duration 10 --window 100
//   dotnet run -c Release -- --endpoint localhost:6379 --workload consumergroup  --duration 10
//   dotnet run -c Release -- --endpoint localhost:6379 --workload all            --duration 10
//
// Notes on methodology:
//   • Workloads run sequentially with separate warmup phases so that one's tail latency
//     doesn't pollute the next's measurements.
//   • XADD is pure-append: every issued op produces a new entry under a single stream
//     key (the realistic hot-path for stream ingest).
//   • XRANGE / XREVRANGE first pre-load `--prepopulate` entries, then issue random-window
//     reads of `--window` consecutive entries each.
//   • Latencies are captured per-request as wall-clock from "command issued" to "reply
//     received" using high-resolution stopwatch ticks. We never await on a hot path that
//     allocates per-call.
//   • Pipelining: each worker issues `--batch` commands then awaits all replies, simulating
//     real RESP clients (StackExchange.Redis batches under the hood as well, but explicit
//     batching is the standard knob for synthetic benchmarks).

internal static class Program
{
    static async Task<int> Main(string[] args)
    {
        var opts = Options.Parse(args);
        if (opts is null) return 2;

        Console.WriteLine($"==> endpoint={opts.Endpoint}  workload={opts.Workload}  threads={opts.Threads}  batch={opts.Batch}  warmup={opts.Warmup}s  duration={opts.Duration}s");

        // One ConfigurationOptions reused for every connection. AbortOnConnectFail=false
        // so a momentary failure during server startup doesn't kill the entire run.
        // We pass the endpoint as an explicit IPEndPoint so .NET doesn't prefer IPv6
        // resolution — WSL2's localhost forwarding only proxies IPv4, and a connection
        // attempt on ::1:6379 fails immediately.
        var (host, port) = ParseEndpoint(opts.Endpoint);
        var config = new ConfigurationOptions
        {
            AbortOnConnectFail = false,
            SyncTimeout = 30_000,
            ConnectTimeout = 5_000,
        };
        if (IPAddress.TryParse(host, out var ip))
        {
            config.EndPoints.Add(new IPEndPoint(ip, port));
        }
        else
        {
            // Hostname — let SE.Redis resolve, but we still pin AddressFamily.
            // Resolve here so we can pick IPv4 explicitly.
            var entry = Dns.GetHostEntry(host);
            var v4 = entry.AddressList.FirstOrDefault(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                ?? entry.AddressList[0];
            config.EndPoints.Add(new IPEndPoint(v4, port));
        }

        await using var connections = new ConnectionPool(config, opts.Threads);
        await connections.ReadyAsync();

        // Each workload reseeds with a fresh stream key so trims/inserts don't interfere
        // with subsequent workloads. We use FLUSHDB only at the very start to give a clean
        // baseline regardless of what the server had before.
        await connections.Pool[0].GetDatabase().ExecuteAsync("FLUSHDB");

        string[] workloads = opts.Workload switch
        {
            "all" => ["xadd", "xrange", "xrevrange", "consumergroup"],
            _ => [opts.Workload]
        };

        var results = new List<WorkloadResult>();
        foreach (var w in workloads)
        {
            results.Add(await RunWorkloadAsync(w, connections, opts));
        }

        WriteSummary(results, opts);
        return 0;
    }

    static async Task<WorkloadResult> RunWorkloadAsync(string workload, ConnectionPool conns, Options opts)
    {
        Console.WriteLine();
        Console.WriteLine($"---- {workload.ToUpperInvariant()} ----");

        // Use a unique key per workload so we don't carry state across runs.
        var streamKey = $"bench:{workload}:{Environment.TickCount64}";

        // Workloads that read need a pre-populated stream. We pre-populate with the same
        // count regardless of duration so reads aren't influenced by stream size variance.
        if (workload is "xrange" or "xrevrange")
        {
            Console.Write($"  prepopulating {opts.Prepopulate:N0} entries... ");
            var sw = Stopwatch.StartNew();
            await PrepopulateAsync(conns.Pool[0].GetDatabase(), streamKey, opts.Prepopulate);
            sw.Stop();
            Console.WriteLine($"done ({sw.ElapsedMilliseconds:N0} ms)");
        }
        else if (workload is "consumergroup")
        {
            // Prepopulate, then create groups and per-thread consumers
            Console.Write($"  prepopulating {opts.Prepopulate:N0} entries... ");
            var sw = Stopwatch.StartNew();
            await PrepopulateAsync(conns.Pool[0].GetDatabase(), streamKey, opts.Prepopulate);
            sw.Stop();
            Console.WriteLine($"done ({sw.ElapsedMilliseconds:N0} ms)");

            var db0 = conns.Pool[0].GetDatabase();
            await db0.ExecuteAsync("XGROUP", "CREATE", streamKey, "bench-group", "0-0");
            Console.WriteLine("  created consumer group 'bench-group' at 0-0");
        }

        // Warmup: run for a few seconds with results discarded so JIT/connection pools/
        // server-side caches are hot before measurement.
        Console.Write($"  warmup {opts.Warmup}s... ");
        await DriveAsync(workload, streamKey, conns, opts, TimeSpan.FromSeconds(opts.Warmup), measure: false);
        Console.WriteLine("done");

        Console.Write($"  measuring {opts.Duration}s... ");
        var measured = await DriveAsync(workload, streamKey, conns, opts, TimeSpan.FromSeconds(opts.Duration), measure: true);
        Console.WriteLine("done");

        var pct = ComputePercentiles(measured.LatenciesMicros);
        var qps = measured.Ops / opts.Duration;
        Console.WriteLine($"  {measured.Ops:N0} ops  |  {qps:N0} ops/s  |  p50={pct.p50}µs  p90={pct.p90}µs  p99={pct.p99}µs  p999={pct.p999}µs  max={pct.max}µs");

        return new WorkloadResult(workload, measured.Ops, qps, pct);
    }

    static async Task<DriveResult> DriveAsync(string workload, string streamKey, ConnectionPool conns, Options opts, TimeSpan duration, bool measure)
    {
        var deadline = Stopwatch.GetTimestamp() + (long)(duration.TotalSeconds * Stopwatch.Frequency);
        long opsTotal = 0;

        // Per-thread latency buffers that we merge at the end. Lock-free.
        var perThread = new List<long>[(int)opts.Threads];
        var tasks = new Task[opts.Threads];

        for (int t = 0; t < opts.Threads; t++)
        {
            int threadIndex = t;
            perThread[t] = measure ? new List<long>(capacity: 1 << 16) : null!;

            tasks[t] = Task.Run(async () =>
            {
                var db = conns.Pool[threadIndex % conns.Pool.Length].GetDatabase();
                var rng = new Random(unchecked((int)(threadIndex * 2654435761u)));
                long localOps = 0;

                // Re-used per loop iteration to avoid per-call allocations on the hot path.
                var batchTasks = new Task<RedisResult>[opts.Batch];

                while (Stopwatch.GetTimestamp() < deadline)
                {
                    long batchStart = measure ? Stopwatch.GetTimestamp() : 0;

                    for (int b = 0; b < opts.Batch; b++)
                    {
                        batchTasks[b] = IssueOneAsync(db, workload, streamKey, opts, rng);
                    }
                    await Task.WhenAll(batchTasks).ConfigureAwait(false);

                    if (measure)
                    {
                        long batchEnd = Stopwatch.GetTimestamp();
                        // Charge the per-op latency as (batch elapsed) / batch size — the
                        // standard convention for pipelined throughput benchmarks. This
                        // captures effective latency a single op experiences when pipelined.
                        long perOpTicks = (batchEnd - batchStart) / opts.Batch;
                        long perOpMicros = TicksToMicros(perOpTicks);
                        var sink = perThread[threadIndex];
                        for (int b = 0; b < opts.Batch; b++)
                            sink.Add(perOpMicros);
                    }

                    localOps += opts.Batch;
                }

                Interlocked.Add(ref opsTotal, localOps);
            });
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        if (!measure) return new DriveResult(opsTotal, []);

        // Merge per-thread latency lists.
        long total = 0;
        for (int t = 0; t < opts.Threads; t++) total += perThread[t].Count;
        var latencies = new long[total];
        int idx = 0;
        for (int t = 0; t < opts.Threads; t++)
        {
            var src = perThread[t];
            src.CopyTo(latencies, idx);
            idx += src.Count;
        }
        Array.Sort(latencies);

        return new DriveResult(opsTotal, latencies);
    }

    static Task<RedisResult> IssueOneAsync(IDatabase db, string workload, string streamKey, Options opts, Random rng)
    {
        switch (workload)
        {
            case "xadd":
                // Auto-id (`*`) + a single short field/value pair — the smallest payload
                // that still goes through normal parsing/serialization paths.
                return db.ExecuteAsync("XADD", streamKey, "*", "f", "v");

            case "xrange":
                {
                    // Pick a random window of `opts.Window` entries somewhere in [1, prepopulate].
                    long start = 1 + rng.NextInt64(0, Math.Max(1, opts.Prepopulate - opts.Window));
                    long end = start + opts.Window - 1;
                    return db.ExecuteAsync("XRANGE", streamKey, $"{start}-0", $"{end}-0", "COUNT", opts.Window.ToString(CultureInfo.InvariantCulture));
                }

            case "xrevrange":
                {
                    long end = 1 + rng.NextInt64(0, Math.Max(1, opts.Prepopulate - opts.Window));
                    long start = end + opts.Window - 1;
                    return db.ExecuteAsync("XREVRANGE", streamKey, $"{start}-0", $"{end}-0", "COUNT", opts.Window.ToString(CultureInfo.InvariantCulture));
                }

            case "consumergroup":
                return IssueConsumerGroupCycleAsync(db, streamKey, rng);

            default:
                throw new InvalidOperationException($"unknown workload {workload}");
        }
    }

    /// <summary>
    /// One consumer group cycle: XREADGROUP (fetch 1 new entry) then XACK it.
    /// Each thread acts as a separate consumer within the same group, so entries
    /// are distributed across workers without duplication — the standard fan-out
    /// pattern for stream processing pipelines.
    /// </summary>
    static async Task<RedisResult> IssueConsumerGroupCycleAsync(IDatabase db, string streamKey, Random rng)
    {
        var consumer = $"consumer-{Environment.CurrentManagedThreadId}";

        // Read one new entry
        var result = await db.ExecuteAsync("XREADGROUP", "GROUP", "bench-group", consumer,
            "COUNT", "1", "STREAMS", streamKey, ">").ConfigureAwait(false);

        // If we got an entry, acknowledge it
        if (result is not null && !result.IsNull)
        {
            // Response: *1 *2 $key *N [*2 $id *M field...] — extract the entry ID
            try
            {
                var streams = (RedisResult[])result!;
                if (streams.Length > 0)
                {
                    var streamData = (RedisResult[])streams[0]!;
                    if (streamData.Length >= 2)
                    {
                        var entries = (RedisResult[])streamData[1]!;
                        if (entries.Length > 0)
                        {
                            var entry = (RedisResult[])entries[0]!;
                            var entryId = (string)entry[0]!;
                            await db.ExecuteAsync("XACK", streamKey, "bench-group", entryId).ConfigureAwait(false);
                        }
                    }
                }
            }
            catch
            {
                // Swallow parse failures — the benchmark keeps running
            }
        }

        return result!;
    }

    static async Task PrepopulateAsync(IDatabase db, string streamKey, long count)
    {
        // Use explicit ms-seq IDs so xrange tests can pick deterministic windows.
        // Pipeline aggressively to keep this phase short.
        const int batch = 1024;
        var pending = new Task<RedisResult>[batch];
        for (long i = 0; i < count; i += batch)
        {
            int n = (int)Math.Min(batch, count - i);
            for (int j = 0; j < n; j++)
            {
                long id = i + j + 1;
                pending[j] = db.ExecuteAsync("XADD", streamKey, $"{id}-0", "f", "v");
            }
            for (int j = n; j < batch; j++) pending[j] = Task.FromResult<RedisResult>(default!);
            await Task.WhenAll(pending.AsSpan(0, n).ToArray()).ConfigureAwait(false);
        }
    }

    static (long p50, long p90, long p99, long p999, long max) ComputePercentiles(long[] sorted)
    {
        if (sorted.Length == 0) return (0, 0, 0, 0, 0);
        long Pick(double q)
        {
            int idx = (int)Math.Min(sorted.Length - 1, Math.Max(0, sorted.Length * q));
            return sorted[idx];
        }
        return (Pick(0.50), Pick(0.90), Pick(0.99), Pick(0.999), sorted[^1]);
    }

    static long TicksToMicros(long ticks) => (long)(ticks * 1_000_000.0 / Stopwatch.Frequency);

    static (string host, int port) ParseEndpoint(string s)
    {
        var idx = s.LastIndexOf(':');
        if (idx <= 0) return (s, 6379);
        return (s[..idx], int.Parse(s[(idx + 1)..]));
    }

    static void WriteSummary(List<WorkloadResult> results, Options opts)
    {
        Console.WriteLine();
        Console.WriteLine("==== SUMMARY ====");
        Console.WriteLine($"endpoint  : {opts.Endpoint}");
        Console.WriteLine($"threads   : {opts.Threads}");
        Console.WriteLine($"batch     : {opts.Batch}");
        Console.WriteLine($"window    : {opts.Window}  (xrange/xrevrange)");
        Console.WriteLine();
        Console.WriteLine($"{"workload",-12}  {"ops/sec",12}  {"p50µs",6}  {"p90µs",6}  {"p99µs",6}  {"p999µs",7}  {"maxµs",7}");
        foreach (var r in results)
        {
            Console.WriteLine($"{r.Workload,-12}  {r.OpsPerSec,12:N0}  {r.Pct.p50,6}  {r.Pct.p90,6}  {r.Pct.p99,6}  {r.Pct.p999,7}  {r.Pct.max,7}");
        }
    }
}

internal sealed record WorkloadResult(string Workload, long TotalOps, double OpsPerSec, (long p50, long p90, long p99, long p999, long max) Pct);
internal readonly record struct DriveResult(long Ops, long[] LatenciesMicros);

internal sealed class ConnectionPool : IAsyncDisposable
{
    public ConnectionMultiplexer[] Pool { get; }
    public ConnectionPool(ConfigurationOptions config, int n)
    {
        // One multiplexer per concurrent worker. SE.Redis multiplexes many in-flight ops
        // over a single connection, but for cleaner per-thread accounting we give each
        // worker its own. Caps at 8 multiplexers (more than that wastes sockets).
        int multis = Math.Min(8, Math.Max(1, n));
        Pool = new ConnectionMultiplexer[multis];
        for (int i = 0; i < multis; i++)
            Pool[i] = ConnectionMultiplexer.Connect(config);
    }
    public Task ReadyAsync() => Task.WhenAll(Pool.Select(p => p.GetDatabase().PingAsync()));
    public async ValueTask DisposeAsync()
    {
        foreach (var c in Pool) await c.DisposeAsync();
    }
}

internal sealed class Options
{
    public string Endpoint { get; init; } = "localhost:6379";
    public string Workload { get; init; } = "all";
    public int Threads { get; init; } = 16;
    public int Batch { get; init; } = 32;
    public int Warmup { get; init; } = 3;
    public int Duration { get; init; } = 10;
    public long Prepopulate { get; init; } = 100_000;
    public int Window { get; init; } = 50;

    public static Options? Parse(string[] args)
    {
        var o = new Options();
        var d = new Dictionary<string, string>();
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i].StartsWith("--") && i + 1 < args.Length && !args[i + 1].StartsWith("--"))
            {
                d[args[i][2..]] = args[i + 1];
                i++;
            }
            else if (args[i] is "-h" or "--help")
            {
                PrintHelp();
                return null;
            }
        }
        return new Options
        {
            Endpoint = d.GetValueOrDefault("endpoint", o.Endpoint),
            Workload = d.GetValueOrDefault("workload", o.Workload),
            Threads = int.Parse(d.GetValueOrDefault("threads", o.Threads.ToString())),
            Batch = int.Parse(d.GetValueOrDefault("batch", o.Batch.ToString())),
            Warmup = int.Parse(d.GetValueOrDefault("warmup", o.Warmup.ToString())),
            Duration = int.Parse(d.GetValueOrDefault("duration", o.Duration.ToString())),
            Prepopulate = long.Parse(d.GetValueOrDefault("prepopulate", o.Prepopulate.ToString())),
            Window = int.Parse(d.GetValueOrDefault("window", o.Window.ToString())),
        };
    }

    static void PrintHelp()
    {
        Console.WriteLine("Usage: StreamBench [options]");
        Console.WriteLine("  --endpoint <host:port>   default localhost:6379");
        Console.WriteLine("  --workload xadd|xrange|xrevrange|consumergroup|all   default all");
        Console.WriteLine("  --threads N              default 16");
        Console.WriteLine("  --batch N                default 32  (pipeline depth per worker)");
        Console.WriteLine("  --warmup N               default 3 seconds");
        Console.WriteLine("  --duration N             default 10 seconds");
        Console.WriteLine("  --prepopulate N          default 100000  (entries pre-loaded for read tests)");
        Console.WriteLine("  --window N               default 50      (entries per XRANGE/XREVRANGE)");
    }
}
