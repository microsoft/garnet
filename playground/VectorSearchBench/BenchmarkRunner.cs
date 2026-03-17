// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace VectorSearchBench;

/// <summary>
/// Drives a workload against two targets, collects results, and formats the report.
/// </summary>
public static class BenchmarkRunner
{
    public static async Task RunAsync(BenchOptions opts, CancellationToken ct = default)
    {
        // Auto-select yfcc workload when --data-dir is provided
        if (opts.DataDir != null && opts.Workload == "vector-search-filter")
            opts.Workload = "yfcc";

        bool isYfcc = opts.Workload.Equals("yfcc", StringComparison.OrdinalIgnoreCase);

        Console.WriteLine("=== Vector Search Benchmark ===");
        Console.WriteLine($"  Garnet : {opts.GarnetHost}:{opts.GarnetPort}");
        Console.WriteLine($"  Redis  : {opts.RedisHost}:{opts.RedisPort}");
        if (isYfcc)
        {
            Console.WriteLine($"  DataDir: {opts.DataDir}");
            Console.WriteLine($"  YfccCount: {(opts.YfccCount > 0 ? opts.YfccCount.ToString() : "all")}");
        }
        else
        {
            Console.WriteLine($"  Vectors: {opts.NumVectors}  Dim: {opts.Dimensions}  Queries: {opts.NumQueries}  TopK: {opts.TopK}");
        }
        Console.WriteLine($"  Workload: {opts.Workload}  Queries: {opts.NumQueries}  TopK: {opts.TopK}");
        if (!isYfcc)
            Console.WriteLine($"  RedisMode: {opts.RedisMode}");
        if (opts.IdPrefix.Length > 0)
            Console.WriteLine($"  IdPrefix: \"{opts.IdPrefix}\"");
        if (opts.SearchEF > 0)
            Console.WriteLine($"  Garnet EF: {opts.SearchEF}");
        Console.WriteLine();

        await using var garnet = await RedisTargetClient.ConnectAsync("Garnet", opts.GarnetHost, opts.GarnetPort);
        await using var redis = await RedisTargetClient.ConnectAsync("Redis", opts.RedisHost, opts.RedisPort);

        Console.WriteLine("[connect] pinging both servers …");
        await garnet.PingAsync();
        await redis.PingAsync();
        Console.WriteLine("[connect] both servers reachable");

        IWorkload workload = opts.Workload.ToLowerInvariant() switch
        {
            "vector-search-filter" or "vsf" => new VectorSearchWorkload(),
            "vector-search-plain" or "vsp" => new VectorSearchPlainWorkload(),
            "yfcc" => new YfccWorkload(),
            _ => throw new ArgumentException($"Unknown workload: {opts.Workload}")
        };

        if (!opts.SkipLoad)
            await workload.LoadAsync(garnet, redis, opts, ct);
        else
            Console.WriteLine("[load] skipped (--skip-load)");

        Console.WriteLine("\n[warmup] running warmup queries …");
        var warmupOpts = new BenchOptions
        {
            GarnetHost    = opts.GarnetHost,
            GarnetPort    = opts.GarnetPort,
            RedisHost     = opts.RedisHost,
            RedisPort     = opts.RedisPort,
            Workload      = opts.Workload,
            NumVectors    = opts.NumVectors,
            Dimensions    = opts.Dimensions,
            NumQueries    = Math.Min(opts.WarmupQueries, opts.NumQueries),
            TopK          = opts.TopK,
            VectorKey     = opts.VectorKey,
            Seed          = opts.Seed,
            WarmupQueries = opts.WarmupQueries,
            SkipLoad      = true,
            IdPrefix      = opts.IdPrefix,
            SearchEF      = opts.SearchEF,
            RedisMode     = opts.RedisMode,
            DataDir       = opts.DataDir,
            YfccConfig    = opts.YfccConfig,
            YfccCount     = opts.YfccCount,
        };
        await workload.RunAsync(garnet, redis, warmupOpts, ct);

        Console.WriteLine($"\n[bench] running {opts.NumQueries} queries per phase …");
        var results = await workload.RunAsync(garnet, redis, opts, ct);

        PrintReport(results, opts);
    }

    private static void PrintReport(List<WorkloadPhaseResult> results, BenchOptions opts)
    {
        bool isYfcc = opts.Workload.Equals("yfcc", StringComparison.OrdinalIgnoreCase);

        Console.WriteLine();
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                           BENCHMARK RESULTS                                      ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════════════╝");

        var phases = results.Select(r => r.PhaseName).Distinct().Order().ToList();
        var recallHeader = $"Recall@{opts.TopK}";

        static string R(string s, int w) => s.PadLeft(w);
        static string Lat(double us) => $"{us:F0}µs";
        static string Recall(double? r) => r.HasValue ? $"{r.Value:P2}" : "—";

        // Side-by-side comparison: one row per phase, Garnet vs Redis columns
        Console.WriteLine();
        Console.WriteLine(
            $"  {"Phase",-20}  │  {R("Garnet mean", 12)}  {R("p50", 10)}  {R("p95", 10)}  {R(recallHeader, 10)}" +
            $"  │  {R("Redis mean", 12)}  {R("p50", 10)}  {R("p95", 10)}  {R(recallHeader, 10)}" +
            $"  │  {R("Speedup", 8)}");
        Console.WriteLine(
            $"  {new string('─', 20)}──┼──{new string('─', 12)}──{new string('─', 10)}──{new string('─', 10)}──{new string('─', 10)}" +
            $"──┼──{new string('─', 12)}──{new string('─', 10)}──{new string('─', 10)}──{new string('─', 10)}" +
            $"──┼──{new string('─', 8)}");

        foreach (var phase in phases)
        {
            var phaseResults = results.Where(r => r.PhaseName == phase).ToList();
            var g = phaseResults.FirstOrDefault(r => r.Target == "Garnet");
            var red = phaseResults.FirstOrDefault(r => r.Target == "Redis");

            var gMean  = g != null ? Lat(g.Latency.MeanUs) : "—";
            var gP50   = g != null ? Lat(g.Latency.P50Us) : "—";
            var gP95   = g != null ? Lat(g.Latency.P95Us) : "—";
            var gRec   = g != null ? Recall(g.RecallAtK) : "—";

            var rMean  = red != null ? Lat(red.Latency.MeanUs) : "—";
            var rP50   = red != null ? Lat(red.Latency.P50Us) : "—";
            var rP95   = red != null ? Lat(red.Latency.P95Us) : "—";
            var rRec   = red != null ? Recall(red.RecallAtK) : "—";

            var speedup = (g != null && red != null && g.Latency.MeanUs > 0)
                ? $"{red.Latency.MeanUs / g.Latency.MeanUs:F2}x"
                : "—";

            Console.WriteLine(
                $"  {phase,-20}  │  {R(gMean, 12)}  {R(gP50, 10)}  {R(gP95, 10)}  {R(gRec, 10)}" +
                $"  │  {R(rMean, 12)}  {R(rP50, 10)}  {R(rP95, 10)}  {R(rRec, 10)}" +
                $"  │  {R(speedup, 8)}");
        }

        Console.WriteLine();
        Console.WriteLine("  Notes:");
        if (isYfcc)
        {
            Console.WriteLine("  • YFCC workload: real-world YFCC10M dataset (uint8, 192 dims)");
            Console.WriteLine("  • Ground-truth: client-side brute-force cosine similarity on loaded data");
            Console.WriteLine("  • Recall@K = |target_topK ∩ bruteforce_exactTopK| / K, averaged across all queries");
            Console.WriteLine("  • Both Garnet and Redis 8 (VSIM) are benchmarked against brute-force ground truth");
        }
        else
        {
            bool isHnsw = opts.RedisMode.Equals("hnsw", StringComparison.OrdinalIgnoreCase);
            bool isVsim = opts.RedisMode.Equals("vsim", StringComparison.OrdinalIgnoreCase);
            if (isVsim)
            {
                Console.WriteLine("  • Redis mode: VSIM (Redis 8 VADD/VSIM) — same commands as Garnet, apples-to-apples");
                Console.WriteLine("  • Ground-truth: VSIM TRUTH (exact linear scan on Redis)");
                Console.WriteLine("  • Recall@K = |target_topK ∩ VSIM_TRUTH_topK| / K, averaged across all queries");
            }
            else if (isHnsw)
            {
                Console.WriteLine("  • Redis mode: HNSW (RediSearch FT.SEARCH) — both Garnet and Redis show recall vs brute-force ground-truth");
                Console.WriteLine("  • Recall@K = |target_topK ∩ bruteforce_exactTopK| / K, averaged across all queries");
            }
            else
            {
                Console.WriteLine("  • Redis latency = client-side brute-force cosine scan (ground-truth only, not HNSW)");
                Console.WriteLine("  • Recall@K = |Garnet_topK ∩ Redis_exactTopK| / K, averaged across all queries");
            }
        }
        Console.WriteLine();
    }
}
