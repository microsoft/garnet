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

        foreach (var phase in phases)
        {
            Console.WriteLine();
            Console.WriteLine($"  Phase: {phase}");
            Console.WriteLine(new string('─', 84));
            Console.WriteLine($"  {"Target",-10}  {"Latency (µs)",-55}  {"Recall@" + opts.TopK,-12}");
            Console.WriteLine(new string('─', 84));

            var phaseResults = results.Where(r => r.PhaseName == phase).ToList();
            foreach (var r in phaseResults)
            {
                var recallStr = r.RecallAtK.HasValue ? $"{r.RecallAtK.Value:P2}" : "(ground-truth)";
                Console.WriteLine($"  {r.Target,-10}  {r.Latency}  {recallStr,-12}");
            }

            // If we have both Garnet and Redis results, print a latency speedup line
            var g = phaseResults.FirstOrDefault(r => r.Target == "Garnet");
            var red = phaseResults.FirstOrDefault(r => r.Target == "Redis");
            if (g != null && red != null && red.Latency.MeanUs > 0)
            {
                var note = red.RecallAtK.HasValue
                    ? "(both approximate)"
                    : "(not an apples-to-apples comparison)";
                double speedup = red.Latency.MeanUs / g.Latency.MeanUs;
                Console.WriteLine($"  → Garnet mean latency is {speedup:F2}x vs Redis {note}");
            }
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
