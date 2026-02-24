// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

namespace VectorSearchBench;

/// <summary>
/// Plain vector-search workload WITHOUT attributes.
///
/// Inserts vectors via VADD without SETATTR, runs only unfiltered VSIM queries.
/// Useful for isolating vector insert/query behavior from attribute handling.
///
/// Redis modes:
///   - bruteforce: stores vectors as hashes, computes cosine similarity
///     client-side for exact ground-truth recall.
///   - vsim: uses Redis 8 VADD/VSIM (same commands as Garnet) for
///     apples-to-apples comparison. Uses VSIM TRUTH for exact ground-truth.
///
/// Recall is defined as:  |target_topK ∩ exactTopK| / K
/// </summary>
public sealed class VectorSearchPlainWorkload : IWorkload
{
    public string Name => "vector-search-plain";

    private record VectorRecord(string Id, float[] Vec);

    private List<VectorRecord> _dataset = [];
    private List<VectorRecord> _queries = [];

    // ── IWorkload ────────────────────────────────────────────────────────────

    public async Task LoadAsync(ITargetClient garnet, ITargetClient redis, BenchOptions opts, CancellationToken ct)
    {
        var rng = new Random(opts.Seed);
        _dataset = GenerateRecords(opts.NumVectors, opts.Dimensions, rng, opts.IdPrefix);
        _queries = GenerateRecords(opts.NumQueries, opts.Dimensions, rng, opts.IdPrefix);

        Console.WriteLine($"[load] inserting {_dataset.Count} vectors (dim={opts.Dimensions}, NO attributes) into Garnet …");
        await LoadGarnetAsync(garnet, opts, ct);

        bool isVsim = opts.RedisMode.Equals("vsim", StringComparison.OrdinalIgnoreCase);
        Console.WriteLine($"[load] inserting {_dataset.Count} vectors into Redis ({(isVsim ? "VADD" : "HSET")}) …");
        await LoadRedisAsync(redis, opts, ct);

        Console.WriteLine("[load] done");
    }

    public async Task<List<WorkloadPhaseResult>> RunAsync(
        ITargetClient garnet, ITargetClient redis, BenchOptions opts, CancellationToken ct)
    {
        if (_dataset.Count != opts.NumVectors || _queries.Count != opts.NumQueries)
        {
            var rng = new Random(opts.Seed);
            _dataset = GenerateRecords(opts.NumVectors, opts.Dimensions, rng, opts.IdPrefix);
            _queries = GenerateRecords(opts.NumQueries, opts.Dimensions, rng, opts.IdPrefix);
        }

        var results = new List<WorkloadPhaseResult>();
        bool isVsim = opts.RedisMode.Equals("vsim", StringComparison.OrdinalIgnoreCase);

        // Only one phase: unfiltered
        Console.WriteLine("\n[run] phase: unfiltered (no attributes)");

        var (garnetLatencies, garnetIds) = await RunVsimQueriesAsync(garnet, opts.VectorKey, opts, false, ct);

        List<long> redisLatencies;
        List<string[]> redisIds;
        List<string[]> groundTruthIds;

        if (isVsim)
        {
            // Pre-compute ground-truth via VSIM TRUTH
            List<string[]> gtIds;
            (_, gtIds) = await RunVsimQueriesAsync(redis, $"{opts.VectorKey}:redis", opts, true, ct);

            (redisLatencies, redisIds) = await RunVsimQueriesAsync(redis, $"{opts.VectorKey}:redis", opts, false, ct);
            groundTruthIds = gtIds;
        }
        else
        {
            // Brute-force ground-truth
            (redisLatencies, redisIds) = RunBruteForceGroundTruth(opts);
            groundTruthIds = redisIds;
        }

        var garnetLatStats = LatencyStats.Compute(garnetLatencies);
        var redisLatStats = LatencyStats.Compute(redisLatencies);

        double garnetRecall = ComputeRecall(garnetIds, groundTruthIds, opts.TopK);

        results.Add(new WorkloadPhaseResult
        {
            PhaseName = "unfiltered-plain",
            Target = garnet.Name,
            Latency = garnetLatStats,
            RecallAtK = garnetRecall,
            K = opts.TopK,
            TotalQueries = opts.NumQueries,
        });

        results.Add(new WorkloadPhaseResult
        {
            PhaseName = "unfiltered-plain",
            Target = redis.Name,
            Latency = redisLatStats,
            RecallAtK = isVsim ? ComputeRecall(redisIds, groundTruthIds, opts.TopK) : null,
            K = opts.TopK,
            TotalQueries = opts.NumQueries,
        });

        return results;
    }

    // ── loading helpers ───────────────────────────────────────────────────────

    private async Task LoadGarnetAsync(ITargetClient garnet, BenchOptions opts, CancellationToken ct)
    {
        try { await garnet.ExecuteAsync("DEL", opts.VectorKey); } catch { /* ignore */ }

        int batch = 0;
        foreach (var r in _dataset)
        {
            ct.ThrowIfCancellationRequested();

            var floatArgs = VecToStringArgs(r.Vec);

            // VADD key VALUES <dim> <f1> <f2> … <element>  (NO SETATTR)
            var args = new List<object>
            {
                opts.VectorKey, "VALUES", opts.Dimensions.ToString()
            };
            args.AddRange(floatArgs.Cast<object>());
            args.Add(r.Id);

            await garnet.ExecuteAsync("VADD", args.ToArray());

            if (++batch % 500 == 0)
                Console.Write($"\r[load] garnet {batch}/{_dataset.Count}  ");
        }
        Console.WriteLine();
    }

    private async Task LoadRedisAsync(ITargetClient redis, BenchOptions opts, CancellationToken ct)
    {
        bool isVsim = opts.RedisMode.Equals("vsim", StringComparison.OrdinalIgnoreCase);

        if (isVsim)
        {
            try { await redis.ExecuteAsync("DEL", $"{opts.VectorKey}:redis"); } catch { /* ignore */ }

            int batch = 0;
            foreach (var r in _dataset)
            {
                ct.ThrowIfCancellationRequested();

                var floatArgs = VecToStringArgs(r.Vec);

                // VADD without SETATTR — same as Garnet
                var args = new List<object>
                {
                    $"{opts.VectorKey}:redis", "VALUES", opts.Dimensions.ToString()
                };
                args.AddRange(floatArgs.Cast<object>());
                args.Add(r.Id);

                await redis.ExecuteAsync("VADD", args.ToArray());

                if (++batch % 500 == 0)
                    Console.Write($"\r[load] redis  {batch}/{_dataset.Count}  ");
            }
            Console.WriteLine();
            return;
        }

        // Brute-force mode: store vectors as hashes (no attributes needed but keep vec + id)
        int hsetBatch = 0;
        foreach (var r in _dataset)
        {
            ct.ThrowIfCancellationRequested();

            var hashKey = $"{opts.VectorKey}:vec:{r.Id}";
            await redis.ExecuteAsync("HSET",
                hashKey,
                "vec", VecToBase64(r.Vec),
                "id", r.Id);

            if (++hsetBatch % 500 == 0)
                Console.Write($"\r[load] redis  {hsetBatch}/{_dataset.Count}  ");
        }
        Console.WriteLine();

        await redis.ExecuteAsync("DEL", $"{opts.VectorKey}:index");
        var ids = _dataset.Select(r => (object)r.Id).ToArray();
        await redis.ExecuteAsync("RPUSH", new object[] { $"{opts.VectorKey}:index" }.Concat(ids).ToArray());
    }

    // ── query helpers ─────────────────────────────────────────────────────────

    private async Task<(List<long> latenciesUs, List<string[]> perQueryIds)> RunVsimQueriesAsync(
        ITargetClient client, string key, BenchOptions opts, bool truth, CancellationToken ct)
    {
        var latencies = new List<long>(opts.NumQueries);
        var perQueryIds = new List<string[]>(opts.NumQueries);

        foreach (var q in _queries)
        {
            ct.ThrowIfCancellationRequested();

            var args = new List<object>
            {
                key, "VALUES", opts.Dimensions.ToString()
            };
            args.AddRange(VecToStringArgs(q.Vec).Cast<object>());
            args.Add("COUNT");
            args.Add(opts.TopK.ToString());
            args.Add("WITHSCORES");

            if (truth)
            {
                args.Add("TRUTH");
            }
            else if (opts.SearchEF > 0)
            {
                args.Add("EF");
                args.Add(opts.SearchEF.ToString());
            }

            var sw = Stopwatch.GetTimestamp();
            var result = await client.ExecuteAsync("VSIM", args.ToArray());
            var elapsed = Stopwatch.GetElapsedTime(sw);

            latencies.Add((long)(elapsed.TotalMicroseconds));
            perQueryIds.Add(ParseVsimResult(result));
        }

        return (latencies, perQueryIds);
    }

    private (List<long> latenciesUs, List<string[]> perQueryIds) RunBruteForceGroundTruth(BenchOptions opts)
    {
        var latencies = new List<long>(opts.NumQueries);
        var perQueryIds = new List<string[]>(opts.NumQueries);

        foreach (var q in _queries)
        {
            var sw = Stopwatch.GetTimestamp();

            var scored = _dataset
                .Select(r => (r.Id, Score: CosineSimilarity(q.Vec, r.Vec)))
                .OrderByDescending(x => x.Score)
                .Take(opts.TopK)
                .Select(x => x.Id)
                .ToArray();

            var elapsed = Stopwatch.GetElapsedTime(sw);
            latencies.Add((long)(elapsed.TotalMicroseconds));
            perQueryIds.Add(scored);
        }

        return (latencies, perQueryIds);
    }

    // ── recall ────────────────────────────────────────────────────────────────

    private static double ComputeRecall(List<string[]> targetResults, List<string[]> groundTruth, int k)
    {
        if (targetResults.Count == 0) return 0;
        double total = 0;
        for (int i = 0; i < targetResults.Count; i++)
        {
            var gt = new HashSet<string>(groundTruth[i].Take(k));
            var got = targetResults[i].Take(k).ToHashSet();
            total += gt.Count == 0 ? 1.0 : (double)got.Intersect(gt).Count() / gt.Count;
        }
        return total / targetResults.Count;
    }

    // ── data generation ───────────────────────────────────────────────────────

    private static List<VectorRecord> GenerateRecords(int count, int dim, Random rng, string idPrefix = "")
    {
        var records = new List<VectorRecord>(count);
        for (int i = 0; i < count; i++)
        {
            var vec = new float[dim];
            for (int d = 0; d < dim; d++)
                vec[d] = (float)(rng.NextDouble() * 2 - 1);
            records.Add(new VectorRecord(
                Id: $"{idPrefix}{i}",
                Vec: vec));
        }
        return records;
    }

    // ── serialisation helpers ─────────────────────────────────────────────────

    private static string[] VecToStringArgs(float[] vec) =>
        vec.Select(f => f.ToString("G9")).ToArray();

    private static string VecToBase64(float[] vec)
    {
        var bytes = new byte[vec.Length * sizeof(float)];
        Buffer.BlockCopy(vec, 0, bytes, 0, bytes.Length);
        return Convert.ToBase64String(bytes);
    }

    private static float CosineSimilarity(float[] a, float[] b)
    {
        double dot = 0, magA = 0, magB = 0;
        for (int i = 0; i < a.Length; i++)
        {
            dot += a[i] * b[i];
            magA += a[i] * a[i];
            magB += b[i] * b[i];
        }
        if (magA == 0 || magB == 0) return 0;
        return (float)(dot / (Math.Sqrt(magA) * Math.Sqrt(magB)));
    }

    private static string[] ParseVsimResult(StackExchange.Redis.RedisResult result)
    {
        if (result.IsNull) return [];
        var arr = (StackExchange.Redis.RedisResult[])result!;
        var ids = new List<string>();
        for (int i = 0; i < arr.Length; i += 2)
            ids.Add(arr[i].ToString()!);
        return [.. ids];
    }
}
