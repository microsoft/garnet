// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Text.Json;

namespace VectorSearchBench;

/// <summary>
/// Synthetic vector-search + filter workload.
///
/// Dataset layout
/// --------------
/// Each element is a random FP32 vector with a JSON attribute blob:
///   { "year": int, "rating": float, "genre": string }
///
/// Three query sub-phases are executed:
///   1. Unfiltered VSIM / VSEARCH
///   2. Numeric filter   (e.g. .year > 2000)
///   3. String filter    (e.g. .genre == "action")
///
/// Redis modes:
///   - bruteforce: stores vectors as hashes, computes cosine similarity
///     client-side for exact ground-truth recall.
///   - hnsw: uses RediSearch FT.CREATE/FT.SEARCH with HNSW index.
///   - vsim: uses Redis 8 VADD/VSIM (same commands as Garnet) for
///     apples-to-apples comparison. Uses VSIM TRUTH for exact ground-truth.
///
/// Recall is defined as:  |target_topK ∩ exactTopK| / K
/// </summary>
public sealed class VectorSearchWorkload : IWorkload
{
    public string Name => "vector-search-filter";

    // ── dataset ────────────────────────────────────────────────────────────────

    private record VectorRecord(
        string Id,
        float[] Vec,
        int Year,
        float Rating,
        string Genre);

    private static readonly string[] Genres = ["action", "drama", "comedy", "thriller", "sci-fi"];

    private List<VectorRecord> _dataset = [];
    private List<VectorRecord> _queries = [];

    // ── IWorkload ────────────────────────────────────────────────────────────

    public async Task LoadAsync(ITargetClient garnet, ITargetClient redis, BenchOptions opts, CancellationToken ct)
    {
        var rng = new Random(opts.Seed);
        _dataset = GenerateRecords(opts.NumVectors, opts.Dimensions, rng, opts.IdPrefix);
        _queries = GenerateRecords(opts.NumQueries, opts.Dimensions, rng, opts.IdPrefix);

        Console.WriteLine($"[load] inserting {_dataset.Count} vectors (dim={opts.Dimensions}) into Garnet …");
        await LoadGarnetAsync(garnet, opts, ct);

        bool isVsim = opts.RedisMode.Equals("vsim", StringComparison.OrdinalIgnoreCase);
        Console.WriteLine($"[load] inserting {_dataset.Count} vectors into Redis ({(isVsim ? "VADD" : "HSET")}) …");
        await LoadRedisAsync(redis, opts, ct);

        Console.WriteLine("[load] done");
    }

    public async Task<List<WorkloadPhaseResult>> RunAsync(
        ITargetClient garnet, ITargetClient redis, BenchOptions opts, CancellationToken ct)
    {
        // Ensure dataset/queries are generated even when --skip-load was used.
        // The same seed produces the same vectors, so queries match the loaded data.
        // Always regenerate if counts don't match the requested sizes.
        if (_dataset.Count != opts.NumVectors || _queries.Count != opts.NumQueries)
        {
            var rng = new Random(opts.Seed);
            _dataset = GenerateRecords(opts.NumVectors, opts.Dimensions, rng, opts.IdPrefix);
            _queries = GenerateRecords(opts.NumQueries, opts.Dimensions, rng, opts.IdPrefix);
        }

        var results = new List<WorkloadPhaseResult>();
        bool isHnsw = opts.RedisMode.Equals("hnsw", StringComparison.OrdinalIgnoreCase);
        bool isVsim = opts.RedisMode.Equals("vsim", StringComparison.OrdinalIgnoreCase);
        bool needsGroundTruth = isHnsw || isVsim;

        var phases = new (string Label, string? Filter)[]
        {
            ("unfiltered",     null),
            ("filter-year",    ".year > 2000"),
            ("filter-genre",   $".genre == \"{Genres[0]}\""),
        };

        // When using HNSW or VSIM mode, pre-compute ground-truth once
        // so we can measure recall for both Garnet and Redis.
        // For VSIM mode, use VSIM TRUTH (exact linear scan on Redis).
        // For HNSW mode, use client-side brute-force.
        List<(string Label, List<string[]> GroundTruth)>? groundTruthPerPhase = null;
        if (needsGroundTruth)
        {
            groundTruthPerPhase = [];
            foreach (var (label, filter) in phases)
            {
                List<string[]> gtIds;
                if (isVsim)
                {
                    (_, gtIds) = await RunRedisVsimTruthAsync(redis, opts, filter, ct);
                }
                else
                {
                    (_, gtIds) = await RunRedisGroundTruthAsync(redis, opts, filter, ct);
                }
                groundTruthPerPhase.Add((label, gtIds));
            }
        }

        for (int p = 0; p < phases.Length; p++)
        {
            var (label, filter) = phases[p];
            Console.WriteLine($"\n[run] phase: {label}  filter={filter ?? "(none)"}");

            var (garnetLatencies, garnetIds) = await RunGarnetQueriesAsync(garnet, opts, filter, ct);

            List<long> redisLatencies;
            List<string[]> redisIds;
            List<string[]> groundTruthIds;

            if (isVsim)
            {
                (redisLatencies, redisIds) = await RunRedisVsimAsync(redis, opts, filter, ct);
                groundTruthIds = groundTruthPerPhase![p].GroundTruth;
            }
            else if (isHnsw)
            {
                (redisLatencies, redisIds) = await RunRedisHNSWAsync(redis, opts, filter, ct);
                groundTruthIds = groundTruthPerPhase![p].GroundTruth;
            }
            else
            {
                (redisLatencies, redisIds) = await RunRedisGroundTruthAsync(redis, opts, filter, ct);
                groundTruthIds = redisIds;
            }

            var garnetLatStats = LatencyStats.Compute(garnetLatencies);
            var redisLatStats  = LatencyStats.Compute(redisLatencies);

            double garnetRecall = ComputeRecall(garnetIds, groundTruthIds, opts.TopK);

            results.Add(new WorkloadPhaseResult
            {
                PhaseName    = label,
                Target       = garnet.Name,
                Latency      = garnetLatStats,
                RecallAtK    = garnetRecall,
                K            = opts.TopK,
                TotalQueries = opts.NumQueries,
            });

            results.Add(new WorkloadPhaseResult
            {
                PhaseName    = label,
                Target       = redis.Name,
                Latency      = redisLatStats,
                RecallAtK    = needsGroundTruth ? ComputeRecall(redisIds, groundTruthIds, opts.TopK) : null,
                K            = opts.TopK,
                TotalQueries = opts.NumQueries,
            });
        }

        return results;
    }

    // ── loading helpers ───────────────────────────────────────────────────────

    private async Task LoadGarnetAsync(ITargetClient garnet, BenchOptions opts, CancellationToken ct)
    {
        // Delete existing key so we start fresh
        try { await garnet.ExecuteAsync("DEL", opts.VectorKey); } catch { /* ignore */ }

        int batch = 0;
        foreach (var r in _dataset)
        {
            ct.ThrowIfCancellationRequested();

            var floatArgs = VecToStringArgs(r.Vec);
            var attrJson  = BuildAttrJson(r);

            // VADD key VALUES <dim> <f1> <f2> … <element> SETATTR <json>
            var args = new List<object>
            {
                opts.VectorKey, "VALUES", opts.Dimensions.ToString()
            };
            args.AddRange(floatArgs.Cast<object>());
            args.Add(r.Id);
            args.Add("SETATTR");
            args.Add(attrJson);

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
            // VSIM mode: use VADD — same commands as Garnet for apples-to-apples comparison
            try { await redis.ExecuteAsync("DEL", $"{opts.VectorKey}:redis"); } catch { /* ignore */ }

            int batch = 0;
            foreach (var r in _dataset)
            {
                ct.ThrowIfCancellationRequested();

                var floatArgs = VecToStringArgs(r.Vec);
                var attrJson  = BuildAttrJson(r);

                var args = new List<object>
                {
                    $"{opts.VectorKey}:redis", "VALUES", opts.Dimensions.ToString()
                };
                args.AddRange(floatArgs.Cast<object>());
                args.Add(r.Id);
                args.Add("SETATTR");
                args.Add(attrJson);

                await redis.ExecuteAsync("VADD", args.ToArray());

                if (++batch % 500 == 0)
                    Console.Write($"\r[load] redis  {batch}/{_dataset.Count}  ");
            }
            Console.WriteLine();
            return;
        }

        // HSET-based modes (bruteforce or hnsw)
        bool isHnsw = opts.RedisMode.Equals("hnsw", StringComparison.OrdinalIgnoreCase);
        int hsetBatch = 0;
        foreach (var r in _dataset)
        {
            ct.ThrowIfCancellationRequested();

            var hashKey = $"{opts.VectorKey}:vec:{r.Id}";
            object vecValue = isHnsw ? (object)VecToBytes(r.Vec) : VecToBase64(r.Vec);
            await redis.ExecuteAsync("HSET",
                hashKey,
                "vec", vecValue,
                "year", r.Year.ToString(),
                "rating", r.Rating.ToString("F4"),
                "genre", r.Genre,
                "id", r.Id);

            if (++hsetBatch % 500 == 0)
                Console.Write($"\r[load] redis  {hsetBatch}/{_dataset.Count}  ");
        }
        Console.WriteLine();

        // Also store the index key list so we can scan efficiently
        await redis.ExecuteAsync("DEL", $"{opts.VectorKey}:index");
        var ids = _dataset.Select(r => (object)r.Id).ToArray();
        await redis.ExecuteAsync("RPUSH", new object[] { $"{opts.VectorKey}:index" }.Concat(ids).ToArray());

        // If HNSW mode, create a RediSearch index for vector similarity search
        if (opts.RedisMode.Equals("hnsw", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine("[load] creating RediSearch vector index …");
            try { await redis.ExecuteAsync("FT.DROPINDEX", $"{opts.VectorKey}:idx"); } catch { /* ignore if not exists */ }

            await redis.ExecuteAsync("FT.CREATE",
                $"{opts.VectorKey}:idx",
                "ON", "HASH",
                "PREFIX", "1", $"{opts.VectorKey}:vec:",
                "SCHEMA",
                "vec", "VECTOR", "HNSW", "6",
                    "TYPE", "FLOAT32",
                    "DIM", opts.Dimensions.ToString(),
                    "DISTANCE_METRIC", "COSINE",
                "year", "NUMERIC",
                "genre", "TAG");
            Console.WriteLine("[load] RediSearch index created");
        }
    }

    // ── query helpers ─────────────────────────────────────────────────────────

    private async Task<(List<long> latenciesUs, List<string[]> perQueryIds)> RunGarnetQueriesAsync(
        ITargetClient garnet, BenchOptions opts, string? filter, CancellationToken ct)
    {
        return await RunVsimQueriesAsync(garnet, opts.VectorKey, opts, filter, false, ct);
    }

    private async Task<(List<long> latenciesUs, List<string[]> perQueryIds)> RunRedisGroundTruthAsync(
        ITargetClient redis, BenchOptions opts, string? filter, CancellationToken ct)
    {
        // Brute-force: compute cosine similarity client-side.
        // This is deliberately slow – its only purpose is recall ground-truth, not perf comparison.
        var latencies  = new List<long>(opts.NumQueries);
        var perQueryIds = new List<string[]>(opts.NumQueries);

        // Use in-memory dataset when available (always except --skip-load),
        // otherwise fetch from Redis (base64 mode only).
        List<StoredRecord> stored;
        if (_dataset.Count > 0)
        {
            stored = _dataset.Select(r => new StoredRecord(r.Id, r.Vec, r.Year, r.Rating, r.Genre)).ToList();
        }
        else
        {
            stored = await FetchAllStoredVectorsAsync(redis, opts);
        }

        foreach (var q in _queries)
        {
            ct.ThrowIfCancellationRequested();

            var sw = Stopwatch.GetTimestamp();

            // Apply filter
            var candidates = filter == null ? stored : ApplyFilter(stored, filter);

            // Score
            var scored = candidates
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

    private async Task<(List<long> latenciesUs, List<string[]> perQueryIds)> RunRedisHNSWAsync(
        ITargetClient redis, BenchOptions opts, string? filter, CancellationToken ct)
    {
        var latencies  = new List<long>(opts.NumQueries);
        var perQueryIds = new List<string[]>(opts.NumQueries);

        var idxName = $"{opts.VectorKey}:idx";

        foreach (var q in _queries)
        {
            ct.ThrowIfCancellationRequested();

            var vecBytes = VecToBytes(q.Vec);

            // Build the FT.SEARCH query string
            string queryStr;
            if (filter == null)
            {
                queryStr = $"*=>[KNN {opts.TopK} @vec $query_vec]";
            }
            else if (filter.StartsWith(".year > "))
            {
                var threshold = filter[".year > ".Length..];
                queryStr = $"@year:[({threshold} +inf]=>[KNN {opts.TopK} @vec $query_vec]";
            }
            else if (filter.StartsWith(".genre == \""))
            {
                var genre = filter[".genre == \"".Length..].TrimEnd('"');
                queryStr = $"@genre:{{{genre}}}=>[KNN {opts.TopK} @vec $query_vec]";
            }
            else
            {
                queryStr = $"*=>[KNN {opts.TopK} @vec $query_vec]";
            }

            var sw = Stopwatch.GetTimestamp();
            var result = await redis.ExecuteAsync("FT.SEARCH",
                idxName,
                queryStr,
                "PARAMS", "2", "query_vec", vecBytes,
                "SORTBY", "__vec_score",
                "DIALECT", "2");
            var elapsed = Stopwatch.GetElapsedTime(sw);

            latencies.Add((long)(elapsed.TotalMicroseconds));
            perQueryIds.Add(ParseFtSearchResult(result, opts));
        }

        return (latencies, perQueryIds);
    }

    private async Task<(List<long> latenciesUs, List<string[]> perQueryIds)> RunRedisVsimAsync(
        ITargetClient redis, BenchOptions opts, string? filter, CancellationToken ct)
    {
        // Same VSIM command as Garnet — apples-to-apples comparison
        return await RunVsimQueriesAsync(redis, $"{opts.VectorKey}:redis", opts, filter, false, ct);
    }

    private async Task<(List<long> latenciesUs, List<string[]> perQueryIds)> RunRedisVsimTruthAsync(
        ITargetClient redis, BenchOptions opts, string? filter, CancellationToken ct)
    {
        // VSIM TRUTH = exact linear scan on Redis, used as ground-truth
        return await RunVsimQueriesAsync(redis, $"{opts.VectorKey}:redis", opts, filter, true, ct);
    }

    private async Task<(List<long> latenciesUs, List<string[]> perQueryIds)> RunVsimQueriesAsync(
        ITargetClient client, string key, BenchOptions opts, string? filter, bool truth, CancellationToken ct)
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

            if (filter != null)
            {
                args.Add("FILTER");
                args.Add(filter);
            }

            var sw = Stopwatch.GetTimestamp();
            var result = await client.ExecuteAsync("VSIM", args.ToArray());
            var elapsed = Stopwatch.GetElapsedTime(sw);

            latencies.Add((long)(elapsed.TotalMicroseconds));
            perQueryIds.Add(ParseVsimResult(result));
        }

        return (latencies, perQueryIds);
    }

    private static string[] ParseFtSearchResult(StackExchange.Redis.RedisResult result, BenchOptions opts)
    {
        // FT.SEARCH returns: [total_count, key1, [field, value, ...], key2, [field, value, ...], ...]
        if (result.IsNull) return [];
        var arr = (StackExchange.Redis.RedisResult[])result!;
        if (arr.Length < 2) return [];

        var ids = new List<string>();
        var prefix = $"{opts.VectorKey}:vec:";

        // arr[0] = total count, then pairs of (key, fields_array)
        for (int i = 1; i + 1 < arr.Length; i += 2)
        {
            var key = arr[i].ToString()!;
            // Strip the hash key prefix to get the original element ID
            var id = key.StartsWith(prefix) ? key[prefix.Length..] : key;
            ids.Add(id);
        }

        return [.. ids];
    }

    private static byte[] VecToBytes(float[] vec)
    {
        var bytes = new byte[vec.Length * sizeof(float)];
        Buffer.BlockCopy(vec, 0, bytes, 0, bytes.Length);
        return bytes;
    }

    private async Task<List<StoredRecord>> FetchAllStoredVectorsAsync(ITargetClient redis, BenchOptions opts)
    {
        var idListKey = $"{opts.VectorKey}:index";
        var idsResult = await redis.ExecuteAsync("LRANGE", idListKey, "0", "-1");
        var ids = ParseStringArray(idsResult);

        var records = new List<StoredRecord>(ids.Length);
        foreach (var id in ids)
        {
            var hashKey = $"{opts.VectorKey}:vec:{id}";
            var fields = await redis.ExecuteAsync("HGETALL", hashKey);
            var dict = ParseHGetAll(fields);

            if (!dict.TryGetValue("vec", out var vecB64)) continue;
            if (!dict.TryGetValue("year", out var yearStr)) continue;
            if (!dict.TryGetValue("rating", out var ratingStr)) continue;
            if (!dict.TryGetValue("genre", out var genre)) continue;

            records.Add(new StoredRecord(
                id,
                Base64ToVec(vecB64),
                int.Parse(yearStr),
                float.Parse(ratingStr),
                genre));
        }

        return records;
    }

    private record StoredRecord(string Id, float[] Vec, int Year, float Rating, string Genre);

    private static IEnumerable<StoredRecord> ApplyFilter(List<StoredRecord> records, string filter)
    {
        // Minimal filter interpreter matching Garnet's filter syntax subset used in this workload.
        if (filter.StartsWith(".year > "))
        {
            int threshold = int.Parse(filter[".year > ".Length..]);
            return records.Where(r => r.Year > threshold);
        }
        if (filter.StartsWith(".genre == \""))
        {
            var genre = filter[".genre == \"".Length..].TrimEnd('"');
            return records.Where(r => r.Genre == genre);
        }
        return records;
    }

    // ── recall ────────────────────────────────────────────────────────────────

    private static double ComputeRecall(List<string[]> garnetResults, List<string[]> groundTruth, int k)
    {
        if (garnetResults.Count == 0) return 0;
        double total = 0;
        for (int i = 0; i < garnetResults.Count; i++)
        {
            var gt  = new HashSet<string>(groundTruth[i].Take(k));
            var got = garnetResults[i].Take(k).ToHashSet();
            total += gt.Count == 0 ? 1.0 : (double)got.Intersect(gt).Count() / gt.Count;
        }
        return total / garnetResults.Count;
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
                Vec: vec,
                Year: rng.Next(1980, 2025),
                Rating: (float)(rng.NextDouble() * 4 + 1),
                Genre: Genres[rng.Next(Genres.Length)]));
        }
        return records;
    }

    // ── serialisation helpers ─────────────────────────────────────────────────

    private static string[] VecToStringArgs(float[] vec) =>
        vec.Select(f => f.ToString("G9")).ToArray();

    private static string BuildAttrJson(VectorRecord r) =>
        JsonSerializer.Serialize(new { year = r.Year, rating = r.Rating, genre = r.Genre });

    private static string VecToBase64(float[] vec)
    {
        var bytes = new byte[vec.Length * sizeof(float)];
        Buffer.BlockCopy(vec, 0, bytes, 0, bytes.Length);
        return Convert.ToBase64String(bytes);
    }

    private static float[] Base64ToVec(string b64)
    {
        var bytes = Convert.FromBase64String(b64);
        var vec   = new float[bytes.Length / sizeof(float)];
        Buffer.BlockCopy(bytes, 0, vec, 0, bytes.Length);
        return vec;
    }

    private static float CosineSimilarity(float[] a, float[] b)
    {
        double dot = 0, magA = 0, magB = 0;
        for (int i = 0; i < a.Length; i++)
        {
            dot  += a[i] * b[i];
            magA += a[i] * a[i];
            magB += b[i] * b[i];
        }
        if (magA == 0 || magB == 0) return 0;
        return (float)(dot / (Math.Sqrt(magA) * Math.Sqrt(magB)));
    }

    // ── result parsing helpers ─────────────────────────────────────────────────

    private static string[] ParseVsimResult(StackExchange.Redis.RedisResult result)
    {
        // VSIM with WITHSCORES returns alternating [element, score, element, score …]
        // Without WITHSCORES it returns a flat array of element names.
        if (result.IsNull) return [];
        var arr = (StackExchange.Redis.RedisResult[])result!;
        var ids = new List<string>();
        for (int i = 0; i < arr.Length; i += 2)
            ids.Add(arr[i].ToString()!);
        return [.. ids];
    }

    private static string[] ParseStringArray(StackExchange.Redis.RedisResult result)
    {
        if (result.IsNull) return [];
        var arr = (StackExchange.Redis.RedisResult[])result!;
        return arr.Select(r => r.ToString()!).ToArray();
    }

    private static Dictionary<string, string> ParseHGetAll(StackExchange.Redis.RedisResult result)
    {
        var dict = new Dictionary<string, string>();
        if (result.IsNull) return dict;
        var arr = (StackExchange.Redis.RedisResult[])result!;
        for (int i = 0; i + 1 < arr.Length; i += 2)
            dict[arr[i].ToString()!] = arr[i + 1].ToString()!;
        return dict;
    }
}
