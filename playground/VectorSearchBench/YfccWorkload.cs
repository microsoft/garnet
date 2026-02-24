// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Binary;
using System.Diagnostics;
using System.Text.Json;

namespace VectorSearchBench;

/// <summary>
/// YFCC 1M scale benchmark workload using real-world data from the YFCC10M dataset.
///
/// Loads uint8 vectors (192 dimensions) with per-element attributes (year, month, camera,
/// country) into both Garnet and Redis 8, then runs 6 query phases at different filter
/// selectivity levels against both servers.
///
/// Ground truth is computed via client-side brute-force cosine similarity on the loaded data.
/// Both Garnet and Redis recall are measured against this brute-force ground truth.
/// </summary>
public sealed class YfccWorkload : IWorkload
{
    public string Name => "yfcc";

    // ── data structures ──────────────────────────────────────────────────────

    private record YfccRecord(string Id, byte[] Vec, string AttrJson, Dictionary<string, string> Attrs);

    private record YfccQuery(byte[] Vec, string GarnetFilter);

    // ── loaded data ──────────────────────────────────────────────────────────

    private int _dims;
    private List<YfccRecord> _baseRecords = [];

    // ── config model ─────────────────────────────────────────────────────────

    private sealed class DatasetConfig
    {
        public BaseConfig Base { get; set; } = new();
        public string UnfilteredQueryVectors { get; set; } = "";
        public List<PhaseConfig> Phases { get; set; } = [];
    }

    private sealed class BaseConfig
    {
        public string Vectors { get; set; } = "";
        public string Labels { get; set; } = "";
    }

    private sealed class PhaseConfig
    {
        public string Name { get; set; } = "";
        public string Vectors { get; set; } = "";
        public string Filters { get; set; } = "";
    }

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    // ── IWorkload ────────────────────────────────────────────────────────────

    public async Task LoadAsync(ITargetClient garnet, ITargetClient redis, BenchOptions opts, CancellationToken ct)
    {
        var dataDir = opts.DataDir ?? throw new InvalidOperationException("--data-dir is required for the yfcc workload");
        var config = LoadConfig(opts);

        // Read base vectors
        Console.WriteLine("[load] reading base vectors …");
        var (count, dims, allVecs) = ReadU8Bin(Path.Combine(dataDir, config.Base.Vectors));
        int limit = opts.YfccCount > 0 ? Math.Min(opts.YfccCount, count) : count;
        _dims = dims;

        Console.WriteLine($"[load] base file: {count} vectors × {dims} dims, using {limit}");

        // Read base labels
        Console.WriteLine("[load] reading base labels …");
        var labels = ReadLabelJsonl(Path.Combine(dataDir, config.Base.Labels), limit);

        // Build records
        _baseRecords = new List<YfccRecord>(limit);
        for (int i = 0; i < limit; i++)
        {
            var vecStart = i * dims;
            var vec = new byte[dims];
            Array.Copy(allVecs, vecStart, vec, 0, dims);

            var attrs = labels[i];
            var attrJson = JsonSerializer.Serialize(attrs);
            _baseRecords.Add(new YfccRecord(i.ToString(), vec, attrJson, attrs));
        }

        // Load into Garnet via VADD XB8
        Console.WriteLine($"[load] inserting {limit} vectors (dim={dims}, XB8) into Garnet …");
        try { await garnet.ExecuteAsync("DEL", opts.VectorKey); } catch { /* ignore */ }

        int loaded = 0;
        foreach (var r in _baseRecords)
        {
            ct.ThrowIfCancellationRequested();

            // VADD key XB8 <binary_data> <element_id> SETATTR <json_attrs>
            await garnet.ExecuteAsync("VADD",
                opts.VectorKey, "XB8", r.Vec, r.Id, "SETATTR", r.AttrJson);

            if (++loaded % 10_000 == 0)
                Console.Write($"\r[load] garnet {loaded}/{limit}  ");
        }
        Console.WriteLine($"\r[load] garnet {loaded}/{limit} done");

        // Load into Redis 8 via VADD FP32 (Redis 8 doesn't support XB8, so convert uint8→float32)
        var redisKey = $"{opts.VectorKey}:redis";
        Console.WriteLine($"[load] inserting {limit} vectors (dim={dims}, FP32) into Redis …");
        try { await redis.ExecuteAsync("DEL", redisKey); } catch { /* ignore */ }

        loaded = 0;
        foreach (var r in _baseRecords)
        {
            ct.ThrowIfCancellationRequested();

            await redis.ExecuteAsync("VADD",
                redisKey, "FP32", U8ToFP32Bytes(r.Vec), r.Id, "SETATTR", r.AttrJson);

            if (++loaded % 10_000 == 0)
                Console.Write($"\r[load] redis  {loaded}/{limit}  ");
        }
        Console.WriteLine($"\r[load] redis  {loaded}/{limit} done");
    }

    public async Task<List<WorkloadPhaseResult>> RunAsync(
        ITargetClient garnet, ITargetClient redis, BenchOptions opts, CancellationToken ct)
    {
        var dataDir = opts.DataDir ?? throw new InvalidOperationException("--data-dir is required for the yfcc workload");
        var config = LoadConfig(opts);
        var results = new List<WorkloadPhaseResult>();
        var redisKey = $"{opts.VectorKey}:redis";

        // If data wasn't loaded (--skip-load), read base data for brute-force GT
        if (_baseRecords.Count == 0)
        {
            Console.WriteLine("[run] reading base data for ground-truth computation …");
            var (count, dims, allVecs) = ReadU8Bin(Path.Combine(dataDir, config.Base.Vectors));
            int limit = opts.YfccCount > 0 ? Math.Min(opts.YfccCount, count) : count;
            _dims = dims;

            var labels = ReadLabelJsonl(Path.Combine(dataDir, config.Base.Labels), limit);
            _baseRecords = new List<YfccRecord>(limit);
            for (int i = 0; i < limit; i++)
            {
                var vecStart = i * dims;
                var vec = new byte[dims];
                Array.Copy(allVecs, vecStart, vec, 0, dims);
                var attrs = labels[i];
                _baseRecords.Add(new YfccRecord(i.ToString(), vec, JsonSerializer.Serialize(attrs), attrs));
            }
        }

        int numQueries = opts.NumQueries;

        // Phase 0: unfiltered — use configured query vectors without filter
        {
            Console.WriteLine("\n[run] phase: unfiltered");
            var (_, qDims, qVecs) = ReadU8Bin(Path.Combine(dataDir, config.UnfilteredQueryVectors));
            int qCount = Math.Min(numQueries, qVecs.Length / qDims);

            var queries = new List<byte[]>(qCount);
            for (int i = 0; i < qCount; i++)
            {
                var v = new byte[qDims];
                Array.Copy(qVecs, i * qDims, v, 0, qDims);
                queries.Add(v);
            }

            var garnetResult = await RunXB8Queries(garnet, opts.VectorKey, opts, queries, null, ct);
            var redisResult = await RunFP32Queries(redis, redisKey, opts, queries, null, ct);
            var gtResult = ComputeBruteForceGT(queries, null, opts.TopK);

            double garnetRecall = ComputeRecall(garnetResult.ids, gtResult.ids, opts.TopK);
            double redisRecall = ComputeRecall(redisResult.ids, gtResult.ids, opts.TopK);

            results.Add(new WorkloadPhaseResult
            {
                PhaseName = "unfiltered",
                Target = "Garnet",
                Latency = LatencyStats.Compute(garnetResult.latencies),
                RecallAtK = garnetRecall,
                K = opts.TopK,
                TotalQueries = qCount,
            });

            results.Add(new WorkloadPhaseResult
            {
                PhaseName = "unfiltered",
                Target = "Redis",
                Latency = LatencyStats.Compute(redisResult.latencies),
                RecallAtK = redisRecall,
                K = opts.TopK,
                TotalQueries = qCount,
            });

            results.Add(new WorkloadPhaseResult
            {
                PhaseName = "unfiltered",
                Target = "BruteForce",
                Latency = LatencyStats.Compute(gtResult.latencies),
                RecallAtK = null,
                K = opts.TopK,
                TotalQueries = qCount,
            });
        }

        // Filtered phases from config
        foreach (var phase in config.Phases)
        {
            Console.WriteLine($"\n[run] phase: {phase.Name}");

            var vecPath = Path.Combine(dataDir, phase.Vectors);
            var filterPath = Path.Combine(dataDir, phase.Filters);

            var (_, qDims, qVecs) = ReadU8Bin(vecPath);
            var filterDocs = ReadQueryFilterJsonl(filterPath, numQueries);
            int qCount = Math.Min(numQueries, Math.Min(qVecs.Length / qDims, filterDocs.Count));

            var queries = new List<byte[]>(qCount);
            var filters = new List<string>(qCount);
            for (int i = 0; i < qCount; i++)
            {
                var v = new byte[qDims];
                Array.Copy(qVecs, i * qDims, v, 0, qDims);
                queries.Add(v);
                filters.Add(TranslateFilter(filterDocs[i]));
            }

            var garnetResult = await RunXB8Queries(garnet, opts.VectorKey, opts, queries, filters, ct);
            var redisResult = await RunFP32Queries(redis, redisKey, opts, queries, filters, ct);
            var gtResult = ComputeBruteForceGT(queries, filters, opts.TopK);

            double garnetRecall = ComputeRecall(garnetResult.ids, gtResult.ids, opts.TopK);
            double redisRecall = ComputeRecall(redisResult.ids, gtResult.ids, opts.TopK);

            results.Add(new WorkloadPhaseResult
            {
                PhaseName = phase.Name,
                Target = "Garnet",
                Latency = LatencyStats.Compute(garnetResult.latencies),
                RecallAtK = garnetRecall,
                K = opts.TopK,
                TotalQueries = qCount,
            });

            results.Add(new WorkloadPhaseResult
            {
                PhaseName = phase.Name,
                Target = "Redis",
                Latency = LatencyStats.Compute(redisResult.latencies),
                RecallAtK = redisRecall,
                K = opts.TopK,
                TotalQueries = qCount,
            });

            results.Add(new WorkloadPhaseResult
            {
                PhaseName = phase.Name,
                Target = "BruteForce",
                Latency = LatencyStats.Compute(gtResult.latencies),
                RecallAtK = null,
                K = opts.TopK,
                TotalQueries = qCount,
            });
        }

        return results;
    }

    // ── config loading ───────────────────────────────────────────────────────

    private static DatasetConfig LoadConfig(BenchOptions opts)
    {
        // Resolve config path: explicit --yfcc-config, or yfcc-config.json in data dir, or next to executable
        string? configPath = opts.YfccConfig;
        if (configPath == null && opts.DataDir != null)
        {
            var candidate = Path.Combine(opts.DataDir, "yfcc-config.json");
            if (File.Exists(candidate))
                configPath = candidate;
        }
        if (configPath == null)
        {
            var exeDir = AppContext.BaseDirectory;
            var candidate = Path.Combine(exeDir, "yfcc-config.json");
            if (File.Exists(candidate))
                configPath = candidate;
        }
        if (configPath == null)
        {
            // Try current working directory
            var candidate = Path.Combine(Directory.GetCurrentDirectory(), "yfcc-config.json");
            if (File.Exists(candidate))
                configPath = candidate;
        }

        if (configPath == null || !File.Exists(configPath))
            throw new FileNotFoundException(
                "YFCC config file not found. Provide --yfcc-config or place yfcc-config.json in the data directory.");

        Console.WriteLine($"[config] loading {configPath}");
        var json = File.ReadAllText(configPath);
        return JsonSerializer.Deserialize<DatasetConfig>(json, JsonOpts)
            ?? throw new InvalidOperationException("Failed to parse YFCC config file");
    }

    // ── VSIM query helpers ─────────────────────────────────────────────────

    private static async Task<(List<long> latencies, List<string[]> ids)> RunXB8Queries(
        ITargetClient client, string key, BenchOptions opts, List<byte[]> queryVecs,
        List<string>? perQueryFilters, CancellationToken ct)
    {
        var latencies = new List<long>(queryVecs.Count);
        var ids = new List<string[]>(queryVecs.Count);

        for (int i = 0; i < queryVecs.Count; i++)
        {
            ct.ThrowIfCancellationRequested();

            // VSIM key XB8 <binary_data> COUNT <k> WITHSCORES [EF <ef>] [FILTER <expr>]
            var args = new List<object>
            {
                key, "XB8", queryVecs[i],
                "COUNT", opts.TopK.ToString(), "WITHSCORES"
            };

            if (opts.SearchEF > 0)
            {
                args.Add("EF");
                args.Add(opts.SearchEF.ToString());
            }

            if (perQueryFilters != null)
            {
                args.Add("FILTER");
                args.Add(perQueryFilters[i]);
            }

            var sw = Stopwatch.GetTimestamp();
            var result = await client.ExecuteAsync("VSIM", args.ToArray());
            var elapsed = Stopwatch.GetElapsedTime(sw);

            latencies.Add((long)elapsed.TotalMicroseconds);
            ids.Add(ParseVsimResult(result));
        }

        return (latencies, ids);
    }

    /// <summary>
    /// Query Redis 8 using FP32 format (Redis doesn't support XB8, so uint8 vectors are converted).
    /// </summary>
    private static async Task<(List<long> latencies, List<string[]> ids)> RunFP32Queries(
        ITargetClient client, string key, BenchOptions opts, List<byte[]> queryVecs,
        List<string>? perQueryFilters, CancellationToken ct)
    {
        var latencies = new List<long>(queryVecs.Count);
        var ids = new List<string[]>(queryVecs.Count);

        for (int i = 0; i < queryVecs.Count; i++)
        {
            ct.ThrowIfCancellationRequested();

            // VSIM key FP32 <binary_data> COUNT <k> WITHSCORES [EF <ef>] [FILTER <expr>]
            var args = new List<object>
            {
                key, "FP32", U8ToFP32Bytes(queryVecs[i]),
                "COUNT", opts.TopK.ToString(), "WITHSCORES"
            };

            if (opts.SearchEF > 0)
            {
                args.Add("EF");
                args.Add(opts.SearchEF.ToString());
            }

            if (perQueryFilters != null)
            {
                args.Add("FILTER");
                args.Add(perQueryFilters[i]);
            }

            var sw = Stopwatch.GetTimestamp();
            var result = await client.ExecuteAsync("VSIM", args.ToArray());
            var elapsed = Stopwatch.GetElapsedTime(sw);

            latencies.Add((long)elapsed.TotalMicroseconds);
            ids.Add(ParseVsimResult(result));
        }

        return (latencies, ids);
    }

    // ── brute-force ground truth ─────────────────────────────────────────────

    private (List<long> latencies, List<string[]> ids) ComputeBruteForceGT(
        List<byte[]> queryVecs, List<string>? perQueryFilters, int topK)
    {
        var latencies = new List<long>(queryVecs.Count);
        var ids = new List<string[]>(queryVecs.Count);

        for (int i = 0; i < queryVecs.Count; i++)
        {
            var sw = Stopwatch.GetTimestamp();

            IEnumerable<YfccRecord> candidates = _baseRecords;
            if (perQueryFilters != null)
                candidates = ApplyGarnetFilter(_baseRecords, perQueryFilters[i]);

            var scored = candidates
                .Select(r => (r.Id, Score: CosineSimilarityU8(queryVecs[i], r.Vec)))
                .OrderByDescending(x => x.Score)
                .Take(topK)
                .Select(x => x.Id)
                .ToArray();

            var elapsed = Stopwatch.GetElapsedTime(sw);
            latencies.Add((long)elapsed.TotalMicroseconds);
            ids.Add(scored);
        }

        return (latencies, ids);
    }

    // ── file I/O ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Read a .u8bin file: [u32 count, u32 dims] header followed by count*dims bytes.
    /// </summary>
    private static (int count, int dims, byte[] allVectors) ReadU8Bin(string path)
    {
        using var fs = File.OpenRead(path);
        Span<byte> header = stackalloc byte[8];
        fs.ReadExactly(header);
        int count = BinaryPrimitives.ReadInt32LittleEndian(header);
        int dims = BinaryPrimitives.ReadInt32LittleEndian(header[4..]);

        var data = new byte[(long)count * dims];
        fs.ReadExactly(data);
        return (count, dims, data);
    }

    /// <summary>
    /// Read base label JSONL. Each line: {"doc_id": N, "year": "2010", "month": "May", "camera": "Panasonic", "country": "US"}
    /// Some fields (e.g., country) may be missing.
    /// </summary>
    private static List<Dictionary<string, string>> ReadLabelJsonl(string path, int maxCount)
    {
        var results = new List<Dictionary<string, string>>(maxCount);
        foreach (var line in File.ReadLines(path))
        {
            if (results.Count >= maxCount) break;

            var doc = JsonDocument.Parse(line);
            var dict = new Dictionary<string, string>();
            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                if (prop.Name == "doc_id") continue; // skip, we use index as ID
                dict[prop.Name] = prop.Value.ToString();
            }
            results.Add(dict);
        }
        return results;
    }

    /// <summary>
    /// Read query filter JSONL. Each line: {"query_id": N, "filter": {...}}
    /// Returns just the filter JsonElement for each query.
    /// </summary>
    private static List<JsonElement> ReadQueryFilterJsonl(string path, int maxCount)
    {
        var results = new List<JsonElement>(maxCount);
        foreach (var line in File.ReadLines(path))
        {
            if (results.Count >= maxCount) break;

            using var doc = JsonDocument.Parse(line);
            // Clone the filter element so it survives after the document is disposed
            results.Add(doc.RootElement.GetProperty("filter").Clone());
        }
        return results;
    }

    // ── filter translation ───────────────────────────────────────────────────

    /// <summary>
    /// Translate MongoDB-style filter JSON to Garnet's dot-syntax filter.
    /// Examples:
    ///   {"year": {"$eq": "2009"}}  →  .year == "2009"
    ///   {"$and": [{"camera": {"$eq": "NIKON"}}, {"year": {"$eq": "2006"}}]}  →  .camera == "NIKON" &amp;&amp; .year == "2006"
    /// </summary>
    internal static string TranslateFilter(JsonElement filter)
    {
        // Check for $and
        if (filter.TryGetProperty("$and", out var andArray))
        {
            var parts = new List<string>();
            foreach (var sub in andArray.EnumerateArray())
            {
                parts.Add(TranslateSingleFilter(sub));
            }
            return string.Join(" && ", parts);
        }

        // Single filter
        return TranslateSingleFilter(filter);
    }

    private static string TranslateSingleFilter(JsonElement filter)
    {
        // Expected shape: {"field": {"$eq": "value"}}
        foreach (var prop in filter.EnumerateObject())
        {
            var field = prop.Name;
            var inner = prop.Value;
            if (inner.TryGetProperty("$eq", out var val))
            {
                return $".{field} == \"{val.GetString()}\"";
            }
        }
        throw new InvalidOperationException($"Unsupported filter format: {filter}");
    }

    // ── client-side filter for brute-force GT ────────────────────────────────

    /// <summary>
    /// Apply a Garnet-syntax filter expression to base records for brute-force GT.
    /// Supports: .field == "value" and expr1 &amp;&amp; expr2
    /// </summary>
    private static IEnumerable<YfccRecord> ApplyGarnetFilter(List<YfccRecord> records, string filter)
    {
        // Parse "&&" conjunctions
        var parts = filter.Split("&&", StringSplitOptions.TrimEntries);
        return records.Where(r =>
        {
            foreach (var part in parts)
            {
                if (!EvalSinglePredicate(r.Attrs, part))
                    return false;
            }
            return true;
        });
    }

    private static bool EvalSinglePredicate(Dictionary<string, string> attrs, string predicate)
    {
        // Expected shape: .field == "value"
        var eqIdx = predicate.IndexOf("==", StringComparison.Ordinal);
        if (eqIdx < 0) return true; // unknown predicate, pass through

        var fieldPart = predicate[..eqIdx].Trim();
        var valuePart = predicate[(eqIdx + 2)..].Trim();

        // Strip leading dot from field name
        if (fieldPart.StartsWith('.'))
            fieldPart = fieldPart[1..];

        // Strip quotes from value
        if (valuePart.StartsWith('"') && valuePart.EndsWith('"'))
            valuePart = valuePart[1..^1];

        // Check if attribute exists and matches
        return attrs.TryGetValue(fieldPart, out var actual) && actual == valuePart;
    }

    // ── vector format conversion ──────────────────────────────────────────

    /// <summary>
    /// Convert a uint8 vector to a little-endian FP32 binary blob for Redis 8 VADD/VSIM.
    /// </summary>
    private static byte[] U8ToFP32Bytes(byte[] u8Vec)
    {
        var fp32 = new byte[u8Vec.Length * sizeof(float)];
        for (int i = 0; i < u8Vec.Length; i++)
            BinaryPrimitives.WriteSingleLittleEndian(fp32.AsSpan(i * sizeof(float)), u8Vec[i]);
        return fp32;
    }

    // ── cosine similarity for uint8 vectors ──────────────────────────────────

    private static double CosineSimilarityU8(byte[] a, byte[] b)
    {
        long dot = 0, magA = 0, magB = 0;
        for (int i = 0; i < a.Length; i++)
        {
            dot += (long)a[i] * b[i];
            magA += (long)a[i] * a[i];
            magB += (long)b[i] * b[i];
        }
        if (magA == 0 || magB == 0) return 0;
        return dot / (Math.Sqrt(magA) * Math.Sqrt(magB));
    }

    // ── recall ───────────────────────────────────────────────────────────────

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

    // ── result parsing ───────────────────────────────────────────────────────

    private static string[] ParseVsimResult(StackExchange.Redis.RedisResult result)
    {
        // VSIM with WITHSCORES returns alternating [element, score, element, score …]
        if (result.IsNull) return [];
        var arr = (StackExchange.Redis.RedisResult[])result!;
        var ids = new List<string>();
        for (int i = 0; i < arr.Length; i += 2)
            ids.Add(arr[i].ToString()!);
        return [.. ids];
    }
}
