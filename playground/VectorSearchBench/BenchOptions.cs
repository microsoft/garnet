// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace VectorSearchBench;

[Verb("run", isDefault: true, HelpText = "Run a benchmark workload")]
public sealed class BenchOptions
{
    [Option("garnet-host", Default = "127.0.0.1", HelpText = "Garnet server hostname")]
    public string GarnetHost { get; set; } = "127.0.0.1";

    [Option("garnet-port", Default = 6380, HelpText = "Garnet server port")]
    public int GarnetPort { get; set; } = 6380;

    [Option("redis-host", Default = "127.0.0.1", HelpText = "Redis server hostname")]
    public string RedisHost { get; set; } = "127.0.0.1";

    [Option("redis-port", Default = 6379, HelpText = "Redis server port (ground-truth)")]
    public int RedisPort { get; set; } = 6379;

    [Option('w', "workload", Default = "vector-search-filter",
        HelpText = "Workload to run: vector-search-filter (vsf), vector-search-plain (vsp)")]
    public string Workload { get; set; } = "vector-search-filter";

    [Option('n', "num-vectors", Default = 5000, HelpText = "Number of vectors to insert")]
    public int NumVectors { get; set; } = 5000;

    [Option('d', "dimensions", Default = 128, HelpText = "Vector dimensionality")]
    public int Dimensions { get; set; } = 128;

    [Option('q', "num-queries", Default = 200, HelpText = "Number of query vectors per phase")]
    public int NumQueries { get; set; } = 200;

    [Option('k', "top-k", Default = 10, HelpText = "Number of neighbours to retrieve (K)")]
    public int TopK { get; set; } = 10;

    [Option("vector-key", Default = "bench:vset", HelpText = "Redis key name for the vector set")]
    public string VectorKey { get; set; } = "bench:vset";

    [Option("seed", Default = 42, HelpText = "Random seed for reproducible data generation")]
    public int Seed { get; set; } = 42;

    [Option("warmup-queries", Default = 20, HelpText = "Number of warmup queries before measurement")]
    public int WarmupQueries { get; set; } = 20;

    [Option("skip-load", Default = false, HelpText = "Skip data loading phase (use existing data)")]
    public bool SkipLoad { get; set; }

    [Option("id-prefix", Default = "", HelpText = "Prefix for element IDs (e.g., 'item:' → 'item:0', 'item:1', ...); empty = numeric only")]
    public string IdPrefix { get; set; } = "";

    [Option("ef", Default = 0, HelpText = "Garnet VSIM search exploration factor (EF). 0 = server default. High values (e.g., 5000) approximate brute-force.")]
    public int SearchEF { get; set; } = 0;

    [Option("redis-mode", Default = "bruteforce", HelpText = "Redis search mode: 'bruteforce' (client-side cosine, exact), 'hnsw' (RediSearch FT.SEARCH, approximate), or 'vsim' (Redis 8 VADD/VSIM, apples-to-apples with Garnet)")]
    public string RedisMode { get; set; } = "bruteforce";

    [Option("data-dir", Default = null, HelpText = "Path to YFCC data directory (sets workload to 'yfcc' automatically)")]
    public string? DataDir { get; set; }

    [Option("yfcc-config", Default = null, HelpText = "Path to YFCC dataset config JSON file (default: yfcc-config.json next to the executable)")]
    public string? YfccConfig { get; set; }

    [Option("yfcc-count", Default = 0, HelpText = "Number of YFCC base vectors to load (0 = all available in the file)")]
    public int YfccCount { get; set; } = 0;
}
