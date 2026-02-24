// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace VectorSearchBench;

/// <summary>
/// Latency statistics computed from a sorted list of measured durations (microseconds).
/// </summary>
public sealed class LatencyStats
{
    public double P50Us { get; init; }
    public double P95Us { get; init; }
    public double P99Us { get; init; }
    public double MeanUs { get; init; }
    public double MinUs { get; init; }
    public double MaxUs { get; init; }
    public int SampleCount { get; init; }

    public static LatencyStats Compute(List<long> microseconds)
    {
        if (microseconds.Count == 0)
            return new LatencyStats();

        microseconds.Sort();
        int n = microseconds.Count;
        double P(double pct) => microseconds[(int)Math.Min(n - 1, Math.Ceiling(pct / 100.0 * n) - 1)];

        return new LatencyStats
        {
            SampleCount = n,
            MinUs = microseconds[0],
            MaxUs = microseconds[n - 1],
            MeanUs = microseconds.Average(),
            P50Us = P(50),
            P95Us = P(95),
            P99Us = P(99),
        };
    }

    public override string ToString() =>
        $"  n={SampleCount,6}  mean={MeanUs,7:F1}µs  p50={P50Us,7:F1}µs  p95={P95Us,7:F1}µs  p99={P99Us,7:F1}µs  min={MinUs,7:F1}µs  max={MaxUs,7:F1}µs";
}

/// <summary>
/// Per-query result from a single target (Garnet or Redis).
/// </summary>
public sealed class QueryResult
{
    public required string[] ReturnedIds { get; init; }
    public long ElapsedUs { get; init; }
}

/// <summary>
/// Aggregated benchmark results for one workload phase on one target.
/// </summary>
public sealed class WorkloadPhaseResult
{
    public required string PhaseName { get; init; }
    public required string Target { get; init; }
    public required LatencyStats Latency { get; init; }
    /// <summary>
    /// Recall@K vs the ground-truth set.  Null when this target IS the ground-truth.
    /// </summary>
    public double? RecallAtK { get; init; }
    public int K { get; init; }
    public int TotalQueries { get; init; }
}

/// <summary>
/// Contract every workload must fulfil.
/// </summary>
public interface IWorkload
{
    string Name { get; }

    /// <summary>
    /// Load data into both targets (idempotent – clears existing data first).
    /// </summary>
    Task LoadAsync(ITargetClient garnet, ITargetClient redis, BenchOptions opts, CancellationToken ct);

    /// <summary>
    /// Execute queries and return per-target results.  Redis results are used as ground-truth recall.
    /// </summary>
    Task<List<WorkloadPhaseResult>> RunAsync(ITargetClient garnet, ITargetClient redis, BenchOptions opts, CancellationToken ct);
}

/// <summary>
/// Thin wrapper around a Redis/Garnet connection that issues raw RESP commands.
/// </summary>
public interface ITargetClient : IAsyncDisposable
{
    string Name { get; }
    /// <summary>Execute a raw Redis command and return the result object.</summary>
    Task<StackExchange.Redis.RedisResult> ExecuteAsync(string command, params object[] args);
    Task PingAsync();
}
