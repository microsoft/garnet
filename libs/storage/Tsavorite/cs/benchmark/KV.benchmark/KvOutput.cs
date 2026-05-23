// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// Output emitter. Three streams share one schema:
    /// <list type="bullet">
    ///   <item>Human (stdout) — config block, per-phase one-liner, optional aggregate line, final summary block.</item>
    ///   <item>JSON — pretty-printed file (<c>--json-output</c>) and/or single-line stdout (<c>--json-stdout</c>).</item>
    ///   <item>CSV — wide schema, one row per phase + aggregate row (<c>--csv-output</c>).</item>
    /// </list>
    /// Implementation is split across partial files: <c>KvOutput.cs</c> (human + final summary + shared helpers),
    /// <c>KvOutput.Json.cs</c>, <c>KvOutput.Csv.cs</c>.
    /// </summary>
    internal sealed partial class KvOutput
    {
        const string SchemaVersion = "1";

        readonly Options _opts;
        readonly string _dataPath;
        readonly string _argv;
        readonly bool _csvEnabled;
        readonly bool _jsonEnabled;

        public KvOutput(Options opts, string dataPath, string[] args)
        {
            _opts = opts;
            _dataPath = dataPath;
            _argv = "[" + string.Join(",", new[] { "dotnet", "KV.benchmark.dll" }.Concat(args).Select(JsonString)) + "]";
            _csvEnabled = !string.IsNullOrWhiteSpace(opts.CsvOutput);
            _jsonEnabled = !string.IsNullOrWhiteSpace(opts.JsonOutput);
        }

        // ====== Human-readable (stdout) ======

        public void EmitConfigHuman(KvNumaPinning pinning)
        {
            if (_opts.Quiet) return;
            Console.WriteLine("=== KV.benchmark config ===");
            Console.WriteLine($"  threads          : {_opts.Threads}  (pinned: {pinning.DescribeWorkerCpus()})");
            Console.WriteLine($"  keys             : {_opts.Keys:N0}");
            Console.WriteLine($"  value-size       : {_opts.ValueSize} bytes (reader copies first {KvSessionFunctions.kReaderCopyBytes} B)");
            Console.WriteLine($"  rumd%            : {string.Join(",", _opts.Rumd)}  (deletes auto-reinsert: {(_opts.RumdHasDeletes() ? "yes" : "n/a")})");
            Console.WriteLine($"  distribution     : {_opts.Distribution}{(_opts.UseZipf ? $" (theta={_opts.ZipfTheta})" : "")}");
            Console.WriteLine($"  seed             : {_opts.Seed}");
            Console.WriteLine($"  iterations       : {_opts.Iterations}");
            Console.WriteLine($"  warmup / runsec  : {_opts.WarmupSec} / {_opts.RunSec} s");
            Console.WriteLine($"  device           : {_opts.ResolvedDeviceType}");
            Console.WriteLine($"  hashpack         : {_opts.Hashpack}  =>  index requested {KvSize.FormatSize(_opts.ResolvedIndexRequestedBytes)}  →  applied {KvSize.FormatSize(_opts.ResolvedIndexAppliedBytes)} (effective hashpack ≈ {EffectiveHashpack():F2})");
            Console.WriteLine($"  log-memory       : {KvSize.FormatSize(_opts.ResolvedLogMemoryBytes)}");
            Console.WriteLine($"  page-size        : {KvSize.FormatSize(_opts.ResolvedPageSizeBytes)}");
            Console.WriteLine($"  segment-size     : {KvSize.FormatSize(_opts.ResolvedSegmentSizeBytes)}");
            Console.WriteLine($"  max-inline-value : {KvSize.FormatSize(_opts.ResolvedMaxInlineValueSizeBytes)} (values larger overflow to heap)");
            Console.WriteLine($"  preallocate-log  : {_opts.PreallocateLog}");
            Console.WriteLine($"  record-size (est): {_opts.ResolvedRecordSizeBytes} B");
            Console.WriteLine($"  data-path        : {_dataPath}");
            Console.WriteLine($"  report-interval  : {(_opts.ReportIntervalSec <= 0 ? "off (reference mode)" : _opts.ReportIntervalSec + "s")}");
            Console.WriteLine($"  NUMA             : {pinning.DiagnosticMessage}");
            Console.WriteLine("===========================");
        }

        public void EmitPhaseHuman(PhaseResult r, int threadCount = 0)
        {
            if (_opts.Quiet) return;
            var tag = r.Phase switch
            {
                "load" => "[load]   ",
                "warmup" => "[warmup] ",
                "run" => threadCount > 0 ? $"[run t={threadCount} i={r.Iteration}] " : $"[run {r.Iteration}] ",
                _ => $"[{r.Phase}]    ",
            };
            var rate = r.OpsPerSec.ToString("N0", CultureInfo.InvariantCulture);
            Console.WriteLine($"{tag}{r.TotalOpsForThroughput:N0} ops in {r.ElapsedSec:N3} s  ({rate} ops/sec)  reads={r.Reads:N0} writes={r.Writes:N0} deletes={r.Deletes:N0} overshoot={r.OvershootOps:N0} exit-lag={r.MaxWorkerExitLagMs:N1}ms gc={r.GcGen0Delta}/{r.GcGen1Delta}/{r.GcGen2Delta} alloc/wkr={r.AllocBytesByWorkerMax}B");
        }

        public void EmitAggregateHuman(IList<PhaseResult> iters, int threadCount = 0)
        {
            if (_opts.Quiet) return;
            var ops = iters.Select(p => p.OpsPerSec).ToArray();
            var mean = ops.Average();
            var stddev = ops.Length > 1 ? Math.Sqrt(ops.Select(o => Math.Pow(o - mean, 2)).Sum() / ops.Length) : 0;
            var pct = mean > 0 ? stddev / mean * 100 : 0;
            var prefix = threadCount > 0 ? $"[aggregate t={threadCount}]" : "[aggregate]";
            Console.WriteLine($"{prefix} iterations={iters.Count} mean={mean:N0} ops/sec stdev={stddev:N1} ({pct:N1}%) min={ops.Min():N0} max={ops.Max():N0}{(iters.Count >= 3 ? $" trimmed={TrimmedMean(ops):N0}" : "")}");
        }

        /// <summary>
        /// Prints a single readable block summarising config, load, and run perf at end of run.
        /// When the run-thread sweep has multiple entries, the run phase is reported as a table
        /// with one row per thread count (with speedup vs the smallest sweep entry).
        /// Always prints (regardless of --quiet) since this is the headline output.
        /// </summary>
        public void EmitFinalSummary(PhaseResult loadResult, IDictionary<int, List<PhaseResult>> sweepResults, KvNumaPinning pinning)
        {
            static string Rate(double opsPerSec) =>
                opsPerSec >= 1e9 ? $"{opsPerSec / 1e9:F2} G ops/sec"
                : opsPerSec >= 1e6 ? $"{opsPerSec / 1e6:F2} M ops/sec"
                : opsPerSec >= 1e3 ? $"{opsPerSec / 1e3:F1} K ops/sec"
                : $"{opsPerSec:F0} ops/sec";

            const string sep = "==============================================================================";
            Console.WriteLine();
            Console.WriteLine(sep);
            Console.WriteLine("  KV.benchmark — final summary");
            Console.WriteLine(sep);

            var dist = _opts.UseZipf ? $"zipf θ={_opts.ZipfTheta:F2}" : "uniform";
            Console.WriteLine($"  workload     : {_opts.Keys:N0} keys × {_opts.ValueSize}B value, rumd={string.Join(",", _opts.Rumd)}, {dist}");
            var runThreadsStr = _opts.ResolvedRunThreadsSweep.Length == 1
                ? _opts.ResolvedRunThreadsSweep[0].ToString(CultureInfo.InvariantCulture)
                : string.Join(",", _opts.ResolvedRunThreadsSweep);
            Console.WriteLine($"  parallelism  : load-threads={_opts.ResolvedLoadThreads}, run-threads={runThreadsStr}, pinned={pinning.DescribeWorkerCpus()} (NUMA node {_opts.NumaNode})");
            Console.WriteLine($"  storage      : hashpack={_opts.Hashpack:F2} → index {KvSize.FormatSize(_opts.ResolvedIndexAppliedBytes)} (effective {EffectiveHashpack():F2})");
            Console.WriteLine($"                 log={KvSize.FormatSize(_opts.ResolvedLogMemoryBytes)} (pages {KvSize.FormatSize(_opts.ResolvedPageSizeBytes)}, segments {KvSize.FormatSize(_opts.ResolvedSegmentSizeBytes)}, record ≈{_opts.ResolvedRecordSizeBytes}B)");
            Console.WriteLine($"                 device={_opts.ResolvedDeviceType}, session=BasicContext (safe path)");
            Console.WriteLine($"  timing       : warmup={_opts.WarmupSec}s, run={_opts.RunSec}s × {_opts.Iterations} iter(s)");

            if (loadResult != null)
            {
                Console.WriteLine();
                Console.WriteLine($"  Load phase ({_opts.ResolvedLoadThreads} thread{(_opts.ResolvedLoadThreads == 1 ? "" : "s")}):");
                Console.WriteLine($"    {loadResult.TotalOpsForThroughput:N0} ops in {loadResult.ElapsedSec:F3} s   →   {Rate(loadResult.OpsPerSec)}");
                Console.WriteLine($"    log tail = {loadResult.LogTail:N0} bytes");
            }

            if (sweepResults != null && sweepResults.Count > 0)
            {
                Console.WriteLine();
                if (sweepResults.Count == 1)
                {
                    // Single thread count: show the same detail as before.
                    var (tc, iters) = (sweepResults.Keys.First(), sweepResults.Values.First());
                    EmitRunPhaseBlock(tc, iters, Rate);
                }
                else
                {
                    // Sweep: print a compact table with speedup vs the smallest entry.
                    Console.WriteLine($"  Run sweep ({_opts.Iterations} iteration{(_opts.Iterations == 1 ? "" : "s")} per thread count):");
                    Console.WriteLine($"    {"threads",7} | {"trimmed",14} | {"mean",14} | {"stdev%",7} | speedup");
                    Console.WriteLine($"    {new string('-', 7)}-+-{new string('-', 14)}-+-{new string('-', 14)}-+-{new string('-', 7)}-+--------");
                    var ordered = sweepResults.OrderBy(kv => kv.Key).ToList();
                    double basis = 0;
                    foreach (var (tc, iters) in ordered)
                    {
                        var ops = iters.Select(p => p.OpsPerSec).ToArray();
                        var mean = ops.Average();
                        var sd = ops.Length > 1 ? Math.Sqrt(ops.Select(o => Math.Pow(o - mean, 2)).Sum() / ops.Length) : 0;
                        var pct = mean > 0 ? sd / mean * 100 : 0;
                        var trimmed = ops.Length >= 3 ? TrimmedMean(ops) : mean;
                        if (basis == 0) basis = trimmed;
                        var speedup = basis > 0 ? trimmed / basis : 0;
                        Console.WriteLine($"    {tc,7} | {Rate(trimmed),14} | {Rate(mean),14} | {pct,6:F1}% | {speedup,6:F2}×");
                    }
                }
            }
            Console.WriteLine(sep);
        }

        void EmitRunPhaseBlock(int threadCount, IList<PhaseResult> iters, Func<double, string> rate)
        {
            var ops = iters.Select(p => p.OpsPerSec).ToArray();
            var mean = ops.Average();
            var stddev = ops.Length > 1 ? Math.Sqrt(ops.Select(o => Math.Pow(o - mean, 2)).Sum() / ops.Length) : 0;
            var pct = mean > 0 ? stddev / mean * 100 : 0;
            var trimmed = ops.Length >= 3 ? TrimmedMean(ops) : mean;

            Console.WriteLine($"  Run phase ({iters.Count} iteration{(iters.Count == 1 ? "" : "s")}, {threadCount} thread{(threadCount == 1 ? "" : "s")}):");
            Console.WriteLine($"    mean      : {rate(mean)}   ± {pct:F1}% (stdev {stddev:N0} ops/sec)");
            if (ops.Length >= 3)
                Console.WriteLine($"    trimmed   : {rate(trimmed)}   (drops hi+lo)");
            Console.WriteLine($"    min..max  : {rate(ops.Min())}  ..  {rate(ops.Max())}");
            Console.Write("    per-iter  : ");
            for (int i = 0; i < ops.Length; i++)
            {
                if (i > 0) Console.Write(", ");
                Console.Write($"{ops[i] / 1e6:F2}M");
            }
            Console.WriteLine();
        }

        // ====== Helpers shared across human / JSON / CSV ======

        /// <summary>Live keys / applied hash buckets — how many keys are mapped per hash bucket.</summary>
        double EffectiveHashpack()
        {
            var buckets = _opts.ResolvedIndexAppliedBytes / 64;
            return buckets > 0 ? (double)_opts.Keys / buckets : 0.0;
        }

        /// <summary>Trims hi+lo and returns the mean. Caller must pass an array with length >= 3.</summary>
        static double TrimmedMean(double[] arr)
        {
            var sorted = arr.OrderBy(x => x).ToArray();
            if (sorted.Length < 3) return sorted.Average();
            return sorted.Skip(1).Take(sorted.Length - 2).Average();
        }

        static string Dbl(double d) => d.ToString("R", CultureInfo.InvariantCulture);

        static string JsonString(string s)
        {
            if (s is null) return "null";
            var sb = new StringBuilder(s.Length + 2);
            sb.Append('"');
            foreach (var c in s)
            {
                switch (c)
                {
                    case '"': sb.Append("\\\""); break;
                    case '\\': sb.Append("\\\\"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\t': sb.Append("\\t"); break;
                    default:
                        if (c < 0x20) sb.Append($"\\u{(int)c:X4}");
                        else sb.Append(c);
                        break;
                }
            }
            sb.Append('"');
            return sb.ToString();
        }

        /// <summary>Pretty-prints a compact JSON string. On parse failure, returns the input unchanged.</summary>
        static string PrettyJson(string compactJson)
        {
            try
            {
                using var doc = JsonDocument.Parse(compactJson);
                using var ms = new MemoryStream();
                using (var writer = new Utf8JsonWriter(ms, new JsonWriterOptions { Indented = true }))
                {
                    doc.WriteTo(writer);
                }
                return Encoding.UTF8.GetString(ms.ToArray());
            }
            catch
            {
                return compactJson;
            }
        }

        static string TryReadFirstLine(string path)
        {
            try { return File.Exists(path) ? File.ReadAllText(path).Trim() : ""; }
            catch { return ""; }
        }

        static string TryGitSha()
        {
            try
            {
                var psi = new System.Diagnostics.ProcessStartInfo("git", "rev-parse --short HEAD")
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                };
                using var p = System.Diagnostics.Process.Start(psi);
                if (p == null) return "";
                var output = p.StandardOutput.ReadToEnd().Trim();
                p.WaitForExit(500);
                return output;
            }
            catch { return ""; }
        }

        static void AppendLine(string path, string line)
        {
            try
            {
                using var w = new StreamWriter(path, append: true);
                w.WriteLine(line);
            }
            catch { /* best-effort */ }
        }
    }
}
