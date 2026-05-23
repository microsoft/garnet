// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// Human / JSON / CSV emitters with a shared schema. The JSON / CSV streams
    /// share the same metadata block so a single row is forensically self-contained.
    /// </summary>
    internal sealed class KvOutput
    {
        const string SchemaVersion = "1";
        const string ResultPrefix = "KV-RESULT-JSON: ";
        const string ProgressPrefix = "KV-PROGRESS-JSON: ";

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
            Console.WriteLine($"  record-size (est): {_opts.ResolvedRecordSizeBytes} B");
            Console.WriteLine($"  data-path        : {_dataPath}");
            Console.WriteLine($"  report-interval  : {(_opts.ReportIntervalSec <= 0 ? "off (reference mode)" : _opts.ReportIntervalSec + "s")}");
            Console.WriteLine($"  NUMA             : {pinning.DiagnosticMessage}");
            Console.WriteLine("===========================");
        }

        public void EmitPhaseHuman(PhaseResult r)
        {
            if (_opts.Quiet) return;
            var tag = r.Phase switch
            {
                "load" => "[load]   ",
                "warmup" => "[warmup] ",
                "run" => $"[run {r.Iteration}] ",
                _ => $"[{r.Phase}]    ",
            };
            var rate = r.OpsPerSec.ToString("N0", CultureInfo.InvariantCulture);
            Console.WriteLine($"{tag}{r.TotalOpsForThroughput:N0} ops in {r.ElapsedSec:N3} s  ({rate} ops/sec)  reads={r.Reads:N0} writes={r.Writes:N0} deletes={r.Deletes:N0} overshoot={r.OvershootOps:N0} exit-lag={r.MaxWorkerExitLagMs:N1}ms gc={r.GcGen0Delta}/{r.GcGen1Delta}/{r.GcGen2Delta} alloc/wkr={r.AllocBytesByWorkerMax}B");
        }

        public void EmitAggregateHuman(IList<PhaseResult> iters)
        {
            if (_opts.Quiet) return;
            var ops = iters.Select(p => p.OpsPerSec).ToArray();
            var mean = ops.Average();
            var stddev = ops.Length > 1 ? Math.Sqrt(ops.Select(o => Math.Pow(o - mean, 2)).Sum() / ops.Length) : 0;
            var pct = mean > 0 ? stddev / mean * 100 : 0;
            Console.WriteLine($"[aggregate] iterations={iters.Count} mean={mean:N0} ops/sec stdev={stddev:N1} ({pct:N1}%) min={ops.Min():N0} max={ops.Max():N0}{(iters.Count >= 3 ? $" trimmed={TrimmedMean(ops):N0}" : "")}");
        }

        public void EmitResultJson(PhaseResult r, KvNumaPinning pinning)
        {
            if (!_jsonEnabled && _opts.Quiet) return;
            var json = BuildResultJson(r, pinning);
            if (!_opts.Quiet) Console.WriteLine(ResultPrefix + json);
            if (_jsonEnabled) AppendLine(_opts.JsonOutput, ResultPrefix + json);
        }

        public void EmitAggregateJson(IList<PhaseResult> iters, KvNumaPinning pinning)
        {
            if (iters == null || iters.Count == 0) return;
            var ops = iters.Select(p => p.OpsPerSec).ToArray();
            var mean = ops.Average();
            var stddev = ops.Length > 1 ? Math.Sqrt(ops.Select(o => Math.Pow(o - mean, 2)).Sum() / ops.Length) : 0;
            var trimmed = ops.Length >= 3 ? TrimmedMean(ops) : mean;

            var sb = new StringBuilder(1024);
            sb.Append('{');
            sb.Append($"\"schema_version\":\"{SchemaVersion}\",");
            sb.Append("\"phase\":\"aggregate\",");
            sb.Append($"\"iterations\":{iters.Count},");
            sb.Append($"\"mean_ops_per_sec\":{Dbl(mean)},");
            sb.Append($"\"stdev_ops_per_sec\":{Dbl(stddev)},");
            sb.Append($"\"stdev_pct\":{Dbl(mean > 0 ? stddev / mean * 100 : 0)},");
            sb.Append($"\"trimmed_mean_ops_per_sec\":{Dbl(trimmed)},");
            sb.Append($"\"min_ops_per_sec\":{Dbl(ops.Min())},");
            sb.Append($"\"max_ops_per_sec\":{Dbl(ops.Max())},");
            sb.Append($"\"timestamp_utc\":\"{DateTime.UtcNow:O}\"");
            sb.Append('}');
            var line = ResultPrefix + sb.ToString();
            if (!_opts.Quiet) Console.WriteLine(line);
            if (_jsonEnabled) AppendLine(_opts.JsonOutput, line);
        }

        public void EmitResultCsv(PhaseResult r, KvNumaPinning pinning)
        {
            if (!_csvEnabled) return;
            var path = _opts.CsvOutput;
            var fresh = !File.Exists(path);
            using var w = new StreamWriter(path, append: true);
            if (fresh) w.WriteLine(CsvHeader());
            w.WriteLine(CsvRow(r, pinning));
        }

        public void EmitAggregateCsv(IList<PhaseResult> iters, KvNumaPinning pinning)
        {
            if (!_csvEnabled || iters == null || iters.Count == 0) return;
            var ops = iters.Select(p => p.OpsPerSec).ToArray();
            var mean = ops.Average();
            var stddev = ops.Length > 1 ? Math.Sqrt(ops.Select(o => Math.Pow(o - mean, 2)).Sum() / ops.Length) : 0;
            var trimmed = ops.Length >= 3 ? TrimmedMean(ops) : mean;

            var path = _opts.CsvOutput;
            var fresh = !File.Exists(path);
            using var w = new StreamWriter(path, append: true);
            if (fresh) w.WriteLine(CsvHeader());
            // Use the same column shape with phase=aggregate; per-iter fields blank, aggregate fields populated.
            var sb = new StringBuilder();
            CsvAppendCommon(sb, "aggregate", iters.Count, pinning);
            sb.Append(",")  // elapsed
              .Append(",")  // total_ops_for_throughput
              .Append(",")  // ops_per_sec (we'll instead emit mean below)
              .Append(",")  // overshoot
              .Append(",")  // exit_lag
              .Append(",")  // reads
              .Append(",")  // writes
              .Append(",")  // deletes
              .Append(",")  // gc0
              .Append(",")  // gc1
              .Append(",")  // gc2
              .Append(",")  // alloc_max
              .Append(",")  // log_begin
              .Append(",")  // log_head
              .Append(",")  // log_readonly
              .Append(",")  // log_tail
              .Append(Dbl(mean)).Append(",")
              .Append(Dbl(stddev)).Append(",")
              .Append(Dbl(trimmed)).Append(",")
              .Append(Dbl(ops.Min())).Append(",")
              .Append(Dbl(ops.Max()));
            w.WriteLine(sb.ToString());
        }

        // ====== JSON / CSV plumbing ======

        string BuildResultJson(PhaseResult r, KvNumaPinning pinning)
        {
            var sb = new StringBuilder(2048);
            sb.Append('{');
            sb.Append($"\"schema_version\":\"{SchemaVersion}\",");
            sb.Append($"\"phase\":\"{r.Phase}\",");
            sb.Append($"\"iteration\":{r.Iteration},");
            sb.Append($"\"ops_per_sec\":{Dbl(r.OpsPerSec)},");
            sb.Append($"\"elapsed_sec\":{Dbl(r.ElapsedSec)},");
            sb.Append($"\"total_ops_for_throughput\":{r.TotalOpsForThroughput},");
            sb.Append($"\"final_total_ops\":{r.FinalTotalOps},");
            sb.Append($"\"overshoot_ops\":{r.OvershootOps},");
            sb.Append($"\"max_worker_exit_lag_ms\":{Dbl(r.MaxWorkerExitLagMs)},");
            sb.Append($"\"reads\":{r.Reads},");
            sb.Append($"\"writes\":{r.Writes},");
            sb.Append($"\"deletes\":{r.Deletes},");
            sb.Append($"\"interrupted\":{(r.Interrupted ? "true" : "false")},");
            sb.Append($"\"error\":{(r.ErrorMessage is null ? "null" : JsonString(r.ErrorMessage))},");
            sb.Append("\"log\":{");
            sb.Append($"\"begin_address\":{r.LogBegin},");
            sb.Append($"\"head_address\":{r.LogHead},");
            sb.Append($"\"readonly_address\":{r.LogReadOnly},");
            sb.Append($"\"tail_address\":{r.LogTail}");
            sb.Append("},");
            sb.Append("\"gc_delta\":{");
            sb.Append($"\"gen0\":{r.GcGen0Delta},");
            sb.Append($"\"gen1\":{r.GcGen1Delta},");
            sb.Append($"\"gen2\":{r.GcGen2Delta},");
            sb.Append($"\"alloc_bytes_by_worker_max\":{r.AllocBytesByWorkerMax}");
            sb.Append("},");
            // Config block
            sb.Append("\"config\":{");
            sb.Append($"\"threads\":{_opts.Threads},");
            sb.Append($"\"keys\":{_opts.Keys},");
            sb.Append($"\"value_size\":{_opts.ValueSize},");
            sb.Append($"\"reader_copy_bytes\":{KvSessionFunctions.kReaderCopyBytes},");
            sb.Append($"\"rumd\":\"{string.Join(",", _opts.Rumd)}\",");
            sb.Append($"\"delete_reinsert\":{(_opts.RumdHasDeletes() ? "true" : "false")},");
            sb.Append($"\"distribution\":\"{_opts.Distribution}\",");
            sb.Append($"\"zipf_theta\":{Dbl(_opts.ZipfTheta)},");
            sb.Append($"\"seed\":{_opts.Seed},");
            sb.Append($"\"hashpack_configured\":{Dbl(_opts.Hashpack)},");
            sb.Append($"\"hashpack_effective\":{Dbl(EffectiveHashpack())},");
            sb.Append($"\"index_size_requested\":{_opts.ResolvedIndexRequestedBytes},");
            sb.Append($"\"index_size_applied\":{_opts.ResolvedIndexAppliedBytes},");
            sb.Append($"\"log_memory\":{_opts.ResolvedLogMemoryBytes},");
            sb.Append($"\"page_size\":{_opts.ResolvedPageSizeBytes},");
            sb.Append($"\"segment_size\":{_opts.ResolvedSegmentSizeBytes},");
            sb.Append($"\"record_size_estimated\":{_opts.ResolvedRecordSizeBytes},");
            sb.Append("\"mutable_fraction\":0.9,");
            sb.Append("\"preallocate_log\":true,");
            sb.Append($"\"device\":\"{_opts.ResolvedDeviceType}\",");
            sb.Append($"\"device_throttle\":{_opts.DeviceThrottle},");
            sb.Append($"\"device_completion_threads\":{_opts.DeviceCompletionThreads},");
            sb.Append($"\"device_io_backend\":\"{_opts.ResolvedIoBackend}\",");
            sb.Append("\"session_context\":\"basic\",");
            sb.Append($"\"warmup_sec\":{_opts.WarmupSec},");
            sb.Append($"\"runsec\":{_opts.RunSec},");
            sb.Append($"\"report_interval_sec\":{_opts.ReportIntervalSec}");
            sb.Append("},");
            // Host block
            sb.Append("\"host\":{");
            sb.Append($"\"hostname\":{JsonString(Environment.MachineName)},");
            sb.Append($"\"os\":{JsonString(RuntimeInformation.OSDescription)},");
            sb.Append($"\"dotnet\":{JsonString(Environment.Version.ToString())},");
            sb.Append($"\"git_sha\":{JsonString(TryGitSha())},");
            sb.Append($"\"cpu_count\":{Environment.ProcessorCount},");
            sb.Append($"\"pinned_numa_node\":{_opts.NumaNode},");
            sb.Append($"\"worker_cpu_mask\":{JsonString(pinning.DescribeWorkerCpus())},");
            sb.Append($"\"server_gc\":{(System.Runtime.GCSettings.IsServerGC ? "true" : "false")},");
            sb.Append($"\"gc_latency_mode\":\"{System.Runtime.GCSettings.LatencyMode}\",");
            sb.Append($"\"tiered_compilation\":{JsonString(Environment.GetEnvironmentVariable("DOTNET_TieredCompilation") ?? "default")},");
            sb.Append($"\"tiered_pgo\":{JsonString(Environment.GetEnvironmentVariable("DOTNET_TieredPGO") ?? "default")},");
            sb.Append($"\"thp_enabled\":{JsonString(TryReadFirstLine("/sys/kernel/mm/transparent_hugepage/enabled"))},");
            sb.Append($"\"data_path\":{JsonString(_dataPath)},");
            sb.Append($"\"ram_total_bytes\":{GC.GetGCMemoryInfo().TotalAvailableMemoryBytes}");
            sb.Append("},");
            sb.Append($"\"argv\":{_argv},");
            sb.Append($"\"timestamp_utc\":\"{DateTime.UtcNow:O}\"");
            sb.Append('}');
            return sb.ToString();
        }

        static string CsvHeader() =>
            "schema_version,timestamp_utc,git_sha,hostname,phase,iteration,threads,keys,value_size,distribution,rumd,delete_reinsert,reader_copy_bytes,device,device_throttle,device_completion_threads,device_io_backend,session_context,hashpack_configured,hashpack_effective,index_size_requested,index_size_applied,log_memory,page_size,segment_size,mutable_fraction,warmup_sec,runsec,elapsed_sec,total_ops_for_throughput,ops_per_sec,overshoot_ops,max_worker_exit_lag_ms,reads,writes,deletes,gc_gen0,gc_gen1,gc_gen2,alloc_bytes_by_worker_max,log_begin,log_head,log_readonly,log_tail,agg_mean_ops_per_sec,agg_stdev_ops_per_sec,agg_trimmed_mean,agg_min,agg_max";

        string CsvRow(PhaseResult r, KvNumaPinning pinning)
        {
            var sb = new StringBuilder(512);
            CsvAppendCommon(sb, r.Phase, r.Iteration, pinning);
            sb.Append(",").Append(Dbl(r.ElapsedSec))
              .Append(",").Append(r.TotalOpsForThroughput)
              .Append(",").Append(Dbl(r.OpsPerSec))
              .Append(",").Append(r.OvershootOps)
              .Append(",").Append(Dbl(r.MaxWorkerExitLagMs))
              .Append(",").Append(r.Reads)
              .Append(",").Append(r.Writes)
              .Append(",").Append(r.Deletes)
              .Append(",").Append(r.GcGen0Delta)
              .Append(",").Append(r.GcGen1Delta)
              .Append(",").Append(r.GcGen2Delta)
              .Append(",").Append(r.AllocBytesByWorkerMax)
              .Append(",").Append(r.LogBegin)
              .Append(",").Append(r.LogHead)
              .Append(",").Append(r.LogReadOnly)
              .Append(",").Append(r.LogTail)
              .Append(",,,,"); // empty aggregate columns
            return sb.ToString();
        }

        void CsvAppendCommon(StringBuilder sb, string phase, int iteration, KvNumaPinning pinning)
        {
            sb.Append(SchemaVersion).Append(",")
              .Append(DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture)).Append(",")
              .Append(CsvEsc(TryGitSha())).Append(",")
              .Append(CsvEsc(Environment.MachineName)).Append(",")
              .Append(phase).Append(",")
              .Append(iteration).Append(",")
              .Append(_opts.Threads).Append(",")
              .Append(_opts.Keys).Append(",")
              .Append(_opts.ValueSize).Append(",")
              .Append(_opts.Distribution).Append(",")
              .Append("\"").Append(string.Join("|", _opts.Rumd)).Append("\"").Append(",")
              .Append(_opts.RumdHasDeletes() ? "true" : "false").Append(",")
              .Append(KvSessionFunctions.kReaderCopyBytes).Append(",")
              .Append(_opts.ResolvedDeviceType).Append(",")
              .Append(_opts.DeviceThrottle).Append(",")
              .Append(_opts.DeviceCompletionThreads).Append(",")
              .Append(_opts.ResolvedIoBackend).Append(",")
              .Append("basic").Append(",")
              .Append(Dbl(_opts.Hashpack)).Append(",")
              .Append(Dbl(EffectiveHashpack())).Append(",")
              .Append(_opts.ResolvedIndexRequestedBytes).Append(",")
              .Append(_opts.ResolvedIndexAppliedBytes).Append(",")
              .Append(_opts.ResolvedLogMemoryBytes).Append(",")
              .Append(_opts.ResolvedPageSizeBytes).Append(",")
              .Append(_opts.ResolvedSegmentSizeBytes).Append(",")
              .Append("0.9").Append(",")
              .Append(_opts.WarmupSec).Append(",")
              .Append(_opts.RunSec);
        }

        // ====== Helpers ======

        double EffectiveHashpack()
        {
            var buckets = _opts.ResolvedIndexAppliedBytes / 64;
            return buckets > 0 ? (double)_opts.Keys / buckets : 0.0;
        }

        static double TrimmedMean(double[] arr)
        {
            var sorted = arr.OrderBy(x => x).ToArray();
            if (sorted.Length < 3) return sorted.Average();
            var trimmed = sorted.Skip(1).Take(sorted.Length - 2).ToArray();
            return trimmed.Average();
        }

        static string Dbl(double d) => d.ToString("R", CultureInfo.InvariantCulture);

        static string CsvEsc(string s)
        {
            if (string.IsNullOrEmpty(s)) return string.Empty;
            if (s.Contains(',') || s.Contains('"') || s.Contains('\n'))
                return "\"" + s.Replace("\"", "\"\"") + "\"";
            return s;
        }

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
