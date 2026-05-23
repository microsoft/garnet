// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace Tsavorite.kvbench
{
    // JSON emit + schema. Compact JSON is built into a StringBuilder; pretty form is
    // produced via System.Text.Json's indented writer. Stdout is one-line (opt-in),
    // file output is pretty.
    internal sealed partial class KvOutput
    {
        const string ResultPrefix = "KV-RESULT-JSON: ";

        public void EmitResultJson(PhaseResult r, KvNumaPinning pinning, int threadCount = 0)
        {
            if (!_jsonEnabled && !_opts.JsonStdout) return;
            var json = BuildResultJson(r, pinning, threadCount);
            if (_opts.JsonStdout) Console.WriteLine(ResultPrefix + json);
            if (_jsonEnabled) AppendLine(_opts.JsonOutput, PrettyJson(json));
        }

        public void EmitAggregateJson(IList<PhaseResult> iters, KvNumaPinning pinning, int threadCount = 0)
        {
            if (iters == null || iters.Count == 0) return;
            if (!_jsonEnabled && !_opts.JsonStdout) return;

            var ops = iters.Select(p => p.OpsPerSec).ToArray();
            var mean = ops.Average();
            var stddev = ops.Length > 1 ? Math.Sqrt(ops.Select(o => Math.Pow(o - mean, 2)).Sum() / ops.Length) : 0;
            var trimmed = ops.Length >= 3 ? TrimmedMean(ops) : mean;

            var sb = new StringBuilder(1024);
            sb.Append('{');
            sb.Append($"\"schema_version\":\"{SchemaVersion}\",");
            sb.Append("\"phase\":\"aggregate\",");
            if (threadCount > 0) sb.Append($"\"threads\":{threadCount},");
            sb.Append($"\"iterations\":{iters.Count},");
            sb.Append($"\"mean_ops_per_sec\":{Dbl(mean)},");
            sb.Append($"\"stdev_ops_per_sec\":{Dbl(stddev)},");
            sb.Append($"\"stdev_pct\":{Dbl(mean > 0 ? stddev / mean * 100 : 0)},");
            sb.Append($"\"trimmed_mean_ops_per_sec\":{Dbl(trimmed)},");
            sb.Append($"\"min_ops_per_sec\":{Dbl(ops.Min())},");
            sb.Append($"\"max_ops_per_sec\":{Dbl(ops.Max())},");
            sb.Append($"\"timestamp_utc\":\"{DateTime.UtcNow:O}\"");
            sb.Append('}');
            var compact = sb.ToString();
            if (_opts.JsonStdout) Console.WriteLine(ResultPrefix + compact);
            if (_jsonEnabled) AppendLine(_opts.JsonOutput, PrettyJson(compact));
        }

        string BuildResultJson(PhaseResult r, KvNumaPinning pinning, int threadCount = 0)
        {
            var sb = new StringBuilder(2048);
            sb.Append('{');
            sb.Append($"\"schema_version\":\"{SchemaVersion}\",");
            sb.Append($"\"phase\":\"{r.Phase}\",");
            sb.Append($"\"iteration\":{r.Iteration},");
            if (threadCount > 0) sb.Append($"\"threads\":{threadCount},");
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

            // Config block — every resolved flag.
            sb.Append("\"config\":{");
            sb.Append($"\"threads\":{_opts.Threads},");
            sb.Append($"\"load_threads\":{_opts.ResolvedLoadThreads},");
            sb.Append($"\"run_threads_sweep\":\"{string.Join(",", _opts.ResolvedRunThreadsSweep)}\",");
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
            sb.Append($"\"max_inline_value_size\":{_opts.ResolvedMaxInlineValueSizeBytes},");
            sb.Append("\"mutable_fraction\":0.9,");
            sb.Append($"\"preallocate_log\":{(_opts.PreallocateLog ? "true" : "false")},");
            sb.Append($"\"device\":\"{_opts.ResolvedDeviceType}\",");
            sb.Append($"\"device_throttle\":{_opts.DeviceThrottle},");
            sb.Append($"\"device_completion_threads\":{_opts.DeviceCompletionThreads},");
            sb.Append($"\"device_io_backend\":\"{_opts.ResolvedIoBackend}\",");
            sb.Append("\"session_context\":\"basic\",");
            sb.Append($"\"warmup_sec\":{_opts.WarmupSec},");
            sb.Append($"\"runsec\":{_opts.RunSec},");
            sb.Append($"\"report_interval_sec\":{_opts.ReportIntervalSec}");
            sb.Append("},");

            // Host block — hardware / OS / runtime forensic info.
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
    }
}
