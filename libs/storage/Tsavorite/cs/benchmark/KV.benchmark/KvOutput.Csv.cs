// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace Tsavorite.kvbench
{
    // CSV emit. Wide schema, one row per phase per iteration plus an aggregate row.
    // Header is written automatically when the file is first created.
    internal sealed partial class KvOutput
    {
        public void EmitResultCsv(PhaseResult r, KvNumaPinning pinning, int threadCount = 0)
        {
            if (!_csvEnabled) return;
            var path = _opts.CsvOutput;
            var fresh = !File.Exists(path);
            using var w = new StreamWriter(path, append: true);
            if (fresh) w.WriteLine(CsvHeader());
            w.WriteLine(CsvRow(r, pinning, threadCount));
        }

        public void EmitAggregateCsv(IList<PhaseResult> iters, KvNumaPinning pinning, int threadCount = 0)
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

            var sb = new StringBuilder();
            CsvAppendCommon(sb, "aggregate", iters.Count, pinning, threadCount);
            sb.Append(",")  // elapsed
              .Append(",")  // total_ops_for_throughput
              .Append(",")  // ops_per_sec
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

        static string CsvHeader() =>
            "schema_version,timestamp_utc,git_sha,hostname,phase,iteration,threads,keys,value_size,distribution,rumd,delete_reinsert,reader_copy_bytes,device,device_throttle,device_completion_threads,device_io_backend,session_context,hashpack_configured,hashpack_effective,index_size_requested,index_size_applied,log_memory,page_size,segment_size,mutable_fraction,warmup_sec,runsec,elapsed_sec,total_ops_for_throughput,ops_per_sec,overshoot_ops,max_worker_exit_lag_ms,reads,writes,deletes,gc_gen0,gc_gen1,gc_gen2,alloc_bytes_by_worker_max,log_begin,log_head,log_readonly,log_tail,agg_mean_ops_per_sec,agg_stdev_ops_per_sec,agg_trimmed_mean,agg_min,agg_max";

        string CsvRow(PhaseResult r, KvNumaPinning pinning, int threadCount = 0)
        {
            var sb = new StringBuilder(512);
            CsvAppendCommon(sb, r.Phase, r.Iteration, pinning, threadCount);
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

        // threadCount > 0 overrides the default _opts.Threads column (used by the run-sweep so
        // each row carries the thread count it was actually measured at).
        void CsvAppendCommon(StringBuilder sb, string phase, int iteration, KvNumaPinning pinning, int threadCount = 0)
        {
            sb.Append(SchemaVersion).Append(",")
              .Append(DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture)).Append(",")
              .Append(CsvEsc(TryGitSha())).Append(",")
              .Append(CsvEsc(Environment.MachineName)).Append(",")
              .Append(phase).Append(",")
              .Append(iteration).Append(",")
              .Append(threadCount > 0 ? threadCount : _opts.Threads).Append(",")
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

        static string CsvEsc(string s)
        {
            if (string.IsNullOrEmpty(s)) return string.Empty;
            if (s.Contains(',') || s.Contains('"') || s.Contains('\n'))
                return "\"" + s.Replace("\"", "\"\"") + "\"";
            return s;
        }
    }
}