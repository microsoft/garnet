// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using CommandLine;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// CLI options for KV.benchmark.
    /// </summary>
    public class Options
    {
        // ===== Workload =====

        [Option('t', "threads", Required = false, Default = 1,
            HelpText = "Worker thread count (default 1; pass nodeCpus to saturate the pinned NUMA node).")]
        public int Threads { get; set; }

        [Option('n', "keys", Required = false, Default = 100_000_000L,
            HelpText = "Number of unique keys in the dataset.")]
        public long Keys { get; set; }

        [Option('v', "value-size", Required = false, Default = 100,
            HelpText = "Value length in bytes. Range: 8..4096 (inline-value path only).")]
        public int ValueSize { get; set; }

        [Option("rumd", Separator = ',', Required = false, Default = new[] { 100, 0, 0, 0 },
            HelpText = "#,#,#,#: Percentages of [(r)eads,(u)pserts,r(m)ws,(d)eletes] (summing to 100). When d% > 0, deletes auto-reinsert.")]
        public IEnumerable<int> Rumd { get; set; }

        [Option('d', "distribution", Required = false, Default = "uniform",
            HelpText = "Key distribution: 'uniform' or 'zipf'.")]
        public string Distribution { get; set; }

        [Option("zipf-theta", Required = false, Default = 0.99,
            HelpText = "Zipf skew parameter (only used when distribution=zipf).")]
        public double ZipfTheta { get; set; }

        [Option("runsec", Required = false, Default = 30,
            HelpText = "Run-phase duration in seconds (excludes warmup).")]
        public int RunSec { get; set; }

        [Option("warmup-sec", Required = false, Default = 5,
            HelpText = "Warmup duration in seconds, discarded from results. 0 disables warmup.")]
        public int WarmupSec { get; set; }

        // ===== Reproducibility =====

        [Option('s', "seed", Required = false, Default = 211UL,
            HelpText = "Base RNG seed. Per-thread seeds are derived via SplitMix64(seed, threadIdx).")]
        public ulong Seed { get; set; }

        [Option('i', "iterations", Required = false, Default = 1,
            HelpText = "Run-phase iterations (load runs once; warmup runs once per iter).")]
        public int Iterations { get; set; }

        // ===== Sizing =====

        [Option("hashpack", Required = false, Default = 2.0,
            HelpText = "Hash packing factor (keys per bucket request, before KVSettings round-down).")]
        public double Hashpack { get; set; }

        [Option("log-memory", Required = false, Default = null,
            HelpText = "Total in-memory log size (e.g. 16GB). Auto-default sizes for whole dataset in mutable region (read-only baseline).")]
        public string LogMemory { get; set; }

        [Option("page-size", Required = false, Default = "16MB",
            HelpText = "Page size (e.g. 16MB).")]
        public string PageSize { get; set; }

        [Option("segment-size", Required = false, Default = "1GB",
            HelpText = "On-disk segment size (e.g. 1GB).")]
        public string SegmentSize { get; set; }

        // ===== Device =====

        [Option("device", Required = false, Default = "default",
            HelpText = "Device backend: native, randomaccess, filestream, null, default.")]
        public string Device { get; set; }

        [Option("device-throttle", Required = false, Default = 0,
            HelpText = "Max in-flight IOs. 0 = device default (120 for every Tsavorite device).")]
        public int DeviceThrottle { get; set; }

        [Option("device-io-backend", Required = false, Default = "default",
            HelpText = "Linux native backend: libaio, uring, default (=libaio).")]
        public string DeviceIoBackend { get; set; }

        [Option("device-completion-threads", Required = false, Default = 0,
            HelpText = "Native completion thread count. 0 = Garnet default (1).")]
        public int DeviceCompletionThreads { get; set; }

        [Option("data-path", Required = false, Default = null,
            HelpText = "Directory where hlog files live. Default OS temp.")]
        public string DataPath { get; set; }

        // ===== Host tuning =====

        [Option("no-numa-pin", Required = false, Default = false,
            HelpText = "Disable in-process NUMA pinning.")]
        public bool NoNumaPin { get; set; }

        [Option("numa-node", Required = false, Default = 0,
            HelpText = "Which NUMA node to pin to.")]
        public int NumaNode { get; set; }

        [Option("no-threadpool-tune", Required = false, Default = false,
            HelpText = "Disable auto ThreadPool.SetMinThreads(max(t*2, 256)).")]
        public bool NoThreadPoolTune { get; set; }

        // ===== Validation =====

        [Option("validate", Required = false, Default = false,
            HelpText = "After load: single-threaded readback of every key. Aborts on mismatch.")]
        public bool Validate { get; set; }

        // ===== Output =====

        [Option("report-interval-sec", Required = false, Default = 1,
            HelpText = "Live throughput reporter tick (seconds). 0 disables — recommended for canonical numbers.")]
        public int ReportIntervalSec { get; set; }

        [Option("json-output", Required = false, Default = null,
            HelpText = "Append JSON summary rows to this file.")]
        public string JsonOutput { get; set; }

        [Option("csv-output", Required = false, Default = null,
            HelpText = "Append CSV summary rows to this file.")]
        public string CsvOutput { get; set; }

        [Option("quiet", Required = false, Default = false,
            HelpText = "Suppress human-readable progress/config (final results still print).")]
        public bool Quiet { get; set; }

        // ===== Resolved values (filled in after parsing) =====

        internal long ResolvedPageSizeBytes;
        internal long ResolvedSegmentSizeBytes;
        internal long ResolvedLogMemoryBytes;
        internal long ResolvedIndexRequestedBytes;
        internal long ResolvedIndexAppliedBytes;
        internal long ResolvedRecordSizeBytes;
        internal int ReadPct, UpsertPctCumulative, RmwPctCumulative;
        internal bool UseZipf;
        internal Tsavorite.core.DeviceType ResolvedDeviceType;
        internal Tsavorite.core.NativeStorageDevice.IoBackend ResolvedIoBackend;

        /// <summary>
        /// Validate inputs and resolve all auto-defaults. Returns null on success or an error message.
        /// </summary>
        internal string Resolve()
        {
            if (Threads < 1) return "--threads must be >= 1";
            if (Keys <= 0) return "--keys must be > 0";
            if (ValueSize < 8 || ValueSize > 4096) return "--value-size must be in [8, 4096] (lean benchmark targets the inline-value path)";
            if (Hashpack <= 0) return "--hashpack must be > 0";
            if (RunSec < 0) return "--runsec must be >= 0";
            if (WarmupSec < 0) return "--warmup-sec must be >= 0";
            if (Iterations < 1) return "--iterations must be >= 1";
            if (ReportIntervalSec < 0) return "--report-interval-sec must be >= 0";

            var dist = (Distribution ?? "uniform").ToLowerInvariant();
            if (dist != "uniform" && dist != "zipf") return "--distribution must be 'uniform' or 'zipf'";
            Distribution = dist;
            UseZipf = dist == "zipf";

            var rumd = Rumd?.ToArray() ?? new[] { 100, 0, 0, 0 };
            if (rumd.Length != 4) return "--rumd must be 4 numbers";
            if (rumd.Any(x => x < 0)) return "--rumd entries must be >= 0";
            if (rumd.Sum() != 100) return $"--rumd must sum to 100 (got {rumd.Sum()})";
            Rumd = rumd;
            ReadPct = rumd[0];
            UpsertPctCumulative = ReadPct + rumd[1];
            RmwPctCumulative = UpsertPctCumulative + rumd[2];

            ResolvedDeviceType = ParseDeviceType(Device);
            ResolvedIoBackend = ParseIoBackend(DeviceIoBackend);

            ResolvedPageSizeBytes = KvSize.ParseSize(PageSize);
            if (ResolvedPageSizeBytes <= 0) return $"--page-size invalid: {PageSize}";
            ResolvedSegmentSizeBytes = KvSize.ParseSize(SegmentSize);
            if (ResolvedSegmentSizeBytes <= 0) return $"--segment-size invalid: {SegmentSize}";

            // Estimated record size: 8 RecordInfo + 5 length-byte hdr + 8 key + value, aligned to 8.
            var rec = 21L + ValueSize;
            ResolvedRecordSizeBytes = (rec + 7) & ~7L;

            // --log-memory auto-default: NextPow2(ceil(keys * record / 0.9)), floored at 2 * page-size.
            if (!string.IsNullOrWhiteSpace(LogMemory))
            {
                ResolvedLogMemoryBytes = KvSize.ParseSize(LogMemory);
                if (ResolvedLogMemoryBytes <= 0) return $"--log-memory invalid: {LogMemory}";
            }
            else
            {
                var dbBytes = Keys * ResolvedRecordSizeBytes;
                var target = (long)Math.Ceiling(dbBytes / 0.9);
                var auto = KvSize.NextPow2(target);
                var floor = 2 * ResolvedPageSizeBytes;
                if (auto < floor) auto = floor;
                ResolvedLogMemoryBytes = ClampToRam(auto);
            }

            // --hashpack -> index_size_requested: (long)(keys / hashpack) << 6. KVSettings rounds DOWN
            // to power of 2 — we track both requested and applied here.
            ResolvedIndexRequestedBytes = (long)(Keys / Hashpack) << 6;
            if (ResolvedIndexRequestedBytes < 64) ResolvedIndexRequestedBytes = 64;
            ResolvedIndexAppliedBytes = PreviousPow2(ResolvedIndexRequestedBytes);

            return null;
        }

        internal long ClampToRam(long autoLogMemory)
        {
            // Only auto-derived log-memory is clamped; explicit user values pass through.
            try
            {
                var available = TryGetAvailableRamBytes();
                if (available <= 0) return autoLogMemory;
                var cap = (long)(available * 0.7) - ResolvedIndexAppliedBytes; // leave index room
                if (cap <= 0) return autoLogMemory; // give up; user will see OOM
                var result = autoLogMemory;
                while (result > cap && result > (2 * ResolvedPageSizeBytes))
                    result /= 2;
                return result;
            }
            catch
            {
                return autoLogMemory;
            }
        }

        private static long TryGetAvailableRamBytes()
        {
            if (OperatingSystem.IsLinux())
            {
                try
                {
                    foreach (var line in System.IO.File.ReadAllLines("/proc/meminfo"))
                    {
                        if (line.StartsWith("MemAvailable:", StringComparison.Ordinal))
                        {
                            var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                            if (parts.Length >= 2 && long.TryParse(parts[1], out var kb))
                                return kb * 1024L;
                        }
                    }
                }
                catch { /* fall through */ }
            }
            return GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        }

        private static long PreviousPow2(long n)
        {
            if (n <= 1) return 1;
            long p = 1;
            while ((p << 1) > 0 && (p << 1) <= n) p <<= 1;
            return p;
        }

        internal static Tsavorite.core.DeviceType ParseDeviceType(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return Tsavorite.core.DeviceType.Default;
            return s.ToLowerInvariant() switch
            {
                "native" => Tsavorite.core.DeviceType.Native,
                "randomaccess" => Tsavorite.core.DeviceType.RandomAccess,
                "filestream" => Tsavorite.core.DeviceType.FileStream,
                "null" => Tsavorite.core.DeviceType.Null,
                "default" => Tsavorite.core.DeviceType.Default,
                _ => Tsavorite.core.DeviceType.Default,
            };
        }

        internal static Tsavorite.core.NativeStorageDevice.IoBackend ParseIoBackend(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return Tsavorite.core.NativeStorageDevice.IoBackend.Default;
            return s.ToLowerInvariant() switch
            {
                "default" => Tsavorite.core.NativeStorageDevice.IoBackend.Default,
                "libaio" => Tsavorite.core.NativeStorageDevice.IoBackend.Libaio,
                "uring" or "io_uring" or "iouring" => Tsavorite.core.NativeStorageDevice.IoBackend.Uring,
                _ => Tsavorite.core.NativeStorageDevice.IoBackend.Default,
            };
        }
    }
}
