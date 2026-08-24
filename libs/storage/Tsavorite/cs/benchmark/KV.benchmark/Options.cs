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
            HelpText = "Default run-phase worker thread count (also used for load if --load-threads is unspecified). Pass nodeCpus to saturate the pinned NUMA node.")]
        public int Threads { get; set; }

        [Option("load-threads", Required = false, Default = 0,
            HelpText = "Threads to use for the load phase. 0 = same as --threads. Useful when you want a fast parallel load followed by a single-thread or sweep run.")]
        public int LoadThreads { get; set; }

        [Option("run-threads-sweep", Separator = ',', Required = false, Default = null,
            HelpText = "Comma-separated list of run-phase thread counts. When specified, the engine loads ONCE and then runs the full --iterations sweep for each thread count (1,2,4,8,16). Overrides --threads for the run phase.")]
        public IEnumerable<int> RunThreadsSweep { get; set; }

        [Option('n', "keys", Required = false, Default = 100_000_000L,
            HelpText = "Number of unique keys in the dataset.")]
        public long Keys { get; set; }

        [Option('v', "value-size", Required = false, Default = 100,
            HelpText = "Value length in bytes. Range: 32..1048576 (must also be <= --max-inline-value-size). In variable mode (--value-size-max > this) it is the MINIMUM per-key size.")]
        public int ValueSize { get; set; }

        [Option("value-size-max", Required = false, Default = "0",
            HelpText = "When > --value-size, enables VARIABLE per-key value sizes drawn log-uniformly in [--value-size, this] (deterministic per key). Must be <= --max-inline-value-size. 0 = fixed size (every key uses --value-size). Use to spread records across buffer-pool size classes, e.g. --value-size 100 --value-size-max 10m.")]
        public string ValueSizeMax { get; set; }

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

        // ===== Pipelining =====

        [Option('b', "batch-size", Required = false, Default = 1024,
            HelpText = "Run-phase batch depth: ops issued per chunk before an opportunistic drain. Mirrors Resp.benchmark -b.")]
        public int BatchSize { get; set; }

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
            HelpText = "Page size (e.g. 16MB, 32MB). Default matches Garnet (defaults.conf PageSize=16m).")]
        public string PageSize { get; set; }

        [Option("segment-size", Required = false, Default = "1GB",
            HelpText = "On-disk segment size (e.g. 1GB). Default matches Garnet (defaults.conf SegmentSize=1g).")]
        public string SegmentSize { get; set; }

        [Option("max-inline-value-size", Required = false, Default = "16KB",
            HelpText = "Max inline value size (KVSettings.MaxInlineValueSize). Values larger than this overflow to a separate heap object. Default matches Garnet (defaults.conf ValueOverflowThreshold=16k).")]
        public string MaxInlineValueSize { get; set; }

        [Option("preallocate-log", Required = false, Default = false,
            HelpText = "Pre-touch every log page at startup to commit physical pages. Default matches Garnet (false). Enable for stable single-thread benchmarks where first-touch page faults would bias the timed window.")]
        public bool PreallocateLog { get; set; }

        // ===== Device =====

        [Option("device", Required = false, Default = "default",
            HelpText = "Device backend: native, randomaccess, filestream, null, localmemory, default.")]
        public string Device { get; set; }

        [Option("device-throttle-limit", Required = false, Default = 0,
            HelpText = "Aggregate max in-flight IOs (software backpressure). 0 = device default (4096 for Native; 120 for " +
                       "the managed in-box devices; maps to the LocalMemory SPSC ring otherwise). The Native 4096 default " +
                       "already saturates a fast NVMe queue; the managed 120 default under-drives one, so raise it (>=512) " +
                       "to reach the IOPS ceiling on those devices. Also size --num-keys so the log spans enough of the " +
                       "device (a small LBA span engages fewer NAND channels and caps IOPS below the device's large-span " +
                       "ceiling). Capped at io-contexts * queue-depth.")]
        public int DeviceThrottleLimit { get; set; }

        [Option("device-io-backend", Required = false, Default = "default",
            HelpText = "Linux native backend: libaio, uring, default (=libaio).")]
        public string DeviceIoBackend { get; set; }

        [Option("device-completion-threads", Required = false, Default = 0,
            HelpText = "Number of background drainer threads for the device's IO completion queue. " +
                       "For DeviceType.Native on Linux: each drainer is bound 1:1 to its own kernel " +
                       "io_context (libaio) or io_uring ring; submitters distribute across rings via " +
                       "per-thread affinity. For DeviceType.LocalMemory: each drainer owns one SPSC " +
                       "ring fed by one submitter (per-thread routing). Throughput scales with this " +
                       "value up to the available submitter concurrency. 0 = default (1 for Native; " +
                       "Environment.ProcessorCount for LocalMemory).")]
        public int DeviceCompletionThreads { get; set; }

        [Option("device-io-contexts", Required = false, Default = 0,
            HelpText = "DeviceType.Native on Linux only: number of independent kernel io_contexts " +
                       "(libaio) / io_uring rings the device creates, decoupled from the number of " +
                       "drainer threads (--device-completion-threads). Submitters map to rings via " +
                       "per-thread affinity, so setting this >= submitter concurrency makes io_submit " +
                       "contention-free (no shared per-context aio ring/completion lock across unrelated " +
                       "submitters) and spreads completion posting across more rings; the drainers each " +
                       "range-drain a contiguous slice of the rings. 0 (default) is backend-specific: io_uring " +
                       "uses a hardware-aware ring count (min(2*ProcessorCount, 64), floored at the drainer count), " +
                       "while libaio uses one ring per drainer. Clamped up to --device-completion-threads.")]
        public int DeviceIoContexts { get; set; }

        [Option("device-queue-depth", Required = false, Default = 0,
            HelpText = "DeviceType.Native on Linux only: per-ring kernel queue depth (io_uring SQ entries / " +
                       "libaio io_context nr_events) for each of the --device-io-contexts rings. 0 = default " +
                       "(4096). Cap 32768 (io_uring hard limit). For libaio, io-contexts * queue-depth is drawn " +
                       "from the global fs.aio-max-nr budget (warned if exceeded; io_setup then fails if the budget is exhausted).")]
        public int DeviceQueueDepth { get; set; }

        [Option("device-uring-sqpoll", Required = false, Default = false,
            HelpText = "DeviceType.Native + --device-io-backend uring only: enable io_uring SQPOLL " +
                       "(IORING_SETUP_SQPOLL) so a kernel thread polls the submission queue and submissions " +
                       "are syscall-free. Each ring gets its own poll thread (no IORING_SETUP_ATTACH_WQ) so " +
                       "submission stays parallel across rings. Ignored for libaio. Off by default (opt-in).")]
        public bool DeviceUringSqPoll { get; set; }

        [Option("device-uring-sqpoll-idle-ms", Required = false, Default = 0,
            HelpText = "io_uring SQPOLL poll-thread idle window in milliseconds (sq_thread_idle): how long the " +
                       "kernel poll thread spins after the last submit before parking. 0 = native default (10s). " +
                       "Only meaningful with --device-uring-sqpoll.")]
        public int DeviceUringSqPollIdleMs { get; set; }

        [Option("device-inline-completion", Required = false, Default = false,
            HelpText = "DeviceType.LocalMemory only: complete IOs inline on the submitting thread (no " +
                       "completion threads or rings; copy + callback run synchronously). Isolates the " +
                       "per-op work from the cross-thread run-thread->completion-thread handoff. " +
                       "Overrides --device-completion-threads.")]
        public bool DeviceInlineCompletion { get; set; }

        [Option("use-native-allocator", Required = false, Default = false,
            HelpText = "Route hash index / log pages / frames through a native (off-managed-heap) direct-VM " +
                       "allocator instead of the GC heap.")]
        public bool UseNativeAllocator { get; set; }

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

        [Option("dump-distribution", Required = false, Default = false,
            HelpText = "After load: print the hash-table bucket distribution (TsavoriteKV.DumpDistribution()).")]
        public bool DumpDistribution { get; set; }

        [Option("pool-stats", Required = false, Default = false,
            HelpText = "Instrument the sector-aligned buffer pool: at the start of each measured run window reset per-size-class allocation/reuse counters, sample them every --report-interval-sec (showing new allocations plateau while cache reuse climbs), and print a per-size-class alloc/reuse table afterward. Requires building with -p:BufferPoolStats=true; without it the recording call sites are compiled out (zero Get-path overhead) and a warning is printed.")]
        public bool PoolStats { get; set; }

        [Option("use-legacy-buffer-pool", Required = false, Default = false,
            HelpText = "Use the legacy per-level ConcurrentQueue SectorAlignedBufferPool (sets SectorAlignedBufferPool.UseOriginReturn=false) instead of the default origin-return per-thread pool, for A/B throughput comparison.")]
        public bool UseLegacyBufferPool { get; set; }

        [Option("pool-budget", Required = false, Default = "0",
            HelpText = "Override the sector-aligned buffer pool's per-pool managed byte budget (SectorAlignedBufferPool.ManagedBudgetBytes; default 1GB) before any pool is created, e.g. 8g. The budget bounds cacheable bytes (25% small classes, 75% large); a budget smaller than the in-flight large-buffer working set forces large reads to allocate-on-Get / drop-on-Return (bounded memory, lower reuse). 0 = leave the default.")]
        public string PoolBudget { get; set; }

        // ===== Output =====

        [Option("report-interval-sec", Required = false, Default = 1,
            HelpText = "Live throughput reporter tick (seconds). 0 disables — recommended for canonical numbers.")]
        public int ReportIntervalSec { get; set; }

        [Option("json-output", Required = false, Default = null,
            HelpText = "Append pretty-printed JSON summary rows to this file (one row per phase).")]
        public string JsonOutput { get; set; }

        [Option("json-stdout", Required = false, Default = false,
            HelpText = "Also emit single-line `KV-RESULT-JSON: {...}` blobs to stdout for log scraping. Off by default.")]
        public bool JsonStdout { get; set; }

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
        internal long ResolvedMaxInlineValueSizeBytes;
        /// <summary>True when --value-size-max enables per-key variable value sizes.</summary>
        internal bool VariableValueSize;
        /// <summary>Maximum per-key value size in variable mode (0 in fixed mode). Min is <see cref="ValueSize"/>.</summary>
        internal long ResolvedValueSizeMaxBytes;
        /// <summary>Override for SectorAlignedBufferPool.ManagedBudgetBytes (0 = leave default).</summary>
        internal long ResolvedPoolBudgetBytes;
        internal int ReadPct, UpsertPctCumulative, RmwPctCumulative;
        internal bool UseZipf;
        internal Tsavorite.core.DeviceType ResolvedDeviceType;
        internal Tsavorite.core.NativeStorageDevice.IoBackend ResolvedIoBackend;

        /// <summary>Thread count used for the load phase (load-threads if specified, else threads).</summary>
        internal int ResolvedLoadThreads;
        /// <summary>Thread counts for the run phase: either the sweep list (if --run-threads-sweep was set) or [Threads].</summary>
        internal int[] ResolvedRunThreadsSweep;
        /// <summary>Maximum worker count across all phases — used to size the scoreboard.</summary>
        internal int ResolvedMaxThreads;

        /// <summary>
        /// Validate inputs and resolve all auto-defaults. Returns null on success or an error message.
        /// </summary>
        internal string Resolve()
        {
            if (Threads < 1) return "--threads must be >= 1";
            if (LoadThreads < 0) return "--load-threads must be >= 0 (0 = same as --threads)";
            ResolvedLoadThreads = LoadThreads > 0 ? LoadThreads : Threads;

            var sweep = RunThreadsSweep?.ToArray() ?? [];
            if (sweep.Length > 0)
            {
                if (sweep.Any(t => t < 1)) return "--run-threads-sweep entries must be >= 1";
                ResolvedRunThreadsSweep = sweep;
            }
            else
            {
                ResolvedRunThreadsSweep = [Threads];
            }
            ResolvedMaxThreads = Math.Max(ResolvedLoadThreads, ResolvedRunThreadsSweep.Max());

            if (Keys <= 0) return "--keys must be > 0";
            // Validate --value-size: lower bound 32 (Reader copies 32 bytes), upper bound 1MB
            // (validated against --max-inline-value-size below for the per-record cap).
            if (ValueSize < 32 || ValueSize > 1024 * 1024) return "--value-size must be in [32, 1048576]";
            if (Hashpack <= 0) return "--hashpack must be > 0";
            if (RunSec < 0) return "--runsec must be >= 0";
            if (WarmupSec < 0) return "--warmup-sec must be >= 0";
            if (Iterations < 1) return "--iterations must be >= 1";
            if (ReportIntervalSec < 0) return "--report-interval-sec must be >= 0";

            var dist = (Distribution ?? "uniform").ToLowerInvariant();
            if (dist != "uniform" && dist != "zipf") return "--distribution must be 'uniform' or 'zipf'";
            Distribution = dist;
            UseZipf = dist == "zipf";
            if (UseZipf)
            {
                // ZipfConstants computes Alpha = 1/(1-theta); theta in [0,1) gives valid alpha.
                // theta == 1 divides by zero; theta < 0 or > 1 produces NaN / negative samples.
                if (!(ZipfTheta >= 0 && ZipfTheta < 1))
                    return $"--zipf-theta must be in [0, 1); got {ZipfTheta}";
            }

            var rumd = Rumd?.ToArray() ?? [100, 0, 0, 0];
            if (rumd.Length != 4) return "--rumd must be 4 numbers";
            if (rumd.Any(x => x < 0)) return "--rumd entries must be >= 0";
            if (rumd.Sum() != 100) return $"--rumd must sum to 100 (got {rumd.Sum()})";
            Rumd = rumd;
            ReadPct = rumd[0];
            UpsertPctCumulative = ReadPct + rumd[1];
            RmwPctCumulative = UpsertPctCumulative + rumd[2];

            ResolvedDeviceType = ParseDeviceType(Device);
            if (ResolvedDeviceType == Tsavorite.core.DeviceType.Default && !IsKnownDeviceName(Device))
                return $"--device must be one of: native, randomaccess, filestream, null, localmemory, default (got: {Device})";
            ResolvedIoBackend = ParseIoBackend(DeviceIoBackend);
            if (ResolvedIoBackend == Tsavorite.core.NativeStorageDevice.IoBackend.Default && !IsKnownIoBackendName(DeviceIoBackend))
                return $"--device-io-backend must be one of: libaio, uring, default (got: {DeviceIoBackend})";

            ResolvedPageSizeBytes = KvSize.ParseSize(PageSize);
            if (ResolvedPageSizeBytes <= 0) return $"--page-size invalid: {PageSize}";
            ResolvedSegmentSizeBytes = KvSize.ParseSize(SegmentSize);
            if (ResolvedSegmentSizeBytes <= 0) return $"--segment-size invalid: {SegmentSize}";
            ResolvedMaxInlineValueSizeBytes = KvSize.ParseSize(MaxInlineValueSize);
            if (ResolvedMaxInlineValueSizeBytes <= 0) return $"--max-inline-value-size invalid: {MaxInlineValueSize}";
            if (ValueSize > ResolvedMaxInlineValueSizeBytes)
                return $"--value-size ({ValueSize}) exceeds --max-inline-value-size ({ResolvedMaxInlineValueSizeBytes}); values larger than the inline threshold overflow to heap and skew the benchmark.";

            // Variable value sizes (--value-size-max): per-key log-uniform in [ValueSize, max].
            ResolvedValueSizeMaxBytes = 0;
            VariableValueSize = false;
            if (!string.IsNullOrWhiteSpace(ValueSizeMax) && ValueSizeMax != "0")
            {
                var vmax = KvSize.ParseSize(ValueSizeMax);
                if (vmax <= 0)
                    return $"--value-size-max invalid: {ValueSizeMax}";
                if (vmax > int.MaxValue)
                    return $"--value-size-max too large: {ValueSizeMax}";
                if (vmax < ValueSize)
                    return $"--value-size-max ({vmax}) must be >= --value-size ({ValueSize})";
                if (vmax > ResolvedMaxInlineValueSizeBytes)
                    return $"--value-size-max ({vmax}) exceeds --max-inline-value-size ({ResolvedMaxInlineValueSizeBytes}); values larger than the inline threshold overflow to heap and would not exercise the inline-record read pool.";
                ResolvedValueSizeMaxBytes = vmax;
                VariableValueSize = vmax > ValueSize;
            }

            // Estimated record size: 8 RecordInfo + 5 length-byte hdr + 8 key + value, aligned to 8.
            // In variable mode use the log-uniform MEAN value size ((b-a)/ln(b/a)) so auto log-memory
            // sizing and the displayed dataset estimate reflect the true average, not the tiny minimum.
            long effValueSize = ValueSize;
            if (VariableValueSize)
            {
                double a = ValueSize, b = ResolvedValueSizeMaxBytes;
                effValueSize = (long)((b - a) / Math.Log(b / a));
                if (effValueSize < ValueSize) effValueSize = ValueSize;
            }
            var rec = 21L + effValueSize;
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

            ResolvedPoolBudgetBytes = 0;
            if (!string.IsNullOrWhiteSpace(PoolBudget) && PoolBudget != "0")
            {
                ResolvedPoolBudgetBytes = KvSize.ParseSize(PoolBudget);
                if (ResolvedPoolBudgetBytes <= 0) return $"--pool-budget invalid: {PoolBudget}";
            }

            return null;
        }

        /// <summary>
        /// Deterministic per-key value size. In fixed mode returns <see cref="ValueSize"/> for every key.
        /// In variable mode returns a log-uniform draw in [ValueSize, ResolvedValueSizeMaxBytes] hashed from
        /// the key and seed (stable across load/validate/run), rounded down to a multiple of 8 and floored at
        /// <see cref="ValueSize"/>. Log-uniform spreads keys evenly across the pool's geometric size classes.
        /// </summary>
        internal int SizeForKey(long k)
        {
            if (!VariableValueSize)
                return ValueSize;

            // SplitMix64(seed ^ mix ^ key) -> [0,1) via the top 53 bits.
            ulong x = Seed ^ 0xA5A55A5A12349E37UL ^ (ulong)k;
            x += 0x9E3779B97F4A7C15UL;
            ulong z = x;
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
            z ^= z >> 31;
            double u = (z >> 11) * (1.0 / (1UL << 53));

            double min = ValueSize, max = ResolvedValueSizeMaxBytes;
            int size = (int)(min * Math.Pow(max / min, u));
            if (size < ValueSize) size = ValueSize;
            if (size > ResolvedValueSizeMaxBytes) size = (int)ResolvedValueSizeMaxBytes;
            size &= ~7;                      // multiple of 8 (record alignment)
            if (size < ValueSize) size = ValueSize;
            return size;
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
                "localmemory" or "localmem" => Tsavorite.core.DeviceType.LocalMemory,
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

        static bool IsKnownDeviceName(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return true;
            return s.ToLowerInvariant() is "native" or "randomaccess" or "filestream" or "null" or "default" or "localmemory" or "localmem";
        }

        static bool IsKnownIoBackendName(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return true;
            return s.ToLowerInvariant() is "default" or "libaio" or "uring" or "io_uring" or "iouring";
        }
    }
}