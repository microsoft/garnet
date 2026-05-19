// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using CommandLine;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    class Options
    {
        [Option('b', "benchmark", Required = false, Default = 0,
            HelpText = "Benchmark to run:" +
                        "\n    0 = YCSB with Fixed-length (long- and int-sized) SpanByte values" +
                        "\n    1 = YCSB with longer SpanByte keys and values" +
                        "\n    2 = YCSB with longer values that may also be represented as overflow byte[] or as Object" +
                        "\n    3 = ConcurrentDictionary")]
        public int Benchmark { get; set; }

        [Option('t', "threads", Required = false, Default = 8,
            HelpText = "Number of threads to run the workload on")]
        public int ThreadCount { get; set; }

        [Option('n', "numa", Required = false, Default = 0,
            HelpText = "NUMA options (Windows only):" +
                        "\n    0 = No sharding across NUMA sockets" +
                        "\n    1 = Sharding across NUMA sockets")]
        public int NumaStyle { get; set; }

        [Option('k', "recover", Required = false, Default = false,
            HelpText = "Enable Backup and Restore of TsavoriteKV for fast test startup." +
                        "\n    True = Recover TsavoriteKV if a Checkpoint is available, else populate TsavoriteKV from data and Checkpoint it so it can be Restored in a subsequent run" +
                        "\n    False = Populate TsavoriteKV from data and do not Checkpoint a backup" +
                        "\n    (Checkpoints are stored in directories under the data path in directories named by distribution, ycsb vs. synthetic data, and key counts)")]
        public bool BackupAndRestore { get; set; }

        [Option('i', "iterations", Required = false, Default = 1,
            HelpText = "Number of iterations of the test to run")]
        public int IterationCount { get; set; }

        [Option('d', "distribution", Required = false, Default = YcsbConstants.UniformDist,
            HelpText = "Distribution of keys in workload")]
        public string DistributionName { get; set; }

        [Option('s', "seed", Required = false, Default = 211,
            HelpText = "Seed for synthetic data distribution")]
        public int RandomSeed { get; set; }

        [Option("rumd", Separator = ',', Required = false, Default = new[] { 50, 50, 0, 0 },
            HelpText = "#,#,#,#: Percentages of [(r)eads,(u)pserts,r(m)ws,(d)eletes] (summing to 100) operations in this run")]
        public IEnumerable<int> RumdPercents { get; set; }

        [Option("sba", Required = false, Default = false,
            HelpText = "Use SpanByteAllocator (default is to use ObjectAllocator)")]
        public bool UseSBA { get; set; }

        [Option("reviv", Required = false, Default = RevivificationLevel.None,
            HelpText = "Revivification of tombstoned records:" +
                        $"\n    {nameof(RevivificationLevel.None)} = No revivification" +
                        $"\n    {nameof(RevivificationLevel.Chain)} = Revivify tombstoned records in tag chain only" +
                        $"\n    {nameof(RevivificationLevel.Full)} = Tag chain and FreeList")]
        public RevivificationLevel RevivificationLevel { get; set; }

        [Option("reviv-bin-record-count", Separator = ',', Required = false, Default = 128,
            HelpText = "#,#,...,#: Number of records in each bin:" +
                       "    Default (not specified): All bins are 128 records" +
                       "    # (one value): All bins have this number of records, else error")]
        public int RevivBinRecordCount { get; set; }

        [Option("di", Required = false, Default = false,
            HelpText = "Delete+insert; immediately reinsert the key after deleting it")]
        public bool DeleteAndReinsert { get; set; }

        [Option("reviv-mutable%", Separator = ',', Required = false, Default = RevivificationSettings.DefaultRevivifiableFraction,
            HelpText = "Percentage of in-memory region that is eligible for revivification")]
        public double RevivifiableFraction { get; set; }

        [Option("synth", Required = false, Default = false,
            HelpText = "Use synthetic data")]
        public bool UseSyntheticData { get; set; }

        [Option("runsec", Required = false, Default = 30,
            HelpText = "Number of seconds to execute experiment")]
        public int RunSeconds { get; set; }

        [Option("sd", Required = false, Default = false,
            HelpText = "Use SmallData in experiment")]
        public bool UseSmallData { get; set; }

        [Option("sm", Required = false, Default = false,
            HelpText = "Use Small Memory log in experiment")]
        public bool UseSmallMemoryLog { get; set; }

        [Option("ovf", Required = false, Default = false,
            HelpText = "Use Small MaxInlineValueSize in SpanByte benchmark to test (ov)er(f)low value allocations")]
        public bool UseOverflowValues { get; set; }

        [Option("obj", Required = false, Default = false,
            HelpText = "Use (obj)ect values")]
        public bool UseObjectValues { get; set; }

        [Option("hashpack", Required = false, Default = 2.0,
            HelpText = "The hash table packing; divide the number of keys by this to cause hash collisions")]
        public double HashPacking { get; set; }

        [Option("safectx", Required = false, Default = false,
            HelpText = "Use 'safe' context (slower, per-operation epoch control) in experiment")]
        public bool UseSafeContext { get; set; }

        [Option("chkptms", Required = false, Default = 0,
            HelpText = "If > 0, the number of milliseconds between checkpoints in experiment (else checkpointing is not done)")]
        public int PeriodicCheckpointMilliseconds { get; set; }

        [Option("chkptsnap", Required = false, Default = false,
            HelpText = "Use Snapshot checkpoint if doing periodic checkpoints (default is FoldOver)")]
        public bool PeriodicCheckpointUseSnapshot { get; set; }

        [Option("dumpdist", Required = false, Default = false,
            HelpText = "Dump the distribution of each non-empty bucket in the hash table")]
        public bool DumpDistribution { get; set; }

        // ===== Storage topology =====

        [Option("page-size", Required = false, Default = null,
            HelpText = "Page size, e.g. '4MB', '32m', '8192' (bytes). Overrides default and --sm.")]
        public string PageSize { get; set; }

        [Option("log-memory", Required = false, Default = null,
            HelpText = "Log memory size, e.g. '64GB', '256m'. Overrides default and --sm.")]
        public string LogMemory { get; set; }

        [Option("segment-size", Required = false, Default = null,
            HelpText = "On-disk segment size, e.g. '1GB'. Default = max(page-size, log-memory).")]
        public string SegmentSize { get; set; }

        [Option("mutable-fraction", Required = false, Default = 0.9,
            HelpText = "Fraction of log memory kept mutable (1.0 = no flushing in pure in-memory runs).")]
        public double MutableFraction { get; set; }

        [Option("preallocate-log", Required = false, Default = true,
            HelpText = "Preallocate all log pages at startup. Disable with --preallocate-log false to defer allocation.")]
        public bool PreallocateLog { get; set; }

        // ===== Device backend =====

        [Option("device", Required = false, Default = "default",
            HelpText = "Device backend: native (libaio/IOCP), randomaccess (.NET RandomAccess; Linux default), filestream (.NET Stream), null (no I/O), default")]
        public string Device { get; set; }

        [Option("device-throttle", Required = false, Default = 0,
            HelpText = "Override IDevice.ThrottleLimit (max concurrent IOs). 0 = device default.")]
        public int DeviceThrottle { get; set; }

        [Option("device-completion-threads", Required = false, Default = 0,
            HelpText = "Native completion thread count (libaio on Linux). 0 = device default (1).")]
        public int DeviceCompletionThreads { get; set; }

        [Option("device-io-backend", Required = false, Default = "default",
            HelpText = "Native device IO backend (Linux only): default (=libaio), libaio, or uring. " +
                       "uring requires the native lib to be built with -DUSE_URING=ON and liburing.so.2 to be available.")]
        public string DeviceIoBackend { get; set; }

        [Option("data-path", Required = false, Default = null,
            HelpText = "Directory for hlog files and checkpoints. Default 'D:/data/TsavoriteYcsbBenchmark'.")]
        public string DataPath { get; set; }

        [Option("use-os-cache", Required = false, Default = false,
            HelpText = "Allow OS page cache for managed devices (default off => O_DIRECT/FILE_FLAG_NO_BUFFERING when supported).")]
        public bool UseOsCache { get; set; }

        // ===== Runtime tuning =====

        [Option("threadpool-min", Required = false, Default = 0,
            HelpText = "Raise ThreadPool.SetMinThreads to this value (worker + completion). 0 = leave default.")]
        public int ThreadPoolMin { get; set; }

        // ===== Phase control =====

        [Option("phase", Required = false, Default = "both",
            HelpText = "Which phases to execute: load (insert only), run (RUMD only; requires -k recover), both (default).")]
        public string Phase { get; set; }

        [Option("load-keys", Required = false, Default = 0L,
            HelpText = "Override #keys to insert in load phase. 0 = default (250M, or 4.6M with --sd).")]
        public long LoadKeyCount { get; set; }

        // ===== Reproducibility / hygiene =====

        [Option("cleanup-data-files", Required = false, Default = false,
            HelpText = "Delete any pre-existing hlog* files in data-path before the run.")]
        public bool CleanupDataFiles { get; set; }

        [Option("drop-page-cache", Required = false, Default = false,
            HelpText = "Linux only: write 3 to /proc/sys/vm/drop_caches before the run (requires root; fails silently otherwise).")]
        public bool DropPageCache { get; set; }

        // ===== Diagnostics / output =====

        [Option("print-config", Required = false, Default = true,
            HelpText = "Print a consolidated config block at start. Disable with --print-config false.")]
        public bool PrintConfig { get; set; }

        [Option("csv-output", Required = false, Default = null,
            HelpText = "Append load and run results as CSV rows to this file (creates the file with a header if missing).")]
        public string CsvOutput { get; set; }

        internal CheckpointType PeriodicCheckpointType => PeriodicCheckpointUseSnapshot ? CheckpointType.Snapshot : CheckpointType.FoldOver;

        public string GetOptionsString()
        {
            static string boolStr(bool value) => value ? "y" : "n";
            var allocator = UseSBA ? "sba" : "oa";
            return $"b: {Benchmark}; a: {allocator}; d: {DistributionName.ToLower()}; n: {NumaStyle}; rumd: {string.Join(',', RumdPercents)}; reviv: {RevivificationLevel}; revivbinrecs: {RevivBinRecordCount};"
                        + $" revivfrac {RevivifiableFraction}; t: {ThreadCount}; i: {IterationCount}; ov: {boolStr(UseOverflowValues)}; obj: {boolStr(UseObjectValues)}; hp: {HashPacking};"
                        + $" sd: {boolStr(UseSmallData)}; sm: {boolStr(UseSmallMemoryLog)}; synth: {boolStr(UseSyntheticData)}; safectx: {boolStr(UseSafeContext)};"
                        + $" chkptms: {PeriodicCheckpointMilliseconds}; chkpttype: {(PeriodicCheckpointMilliseconds > 0 ? PeriodicCheckpointType.ToString() : "None")}";
        }
    }
}