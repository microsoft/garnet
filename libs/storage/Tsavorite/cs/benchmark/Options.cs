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
                        "\n    0 = YCSB" +
                        "\n    1 = YCSB with SpanByte" +
                        "\n    2 = ConcurrentDictionary")]
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
                        "\n    (Checkpoints are stored in directories under " + TestLoader.DataPath + " in directories named by distribution, ycsb vs. synthetic data, and key counts)")]
        public bool BackupAndRestore { get; set; }

        [Option('z', "locking", Required = false, Default = ConcurrencyControlMode.None,
             HelpText = "Locking Implementation:" +
                       $"\n    {nameof(ConcurrencyControlMode.None)} = No Locking (default)" +
                       $"\n    {nameof(ConcurrencyControlMode.LockTable)} = Locking using main HashTable buckets" +
                       $"\n    {nameof(ConcurrencyControlMode.RecordIsolation)} = RecordInfo locking only within concurrent IFunctions callbacks")]
        public ConcurrencyControlMode ConcurrencyControlMode { get; set; }

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

        [Option("hashpack", Required = false, Default = 2.0,
            HelpText = "The hash table packing; divide the number of keys by this to cause hash collisions")]
        public double HashPacking { get; set; }

        [Option("safectx", Required = false, Default = false,
            HelpText = "Use 'safe' context (slower, per-operation epoch control) in experiment")]
        public bool UseSafeContext { get; set; }

        [Option("chkptms", Required = false, Default = 0,
            HelpText = "If > 0, the number of milliseconds between checkpoints in experiment (else checkpointing is not done")]
        public int PeriodicCheckpointMilliseconds { get; set; }

        [Option("chkptsnap", Required = false, Default = false,
            HelpText = "Use Snapshot checkpoint if doing periodic checkpoints (default is FoldOver)")]
        public bool PeriodicCheckpointUseSnapshot { get; set; }

        [Option("chkptincr", Required = false, Default = false,
            HelpText = "Try incremental checkpoint if doing periodic checkpoints")]
        public bool PeriodicCheckpointTryIncremental { get; set; }

        [Option("dumpdist", Required = false, Default = false,
            HelpText = "Dump the distribution of each non-empty bucket in the hash table")]
        public bool DumpDistribution { get; set; }

        internal CheckpointType PeriodicCheckpointType => PeriodicCheckpointUseSnapshot ? CheckpointType.Snapshot : CheckpointType.FoldOver;

        public string GetOptionsString()
        {
            static string boolStr(bool value) => value ? "y" : "n";
            return $"-b: {Benchmark}; d: {DistributionName.ToLower()}; n: {NumaStyle}; rumd: {string.Join(',', RumdPercents)}; reviv: {RevivificationLevel}; revivbinrecs: {RevivBinRecordCount};"
                        + $" revivfrac {RevivifiableFraction}; t: {ThreadCount}; z: {ConcurrencyControlMode}; i: {IterationCount}; hp: {HashPacking};"
                        + $" sd: {boolStr(UseSmallData)}; sm: {boolStr(UseSmallMemoryLog)}; sy: {boolStr(UseSyntheticData)}; safectx: {boolStr(UseSafeContext)};"
                        + $" chkptms: {PeriodicCheckpointMilliseconds}; chkpttype: {(PeriodicCheckpointMilliseconds > 0 ? PeriodicCheckpointType.ToString() : "None")};"
                        + $" chkptincr: {boolStr(PeriodicCheckpointTryIncremental)}";
        }
    }
}