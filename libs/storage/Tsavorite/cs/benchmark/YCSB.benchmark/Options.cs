// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
                        "\n    2 = ConcurrentDictionary " +
                        "\n    3 = TsavoriteLog")]
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

        [Option("aof-memory", Required = false, Default = "64m", HelpText = "Total AOF memory buffer used in bytes (rounds down to power of 2) - spills to disk after this limit")]
        public string AofMemorySize { get; set; }

        [Option("aof-page-size", Required = false, Default = "4m", HelpText = "Size of each AOF page in bytes(rounds down to power of 2)")]
        public string AofPageSize { get; set; }

        [Option("aof-null-device", Required = false, Default = false, HelpText = "With main-memory replication, use null device for AOF. Ensures no disk IO, but can cause data loss during replication.")]
        public bool UseAofNullDevice { get; set; }

        [Option("aof-commit-freq", Required = false, Default = 0, HelpText = "Write ahead logging (append-only file) commit issue frequency in milliseconds. 0 = issue an immediate commit per operation, -1 = manually issue commits using COMMITAOF command")]
        public int CommitFrequencyMs { get; set; }

        [Option('c', "checkpointdir", Required = false, HelpText = "Storage directory for checkpoints. Uses logdir if unspecified.")]
        public string CheckpointDir { get; set; }

        /// <summary>
        /// Get AOF memory size in bits
        /// </summary>
        /// <returns></returns>
        public int AofMemorySizeBits()
        {
            var size = ParseSize(AofMemorySize, out _);
            var adjustedSize = PreviousPowerOf2(size);
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get AOF Page size in bits
        /// </summary>
        /// <returns></returns>
        public int AofPageSizeBits()
        {
            var size = ParseSize(AofPageSize, out _);
            var adjustedSize = PreviousPowerOf2(size);
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Parse size from string specification
        /// </summary>
        /// <param name="value"></param>
        /// <param name="bytesRead"></param>
        /// <returns></returns>
        public static long ParseSize(string value, out int bytesRead)
        {
            char[] suffix = ['k', 'm', 'g', 't', 'p'];
            long result = 0;
            bytesRead = 0;
            for (var i = 0; i < value.Length; i++)
            {
                var c = value[i];
                if (char.IsDigit(c))
                {
                    result = (result * 10) + (byte)c - '0';
                    bytesRead++;
                }
                else
                {
                    for (var s = 0; s < suffix.Length; s++)
                    {
                        if (char.ToLower(c) == suffix[s])
                        {
                            result *= (long)Math.Pow(1024, s + 1);
                            bytesRead++;

                            if (i + 1 < value.Length && char.ToLower(value[i + 1]) == 'b')
                                bytesRead++;

                            return result;
                        }
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Previous power of 2
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        internal static long PreviousPowerOf2(long v)
        {
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v - (v >> 1);
        }

        /// <summary>
        /// Instance of interface to create named device factories
        /// </summary>
        INamedDeviceFactoryCreator DeviceFactoryCreator = new LocalStorageNamedDeviceFactoryCreator();

        /// <summary>
        /// Get device for AOF
        /// </summary>
        /// <returns></returns>
        public IDevice GetAofDevice()
        {
            if (UseAofNullDevice) return new NullDevice();

            return GetInitializedDeviceFactory(AppendOnlyFileBaseDirectory)
                .Get(new FileDescriptor("AOF", "aof.log"));
        }

        /// <summary>
        /// Gets a new instance of device factory initialized with the supplied baseName.
        /// </summary>
        /// <param name="baseName"></param>
        /// <returns></returns>
        public INamedDeviceFactory GetInitializedDeviceFactory(string baseName)
        {
            return DeviceFactoryCreator.Create(baseName);
        }

        /// <summary>
        /// Gets the base directory for storing AOF commits
        /// </summary>
        public string AppendOnlyFileBaseDirectory => CheckpointDir ?? string.Empty;

        internal CheckpointType PeriodicCheckpointType => PeriodicCheckpointUseSnapshot ? CheckpointType.Snapshot : CheckpointType.FoldOver;

        public string GetOptionsString()
        {
            static string boolStr(bool value) => value ? "y" : "n";
            return $"b: {Benchmark}; d: {DistributionName.ToLower()}; n: {NumaStyle}; rumd: {string.Join(',', RumdPercents)}; reviv: {RevivificationLevel}; revivbinrecs: {RevivBinRecordCount};"
                        + $" revivfrac {RevivifiableFraction}; t: {ThreadCount}; i: {IterationCount}; hp: {HashPacking};"
                        + $" sd: {boolStr(UseSmallData)}; sm: {boolStr(UseSmallMemoryLog)}; sy: {boolStr(UseSyntheticData)}; safectx: {boolStr(UseSafeContext)};"
                        + $" chkptms: {PeriodicCheckpointMilliseconds}; chkpttype: {(PeriodicCheckpointMilliseconds > 0 ? PeriodicCheckpointType.ToString() : "None")};"
                        + $" chkptincr: {boolStr(PeriodicCheckpointTryIncremental)}";
        }
    }
}