// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using CommandLine;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
#pragma warning disable IDE0065 // Misplaced using directive
    using FixedLenStoreFunctions = StoreFunctions<FixedLengthKey.Comparer, SpanByteRecordDisposer>;
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>;

    internal interface IKeySetter<TKey>
    {
        void Set(TKey[] vector, long idx, long value);
    }

    class TestLoader
    {
        internal readonly Options Options;
        internal readonly string Distribution;
        internal FixedLengthKey[] init_keys = default;
        internal FixedLengthKey[] txn_keys = default;
        internal KeySpanByte[] init_span_keys = default;
        internal KeySpanByte[] txn_span_keys = default;

        internal readonly BenchmarkType BenchmarkType;
        internal readonly long InitCount;
        internal readonly long TxnCount;
        internal readonly int MaxKey;
        internal readonly bool RecoverMode;
        internal readonly bool error;

        // RUMD percentages. Delete is not a percent; it will fire automatically if the others do not sum to 100, saving an 'if'.
        internal readonly int ReadPercent, UpsertPercent, RmwPercent;

        internal TestLoader(string[] args)
        {
            error = true;
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed)
                return;

            Options = result.MapResult(o => o, xs => new Options());

            static bool verifyOption(bool isValid, string name, string info = null)
            {
                if (!isValid)
                    Console.WriteLine($"Invalid {name} argument" + (string.IsNullOrEmpty(info) ? string.Empty : $": {info}"));
                return isValid;
            }

            BenchmarkType = (BenchmarkType)Options.Benchmark;
            if (!verifyOption(Enum.IsDefined(typeof(BenchmarkType), BenchmarkType), "Benchmark"))
                return;

            if (!verifyOption(Options.NumaStyle >= 0 && Options.NumaStyle <= 1, "NumaStyle"))
                return;

            if (!verifyOption(Options.IterationCount > 0, "Iteration Count"))
                return;

            if (!verifyOption(Options.HashPacking > 0, "Hash Packing"))
                return;

            Distribution = Options.DistributionName.ToLower();
            if (!verifyOption(Distribution == YcsbConstants.UniformDist || Distribution == YcsbConstants.ZipfDist, "Distribution"))
                return;

            if (!verifyOption(Options.RunSeconds >= 0, "RunSeconds"))
                return;

            var rumdPercents = Options.RumdPercents.ToArray();  // Will be non-null because we specified a default
            if (!verifyOption(rumdPercents.Length == 4 && Options.RumdPercents.Sum() == 100 && !Options.RumdPercents.Any(x => x < 0), "rmud",
                    "Percentages of [(r)eads,(u)pserts,r(m)ws,(d)eletes] must be empty or must sum to 100 with no negative elements"))
                return;
            if (Options.UseOverflowValues && Options.UseObjectValues)
            {
                Console.WriteLine($"Cannot specify both UseOverflowValues and UseObjectValues");
                return;
            }
            if ((Options.UseOverflowValues || Options.UseObjectValues) && BenchmarkType != BenchmarkType.Object)
            {
                Console.WriteLine($"Can only specify UseOverflowValues or UseObjectValues with BenchmarkType.Object");
                return;
            }
            if (Options.UseSBA && BenchmarkType == BenchmarkType.Object)
            {
                Console.WriteLine($"SpanByteAllocator is not supported with BenchmarkType.Object");
                return;
            }
            ReadPercent = rumdPercents[0];
            UpsertPercent = ReadPercent + rumdPercents[1];
            RmwPercent = UpsertPercent + rumdPercents[2];

            // This is the max possible count for zipf smalldata
            InitCount = Options.UseSmallData ? 4599680 : 250000000;
            TxnCount = Options.UseSmallData ? 10000000 : 1000000000;
            MaxKey = Options.UseSmallData ? 1 << 22 : 1 << 28;
            RecoverMode = Options.BackupAndRestore && Options.PeriodicCheckpointMilliseconds <= 0;

            error = false;
        }

        internal long GetHashTableSize() => (long)(MaxKey / Options.HashPacking) << 6;  // << 6 for consistency with pre-StoreFunctions (because it will be converted to cache lines)

        internal void LoadData()
        {
            Thread worker = new(LoadDataThreadProc);
            worker.Start();
            worker.Join();
        }

        private void LoadDataThreadProc()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                Native32.AffinitizeThreadShardedNuma(0, 2);

            switch (BenchmarkType)
            {
                case BenchmarkType.FixedLen:
                    if (Options.UseSBA)
                        FixedLenYcsbBenchmark<SpanByteAllocator<FixedLenStoreFunctions>>.CreateKeyVectors(this, out init_keys, out txn_keys);
                    else
                        FixedLenYcsbBenchmark<ObjectAllocator<FixedLenStoreFunctions>>.CreateKeyVectors(this, out init_keys, out txn_keys);
                    LoadData(this, init_keys, txn_keys, new FixedLenYcsbKeySetter());
                    break;
                case BenchmarkType.SpanByte:
                    if (Options.UseSBA)
                        SpanByteYcsbBenchmark<SpanByteAllocator<SpanByteStoreFunctions>>.CreateKeyVectors(this, out init_span_keys, out txn_span_keys);
                    else
                        SpanByteYcsbBenchmark<ObjectAllocator<SpanByteStoreFunctions>>.CreateKeyVectors(this, out init_span_keys, out txn_span_keys);
                    LoadData(this, init_span_keys, txn_span_keys, new SpanByteYcsbKeySetter());
                    break;
                case BenchmarkType.Object:
                    ObjectYcsbBenchmark.CreateKeyVectors(this, out init_keys, out txn_keys);
                    LoadData(this, init_keys, txn_keys, new ObjectYcsbBenchmark.KeySetter());
                    break;
                case BenchmarkType.ConcurrentDictionary:
                    ConcurrentDictionary_YcsbBenchmark.CreateKeyVectors(this, out init_keys, out txn_keys);
                    LoadData(this, init_keys, txn_keys, new ConcurrentDictionary_YcsbBenchmark.KeySetter());
                    break;
                default:
                    throw new ApplicationException("Unknown benchmark type");
            }
        }

        private void LoadData<TKey, TKeySetter>(TestLoader testLoader, TKey[] init_keys, TKey[] txn_keys, TKeySetter keySetter)
            where TKeySetter : IKeySetter<TKey>
        {
            if (testLoader.Options.UseSyntheticData)
            {
                LoadSyntheticData(testLoader.Distribution, (uint)testLoader.Options.RandomSeed, init_keys, txn_keys, keySetter);
                return;
            }

            string filePath = "C:/ycsb_files";

            if (!Directory.Exists(filePath))
            {
                filePath = "D:/ycsb_files";
            }
            if (!Directory.Exists(filePath))
            {
                filePath = "E:/ycsb_files";
            }

            if (Directory.Exists(filePath))
            {
                LoadDataFromFile(filePath, testLoader.Distribution, init_keys, txn_keys, keySetter);
            }
            else
            {
                Console.WriteLine("WARNING: Could not find YCSB directory, loading synthetic data instead");
                LoadSyntheticData(testLoader.Distribution, (uint)testLoader.Options.RandomSeed, init_keys, txn_keys, keySetter);
            }
        }

        private unsafe void LoadDataFromFile<TKey, TKeySetter>(string filePath, string distribution, TKey[] init_keys, TKey[] txn_keys, TKeySetter keySetter)
            where TKeySetter : IKeySetter<TKey>
        {
            string init_filename = filePath + "/load_" + distribution + "_250M_raw.dat";
            string txn_filename = filePath + "/run_" + distribution + "_250M_1000M_raw.dat";

            var sw = Stopwatch.StartNew();

            if (Options.UseSmallData)
            {
                Console.WriteLine($"loading subset of keys and txns from {txn_filename} into memory...");
                using FileStream stream = File.Open(txn_filename, FileMode.Open, FileAccess.Read, FileShare.Read);

                var initKeySet = new HashSet<long>(init_keys.Length);
                long[] initKeyArray = null;

                long txn_count = 0;
                long offset = 0;
                RandomGenerator rng = new((uint)Options.RandomSeed);

                byte[] chunk = new byte[YcsbConstants.kFileChunkSize];
                fixed (byte* chunk_ptr = chunk)
                {
                    while (true)
                    {
                        stream.Position = offset;
                        int size = stream.Read(chunk, 0, YcsbConstants.kFileChunkSize);
                        for (int idx = 0; idx < size && txn_count < txn_keys.Length; idx += 8)
                        {
                            var key = *(long*)(chunk_ptr + idx);
                            if (!initKeySet.Contains(key))
                            {
                                if (initKeySet.Count >= init_keys.Length)
                                {
                                    // Zipf txn has a high hit rate in the init array, so we'll fill up the small-txn count by just skipping out-of-range txn keys.
                                    if (distribution == YcsbConstants.ZipfDist)
                                        continue;

                                    // Uniform txn at current small-data counts has about a 1% hit rate in the init array, too low to fill the small-txn count,
                                    // so convert the init_key set to an array for random indexing get a random key from init_keys.
                                    initKeyArray ??= [.. initKeySet];
                                    key = initKeyArray[rng.Generate((uint)initKeySet.Count)];
                                }
                                else
                                {
                                    keySetter.Set(init_keys, initKeySet.Count, key);
                                    _ = initKeySet.Add(key);
                                }
                            }
                            keySetter.Set(txn_keys, txn_count, key);
                            ++txn_count;
                        }
                        if (size == YcsbConstants.kFileChunkSize)
                            offset += YcsbConstants.kFileChunkSize;
                        else
                            break;

                        if (txn_count == txn_keys.Length)
                            break;
                    }
                }

                sw.Stop();

                if (initKeySet.Count != init_keys.Length)
                    throw new InvalidDataException($"Init file subset load fail! Expected {init_keys.Length} keys; found {initKeySet.Count}");
                if (txn_count != txn_keys.Length)
                    throw new InvalidDataException($"Txn file subset load fail! Expected {txn_keys.Length} keys; found {txn_count}");

                Console.WriteLine($"loaded {init_keys.Length:N0} keys and {txn_keys.Length:N0} txns in {(double)sw.ElapsedMilliseconds / 1000:N3} seconds");
                return;
            }

            Console.WriteLine($"loading all keys from {init_filename} into memory...");
            long count = 0;

            using (FileStream stream = File.Open(init_filename, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                long offset = 0;

                byte[] chunk = new byte[YcsbConstants.kFileChunkSize];
                fixed (byte* chunk_ptr = chunk)
                {
                    while (true)
                    {
                        stream.Position = offset;
                        int size = stream.Read(chunk, 0, YcsbConstants.kFileChunkSize);
                        for (int idx = 0; idx < size; idx += 8)
                        {
                            keySetter.Set(init_keys, count, *(long*)(chunk_ptr + idx));
                            ++count;
                            if (count == init_keys.Length)
                                break;
                        }
                        if (size == YcsbConstants.kFileChunkSize)
                            offset += YcsbConstants.kFileChunkSize;
                        else
                            break;

                        if (count == init_keys.Length)
                            break;
                    }
                }

                if (count != init_keys.Length)
                    throw new InvalidDataException($"Init file load fail! Expected {init_keys.Length} keys; found {count}");
            }

            sw.Stop();
            Console.WriteLine($"loaded {init_keys.Length:N0} keys in {(double)sw.ElapsedMilliseconds / 1000:N3} seconds");

            Console.WriteLine($"loading all txns from {txn_filename} into memory...");
            sw.Restart();

            using (FileStream stream = File.Open(txn_filename, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                count = 0;
                long offset = 0;

                byte[] chunk = new byte[YcsbConstants.kFileChunkSize];
                fixed (byte* chunk_ptr = chunk)
                {
                    while (true)
                    {
                        stream.Position = offset;
                        int size = stream.Read(chunk, 0, YcsbConstants.kFileChunkSize);
                        for (int idx = 0; idx < size; idx += 8)
                        {
                            keySetter.Set(txn_keys, count, *(long*)(chunk_ptr + idx));
                            ++count;
                            if (count == txn_keys.Length)
                                break;
                        }
                        if (size == YcsbConstants.kFileChunkSize)
                            offset += YcsbConstants.kFileChunkSize;
                        else
                            break;

                        if (count == txn_keys.Length)
                            break;
                    }
                }

                if (count != txn_keys.Length)
                    throw new InvalidDataException($"Txn file load fail! Expected {txn_keys.Length} keys; found {count}");
            }

            sw.Stop();
            Console.WriteLine($"loaded {txn_keys.Length:N0} txns in {(double)sw.ElapsedMilliseconds / 1000:N3} seconds");
        }

        private static void LoadSyntheticData<TKey, TKeySetter>(string distribution, uint seed, TKey[] init_keys, TKey[] txn_keys, TKeySetter keySetter)
            where TKeySetter : IKeySetter<TKey>
        {
            Console.WriteLine($"Loading synthetic data ({distribution} distribution {(distribution == "zipf" ? "theta " + YcsbConstants.SyntheticZipfTheta : "")}), seed = {seed}");
            var sw = Stopwatch.StartNew();

            long val = 0;
            for (int idx = 0; idx < init_keys.Length; idx++)
            {
                keySetter.Set(init_keys, idx, val++);
            }

            sw.Stop();
            Console.WriteLine($"loaded {init_keys.Length:N0} keys in {(double)sw.ElapsedMilliseconds / 1000:N3} seconds");

            RandomGenerator generator = new(seed);
            ZipfGenerator zipf = new(generator, (int)init_keys.Length, theta: YcsbConstants.SyntheticZipfTheta);

            sw.Restart();
            for (int idx = 0; idx < txn_keys.Length; idx++)
            {
                var rand = distribution == YcsbConstants.UniformDist ? (long)generator.Generate64((ulong)init_keys.Length) : zipf.Next();
                keySetter.Set(txn_keys, idx, rand);
            }

            sw.Stop();
            Console.WriteLine($"loaded {txn_keys.Length:N0} txns in {(double)sw.ElapsedMilliseconds / 1000:N3} seconds");
        }

        internal const string DataPath = "D:/data/TsavoriteYcsbBenchmark";

        internal static string DevicePath => $"{DataPath}/hlog";

        internal string BackupPath => $"{DataPath}/{Distribution}_{(Options.UseSyntheticData ? "synthetic" : "ycsb")}_{(Options.UseSmallData ? "2.5M_10M" : "250M_1000M")}";

        internal bool MaybeRecoverStore<SF, A>(TsavoriteKV<SF, A> store)
            where SF : IStoreFunctions
            where A : IAllocator<SF>
        {
            // Recover database for fast benchmark repeat runs.
            if (RecoverMode)
            {
                if (Options.UseSmallData)
                {
                    Console.WriteLine("Skipping Recover() for kSmallData");
                    return false;
                }

                Console.WriteLine($"Recovering TsavoriteKV from {BackupPath} for fast restart");
                try
                {
                    var sw = Stopwatch.StartNew();
                    store.Recover();
                    sw.Stop();
                    Console.WriteLine($"  Completed recovery in {(double)sw.ElapsedMilliseconds / 1000:N3} seconds");
                    return true;
                }
                catch (Exception ex)
                {
                    var suffix = Directory.Exists(BackupPath) ? "" : " (directory does not exist)";
                    Console.WriteLine($"Unable to recover prior store: {ex.Message}{suffix}");
                }
            }
            return false;
        }

        internal void MaybeCheckpointStore<SF, A>(TsavoriteKV<SF, A> store)
            where SF : IStoreFunctions
            where A : IAllocator<SF>
        {
            // Checkpoint database for fast benchmark repeat runs.
            if (RecoverMode)
            {
                Console.WriteLine($"Checkpointing TsavoriteKV to {BackupPath} for fast restart");
                var sw = Stopwatch.StartNew();
                store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot);
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
                sw.Stop();
                Console.WriteLine($"  Completed checkpoint in {(double)sw.ElapsedMilliseconds / 1000:N3} seconds");
            }
        }
    }
}