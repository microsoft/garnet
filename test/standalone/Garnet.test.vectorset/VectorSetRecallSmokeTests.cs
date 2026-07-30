// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Recall-oriented smoke tests that stress a DiskANN Vector Set graph across the physical
    /// storage configurations we care about:
    ///   * quantization mode (NOQUANT / Q8 / BIN),
    ///   * larger-than-memory (records spill to the storage tier during load, or are evicted to disk),
    ///   * read-cache vs copy-reads-to-tail vs neither,
    ///   * save -> restart -> recover (optionally recovering into a much smaller log).
    ///
    /// The invariant under test is <b>physical robustness</b>: a graph that answers queries well while
    /// resident in memory must keep answering them well once the same records are served from disk, or
    /// recovered from a checkpoint. We build a small, well-clustered graph, measure recall against a
    /// brute-force ground truth, apply a physical stressor, and assert recall does not collapse.
    ///
    /// Everything is single-threaded (one connection, sequential VADD) and deterministic (fixed seed,
    /// fixed graph parameters) so the tests are stable and fast.
    ///
    /// NOTE: the Q8 (8-bit scalar) cases that read from disk (FlushAndEvict, larger-than-memory load,
    /// and recover) used to fail against diskann-garnet 4.0.0/4.0.1 because the per-dimension Q8
    /// quantization table was native in-memory-only state that was lost when the index is recreated
    /// after eviction/recovery, so codes were decoded with a missing table and recall collapsed
    /// (~1.0 -> ~0.02-0.11). This was fixed in diskann-garnet 4.0.2; the Q8 disk cases now pass and
    /// stand as regression guards. NOQUANT and BIN (table-free) were always robust.
    /// </summary>
    [TestFixture]
    public class VectorSetRecallSmokeTests : TestBase
    {
        // Small graph so the tests finish quickly while still exercising the disk paths.
        private const int Dim = 32;
        private const int Clusters = 16;
        private const int EfBuild = 64;
        private const int EfSearch = 64;
        private const int M = 16;
        private const int K = 10;
        private const int NumQueries = 40;
        private const int Seed = 2026_07_28;
        private const string Metric = "COSINE";

        // Sanity floor: an in-memory graph with these parameters recalls ~1.0 on this clustered data,
        // so 0.80 is a comfortable margin that still fails hard if the graph is broken.
        private const double MinInMemoryRecall = 0.80;

        // Robustness budget: a physical-config change (evict-to-disk / recover) may only cost this much
        // recall. NOQUANT/BIN/Q8 all cost ~0.0 on 4.0.2; the pre-4.0.2 Q8 disk bug cost ~0.9 and blew
        // through this budget.
        private const double MaxRecallDrop = 0.20;

        private global::Garnet.GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            try { server?.Dispose(); } catch { /* best effort */ }
            server = null;
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Build a graph fully in memory, then flush+evict the main store to disk and re-query.
        /// Recall must survive being served from disk, under each read-back mode (none / read cache /
        /// copy-reads-to-tail). This is the core "larger than memory" served-from-disk guard.
        /// </summary>
        [Test]
        public void RecallSurvivesFlushAndEvict(
            [Values("NOQUANT", "Q8", "BIN")] string quant,
            [Values("none", "readcache", "copytotail")] string readMode)
        {
            var enableReadCache = readMode == "readcache";
            var copyReadsToTail = readMode == "copytotail";

            server = TestUtils.CreateGarnetServer(
                TestUtils.MethodTestDir,
                memorySize: "16m",
                pageSize: "1m",
                enableVectorSetPreview: true,
                enableReadCache: enableReadCache,
                copyReadsToTail: copyReadsToTail);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            var data = GenerateData(600);
            var key = $"evict_{quant}_{readMode}";
            LoadVectorSet(db, key, data, quant);

            var inMemory = MeasureRecall(db, key, data);
            ClassicAssert.GreaterOrEqual(inMemory, MinInMemoryRecall,
                $"{quant}: in-memory recall {inMemory:F3} is below the sanity floor — the graph is not usable even in memory");

            // Force the main store's hybrid log to disk; subsequent reads are served from the storage tier.
            _ = db.Execute("DEBUG", "FLUSHANDEVICT");
            AssertEvictedToDisk(redis, "records should have been evicted to disk by FLUSHANDEVICT");

            var onDisk = MeasureRecall(db, key, data);
            ClassicAssert.GreaterOrEqual(onDisk, inMemory - MaxRecallDrop,
                $"{quant} [{readMode}]: recall collapsed after eviction to disk " +
                $"(in-memory {inMemory:F3} -> on-disk {onDisk:F3}). For Q8, a regression here is the " +
                "quantization-table-lost-on-recreate bug fixed in diskann-garnet 4.0.2.");
        }

        /// <summary>
        /// Build a graph in a log far smaller than the data so records spill to the storage tier
        /// <b>during</b> construction — the graph is both built and served larger-than-memory, and graph
        /// construction reads earlier records back via pending disk IO. Recall must still be usable.
        /// </summary>
        [Test]
        public void RecallSurvivesLargerThanMemoryLoad(
            [Values("NOQUANT", "Q8", "BIN")] string quant)
        {
            // A 256 KB log with 32 KB pages is much smaller than the ~800-node graph, so inserts spill
            // and construction pages earlier records back from disk. (The 4 KB-page lowMemory helper also
            // forces spill but is an order of magnitude slower here.)
            server = TestUtils.CreateGarnetServer(
                TestUtils.MethodTestDir,
                memorySize: "256k",
                pageSize: "32k",
                enableVectorSetPreview: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            var data = GenerateData(800);
            var key = $"largerthanmem_{quant}";
            LoadVectorSet(db, key, data, quant);

            AssertEvictedToDisk(redis, "the graph should exceed the log and spill to disk during load");

            var onDisk = MeasureRecall(db, key, data);
            ClassicAssert.GreaterOrEqual(onDisk, MinInMemoryRecall,
                $"{quant}: recall {onDisk:F3} is broken for a graph built and served larger-than-memory. " +
                "For Q8, a regression here is the quantization-table-lost-on-recreate bug fixed in diskann-garnet 4.0.2.");
        }

        /// <summary>
        /// Build a graph, checkpoint it, restart the server and recover. Recall must survive the round
        /// trip. When <paramref name="recoverIntoSmallerLog"/> is set, the graph is recovered into a much
        /// smaller log (same, valid page geometry) so its records no longer fit in memory and are served
        /// from disk — recovering onto a "smaller box" must still work correctly.
        /// </summary>
        [Test]
        public async Task RecallSurvivesSaveAndRecover(
            [Values("NOQUANT", "Q8", "BIN")] string quant,
            [Values(false, true)] bool recoverIntoSmallerLog)
        {
            // Vector sets require PageSize >= 16K; keep it fixed across build + recover (checkpoints are
            // page-size sensitive), and only shrink the log *memory* on recover.
            server = TestUtils.CreateGarnetServer(
                TestUtils.MethodTestDir,
                memorySize: "8m",
                pageSize: "16k",
                enableAOF: true,
                aofMemorySize: "2g",
                enableVectorSetPreview: true);
            server.Start();

            var data = GenerateData(500);
            var key = $"recover_{quant}_{recoverIntoSmallerLog}";

            double before;
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                LoadVectorSet(db, key, data, quant);

                before = MeasureRecall(db, key, data);
                ClassicAssert.GreaterOrEqual(before, MinInMemoryRecall,
                    $"{quant}: in-memory recall {before:F3} is below the sanity floor before checkpoint");

                // Take a foreground checkpoint and wait for it to be durable.
#pragma warning disable CS0618 // ForegroundSave is obsolete but is what the recovery tests use
                redis.GetServers()[0].Save(SaveType.ForegroundSave);
#pragma warning restore CS0618
                var committed = await server.Store.WaitForCommitAsync();
                ClassicAssert.IsTrue(committed, "checkpoint commit did not complete");
            }

            // Restart and recover. Optionally recover into a much smaller log (same 16K pages) so the
            // recovered graph spills to disk — "recover onto a smaller box".
            server.Dispose(deleteDir: false);
            server = TestUtils.CreateGarnetServer(
                TestUtils.MethodTestDir,
                memorySize: recoverIntoSmallerLog ? "64k" : "8m",
                pageSize: "16k",
                enableAOF: true,
                aofMemorySize: "2g",
                tryRecover: true,
                enableVectorSetPreview: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                if (recoverIntoSmallerLog)
                    AssertEvictedToDisk(redis, "recovering into a smaller log should leave records on disk");

                var after = MeasureRecall(db, key, data);
                ClassicAssert.GreaterOrEqual(after, before - MaxRecallDrop,
                    $"{quant} [recoverIntoSmallerLog={recoverIntoSmallerLog}]: recall collapsed after save+restart+recover " +
                    $"(before {before:F3} -> after {after:F3}). For Q8, a regression here is the " +
                    "quantization-table-lost-on-recreate bug fixed in diskann-garnet 4.0.2.");
            }
        }

        // ---- helpers ------------------------------------------------------------------------------

        /// <summary>Deterministic, well-separated clustered data so nearest neighbours are stable.</summary>
        private sealed class Dataset
        {
            public float[][] Vectors;   // inserted vectors
            public byte[][] Ids;        // 4-byte little-endian element ids (== index)
            public float[][] Queries;   // held-out query vectors from the same clusters
        }

        private static Dataset GenerateData(int count)
        {
            var rng = new Random(Seed);

            var centers = new float[Clusters][];
            for (var c = 0; c < Clusters; c++)
                centers[c] = Normalized(RandomGaussianVector(rng));

            var vectors = new float[count][];
            var ids = new byte[count][];
            for (var i = 0; i < count; i++)
            {
                vectors[i] = Normalized(Jitter(centers[rng.Next(Clusters)], rng));
                var id = new byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(id, i);
                ids[i] = id;
            }

            var queries = new float[NumQueries][];
            for (var q = 0; q < NumQueries; q++)
                queries[q] = Normalized(Jitter(centers[rng.Next(Clusters)], rng));

            return new Dataset { Vectors = vectors, Ids = ids, Queries = queries };
        }

        private static float[] RandomGaussianVector(Random rng)
        {
            var v = new float[Dim];
            for (var d = 0; d < Dim; d++)
                v[d] = NextGaussian(rng);
            return v;
        }

        private static float[] Jitter(float[] center, Random rng)
        {
            var v = new float[Dim];
            for (var d = 0; d < Dim; d++)
                v[d] = center[d] + (0.1f * NextGaussian(rng));
            return v;
        }

        private static float NextGaussian(Random rng)
        {
            // Box-Muller transform.
            var u1 = 1.0 - rng.NextDouble();
            var u2 = 1.0 - rng.NextDouble();
            return (float)(Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2));
        }

        private static float[] Normalized(float[] v)
        {
            double norm = 0;
            for (var d = 0; d < v.Length; d++)
                norm += (double)v[d] * v[d];
            norm = Math.Sqrt(norm);
            if (norm == 0)
                return v;
            for (var d = 0; d < v.Length; d++)
                v[d] = (float)(v[d] / norm);
            return v;
        }

        private static void LoadVectorSet(IDatabase db, string key, Dataset data, string quant)
        {
            for (var i = 0; i < data.Vectors.Length; i++)
            {
                var vecBytes = MemoryMarshal.Cast<float, byte>(data.Vectors[i].AsSpan()).ToArray();
                var res = db.Execute("VADD",
                    [key, "FP32", vecBytes, data.Ids[i], quant, "EF", EfBuild.ToString(), "M", M.ToString(), "XDISTANCE_METRIC", Metric]);
                ClassicAssert.AreEqual(1, (int)res, $"VADD #{i} should succeed");
            }
        }

        /// <summary>Mean recall@K of VSIM against a brute-force cosine ground truth.</summary>
        private static double MeasureRecall(IDatabase db, string key, Dataset data)
        {
            double total = 0;
            for (var q = 0; q < data.Queries.Length; q++)
            {
                var groundTruth = BruteForceTopK(data.Vectors, data.Queries[q]);

                var queryBytes = MemoryMarshal.Cast<float, byte>(data.Queries[q].AsSpan()).ToArray();
                var res = (byte[][])db.Execute("VSIM",
                    [key, "FP32", queryBytes, "COUNT", K.ToString(), "EF", EfSearch.ToString()]);

                var returned = new HashSet<int>();
                foreach (var idBytes in res)
                    returned.Add(BinaryPrimitives.ReadInt32LittleEndian(idBytes));

                var hits = groundTruth.Count(returned.Contains);
                total += (double)hits / K;
            }
            return total / data.Queries.Length;
        }

        private static int[] BruteForceTopK(float[][] vectors, float[] query)
        {
            var scored = new (float sim, int idx)[vectors.Length];
            for (var i = 0; i < vectors.Length; i++)
                scored[i] = (Dot(vectors[i], query), i);

            // Descending cosine similarity (vectors are normalized, so dot == cosine); nearest first.
            Array.Sort(scored, static (a, b) => b.sim.CompareTo(a.sim));

            var top = new int[K];
            for (var i = 0; i < K; i++)
                top[i] = scored[i].idx;
            return top;
        }

        private static float Dot(float[] a, float[] b)
        {
            float sum = 0;
            for (var d = 0; d < a.Length; d++)
                sum += a[d] * b[d];
            return sum;
        }

        private static void AssertEvictedToDisk(ConnectionMultiplexer redis, string message)
        {
            var info = TestUtils.GetStoreAddressInfo(redis.GetServers()[0]);
            ClassicAssert.Greater(info.HeadAddress, info.BeginAddress, message);
        }
    }
}