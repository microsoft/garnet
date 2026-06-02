// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Recall tests using synthetic vector datasets (grid, circle).
    /// Generates vectors → VADD to Garnet → VSIM query every vector →
    /// compare results against brute-force nearest neighbors → assert recall ≥ threshold.
    /// </summary>
    [TestFixture]
    public class DiskANNSyntheticRecallTests : TestBase
    {
        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        [TestCase(100, 1, 1)]
        [TestCase(7, 3, 1)]
        [TestCase(5, 4, 1)]
        public void GridRecall_FP32_NoQuant(int gridSize, int dimensions, int topK)
        {
            var vectors = GenerateGridVectors(dimensions, gridSize);
            RunRecallTest(vectors, dimensions, "NOQUANT", topK,
                $"grid={gridSize} dim={dimensions} FP32 NOQUANT topK={topK}",
                (db, key, vecs, dim, qt) => AddVectors_FP32(db, key, vecs, dim, qt),
                (db, key, vec, k) => VsimQuery_FP32(db, key, vec, dimensions, k),
                SquaredL2Distance_Raw);
        }

        [Test]
        [TestCase(100, 1, 1)]
        [TestCase(7, 3, 1)]
        [TestCase(5, 4, 1)]
        public void GridRecall_XB8_XPREQ8(int gridSize, int dimensions, int topK)
        {
            var vectors = GenerateGridVectorsUInt8(dimensions, gridSize);
            RunRecallTest(vectors, dimensions, "XPREQ8", topK,
                $"grid={gridSize} dim={dimensions} XB8 XPREQ8 topK={topK}",
                (db, key, vecs, dim, qt) => AddVectors_XB8(db, key, vecs, dim, qt),
                (db, key, vec, k) => VsimQuery_XB8(db, key, vec, k),
                (a, b) => (double)SquaredL2Distance_XB8(a, b));
        }

        [Test]
        [TestCase(100, 1.0f, 5)]
        [TestCase(93, 534.0f, 5)]
        [TestCase(101, 0.0f, 5)] // 0 = variousRadii
        public void CircleRecall_FP32_NoQuant(int pointCount, float radius, int topK)
        {
            var vectors = radius != 0.0f
                ? GenerateCircleVectors(pointCount, radius)
                : GenerateVariousRadiiCircleVectors(pointCount);
            RunRecallTest(vectors, 2, "NOQUANT", topK,
                $"circle={pointCount}pt {(radius != 0.0f ? $"r={radius}" : "variousRadii")} FP32 NOQUANT COSINE topK={topK}",
                (db, key, vecs, dim, qt) => AddVectors_FP32(db, key, vecs, dim, qt, "COSINE"),
                (db, key, vec, k) => VsimQuery_FP32(db, key, vec, 2, k),
                CosineDistance);
        }

        private void RunRecallTest<TVec>(
            List<TVec> vectors,
            int dimensions,
            string quantType,
            int topK,
            string tag,
            Func<IDatabase, string, List<TVec>, int, string, List<VectorEntry<TVec>>> addVectors,
            Func<IDatabase, string, TVec, int, HashSet<int>> queryVsim,
            Func<TVec, TVec, double> getDistance,
            double minRecall = 0.99)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = $"recalltest_{tag.Replace(' ', '_')}";

            var entries = addVectors(db, key, vectors, dimensions, quantType);
            var idToVector = entries.ToDictionary(e => e.Id, e => e.Vector);
            var allVectors = entries.Select(e => e.Vector).ToList();
            var k = Math.Min(topK, entries.Count);
            var totalMatchCount = 0;
            var totalExpectedCount = 0;

            foreach (var entry in entries)
            {
                var vsimIds = queryVsim(db, key, entry.Vector, k);
                ClassicAssert.AreEqual(k, vsimIds.Count,
                    $"VSIM should return {k} results for vector {entry.Id}");

                var actualNeighbors = vsimIds.Select(id => idToVector[id]).ToList();
                var expectedNeighbors = BruteForceNearestNeighbors(entry.Vector, allVectors, k, getDistance);
                var expectedDist = CountPerDistance(entry.Vector, expectedNeighbors, getDistance);
                var actualDist = CountPerDistance(entry.Vector, actualNeighbors, getDistance);
                var matchCount = CountDictionaryIntersection(expectedDist, actualDist);

                totalMatchCount += matchCount;
                totalExpectedCount += expectedNeighbors.Count;
            }

            var recall = (double)totalMatchCount / totalExpectedCount;
            ClassicAssert.GreaterOrEqual(recall, minRecall,
                $"Recall {recall:F4} is below threshold {minRecall} for [{tag}]");
        }

        private static List<VectorEntry<float[]>> AddVectors_FP32(IDatabase db, string key, List<float[]> vectors, int dimensions, string quantType, string distanceMetric = null)
        {
            var entries = new List<VectorEntry<float[]>>(vectors.Count);

            // VADD key VALUES <dim> [dim values...] <idBytes> <quantType> EF 200 M 16 [XDISTANCE_METRIC <metric>]
            List<object> baseArgs =
            [
                key, "VALUES", dimensions.ToString(),
            ];

            var dimensionsPos = baseArgs.Count;
            for (var i = 0; i < dimensions; i++)
                baseArgs.Add(null);

            var idBytesPos = baseArgs.Count;
            baseArgs.Add(null);
            baseArgs.Add(quantType);
            baseArgs.Add("EF");
            baseArgs.Add("200");
            baseArgs.Add("M");
            baseArgs.Add("16");

            if (distanceMetric != null)
            {
                baseArgs.Add("XDISTANCE_METRIC");
                baseArgs.Add(distanceMetric);
            }

            var args = baseArgs.ToArray();

            for (var idx = 0; idx < vectors.Count; idx++)
            {
                var vectorId = idx + 1;
                var idStr = $"vec_{vectorId}";
                var vector = vectors[idx];

                for (var d = 0; d < dimensions; d++)
                    args[dimensionsPos + d] = vector[d].ToString("G", CultureInfo.InvariantCulture);

                args[idBytesPos] = idStr;

                var res = db.Execute("VADD", args);
                ClassicAssert.AreEqual(1, (int)res, $"VADD should return 1 for new vector {vectorId}");

                entries.Add(new VectorEntry<float[]>
                {
                    Id = vectorId,
                    IdStr = idStr,
                    Vector = vector,
                });
            }

            return entries;
        }

        private static HashSet<int> VsimQuery_FP32(IDatabase db, string key, float[] queryVector, int dimensions, int count)
        {
            var args = new object[9 + dimensions];
            args[0] = key;
            args[1] = "VALUES";
            args[2] = dimensions.ToString();
            for (var i = 0; i < dimensions; i++)
                args[3 + i] = queryVector[i].ToString("G", CultureInfo.InvariantCulture);
            args[3 + dimensions] = "COUNT";
            args[4 + dimensions] = count.ToString();
            args[5 + dimensions] = "EPSILON";
            args[6 + dimensions] = "1.0";
            args[7 + dimensions] = "EF";
            args[8 + dimensions] = "200";

            var res = db.Execute("VSIM", args);
            return ParseVsimIds((RedisResult[])res);
        }

        private static List<VectorEntry<byte[]>> AddVectors_XB8(IDatabase db, string key, List<byte[]> vectors, int dimensions, string quantType)
        {
            var entries = new List<VectorEntry<byte[]>>(vectors.Count);

            // VADD key XB8 <vectorBytes> <idBytes> <quantType> EF 200 M 16
            var args = new object[]
            {
                key, "XB8", null, null, quantType,
                "EF", "200", "M", "16",
            };

            for (var idx = 0; idx < vectors.Count; idx++)
            {
                var vectorId = idx + 1;
                var idStr = $"vec_{vectorId}";

                args[2] = vectors[idx];
                args[3] = idStr;

                var res = db.Execute("VADD", args);
                ClassicAssert.AreEqual(1, (int)res, $"VADD should return 1 for new vector {vectorId}");

                entries.Add(new VectorEntry<byte[]>
                {
                    Id = vectorId,
                    IdStr = idStr,
                    Vector = vectors[idx],
                });
            }

            return entries;
        }

        private static HashSet<int> VsimQuery_XB8(IDatabase db, string key, byte[] queryVectorBytes, int count)
        {
            // VSIM key XB8 <vectorBytes> COUNT <n> EPSILON 1.0 EF 40
            var args = new object[] { key, "XB8", queryVectorBytes, "COUNT", count.ToString(), "EPSILON", "1.0", "EF", "40" };

            var res = db.Execute("VSIM", args);
            return ParseVsimIds((RedisResult[])res);
        }

        private static List<float[]> GenerateGridVectors(int dimensions, int gridSize)
        {
            var totalVectors = (int)Math.Pow(gridSize, dimensions);
            var vectors = new List<float[]>(totalVectors);

            for (var i = 0; i < totalVectors; i++)
            {
                var vector = new float[dimensions];
                var value = i;
                for (var d = 0; d < dimensions; d++)
                {
                    vector[dimensions - d - 1] = value % gridSize;
                    value /= gridSize;
                }

                vectors.Add(vector);
            }

            return vectors;
        }

        private static List<byte[]> GenerateGridVectorsUInt8(int dimensions, int gridSize)
        {
            var totalVectors = (int)Math.Pow(gridSize, dimensions);
            var vectors = new List<byte[]>(totalVectors);

            for (var i = 0; i < totalVectors; i++)
            {
                var vector = new byte[dimensions];
                var value = i;
                for (var d = 0; d < dimensions; d++)
                {
                    vector[dimensions - d - 1] = (byte)(value % gridSize);
                    value /= gridSize;
                }

                vectors.Add(vector);
            }

            return vectors;
        }

        private static List<float[]> GenerateCircleVectors(int pointCount, float radius)
        {
            var vectors = new List<float[]>(pointCount);

            for (var i = 0; i < pointCount; i++)
            {
                var theta = (float)(2.0 * Math.PI * i / pointCount);
                vectors.Add([MathF.Cos(theta) * radius, MathF.Sin(theta) * radius]);
            }

            return vectors;
        }

        private static List<float[]> GenerateVariousRadiiCircleVectors(int pointCount)
        {
            var vectors = new List<float[]>(pointCount);

            for (var i = 0; i < pointCount; i++)
            {
                var theta = (float)(2.0 * Math.PI * i / pointCount);
                var radius = 1.0f + 2.0f * (i % 7);
                vectors.Add([MathF.Cos(theta) * radius, MathF.Sin(theta) * radius]);
            }

            return vectors;
        }

        private static HashSet<int> ParseVsimIds(RedisResult[] results)
        {
            var ids = new HashSet<int>();
            foreach (var item in results)
            {
                var str = (string)item;
                var id = int.Parse(str.AsSpan(4)); // skip "vec_" prefix
                ids.Add(id);
            }

            return ids;
        }

        private static List<TVec> BruteForceNearestNeighbors<TVec>(TVec targetVector, List<TVec> candidates, int count, Func<TVec, TVec, double> getDistance)
        {
            var pq = new PriorityQueue<TVec, double>();
            foreach (var candidate in candidates)
            {
                pq.Enqueue(candidate, -getDistance(targetVector, candidate));
                if (pq.Count > count)
                    pq.Dequeue();
            }

            var result = new List<TVec>(count);
            while (pq.Count > 0)
                result.Add(pq.Dequeue());
            return result;
        }

        private static int CountDictionaryIntersection(Dictionary<long, int> expected, Dictionary<long, int> actual)
        {
            var intersection = 0;
            foreach (var kvp in expected)
            {
                if (actual.TryGetValue(kvp.Key, out var actualCount))
                    intersection += Math.Min(kvp.Value, actualCount);
            }

            return intersection;
        }

        private static Dictionary<long, int> CountPerDistance<TVec>(TVec targetVector, IEnumerable<TVec> otherVectors, Func<TVec, TVec, double> getDistance)
        {
            var counts = new Dictionary<long, int>();
            foreach (var vec in otherVectors)
            {
                var key = (long)Math.Round(getDistance(targetVector, vec) * 100);
                counts[key] = counts.GetValueOrDefault(key) + 1;
            }

            return counts;
        }

        private static double SquaredL2Distance_Raw(float[] a, float[] b)
        {
            double dist = 0;
            for (var i = 0; i < a.Length; i++)
            {
                double diff = a[i] - b[i];
                dist += diff * diff;
            }

            return dist;
        }

        private static long SquaredL2Distance_XB8(byte[] a, byte[] b)
        {
            long dist = 0;
            for (var i = 0; i < a.Length; i++)
            {
                long diff = a[i] - b[i];
                dist += diff * diff;
            }

            return dist;
        }

        private static double CosineDistance(float[] a, float[] b)
        {
            double dot = 0, normA = 0, normB = 0;
            for (var i = 0; i < a.Length; i++)
            {
                dot += (double)a[i] * b[i];
                normA += (double)a[i] * a[i];
                normB += (double)b[i] * b[i];
            }

            var denom = Math.Sqrt(normA) * Math.Sqrt(normB);
            return denom == 0 ? 1.0 : 1.0 - dot / denom;
        }

        private class VectorEntry<TVec>
        {
            public int Id;
            public string IdStr;
            public TVec Vector;
        }

    }
}