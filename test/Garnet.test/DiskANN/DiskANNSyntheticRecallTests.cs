// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Recall tests using synthetic vector datasets (grid, circle).
    /// Generates vectors → VADD to Garnet → VSIM query every vector →
    /// compare results against brute-force nearest neighbors → assert recall ≥ threshold.
    /// Ported from CosmosDB Backend VectorIndex recall tests.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    public class DiskANNSyntheticRecallTests : AllureTestBase
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

        #region Grid Tests — FP32 + NOQUANT

        [Test]
        [TestCase(100, 1, 1)]
        [TestCase(7, 3, 1)]
        [TestCase(5, 4, 1)]
        public void GridRecall_FP32_NoQuant(int gridSize, int dimensions, int topK)
        {
            var vectors = GenerateGridVectors(dimensions, gridSize);
            RunRecallTest_FP32(vectors, dimensions, "NOQUANT", topK,
                tag: $"grid={gridSize} dim={dimensions} FP32 NOQUANT topK={topK}");
        }

        #endregion

        #region Grid Tests — FP32 + XPREQ8

        [Test]
        [TestCase(100, 1, 1)]
        [TestCase(7, 3, 1)]
        [TestCase(5, 4, 1)]
        public void GridRecall_FP32_XPREQ8(int gridSize, int dimensions, int topK)
        {
            var vectors = GenerateGridVectors(dimensions, gridSize);
            RunRecallTest_FP32(vectors, dimensions, "XPREQ8", topK,
                tag: $"grid={gridSize} dim={dimensions} FP32 XPREQ8 topK={topK}");
        }

        #endregion

        #region Grid Tests — XB8 + XPREQ8

        [Test]
        [TestCase(100, 1, 1)]
        [TestCase(7, 3, 1)]
        [TestCase(5, 4, 1)]
        public void GridRecall_XB8_XPREQ8(int gridSize, int dimensions, int topK)
        {
            var vectors = GenerateGridVectorsUInt8(dimensions, gridSize);
            RunRecallTest_XB8(vectors, dimensions, "XPREQ8", topK,
                tag: $"grid={gridSize} dim={dimensions} XB8 XPREQ8 topK={topK}");
        }

        #endregion

        #region Circle Tests — FP32 + NOQUANT

        [Test]
        [TestCase(100, 1.0f, 5)]
        [TestCase(93, 534.0f, 5)]
        [TestCase(101, 0.0f, 5)] // 0 = variousRadii
        public void CircleRecall_FP32_NoQuant(int pointCount, float radius, int topK)
        {
            var vectors = radius != 0.0f
                ? GenerateCircleVectors(pointCount, radius)
                : GenerateVariousRadiiCircleVectors(pointCount);
            RunRecallTest_FP32(vectors, 2, "NOQUANT", topK,
                tag: $"circle={pointCount}pt {(radius != 0.0f ? $"r={radius}" : "variousRadii")} FP32 NOQUANT topK={topK}");
        }

        #endregion

        #region Circle Tests — FP32 + XPREQ8

        [Test]
        [TestCase(93, 534.0f, 5)]
        [TestCase(100, 1.0f, 5)]
        [TestCase(101, 0.0f, 5)] // 0 = variousRadii
        public void CircleRecall_FP32_XPREQ8(int pointCount, float radius, int topK)
        {
            var vectors = radius != 0.0f
                ? GenerateCircleVectors(pointCount, radius)
                : GenerateVariousRadiiCircleVectors(pointCount);
            RunRecallTest_FP32(vectors, 2, "XPREQ8", topK,
                tag: $"circle={pointCount}pt {(radius != 0.0f ? $"r={radius}" : "variousRadii")} FP32 XPREQ8 topK={topK}");
        }

        #endregion

        #region Recall Test Runners

        private void RunRecallTest_FP32(List<float[]> vectors, int dimensions, string quantType, int topK, string tag, double minRecall = 0.99)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = $"recalltest_{tag.Replace(' ', '_')}";

            var entries = AddVectors_FP32(db, key, vectors, dimensions, quantType);
            var k = Math.Min(topK, entries.Count);

            var totalMatchCount = 0;
            var totalExpectedCount = 0;

            foreach (var entry in entries)
            {
                var vsimIds = VsimQuery_FP32(db, key, entry.Vector, dimensions, k);
                ClassicAssert.AreEqual(k, vsimIds.Count,
                    $"VSIM should return {k} results for vector {entry.Id}");

                var useQuantized = quantType != "NOQUANT";
                var expectedNN = BruteForceNearestNeighbors_FP32(entries, entry.Vector, k, useQuantized);
                var matchCount = CountDistanceBasedIntersection_FP32(
                    entries, entry.Vector, expectedNN, vsimIds, useQuantized);

                totalMatchCount += matchCount;
                totalExpectedCount += expectedNN.Count;
            }

            var recall = (double)totalMatchCount / totalExpectedCount;
            ClassicAssert.GreaterOrEqual(recall, minRecall,
                $"Recall {recall:F4} is below threshold {minRecall} for [{tag}]");
        }

        private void RunRecallTest_XB8(List<byte[]> vectors, int dimensions, string quantType, int topK, string tag, double minRecall = 0.99)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = $"recalltest_{tag.Replace(' ', '_')}";

            var entries = AddVectors_XB8(db, key, vectors, dimensions, quantType);
            var k = Math.Min(topK, entries.Count);

            var totalMatchCount = 0;
            var totalExpectedCount = 0;

            foreach (var entry in entries)
            {
                var vsimIds = VsimQuery_XB8(db, key, entry.VectorBytes, k);
                ClassicAssert.AreEqual(k, vsimIds.Count,
                    $"VSIM should return {k} results for vector {entry.Id}");

                var expectedNN = BruteForceNearestNeighbors_XB8(entries, entry.VectorBytes, k);
                var matchCount = CountDistanceBasedIntersection_XB8(
                    entries, entry.VectorBytes, expectedNN, vsimIds);

                totalMatchCount += matchCount;
                totalExpectedCount += expectedNN.Count;
            }

            var recall = (double)totalMatchCount / totalExpectedCount;
            ClassicAssert.GreaterOrEqual(recall, minRecall,
                $"Recall {recall:F4} is below threshold {minRecall} for [{tag}]");
        }

        #endregion

        #region VADD / VSIM — FP32 (VALUES mode)

        private static List<FP32VectorEntry> AddVectors_FP32(IDatabase db, string key, List<float[]> vectors, int dimensions, string quantType)
        {
            var entries = new List<FP32VectorEntry>(vectors.Count);

            // VADD key VALUES <dim> [dim values...] <idBytes> <quantType> EF 200 M 16
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

            var args = baseArgs.ToArray();

            for (var idx = 0; idx < vectors.Count; idx++)
            {
                var vectorId = idx + 1;
                var idBytes = BitConverter.GetBytes(vectorId);
                var vector = vectors[idx];

                for (var d = 0; d < dimensions; d++)
                    args[dimensionsPos + d] = vector[d].ToString("G");

                args[idBytesPos] = idBytes;

                var res = db.Execute("VADD", args);
                ClassicAssert.AreEqual(1, (int)res, $"VADD should return 1 for new vector {vectorId}");

                entries.Add(new FP32VectorEntry
                {
                    Id = vectorId,
                    IdBytes = idBytes,
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
                args[3 + i] = queryVector[i].ToString("G");
            args[3 + dimensions] = "COUNT";
            args[4 + dimensions] = count.ToString();
            args[5 + dimensions] = "EPSILON";
            args[6 + dimensions] = "1.0";
            args[7 + dimensions] = "EF";
            args[8 + dimensions] = "200";

            var res = db.Execute("VSIM", args);
            return ParseVsimIds((RedisResult[])res);
        }

        #endregion

        #region VADD / VSIM — XB8 (raw byte blob mode)

        private static List<XB8VectorEntry> AddVectors_XB8(IDatabase db, string key, List<byte[]> vectors, int dimensions, string quantType)
        {
            var entries = new List<XB8VectorEntry>(vectors.Count);

            // VADD key XB8 <vectorBytes> <idBytes> <quantType> EF 200 M 16
            var args = new object[]
            {
                key, "XB8", null, null, quantType,
                "EF", "200", "M", "16",
            };

            for (var idx = 0; idx < vectors.Count; idx++)
            {
                var vectorId = idx + 1;
                var idBytes = BitConverter.GetBytes(vectorId);

                args[2] = vectors[idx];
                args[3] = idBytes;

                var res = db.Execute("VADD", args);
                ClassicAssert.AreEqual(1, (int)res, $"VADD should return 1 for new vector {vectorId}");

                entries.Add(new XB8VectorEntry
                {
                    Id = vectorId,
                    IdBytes = idBytes,
                    VectorBytes = vectors[idx],
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

        #endregion

        #region Vector Generators

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

        #endregion

        #region Common Helpers

        private static HashSet<int> ParseVsimIds(RedisResult[] results)
        {
            var ids = new HashSet<int>();
            foreach (var item in results)
            {
                var bytes = (byte[])item;
                var id = BitConverter.ToInt32(bytes, 0);
                ids.Add(id);
            }

            return ids;
        }

        #endregion

        #region Brute-Force Nearest Neighbors — FP32

        private static byte QuantizeToUint8(float v) => (byte)Math.Clamp((int)Math.Floor(v), 0, 255);

        private static byte[] QuantizeVector(float[] vector)
        {
            var result = new byte[vector.Length];
            for (var i = 0; i < vector.Length; i++)
                result[i] = QuantizeToUint8(vector[i]);
            return result;
        }

        private static HashSet<int> BruteForceNearestNeighbors_FP32(List<FP32VectorEntry> entries, float[] queryVector, int count, bool useQuantized)
        {
            if (useQuantized)
            {
                var quantQuery = QuantizeVector(queryVector);
                var pq = new PriorityQueue<int, long>();
                foreach (var entry in entries)
                {
                    var dist = SquaredL2Distance_Quantized(entry.Vector, quantQuery);
                    pq.Enqueue(entry.Id, -dist);
                    if (pq.Count > count)
                        pq.Dequeue();
                }

                var result = new HashSet<int>();
                while (pq.Count > 0)
                    result.Add(pq.Dequeue());
                return result;
            }
            else
            {
                var pq = new PriorityQueue<int, double>();
                foreach (var entry in entries)
                {
                    var dist = SquaredL2Distance_Raw(queryVector, entry.Vector);
                    pq.Enqueue(entry.Id, -dist);
                    if (pq.Count > count)
                        pq.Dequeue();
                }

                var result = new HashSet<int>();
                while (pq.Count > 0)
                    result.Add(pq.Dequeue());
                return result;
            }
        }

        private static int CountDistanceBasedIntersection_FP32(List<FP32VectorEntry> entries, float[] queryVector, HashSet<int> expectedIds, HashSet<int> actualIds, bool useQuantized)
        {
            if (useQuantized)
            {
                var quantQuery = QuantizeVector(queryVector);
                var expectedDistCounts = CountPerDistance_FP32_Quantized(entries, quantQuery, expectedIds);
                var actualDistCounts = CountPerDistance_FP32_Quantized(entries, quantQuery, actualIds);

                var intersection = 0;
                foreach (var kvp in expectedDistCounts)
                {
                    if (actualDistCounts.TryGetValue(kvp.Key, out var actualCount))
                        intersection += Math.Min(kvp.Value, actualCount);
                }

                return intersection;
            }
            else
            {
                var expectedDistCounts = CountPerDistance_FP32_Raw(entries, queryVector, expectedIds);
                var actualDistCounts = CountPerDistance_FP32_Raw(entries, queryVector, actualIds);

                var intersection = 0;
                foreach (var kvp in expectedDistCounts)
                {
                    if (actualDistCounts.TryGetValue(kvp.Key, out var actualCount))
                        intersection += Math.Min(kvp.Value, actualCount);
                }

                return intersection;
            }
        }

        private static Dictionary<long, int> CountPerDistance_FP32_Quantized(List<FP32VectorEntry> entries, byte[] quantQuery, HashSet<int> ids)
        {
            var counts = new Dictionary<long, int>();
            foreach (var entry in entries)
            {
                if (!ids.Contains(entry.Id))
                    continue;
                var dist = SquaredL2Distance_Quantized(entry.Vector, quantQuery);
                if (!counts.ContainsKey(dist))
                    counts[dist] = 0;
                counts[dist]++;
            }

            return counts;
        }

        private static Dictionary<double, int> CountPerDistance_FP32_Raw(List<FP32VectorEntry> entries, float[] queryVector, HashSet<int> ids)
        {
            var counts = new Dictionary<double, int>();
            foreach (var entry in entries)
            {
                if (!ids.Contains(entry.Id))
                    continue;
                var dist = SquaredL2Distance_Raw(queryVector, entry.Vector);
                if (!counts.ContainsKey(dist))
                    counts[dist] = 0;
                counts[dist]++;
            }

            return counts;
        }

        private static long SquaredL2Distance_Quantized(float[] a, byte[] quantB)
        {
            long dist = 0;
            for (var i = 0; i < a.Length; i++)
            {
                long qa = QuantizeToUint8(a[i]);
                long diff = qa - quantB[i];
                dist += diff * diff;
            }

            return dist;
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

        #endregion

        #region Brute-Force Nearest Neighbors — XB8

        private static HashSet<int> BruteForceNearestNeighbors_XB8(List<XB8VectorEntry> entries, byte[] queryVector, int count)
        {
            var pq = new PriorityQueue<int, long>();
            foreach (var entry in entries)
            {
                var dist = SquaredL2Distance_XB8(entry.VectorBytes, queryVector);
                pq.Enqueue(entry.Id, -dist);
                if (pq.Count > count)
                    pq.Dequeue();
            }

            var result = new HashSet<int>();
            while (pq.Count > 0)
                result.Add(pq.Dequeue());

            return result;
        }

        private static int CountDistanceBasedIntersection_XB8(List<XB8VectorEntry> entries, byte[] queryVector, HashSet<int> expectedIds, HashSet<int> actualIds)
        {
            var expectedDistCounts = CountPerDistance_XB8(entries, queryVector, expectedIds);
            var actualDistCounts = CountPerDistance_XB8(entries, queryVector, actualIds);

            var intersection = 0;
            foreach (var kvp in expectedDistCounts)
            {
                if (actualDistCounts.TryGetValue(kvp.Key, out var actualCount))
                    intersection += Math.Min(kvp.Value, actualCount);
            }

            return intersection;
        }

        private static Dictionary<long, int> CountPerDistance_XB8(List<XB8VectorEntry> entries, byte[] queryVector, HashSet<int> ids)
        {
            var counts = new Dictionary<long, int>();
            foreach (var entry in entries)
            {
                if (!ids.Contains(entry.Id))
                    continue;
                var dist = SquaredL2Distance_XB8(entry.VectorBytes, queryVector);
                if (!counts.ContainsKey(dist))
                    counts[dist] = 0;
                counts[dist]++;
            }

            return counts;
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

        #endregion

        #region Inner Types

        private class FP32VectorEntry
        {
            public int Id;
            public byte[] IdBytes;
            public float[] Vector;
        }

        private class XB8VectorEntry
        {
            public int Id;
            public byte[] IdBytes;
            public byte[] VectorBytes;
        }

        #endregion
    }
}
