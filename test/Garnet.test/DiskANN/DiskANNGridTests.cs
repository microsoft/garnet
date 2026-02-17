// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class DiskANNGridTests : AllureTestBase
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
        [TestCase(100, 1, VectorQuantType.XPreQ8)]
        [TestCase(10, 2, VectorQuantType.XPreQ8)]
        [TestCase(3, 7, VectorQuantType.XPreQ8)]
        [TestCase(4, 5, VectorQuantType.XPreQ8)]
        public void SearchVectorsInGrid(int gridSize, int dimension, VectorQuantType quantType)
        {
            string quantTypeStr = quantType switch
            {
                VectorQuantType.NoQuant => "NOQUANT",
                VectorQuantType.Bin => "BIN",
                VectorQuantType.Q8 => "Q8",
                VectorQuantType.XPreQ8 => "XPREQ8",
                _ => throw new ArgumentException("Invalid quant type")
            };

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = $"gridset_{gridSize}_{dimension}_{quantTypeStr}";
            var addedGridVectors = AddGridVectors(db, key, dimension, gridSize, quantTypeStr);

            // VSIM using existing elements
            var vsimArgs = new object[5];
            vsimArgs[0] = key;
            vsimArgs[1] = "ELE";
            vsimArgs[3] = "COUNT";
            vsimArgs[4] = "25";
            foreach (var addedGridVector in addedGridVectors.Values)
            {
                vsimArgs[2] = addedGridVector.IdBytes;
                var res = db.Execute("VSIM", vsimArgs);
                ClassicAssert.AreEqual(25, res.Length);
                var vsimIds = GetVectorIdsForVsimResults((RedisResult[])res);
                var expectedNN = BruteForceNearestNeighbors(addedGridVectors, addedGridVector.Vector, 25);
                var intersectionCount = CalculateDistanceCountsIntersection(addedGridVectors, addedGridVector.Vector, expectedNN, vsimIds);
                var recall = (double)intersectionCount / expectedNN.Count;
                ClassicAssert.GreaterOrEqual(recall, 0.99, $"Recall too low: {recall} for vector ID {addedGridVector.Id}");
            }

            // VSIM using values
            var vsimValuesArgs = new object[5 + dimension];
            vsimValuesArgs[0] = key;
            vsimValuesArgs[1] = "VALUES";
            vsimValuesArgs[2] = dimension.ToString();
            var valuesPos = 3;
            vsimValuesArgs[3 + dimension] = "COUNT";
            vsimValuesArgs[4 + dimension] = "25";
            foreach (var addedGridVector in addedGridVectors.Values)
            {
                for (var i = 0; i < dimension; i++)
                {
                    vsimValuesArgs[valuesPos + i] = addedGridVector.VectorStringValues[i];
                }

                var res = db.Execute("VSIM", vsimValuesArgs);
                ClassicAssert.AreEqual(25, res.Length);
                var vsimIds = GetVectorIdsForVsimResults((RedisResult[])res);
                var expectedNN = BruteForceNearestNeighbors(addedGridVectors, addedGridVector.Vector, 25);
                var intersectionCount = CalculateDistanceCountsIntersection(addedGridVectors, addedGridVector.Vector, expectedNN, vsimIds);
                var recall = (double)intersectionCount / expectedNN.Count;
                ClassicAssert.GreaterOrEqual(recall, 0.99, $"Recall too low: {recall} for vector ID {addedGridVector.Id}");
            }
        }

        private static List<GridVector> GenerateGridVectors(int dimensions, int gridSize)
        {
            List<GridVector> vectors = [];
            var totalVectors = (int)Math.Pow(gridSize, dimensions);
            for (var i = 0; i < totalVectors; i++)
            {
                var vector = new int[dimensions];
                var pos = i;
                for (var d = 0; d < dimensions; d++)
                {
                    vector[d] = pos % gridSize;
                    pos /= gridSize;
                }

                var vectorId = i + 1;
                var idBytes = new byte[4];
                idBytes[0] = (byte)(vectorId & 0xFF);
                idBytes[1] = (byte)((vectorId >> 8) & 0xFF);
                idBytes[2] = (byte)((vectorId >> 16) & 0xFF);
                idBytes[3] = (byte)((vectorId >> 24) & 0xFF);

                vectors.Add(new GridVector
                {
                    Id = vectorId,
                    Vector = vector,
                    IdBytes = idBytes,
                    VectorStringValues = vector.Select(v => v.ToString()).ToArray()
                });
            }

            return vectors;
        }

        private static Dictionary<int, GridVector> AddGridVectors(IDatabase db, string key, int dimension, int gridSize, string quantType)
        {
            var gridVectors = GenerateGridVectors(dimension, gridSize);
            List<object> baseArgs = [];

            baseArgs.Add(key);
            baseArgs.Add("VALUES");
            baseArgs.Add(dimension.ToString());
            var dimensionsPos = baseArgs.Count;
            for (var i = 0; i < dimension; i++)
            {
                baseArgs.Add(null);
            }

            var idBytesPos = baseArgs.Count;
            baseArgs.Add(null);
            baseArgs.Add(quantType);
            baseArgs.Add("EF");
            baseArgs.Add("10");
            baseArgs.Add("M");
            baseArgs.Add(Math.Max(5, dimension * 2).ToString());

            var args = baseArgs.ToArray();
            foreach (var gridVector in gridVectors)
            {
                args[idBytesPos] = gridVector.IdBytes;
                for (var i = 0; i < dimension; i++)
                {
                    args[dimensionsPos + i] = gridVector.VectorStringValues[i];
                }

                var res = db.Execute("VADD", args);
                ClassicAssert.AreEqual(1, (int)res);
            }

            return gridVectors.ToDictionary(gv => gv.Id);
        }

        private static HashSet<int> GetVectorIdsForVsimResults(RedisResult[] vsimResults)
        {
            HashSet<int> ids = [];
            foreach (var item in vsimResults)
            {
                var bytes = (byte[])item;
                var id = bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24);
                ids.Add(id);
            }

            return ids;
        }

        private static int CalculateDistanceCountsIntersection(Dictionary<int, GridVector> gridVectors, int[] queryVector, HashSet<int> bruteForceSearch, HashSet<int> vsimSearch)
        {
            var expectedDistances = CalculateCountsPerDistance(gridVectors, queryVector, bruteForceSearch);
            var actualDistances = CalculateCountsPerDistance(gridVectors, queryVector, vsimSearch);

            // Intersect distance counts
            var intersectionCount = 0;
            foreach (var kvp in expectedDistances)
            {
                var dist = kvp.Key;
                var expectedCount = kvp.Value;
                if (actualDistances.ContainsKey(dist))
                {
                    var actualCount = actualDistances[dist];
                    intersectionCount += Math.Min(expectedCount, actualCount);
                }
            }

            return intersectionCount;
        }

        private static Dictionary<long, int> CalculateCountsPerDistance(Dictionary<int, GridVector> gridVectors, int[] queryVector, HashSet<int> vsimIdResults)
        {
            Dictionary<long, int> countsPerDistance = [];
            foreach (var id in vsimIdResults)
            {
                var gridVector = gridVectors[id];
                var dist = CalculateSquaredL2Distance(gridVector.Vector, queryVector);
                if (!countsPerDistance.ContainsKey(dist))
                {
                    countsPerDistance[dist] = 0;
                }

                countsPerDistance[dist]++;
            }

            return countsPerDistance;
        }

        private static HashSet<int> BruteForceNearestNeighbors(Dictionary<int, GridVector> gridVectors, int[] queryVector, int count)
        {
            PriorityQueue<int, double> pq = new();
            foreach (var gridVector in gridVectors.Values)
            {
                double dist = 0;
                for (var i = 0; i < queryVector.Length; i++)
                {
                    double diff = gridVector.Vector[i] - queryVector[i];
                    dist += diff * diff;
                }
                pq.Enqueue(gridVector.Id, -dist);
                if (pq.Count > count)
                {
                    pq.Dequeue();
                }
            }

            HashSet<int> result = [];
            while (pq.Count > 0)
            {
                result.Add(pq.Dequeue());
            }

            return result;
        }

        private static long CalculateSquaredL2Distance(int[] vec1, int[] vec2)
        {
            long dist = 0;
            for (var i = 0; i < vec1.Length; i++)
            {
                long diff = vec1[i] - vec2[i];
                dist += diff * diff;
            }

            return dist;
        }

        private class GridVector
        {
            public int Id;
            public byte[] IdBytes;
            public int[] Vector;
            public string[] VectorStringValues;
        }
    }
}