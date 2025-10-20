// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterVectorSetTests
    {
        private const int DefaultShards = 2;
        private const int HighReplicationShards = 6;

        private static readonly Dictionary<string, LogLevel> MonitorTests =
            new()
            {
                [nameof(BasicVADDReplicates)] = LogLevel.Error,
            };


        private ClusterTestContext context;

        [SetUp]
        public virtual void Setup()
        {
            context = new ClusterTestContext();
            context.logTextWriter = TestContext.Progress;
            context.Setup(MonitorTests);
        }

        [TearDown]
        public virtual void TearDown()
        {
            context?.TearDown();
        }

        [Test]
        [TestCase("XB8", "XPREQ8")]
        [TestCase("XB8", "Q8")]
        [TestCase("XB8", "BIN")]
        [TestCase("XB8", "NOQUANT")]
        [TestCase("FP32", "XPREQ8")]
        [TestCase("FP32", "Q8")]
        [TestCase("FP32", "BIN")]
        [TestCase("FP32", "NOQUANT")]
        public void BasicVADDReplicates(string vectorFormat, string quantizer)
        {
            // TODO: also test VALUES format?

            const int PrimaryIndex = 0;
            const int SecondaryIndex = 1;

            ClassicAssert.IsTrue(Enum.TryParse<VectorValueType>(vectorFormat, ignoreCase: true, out var vectorFormatParsed));
            ClassicAssert.IsTrue(Enum.TryParse<VectorQuantType>(quantizer, ignoreCase: true, out var quantTypeParsed));

            context.CreateInstances(DefaultShards, useTLS: true, enableAOF: true);
            context.CreateConnection(useTLS: true);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 1, logger: context.logger);

            var primary = (IPEndPoint)context.endpoints[PrimaryIndex];
            var secondary = (IPEndPoint)context.endpoints[SecondaryIndex];

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary).Value);

            byte[] vectorAddData;
            if (vectorFormatParsed == VectorValueType.XB8)
            {
                vectorAddData = new byte[75];
                vectorAddData[0] = 1;
                for (var i = 1; i < vectorAddData.Length; i++)
                {
                    vectorAddData[i] = (byte)(vectorAddData[i - 1] + 1);
                }
            }
            else if (vectorFormatParsed == VectorValueType.FP32)
            {
                var floats = new float[75];
                floats[0] = 1;
                for (var i = 1; i < floats.Length; i++)
                {
                    floats[i] = floats[i - 1] + 1;
                }

                vectorAddData = MemoryMarshal.Cast<float, byte>(floats).ToArray();
            }
            else
            {
                ClassicAssert.Fail("Unexpected vector format");
                return;
            }

            var addRes = (int)context.clusterTestUtils.Execute(primary, "VADD", ["foo", vectorFormat, vectorAddData, new byte[] { 0, 0, 0, 0 }, quantizer]);
            ClassicAssert.AreEqual(1, addRes);

            byte[] vectorSimData;
            if (vectorFormatParsed == VectorValueType.XB8)
            {
                vectorSimData = new byte[75];
                vectorSimData[0] = 2;
                for (var i = 1; i < vectorSimData.Length; i++)
                {
                    vectorSimData[i] = (byte)(vectorSimData[i - 1] + 1);
                }
            }
            else if (vectorFormatParsed == VectorValueType.FP32)
            {
                var floats = new float[75];
                floats[0] = 2;
                for (var i = 1; i < floats.Length; i++)
                {
                    floats[i] = floats[i - 1] + 1;
                }

                vectorSimData = MemoryMarshal.Cast<float, byte>(floats).ToArray();
            }
            else
            {
                ClassicAssert.Fail("Unexpected vector format");
                return;
            }

            var simRes = (byte[][])context.clusterTestUtils.Execute(primary, "VSIM", ["foo", vectorFormat, vectorSimData]);
            ClassicAssert.IsTrue(simRes.Length > 0);

            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, SecondaryIndex);

            var readonlyOnReplica = (string)context.clusterTestUtils.Execute(secondary, "READONLY", []);
            ClassicAssert.AreEqual("OK", readonlyOnReplica);

            var simOnReplica = context.clusterTestUtils.Execute(secondary, "VSIM", ["foo", vectorFormat, vectorSimData]);
            ClassicAssert.IsTrue(simOnReplica.Length > 0);
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public async Task ConcurrentVADDReplicatedVSimsAsync(bool withAttributes)
        {
            const int PrimaryIndex = 0;
            const int SecondaryIndex = 1;
            const int Vectors = 2_000;
            const string Key = nameof(ConcurrentVADDReplicatedVSimsAsync);

            context.CreateInstances(DefaultShards, useTLS: true, enableAOF: true);
            context.CreateConnection(useTLS: true);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 1, logger: context.logger);

            var primary = (IPEndPoint)context.endpoints[PrimaryIndex];
            var secondary = (IPEndPoint)context.endpoints[SecondaryIndex];

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary).Value);

            // Build some repeatably random data for inserts
            var vectors = new byte[Vectors][];
            {
                var r = new Random(2025_09_15_00);

                for (var i = 0; i < vectors.Length; i++)
                {
                    vectors[i] = new byte[75];
                    r.NextBytes(vectors[i]);
                }
            }

            using var sync = new SemaphoreSlim(2);

            var writeTask =
                Task.Run(
                    async () =>
                    {
                        await sync.WaitAsync();

                        var key = new byte[4];
                        for (var i = 0; i < vectors.Length; i++)
                        {
                            BinaryPrimitives.WriteInt32LittleEndian(key, i);
                            var val = vectors[i];
                            int addRes;
                            if (withAttributes)
                            {
                                addRes = (int)context.clusterTestUtils.Execute(primary, "VADD", [Key, "XB8", val, key, "XPREQ8", "SETATTR", $"{{ \"id\": {i} }}"]);
                            }
                            else
                            {
                                addRes = (int)context.clusterTestUtils.Execute(primary, "VADD", [Key, "XB8", val, key, "XPREQ8"]);
                            }
                            ClassicAssert.AreEqual(1, addRes);
                        }
                    }
                );

            using var cts = new CancellationTokenSource();

            var readTask =
                Task.Run(
                    async () =>
                    {
                        var r = new Random(2025_09_15_01);

                        var readonlyOnReplica = (string)context.clusterTestUtils.Execute(secondary, "READONLY", []);
                        ClassicAssert.AreEqual("OK", readonlyOnReplica);

                        await sync.WaitAsync();

                        var nonZeroReturns = 0;
                        var gotAttrs = 0;

                        while (!cts.Token.IsCancellationRequested)
                        {
                            var val = vectors[r.Next(vectors.Length)];

                            if (withAttributes)
                            {
                                var readRes = (byte[][])context.clusterTestUtils.Execute(secondary, "VSIM", [Key, "XB8", val, "WITHATTRIBS"]);
                                if (readRes.Length > 0)
                                {
                                    nonZeroReturns++;
                                }

                                for (var i = 0; i < readRes.Length; i += 2)
                                {
                                    var id = readRes[i];
                                    var attr = readRes[i + 1];

                                    // TODO: Null is possible because of attributes are hacked up today
                                    //       when they are NOT hacky we can make null illegal
                                    if ((attr?.Length ?? 0) > 0)
                                    {
                                        var asInt = BinaryPrimitives.ReadInt32LittleEndian(id);

                                        var actualAttr = Encoding.UTF8.GetString(attr);
                                        var expectedAttr = $"{{ \"id\": {asInt} }}";

                                        ClassicAssert.AreEqual(expectedAttr, actualAttr);

                                        gotAttrs++;
                                    }
                                }
                            }
                            else
                            {
                                var readRes = (byte[][])context.clusterTestUtils.Execute(secondary, "VSIM", [Key, "XB8", val]);
                                if (readRes.Length > 0)
                                {
                                    nonZeroReturns++;
                                }
                            }
                        }

                        return (nonZeroReturns, gotAttrs);
                    }
                );

            _ = sync.Release(2);
            await writeTask;

            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, SecondaryIndex);

            cts.CancelAfter(TimeSpan.FromSeconds(1));

            var (searchesWithNonZeroResults, searchesWithAttrs) = await readTask;

            ClassicAssert.IsTrue(searchesWithNonZeroResults > 0);

            if (withAttributes)
            {
                ClassicAssert.IsTrue(searchesWithAttrs > 0);
            }

            // Validate all nodes have same vector embeddings
            {
                var idBytes = new byte[4];
                for (var id = 0; id < vectors.Length; id++)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(idBytes, id);
                    var expected = vectors[id];

                    var fromPrimary = (string[])context.clusterTestUtils.Execute(primary, "VEMB", [Key, idBytes]);
                    var fromSecondary = (string[])context.clusterTestUtils.Execute(secondary, "VEMB", [Key, idBytes]);

                    ClassicAssert.AreEqual(expected.Length, fromPrimary.Length);
                    ClassicAssert.AreEqual(expected.Length, fromSecondary.Length);

                    for (var i = 0; i < expected.Length; i++)
                    {
                        var p = (byte)float.Parse(fromPrimary[i]);
                        var s = (byte)float.Parse(fromSecondary[i]);

                        ClassicAssert.AreEqual(expected[i], p);
                        ClassicAssert.AreEqual(expected[i], s);
                    }
                }
            }
        }

        [Test]
        public void RepeatedCreateDelete()
        {
            const int PrimaryIndex = 0;
            const int SecondaryIndex = 1;

            context.CreateInstances(DefaultShards, useTLS: true, enableAOF: true);
            context.CreateConnection(useTLS: true);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 1, logger: context.logger);

            var primary = (IPEndPoint)context.endpoints[PrimaryIndex];
            var secondary = (IPEndPoint)context.endpoints[SecondaryIndex];

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary).Value);

            var bytes1 = new byte[75];
            bytes1[0] = 1;
            for (var j = 1; j < bytes1.Length; j++)
            {
                bytes1[j] = (byte)(bytes1[j - 1] + 1);
            }

            var bytes2 = new byte[75];
            bytes2[0] = 5;
            for (var j = 1; j < bytes2.Length; j++)
            {
                bytes2[j] = (byte)(bytes2[j - 1] + 1);
            }

            var bytes3 = new byte[75];
            bytes3[0] = 10;
            for (var j = 1; j < bytes3.Length; j++)
            {
                bytes3[j] = (byte)(bytes3[j - 1] + 1);
            }

            for (var i = 0; i < 1_000; i++)
            {
                var delRes = (int)context.clusterTestUtils.Execute(primary, "DEL", ["foo"]);

                if (i != 0)
                {
                    ClassicAssert.AreEqual(1, delRes);
                }
                else
                {
                    ClassicAssert.AreEqual(0, delRes);
                }

                var addRes1 = (int)context.clusterTestUtils.Execute(primary, "VADD", ["foo", "XB8", bytes1, new byte[] { 0, 0, 0, 0 }, "XPREQ8"]);
                ClassicAssert.AreEqual(1, addRes1);

                var addRes2 = (int)context.clusterTestUtils.Execute(primary, "VADD", ["foo", "XB8", bytes2, new byte[] { 0, 0, 0, 1 }, "XPREQ8"]);
                ClassicAssert.AreEqual(1, addRes2);

                var readPrimaryExc = (string)context.clusterTestUtils.Execute(primary, "GET", ["foo"]);
                ClassicAssert.IsTrue(readPrimaryExc.StartsWith("WRONGTYPE "));

                var queryPrimary = (byte[][])context.clusterTestUtils.Execute(primary, "VSIM", ["foo", "XB8", bytes3]);
                ClassicAssert.AreEqual(2, queryPrimary.Length);

                _ = context.clusterTestUtils.Execute(secondary, "READONLY", []);

                // The vector set has either replicated, or not
                // If so - we get WRONGTYPE
                // If not - we get a null
                var readSecondary = (string)context.clusterTestUtils.Execute(secondary, "GET", ["foo"]);
                ClassicAssert.IsTrue(readSecondary is null || readSecondary.StartsWith("WRONGTYPE "));

                var start = Stopwatch.GetTimestamp();
                while (true)
                {
                    var querySecondary = (byte[][])context.clusterTestUtils.Execute(secondary, "VSIM", ["foo", "XB8", bytes3]);
                    if (querySecondary.Length == 2)
                    {
                        break;
                    }

                    ClassicAssert.IsTrue(Stopwatch.GetElapsedTime(start) < TimeSpan.FromSeconds(5), "Too long has passed without a vector set catching up on the secondary");
                }
            }
        }

        [Test]
        public async Task MultipleReplicasWithVectorSetsAsync()
        {
            const int PrimaryIndex = 0;
            const int SecondaryStartIndex = 1;
            const int SecondaryEndIndex = 5;
            const int Vectors = 2_000;
            const string Key = nameof(MultipleReplicasWithVectorSetsAsync);

            context.CreateInstances(HighReplicationShards, useTLS: true, enableAOF: true);
            context.CreateConnection(useTLS: true);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 5, logger: context.logger);

            var primary = (IPEndPoint)context.endpoints[PrimaryIndex];
            var secondaries = new IPEndPoint[SecondaryEndIndex - SecondaryStartIndex + 1];
            for (var i = SecondaryStartIndex; i <= SecondaryEndIndex; i++)
            {
                secondaries[i - SecondaryStartIndex] = (IPEndPoint)context.endpoints[i];
            }

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary).Value);

            foreach (var secondary in secondaries)
            {
                ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary).Value);
            }

            // Build some repeatably random data for inserts
            var vectors = new byte[Vectors][];
            {
                var r = new Random(2025_09_23_00);

                for (var i = 0; i < vectors.Length; i++)
                {
                    vectors[i] = new byte[75];
                    r.NextBytes(vectors[i]);
                }
            }

            using var sync = new SemaphoreSlim(2);

            var writeTask =
                Task.Run(
                    async () =>
                    {
                        await sync.WaitAsync();

                        var key = new byte[4];
                        for (var i = 0; i < vectors.Length; i++)
                        {
                            BinaryPrimitives.WriteInt32LittleEndian(key, i);
                            var val = vectors[i];
                            var addRes = (int)context.clusterTestUtils.Execute(primary, "VADD", [Key, "XB8", val, key, "XPREQ8"]);
                            ClassicAssert.AreEqual(1, addRes);
                        }
                    }
                );

            using var cts = new CancellationTokenSource();

            var readTasks = new Task<int>[secondaries.Length];

            for (var i = 0; i < secondaries.Length; i++)
            {
                var secondary = secondaries[i];
                var readTask =
                    Task.Run(
                        async () =>
                        {
                            var r = new Random(2025_09_23_01);

                            var readonlyOnReplica = (string)context.clusterTestUtils.Execute(secondary, "READONLY", []);
                            ClassicAssert.AreEqual("OK", readonlyOnReplica);

                            await sync.WaitAsync();

                            var nonZeroReturns = 0;

                            while (!cts.Token.IsCancellationRequested)
                            {
                                var val = vectors[r.Next(vectors.Length)];

                                var readRes = (byte[][])context.clusterTestUtils.Execute(secondary, "VSIM", [Key, "XB8", val]);
                                if (readRes.Length > 0)
                                {
                                    nonZeroReturns++;
                                }
                            }

                            return nonZeroReturns;
                        }
                    );

                readTasks[i] = readTask;
            }

            _ = sync.Release(secondaries.Length + 1);
            await writeTask;

            for (var secondaryIndex = SecondaryStartIndex; secondaryIndex <= SecondaryEndIndex; secondaryIndex++)
            {
                context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, secondaryIndex);
            }

            cts.CancelAfter(TimeSpan.FromSeconds(1));

            var searchesWithNonZeroResults = await Task.WhenAll(readTasks);

            ClassicAssert.IsTrue(searchesWithNonZeroResults.All(static x => x > 0));


            // Validate all nodes have same vector embeddings
            {
                var idBytes = new byte[4];
                for (var id = 0; id < vectors.Length; id++)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(idBytes, id);
                    var expected = vectors[id];

                    var fromPrimary = (string[])context.clusterTestUtils.Execute(primary, "VEMB", [Key, idBytes]);

                    ClassicAssert.AreEqual(expected.Length, fromPrimary.Length);

                    for (var i = 0; i < expected.Length; i++)
                    {
                        var p = (byte)float.Parse(fromPrimary[i]);
                        ClassicAssert.AreEqual(expected[i], p);
                    }

                    for (var secondaryIx = 0; secondaryIx < secondaries.Length; secondaryIx++)
                    {
                        var secondary = secondaries[secondaryIx];
                        var fromSecondary = (string[])context.clusterTestUtils.Execute(secondary, "VEMB", [Key, idBytes]);

                        ClassicAssert.AreEqual(expected.Length, fromSecondary.Length);

                        for (var i = 0; i < expected.Length; i++)
                        {
                            var s = (byte)float.Parse(fromSecondary[i]);
                            ClassicAssert.AreEqual(expected[i], s);
                        }
                    }
                }
            }
        }

        [Test]
        public async Task MultipleReplicasWithVectorSetsAndDeletesAsync()
        {
            const int PrimaryIndex = 0;
            const int SecondaryStartIndex = 1;
            const int SecondaryEndIndex = 5;
            const int Vectors = 2_000;
            const int Deletes = Vectors / 10;
            const string Key = nameof(MultipleReplicasWithVectorSetsAndDeletesAsync);

            context.CreateInstances(HighReplicationShards, useTLS: true, enableAOF: true);
            context.CreateConnection(useTLS: true);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 5, logger: context.logger);

            var primary = (IPEndPoint)context.endpoints[PrimaryIndex];
            var secondaries = new IPEndPoint[SecondaryEndIndex - SecondaryStartIndex + 1];
            for (var i = SecondaryStartIndex; i <= SecondaryEndIndex; i++)
            {
                secondaries[i - SecondaryStartIndex] = (IPEndPoint)context.endpoints[i];
            }

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary).Value);

            foreach (var secondary in secondaries)
            {
                ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary).Value);
            }

            // Build some repeatably random data for inserts
            var vectors = new byte[Vectors][];
            var toDeleteVectors = new HashSet<int>();
            var pendingRemove = new List<int>();
            {
                var r = new Random(2025_10_20_00);

                for (var i = 0; i < vectors.Length; i++)
                {
                    vectors[i] = new byte[75];
                    r.NextBytes(vectors[i]);
                }

                while (toDeleteVectors.Count < Deletes)
                {
                    _ = toDeleteVectors.Add(r.Next(vectors.Length));
                }

                pendingRemove.AddRange(toDeleteVectors);
            }

            using var sync = new SemaphoreSlim(2);

            var writeTask =
                Task.Run(
                    async () =>
                    {
                        await sync.WaitAsync();

                        var key = new byte[4];
                        for (var i = 0; i < vectors.Length; i++)
                        {
                            BinaryPrimitives.WriteInt32LittleEndian(key, i);
                            var val = vectors[i];
                            var addRes = (int)context.clusterTestUtils.Execute(primary, "VADD", [Key, "XB8", val, key, "XPREQ8"]);
                            ClassicAssert.AreEqual(1, addRes);
                        }
                    }
                );

            var deleteTask =
                Task.Run(
                    async () =>
                    {
                        await sync.WaitAsync();

                        var key = new byte[4];

                        while (pendingRemove.Count > 0)
                        {
                            var i = Random.Shared.Next(pendingRemove.Count);
                            var id = pendingRemove[i];

                            BinaryPrimitives.WriteInt32LittleEndian(key, id);
                            var remRes = (int)context.clusterTestUtils.Execute(primary, "VREM", [Key, key]);
                            if (remRes == 1)
                            {
                                pendingRemove.RemoveAt(i);
                            }
                        }
                    }
                );

            using var cts = new CancellationTokenSource();

            var readTasks = new Task<int>[secondaries.Length];

            for (var i = 0; i < secondaries.Length; i++)
            {
                var secondary = secondaries[i];
                var readTask =
                    Task.Run(
                        async () =>
                        {
                            var r = new Random(2025_09_23_01);

                            var readonlyOnReplica = (string)context.clusterTestUtils.Execute(secondary, "READONLY", []);
                            ClassicAssert.AreEqual("OK", readonlyOnReplica);

                            await sync.WaitAsync();

                            var nonZeroReturns = 0;

                            while (!cts.Token.IsCancellationRequested)
                            {
                                var val = vectors[r.Next(vectors.Length)];

                                var readRes = (byte[][])context.clusterTestUtils.Execute(secondary, "VSIM", [Key, "XB8", val]);
                                if (readRes.Length > 0)
                                {
                                    nonZeroReturns++;
                                }
                            }

                            return nonZeroReturns;
                        }
                    );

                readTasks[i] = readTask;
            }

            _ = sync.Release(secondaries.Length + 2);
            await writeTask;
            await deleteTask;

            for (var secondaryIndex = SecondaryStartIndex; secondaryIndex <= SecondaryEndIndex; secondaryIndex++)
            {
                context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, secondaryIndex);
            }

            cts.CancelAfter(TimeSpan.FromSeconds(1));

            var searchesWithNonZeroResults = await Task.WhenAll(readTasks);

            ClassicAssert.IsTrue(searchesWithNonZeroResults.All(static x => x > 0));

            // Validate all nodes have same vector embeddings
            {
                var idBytes = new byte[4];
                for (var id = 0; id < vectors.Length; id++)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(idBytes, id);
                    var expected = vectors[id];

                    var fromPrimary = (string[])context.clusterTestUtils.Execute(primary, "VEMB", [Key, idBytes]);

                    var shouldBePresent = !toDeleteVectors.Contains(id);
                    if (shouldBePresent)
                    {
                        ClassicAssert.AreEqual(expected.Length, fromPrimary.Length);

                        for (var i = 0; i < expected.Length; i++)
                        {
                            var p = (byte)float.Parse(fromPrimary[i]);
                            ClassicAssert.AreEqual(expected[i], p);
                        }
                    }
                    else
                    {
                        ClassicAssert.IsEmpty(fromPrimary);
                    }

                    for (var secondaryIx = 0; secondaryIx < secondaries.Length; secondaryIx++)
                    {
                        var secondary = secondaries[secondaryIx];
                        var fromSecondary = (string[])context.clusterTestUtils.Execute(secondary, "VEMB", [Key, idBytes]);

                        if (shouldBePresent)
                        {
                            ClassicAssert.AreEqual(expected.Length, fromSecondary.Length);

                            for (var i = 0; i < expected.Length; i++)
                            {
                                var s = (byte)float.Parse(fromSecondary[i]);
                                ClassicAssert.AreEqual(expected[i], s);
                            }
                        }
                        else
                        {
                            ClassicAssert.IsEmpty(fromSecondary);
                        }
                    }
                }
            }
        }
    }
}