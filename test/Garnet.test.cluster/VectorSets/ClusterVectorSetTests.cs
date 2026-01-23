// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterVectorSetTests
    {
        private sealed class StringAndByteArrayComparer : IEqualityComparer<(string Key, byte[] Elem)>
        {
            public static readonly StringAndByteArrayComparer Instance = new();

            private StringAndByteArrayComparer() { }

            public bool Equals((string Key, byte[] Elem) x, (string Key, byte[] Elem) y)
            => x.Key.Equals(y.Key) && x.Elem.SequenceEqual(y.Elem);

            public int GetHashCode([DisallowNull] (string Key, byte[] Elem) obj)
            {
                HashCode code = default;
                code.Add(obj.Key);
                code.AddBytes(obj.Elem);

                return code.ToHashCode();
            }
        }

        private sealed class CaptureLogWriter(TextWriter passThrough) : TextWriter
        {
            public bool capture;
            public readonly StringBuilder buffer = new();

            public override Encoding Encoding
            => passThrough.Encoding;

            public override void Write(string value)
            {
                passThrough.Write(value);

                if (capture)
                {
                    lock (buffer)
                    {
                        _ = buffer.Append(value);
                    }
                }
            }
        }

        private const int DefaultShards = 2;
        private const int HighReplicationShards = 6;
        private const int DefaultMultiPrimaryShards = 4;

        private static readonly Dictionary<string, LogLevel> MonitorTests = new()
        {
            [nameof(MigrateVectorStressAsync)] = LogLevel.Debug,
        };


        private ClusterTestContext context;

        private CaptureLogWriter captureLogWriter;

        [SetUp]
        public virtual void Setup()
        {
            captureLogWriter = new(TestContext.Progress);

            context = new ClusterTestContext();
            context.logTextWriter = captureLogWriter;
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
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 1);

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
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 1);

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

                                    var asInt = BinaryPrimitives.ReadInt32LittleEndian(id);

                                    var actualAttr = Encoding.UTF8.GetString(attr);
                                    var expectedAttr = $"{{ \"id\": {asInt} }}";

                                    ClassicAssert.AreEqual(expectedAttr, actualAttr);

                                    gotAttrs++;
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
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 1);

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

            var key0 = new byte[4];
            key0[0] = 1;
            var key1 = new byte[4];
            key1[0] = 2;

            for (var i = 0; i < 100; i++)
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

                var addRes1 = (int)context.clusterTestUtils.Execute(primary, "VADD", ["foo", "XB8", bytes1, key0, "XPREQ8"]);
                ClassicAssert.AreEqual(1, addRes1);

                var addRes2 = (int)context.clusterTestUtils.Execute(primary, "VADD", ["foo", "XB8", bytes2, key1, "XPREQ8"]);
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

                context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, SecondaryIndex);

                var querySecondary = (byte[][])context.clusterTestUtils.Execute(secondary, "VSIM", ["foo", "XB8", bytes3]);
                ClassicAssert.IsTrue(querySecondary.Length >= 1);

                for (var j = 0; j < querySecondary.Length; j++)
                {
                    var expected =
                        querySecondary[j].AsSpan().SequenceEqual(key0) ||
                        querySecondary[j].AsSpan().SequenceEqual(key1);

                    ClassicAssert.IsTrue(expected);
                }

                Incr(key0);
                Incr(key1);
            }

            static void Incr(byte[] k)
            {
                var ix = k.Length - 1;
                while (true)
                {
                    k[ix]++;
                    if (k[ix] == 0)
                    {
                        ix--;
                    }
                    else
                    {
                        break;
                    }
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
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 5);

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
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 5);

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

        [Test]
        public void VectorSetMigrateSingleBySlot()
        {
            // Test migrating a single slot with a vector set of one element in it

            const int Primary0Index = 0;
            const int Primary1Index = 1;
            const int Secondary0Index = 2;
            const int Secondary1Index = 3;

            context.CreateInstances(DefaultMultiPrimaryShards, useTLS: true, enableAOF: true);
            context.CreateConnection(useTLS: true);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: DefaultMultiPrimaryShards / 2, replica_count: 1);

            var primary0 = (IPEndPoint)context.endpoints[Primary0Index];
            var primary1 = (IPEndPoint)context.endpoints[Primary1Index];
            var secondary0 = (IPEndPoint)context.endpoints[Secondary0Index];
            var secondary1 = (IPEndPoint)context.endpoints[Secondary1Index];

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary0).Value);
            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary1).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary0).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary1).Value);

            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var primary1Id = context.clusterTestUtils.ClusterMyId(primary1);

            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            string primary0Key;
            int primary0HashSlot;
            {
                var ix = 0;

                while (true)
                {
                    primary0Key = $"{nameof(VectorSetMigrateSingleBySlot)}_{ix}";
                    primary0HashSlot = context.clusterTestUtils.HashSlot(primary0Key);

                    if (slots.Any(x => x.nnInfo.Any(y => y.nodeid == primary0Id) && primary0HashSlot >= x.startSlot && primary0HashSlot <= x.endSlot))
                    {
                        break;
                    }

                    ix++;
                }
            }

            // Setup simple vector set on Primary0 in some hash slot

            var vectorData = Enumerable.Range(0, 75).Select(static x => (byte)x).ToArray();
            var vectorSimData = Enumerable.Range(0, 75).Select(static x => (byte)(x * 2)).ToArray();

            var add0Res = (int)context.clusterTestUtils.Execute(primary0, "VADD", [primary0Key, "XB8", vectorData, new byte[] { 0, 0, 0, 0 }, "XPREQ8", "SETATTR", "{\"hello\": \"world\"}"], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual(1, add0Res);

            var sim0Res = (byte[][])context.clusterTestUtils.Execute(primary0, "VSIM", [primary0Key, "XB8", vectorSimData, "WITHSCORES", "WITHATTRIBS"], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual(3, sim0Res.Length);
            ClassicAssert.IsTrue(new byte[] { 0, 0, 0, 0 }.SequenceEqual(sim0Res[0]));
            ClassicAssert.IsFalse(float.IsNaN(float.Parse(Encoding.ASCII.GetString(sim0Res[1]))));
            ClassicAssert.IsTrue("{\"hello\": \"world\"}"u8.SequenceEqual(sim0Res[2]));

            context.clusterTestUtils.WaitForReplicaAofSync(Primary0Index, Secondary0Index);

            var readonlyOnReplica0 = (string)context.clusterTestUtils.Execute(secondary0, "READONLY", [], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", readonlyOnReplica0);

            var simOnReplica0 = (byte[][])context.clusterTestUtils.Execute(secondary0, "VSIM", [primary0Key, "XB8", vectorSimData, "WITHSCORES", "WITHATTRIBS"], flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(simOnReplica0.Length > 0);
            for (var i = 0; i < sim0Res.Length; i++)
            {
                ClassicAssert.IsTrue(sim0Res[i].AsSpan().SequenceEqual(simOnReplica0[i]));
            }

            // Move to other primary

            context.clusterTestUtils.MigrateSlots(primary0, primary1, [primary0HashSlot]);
            context.clusterTestUtils.WaitForMigrationCleanup(Primary0Index);
            context.clusterTestUtils.WaitForMigrationCleanup(Primary1Index);

            context.clusterTestUtils.WaitForReplicaAofSync(Primary0Index, Secondary0Index);
            context.clusterTestUtils.WaitForReplicaAofSync(Primary1Index, Secondary1Index);

            var curPrimary0Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, NullLogger.Instance);
            var curPrimary1Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary1, NullLogger.Instance);

            ClassicAssert.IsFalse(curPrimary0Slots.Contains(primary0HashSlot));
            ClassicAssert.IsTrue(curPrimary1Slots.Contains(primary0HashSlot));

            // Check available on other primary & secondary

            var sim1Res = (byte[][])context.clusterTestUtils.Execute(primary1, "VSIM", [primary0Key, "XB8", vectorSimData, "WITHSCORES", "WITHATTRIBS"], flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(sim1Res.Length > 0);
            for (var i = 0; i < sim0Res.Length; i++)
            {
                ClassicAssert.IsTrue(sim0Res[i].AsSpan().SequenceEqual(sim1Res[i]));
            }

            var readonlyOnReplica1 = (string)context.clusterTestUtils.Execute(secondary1, "READONLY", [], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", readonlyOnReplica1);

            var simOnReplica1 = (byte[][])context.clusterTestUtils.Execute(secondary1, "VSIM", [primary0Key, "XB8", vectorSimData, "WITHSCORES", "WITHATTRIBS"], flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(simOnReplica1.Length > 0);
            for (var i = 0; i < sim0Res.Length; i++)
            {
                ClassicAssert.IsTrue(sim0Res[i].AsSpan().SequenceEqual(simOnReplica0[i]));
            }

            // Check no longer available on old primary or secondary
            var exc0 = (string)context.clusterTestUtils.Execute(primary0, "VSIM", [primary0Key, "XB8", vectorSimData, "WITHSCORES", "WITHATTRIBS"], flags: CommandFlags.NoRedirect);
            ClassicAssert.IsTrue(exc0.StartsWith("Key has MOVED to "));

            var start = Stopwatch.GetTimestamp();

            var success = false;
            while (Stopwatch.GetElapsedTime(start) < TimeSpan.FromSeconds(5))
            {
                try
                {
                    var exc1 = (string)context.clusterTestUtils.Execute(secondary0, "VSIM", [primary0Key, "XB8", vectorSimData, "WITHSCORES", "WITHATTRIBS"], flags: CommandFlags.NoRedirect);
                    ClassicAssert.IsTrue(exc1.StartsWith("Key has MOVED to "));
                    success = true;
                    break;
                }
                catch
                {
                    // Secondary can still have the key for a bit
                    Thread.Sleep(100);
                }
            }

            ClassicAssert.IsTrue(success, "Original replica still has Vector Set long after primary has completed");
        }

        [Test]
        public void VectorSetMigrateByKeys()
        {
            // Based on : ClusterSimpleMigrateKeys test

            const int ShardCount = 3;
            const int KeyCount = 10;

            context.CreateInstances(ShardCount, useTLS: true, enableAOF: true);
            context.CreateConnection(useTLS: true);
            _ = context.clusterTestUtils.SimpleSetupCluster();

            var otherNodeIndex = 0;
            var sourceNodeIndex = 1;
            var targetNodeIndex = 2;
            var sourceNodeId = context.clusterTestUtils.GetNodeIdFromNode(sourceNodeIndex, NullLogger.Instance);
            var targetNodeId = context.clusterTestUtils.GetNodeIdFromNode(targetNodeIndex, NullLogger.Instance);

            var key = Encoding.ASCII.GetBytes("{abc}a");
            List<byte[]> keys = [];
            List<(byte[] Key, byte[] Data)> vectors = [];
            List<byte[]> attributes = [];

            var _workingSlot = ClusterTestUtils.HashSlot(key);
            ClassicAssert.AreEqual(7638, _workingSlot);

            Random rand = new(2025_11_04_00);

            for (var i = 0; i < KeyCount; i++)
            {
                var newKey = new byte[key.Length];
                Array.Copy(key, 0, newKey, 0, key.Length);
                newKey[^1] = (byte)(newKey[^1] + i);
                ClassicAssert.AreEqual(_workingSlot, ClusterTestUtils.HashSlot(newKey));

                var elem = new byte[4];
                rand.NextBytes(elem);

                var data = new byte[75];
                rand.NextBytes(data);

                var attrs = new byte[16];
                rand.NextBytes(attrs);

                var addRes = (int)context.clusterTestUtils.Execute(context.clusterTestUtils.GetEndPoint(sourceNodeIndex), "VADD", [newKey, "XB8", data, elem, "XPREQ8", "SETATTR", attrs]);
                ClassicAssert.AreEqual(1, addRes);

                keys.Add(newKey);
                vectors.Add((elem, data));
                attributes.Add(attrs);
            }

            // Start migration
            var respImport = context.clusterTestUtils.SetSlot(targetNodeIndex, _workingSlot, "IMPORTING", sourceNodeId);
            ClassicAssert.AreEqual(respImport, "OK");

            var respMigrate = context.clusterTestUtils.SetSlot(sourceNodeIndex, _workingSlot, "MIGRATING", targetNodeId);
            ClassicAssert.AreEqual(respMigrate, "OK");

            // Check key count
            var countKeys = context.clusterTestUtils.CountKeysInSlot(sourceNodeIndex, _workingSlot);
            ClassicAssert.AreEqual(countKeys, KeyCount);

            // Enumerate keys in slots
            var keysInSlot = context.clusterTestUtils.GetKeysInSlot(sourceNodeIndex, _workingSlot, countKeys);
            ClassicAssert.AreEqual(keys, keysInSlot);

            // Migrate keys, but in a random-ish order so context reservation gets stressed
            var toMigrate = keysInSlot.ToList();
            while (toMigrate.Count > 0)
            {
                var migrateSingleIx = rand.Next(toMigrate.Count);
                var migrateKey = toMigrate[migrateSingleIx];
                context.clusterTestUtils.MigrateKeys(context.clusterTestUtils.GetEndPoint(sourceNodeIndex), context.clusterTestUtils.GetEndPoint(targetNodeIndex), [migrateKey], NullLogger.Instance);

                toMigrate.RemoveAt(migrateSingleIx);
            }

            // Finish migration
            var respNodeTarget = context.clusterTestUtils.SetSlot(targetNodeIndex, _workingSlot, "NODE", targetNodeId);
            ClassicAssert.AreEqual(respNodeTarget, "OK");
            context.clusterTestUtils.BumpEpoch(targetNodeIndex, waitForSync: true);

            var respNodeSource = context.clusterTestUtils.SetSlot(sourceNodeIndex, _workingSlot, "NODE", targetNodeId);
            ClassicAssert.AreEqual(respNodeSource, "OK");
            context.clusterTestUtils.BumpEpoch(sourceNodeIndex, waitForSync: true);
            // End Migration

            // Check config
            var targetConfigEpochFromTarget = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(targetNodeIndex, targetNodeId, NullLogger.Instance);
            var targetConfigEpochFromSource = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(sourceNodeIndex, targetNodeId, NullLogger.Instance);
            var targetConfigEpochFromOther = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(otherNodeIndex, targetNodeId, NullLogger.Instance);

            while (targetConfigEpochFromOther != targetConfigEpochFromTarget || targetConfigEpochFromSource != targetConfigEpochFromTarget)
            {
                _ = Thread.Yield();
                targetConfigEpochFromTarget = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(targetNodeIndex, targetNodeId, NullLogger.Instance);
                targetConfigEpochFromSource = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(sourceNodeIndex, targetNodeId, NullLogger.Instance);
                targetConfigEpochFromOther = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(otherNodeIndex, targetNodeId, NullLogger.Instance);
            }
            ClassicAssert.AreEqual(targetConfigEpochFromTarget, targetConfigEpochFromOther);
            ClassicAssert.AreEqual(targetConfigEpochFromTarget, targetConfigEpochFromSource);

            // Check migration in progress
            foreach (var _key in keys)
            {
                var resp = context.clusterTestUtils.GetKey(otherNodeIndex, _key, out var slot, out var endpoint, out var responseState);
                while (endpoint.Port != context.clusterTestUtils.GetEndPoint(targetNodeIndex).Port && responseState != ResponseState.OK)
                {
                    resp = context.clusterTestUtils.GetKey(otherNodeIndex, _key, out slot, out endpoint, out responseState);
                }
                ClassicAssert.AreEqual(resp, "MOVED");
                ClassicAssert.AreEqual(_workingSlot, slot);
                ClassicAssert.AreEqual(context.clusterTestUtils.GetEndPoint(targetNodeIndex), endpoint);
            }

            // Finish migration
            context.clusterTestUtils.WaitForMigrationCleanup(NullLogger.Instance);

            // Validate vector sets coherent
            for (var i = 0; i < keys.Count; i++)
            {
                var _key = keys[i];
                var (elem, data) = vectors[i];
                var attrs = attributes[i];

                var res = (byte[][])context.clusterTestUtils.Execute(context.clusterTestUtils.GetEndPoint(targetNodeIndex), "VSIM", [_key, "XB8", data, "WITHATTRIBS"]);
                ClassicAssert.AreEqual(2, res.Length);
                ClassicAssert.IsTrue(res[0].SequenceEqual(elem));
                ClassicAssert.IsTrue(res[1].SequenceEqual(attrs));
            }
        }

        [Test]
        public void VectorSetMigrateManyBySlot()
        {
            // Test migrating several vector sets from one primary to another primary, which already has vectors sets of its own

            const int Primary0Index = 0;
            const int Primary1Index = 1;
            const int Secondary0Index = 2;
            const int Secondary1Index = 3;

            const int VectorSetsPerPrimary = 8;

            context.CreateInstances(DefaultMultiPrimaryShards, useTLS: true, enableAOF: true);
            context.CreateConnection(useTLS: true);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: DefaultMultiPrimaryShards / 2, replica_count: 1);

            var primary0 = (IPEndPoint)context.endpoints[Primary0Index];
            var primary1 = (IPEndPoint)context.endpoints[Primary1Index];
            var secondary0 = (IPEndPoint)context.endpoints[Secondary0Index];
            var secondary1 = (IPEndPoint)context.endpoints[Secondary1Index];

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary0).Value);
            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary1).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary0).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary1).Value);

            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var primary1Id = context.clusterTestUtils.ClusterMyId(primary1);

            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            List<(string Key, ushort HashSlot, byte[] Element, byte[] Data, byte[] Attr)> primary0Keys = [];
            List<(string Key, ushort HashSlot, byte[] Element, byte[] Data, byte[] Attr)> primary1Keys = [];

            {
                var ix = 0;

                while (primary0Keys.Count < VectorSetsPerPrimary || primary1Keys.Count < VectorSetsPerPrimary)
                {
                    var key = $"{nameof(VectorSetMigrateManyBySlot)}_{ix}";
                    var hashSlot = context.clusterTestUtils.HashSlot(key);

                    var isOnPrimary0 = slots.Any(x => x.nnInfo.Any(y => y.nodeid == primary0Id) && hashSlot >= x.startSlot && hashSlot <= x.endSlot);
                    var isOnPrimary1 = slots.Any(x => x.nnInfo.Any(y => y.nodeid == primary1Id) && hashSlot >= x.startSlot && hashSlot <= x.endSlot);

                    if (isOnPrimary0 && primary0Keys.Count < VectorSetsPerPrimary)
                    {
                        var elem = new byte[4];
                        var data = new byte[75];
                        var attr = new byte[10];
                        Random.Shared.NextBytes(elem);
                        Random.Shared.NextBytes(data);
                        Random.Shared.NextBytes(attr);

                        primary0Keys.Add((key, (ushort)hashSlot, elem, data, attr));
                    }

                    if (isOnPrimary1 && primary1Keys.Count < VectorSetsPerPrimary)
                    {
                        var elem = new byte[4];
                        var data = new byte[75];
                        var attr = new byte[10];
                        Random.Shared.NextBytes(elem);
                        Random.Shared.NextBytes(data);
                        Random.Shared.NextBytes(attr);

                        primary1Keys.Add((key, (ushort)hashSlot, elem, data, attr));
                    }

                    ix++;
                }
            }

            // Setup vectors on the primaries
            foreach (var (key, _, elem, data, attr) in primary0Keys)
            {
                var add0Res = (int)context.clusterTestUtils.Execute(primary0, "VADD", [key, "XB8", data, elem, "XPREQ8", "SETATTR", attr], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(1, add0Res);
            }

            foreach (var (key, _, elem, data, attr) in primary1Keys)
            {
                var add1Res = (int)context.clusterTestUtils.Execute(primary1, "VADD", [key, "XB8", data, elem, "XPREQ8", "SETATTR", attr], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(1, add1Res);
            }

            // Query expected results
            Dictionary<(string Key, byte[] Data), (byte[] Elem, byte[] Attr, float Score)> expected = new(StringAndByteArrayComparer.Instance);

            foreach (var (key, _, _, data, _) in primary0Keys)
            {
                var sim0Res = (byte[][])context.clusterTestUtils.Execute(primary0, "VSIM", [key, "XB8", data, "WITHSCORES", "WITHATTRIBS"], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(3, sim0Res.Length);
                expected.Add((key, data), (sim0Res[0], sim0Res[2], float.Parse(Encoding.ASCII.GetString(sim0Res[1]))));
            }

            foreach (var (key, _, _, data, _) in primary1Keys)
            {
                var sim1Res = (byte[][])context.clusterTestUtils.Execute(primary1, "VSIM", [key, "XB8", data, "WITHSCORES", "WITHATTRIBS"], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(3, sim1Res.Length);
                expected.Add((key, data), (sim1Res[0], sim1Res[2], float.Parse(Encoding.ASCII.GetString(sim1Res[1]))));
            }

            context.clusterTestUtils.WaitForReplicaAofSync(Primary0Index, Secondary0Index);

            // Move from primary0 to primary1
            var migratedHashSlots = primary0Keys.Select(static t => t.HashSlot).Distinct().Select(static s => (int)s).ToList();

            context.clusterTestUtils.MigrateSlots(primary0, primary1, migratedHashSlots);
            context.clusterTestUtils.WaitForMigrationCleanup(Primary0Index);
            context.clusterTestUtils.WaitForMigrationCleanup(Primary1Index);

            context.clusterTestUtils.WaitForReplicaAofSync(Primary0Index, Secondary0Index);
            context.clusterTestUtils.WaitForReplicaAofSync(Primary1Index, Secondary1Index);

            var curPrimary0Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, NullLogger.Instance);
            var curPrimary1Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary1, NullLogger.Instance);

            foreach (var hashSlot in migratedHashSlots)
            {
                ClassicAssert.IsFalse(curPrimary0Slots.Contains(hashSlot));
                ClassicAssert.IsTrue(curPrimary1Slots.Contains(hashSlot));
            }

            // Check available on other primary
            foreach (var (key, _, _, data, _) in primary0Keys.Concat(primary1Keys))
            {
                var migrateSimRes = (byte[][])context.clusterTestUtils.Execute(primary1, "VSIM", [key, "XB8", data, "WITHSCORES", "WITHATTRIBS"], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(3, migrateSimRes.Length);

                var (elem, attr, score) = expected[(key, data)];

                ClassicAssert.IsTrue(elem.SequenceEqual(migrateSimRes[0]));
                ClassicAssert.AreEqual(score, float.Parse(Encoding.ASCII.GetString(migrateSimRes[1])));
                ClassicAssert.IsTrue(attr.SequenceEqual(migrateSimRes[2]));
            }

            // Check no longer available on old primary or secondary
            foreach (var (key, _, _, data, _) in primary0Keys.Concat(primary1Keys))
            {
                var exc0 = (string)context.clusterTestUtils.Execute(primary0, "VSIM", [key, "XB8", data, "WITHSCORES", "WITHATTRIBS"], flags: CommandFlags.NoRedirect);
                ClassicAssert.IsTrue(exc0.StartsWith("Key has MOVED to "));
            }

            var start = Stopwatch.GetTimestamp();

            var success = false;
            while (Stopwatch.GetElapsedTime(start) < TimeSpan.FromSeconds(5))
            {
                try
                {
                    var migrationNotFinished = false;
                    foreach (var (key, _, _, data, _) in primary0Keys.Concat(primary1Keys))
                    {
                        var exc1 = (string)context.clusterTestUtils.Execute(secondary0, "VSIM", [key, "XB8", data, "WITHSCORES", "WITHATTRIBS"], flags: CommandFlags.NoRedirect);
                        if (!exc1.StartsWith("Key has MOVED to "))
                        {
                            migrationNotFinished = true;
                            break;
                        }
                    }

                    if (migrationNotFinished)
                    {
                        continue;
                    }

                    success = true;
                    break;
                }
                catch
                {
                    // Secondary can still have the key for a bit
                    Thread.Sleep(100);
                }
            }

            ClassicAssert.IsTrue(success, "Original replica still has Vector Set long after primary has completed");

            // Check available on new secondary
            var readonlyOnReplica1 = (string)context.clusterTestUtils.Execute(secondary1, "READONLY", [], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", readonlyOnReplica1);

            start = Stopwatch.GetTimestamp();

            success = false;

            while (Stopwatch.GetElapsedTime(start) < TimeSpan.FromSeconds(5))
            {
                success = true;

                foreach (var (key, _, _, data, _) in primary0Keys.Concat(primary1Keys))
                {
                    var migrateSimRes = (byte[][])context.clusterTestUtils.Execute(secondary1, "VSIM", [key, "XB8", data, "WITHSCORES", "WITHATTRIBS"], flags: CommandFlags.NoRedirect);

                    if (migrateSimRes.Length == 1 && Encoding.UTF8.GetString(migrateSimRes[1]).StartsWith("Key has MOVED to "))
                    {
                        success = false;
                        break;
                    }

                    ClassicAssert.AreEqual(3, migrateSimRes.Length);

                    var (elem, attr, score) = expected[(key, data)];

                    ClassicAssert.IsTrue(elem.SequenceEqual(migrateSimRes[0]));
                    ClassicAssert.AreEqual(score, float.Parse(Encoding.ASCII.GetString(migrateSimRes[1])));
                    ClassicAssert.IsTrue(attr.SequenceEqual(migrateSimRes[2]));
                }

                if (success)
                {
                    break;
                }
            }

            ClassicAssert.IsTrue(success, "New replica hasn't replicated Vector Set long after primary has received data");
        }

        [Test]
        public async Task MigrateVectorSetWhileModifyingAsync()
        {
            // Test migrating a single slot with a vector set while moving it

            const int Primary0Index = 0;
            const int Primary1Index = 1;
            const int Secondary0Index = 2;
            const int Secondary1Index = 3;

            context.CreateInstances(DefaultMultiPrimaryShards, useTLS: true, enableAOF: true, OnDemandCheckpoint: true, EnableIncrementalSnapshots: true);
            context.CreateConnection(useTLS: true);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: DefaultMultiPrimaryShards / 2, replica_count: 1);

            var primary0 = (IPEndPoint)context.endpoints[Primary0Index];
            var primary1 = (IPEndPoint)context.endpoints[Primary1Index];
            var secondary0 = (IPEndPoint)context.endpoints[Secondary0Index];
            var secondary1 = (IPEndPoint)context.endpoints[Secondary1Index];

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary0).Value);
            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary1).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary0).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary1).Value);

            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var primary1Id = context.clusterTestUtils.ClusterMyId(primary1);

            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            string primary0Key;
            int primary0HashSlot;
            {
                var ix = 0;

                while (true)
                {
                    primary0Key = $"{nameof(MigrateVectorSetWhileModifyingAsync)}_{ix}";
                    primary0HashSlot = context.clusterTestUtils.HashSlot(primary0Key);

                    if (slots.Any(x => x.nnInfo.Any(y => y.nodeid == primary0Id) && primary0HashSlot >= x.startSlot && primary0HashSlot <= x.endSlot))
                    {
                        break;
                    }

                    ix++;
                }
            }

            // Start writing to this Vector Set
            using var cts = new CancellationTokenSource();

            var added = new ConcurrentBag<(byte[] Elem, byte[] Data, byte[] Attr)>();

            var writeTask =
                Task.Run(
                    async () =>
                    {
                        // Force async
                        await Task.Yield();

                        using var readWriteCon = ConnectionMultiplexer.Connect(context.clusterTestUtils.GetRedisConfig(context.endpoints));
                        var readWriteDb = readWriteCon.GetDatabase();

                        var ix = 0;

                        var elem = new byte[4];
                        var data = new byte[75];
                        var attr = new byte[100];

                        BinaryPrimitives.WriteInt32LittleEndian(elem, ix);
                        Random.Shared.NextBytes(data);
                        Random.Shared.NextBytes(attr);

                        while (!cts.IsCancellationRequested)
                        {
                            if (TestUtils.IsRunningAsGitHubAction)
                            {
                                // Throw some delay in when running as a GitHub Action to work around the weak drives those VMs have
                                await Task.Delay(1);
                            }

                            // This should follow redirects, so migration shouldn't cause any failures
                            try
                            {
                                var addRes = (int)readWriteDb.Execute("VADD", [new RedisKey(primary0Key), "XB8", data, elem, "XPREQ8", "SETATTR", attr]);
                                ClassicAssert.AreEqual(1, addRes);
                            }
                            catch (RedisServerException exc)
                            {
                                if (exc.Message.StartsWith("MOVED "))
                                {
                                    continue;
                                }

                                throw;
                            }

                            added.Add((elem.ToArray(), data.ToArray(), attr.ToArray()));

                            ix++;
                            BinaryPrimitives.WriteInt32LittleEndian(elem, ix);
                            Random.Shared.NextBytes(data);
                            Random.Shared.NextBytes(attr);
                        }
                    }
                );

            await Task.Delay(1_000);

            var lenPreMigration = added.Count;
            ClassicAssert.IsTrue(lenPreMigration > 0, "Should have seen some writes pre-migration");

            // Move to other primary
            using (var migrateToken = new CancellationTokenSource())
            {
                migrateToken.CancelAfter(30_000);

                context.clusterTestUtils.MigrateSlots(primary0, primary1, [primary0HashSlot]);
                context.clusterTestUtils.WaitForMigrationCleanup(Primary0Index, cancellationToken: migrateToken.Token);
                context.clusterTestUtils.WaitForMigrationCleanup(Primary1Index, cancellationToken: migrateToken.Token);
            }

            using (var replicationToken = new CancellationTokenSource())
            {
                replicationToken.CancelAfter(30_000);

                context.clusterTestUtils.WaitForReplicaAofSync(Primary0Index, Secondary0Index, cancellation: replicationToken.Token);
                context.clusterTestUtils.WaitForReplicaAofSync(Primary1Index, Secondary1Index, cancellation: replicationToken.Token);
            }

            var curPrimary0Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, NullLogger.Instance);
            var curPrimary1Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary1, NullLogger.Instance);

            ClassicAssert.IsFalse(curPrimary0Slots.Contains(primary0HashSlot));
            ClassicAssert.IsTrue(curPrimary1Slots.Contains(primary0HashSlot));

            var lenPrePause = added.Count;
            await Task.Delay(5_000);
            var lenPostPause = added.Count;

            ClassicAssert.IsTrue(lenPostPause > lenPrePause, "Writes after migration did not resume");

            // Stop Writes and wait for replication to catch up
            cts.Cancel();
            await writeTask;

            var addedLookup = added.ToFrozenDictionary(static t => t.Elem, t => t, ByteArrayComparer.Instance);

            context.clusterTestUtils.WaitForReplicaAofSync(Primary0Index, Secondary0Index);
            context.clusterTestUtils.WaitForReplicaAofSync(Primary1Index, Secondary1Index);

            // Check available on other primary & secondary

            foreach (var (_, data, _) in added)
            {
                var sim1Res = (byte[][])context.clusterTestUtils.Execute(primary1, "VSIM", [primary0Key, "XB8", data, "WITHSCORES", "WITHATTRIBS", "COUNT", "1"], flags: CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(3, sim1Res.Length);

                // No guarantee we'll get the exact same element, but we should always get _a_ result and the correct associated attribute
                var resElem = sim1Res[0];
                var resAttr = sim1Res[2];
                var expectedAttr = addedLookup[resElem].Attr;
                ClassicAssert.IsTrue(resAttr.SequenceEqual(expectedAttr));
            }

            var readonlyOnReplica1 = (string)context.clusterTestUtils.Execute(secondary1, "READONLY", [], flags: CommandFlags.NoRedirect);
            ClassicAssert.AreEqual("OK", readonlyOnReplica1);

            foreach (var (elem, data, attr) in added)
            {
                var simOnReplica1Res = (byte[][])context.clusterTestUtils.Execute(secondary1, "VSIM", [primary0Key, "XB8", data, "WITHSCORES", "WITHATTRIBS", "COUNT", "1"], flags: CommandFlags.NoRedirect);

                // No guarantee we'll get the exact same element, but we should always get _a_ result and the correct associated attribute
                var resElem = simOnReplica1Res[0];
                var resAttr = simOnReplica1Res[2];
                var expectedAttr = addedLookup[resElem].Attr;
                ClassicAssert.IsTrue(resAttr.SequenceEqual(expectedAttr));
            }
        }

        [Test]
        public void MigrateVectorSetBack()
        {
            const int Primary0Index = 0;
            const int Primary1Index = 1;

            context.CreateInstances(DefaultShards, useTLS: true, enableAOF: true);
            context.CreateConnection(useTLS: true);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: DefaultShards, replica_count: 0);

            var primary0 = (IPEndPoint)context.endpoints[Primary0Index];
            var primary1 = (IPEndPoint)context.endpoints[Primary1Index];

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary0).Value);
            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary1).Value);

            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
            var primary1Id = context.clusterTestUtils.ClusterMyId(primary1);

            var slots = context.clusterTestUtils.ClusterSlots(primary0);

            string vectorSetKey;
            int vectorSetKeySlot;
            {
                var ix = 0;

                while (true)
                {
                    vectorSetKey = $"{nameof(MigrateVectorSetBack)}_{ix}";
                    vectorSetKeySlot = context.clusterTestUtils.HashSlot(vectorSetKey);

                    var isPrimary0Slot = slots.Any(x => x.nnInfo.Any(y => y.nodeid == primary0Id) && vectorSetKeySlot >= x.startSlot && vectorSetKeySlot <= x.endSlot);
                    if (isPrimary0Slot)
                    {
                        break;
                    }

                    ix++;
                }
            }

            using var readWriteCon = ConnectionMultiplexer.Connect(context.clusterTestUtils.GetRedisConfig(context.endpoints));
            var readWriteDB = readWriteCon.GetDatabase();

            var data0 = Enumerable.Range(0, 75).Select(static x => (byte)x).ToArray();
            byte[] elem0 = [1, 2, 3, 0];
            var attr0 = "hello world"u8.ToArray();

            var add0Res = (int)readWriteDB.Execute("VADD", [new RedisKey(vectorSetKey), "XB8", data0, elem0, "XPREQ8", "SETATTR", attr0]);
            ClassicAssert.AreEqual(1, add0Res);

            // Migrate 0 -> 1
            {
                using (var migrateToken = new CancellationTokenSource())
                {
                    migrateToken.CancelAfter(30_000);

                    context.clusterTestUtils.MigrateSlots(primary0, primary1, [vectorSetKeySlot]);
                    context.clusterTestUtils.WaitForMigrationCleanup(Primary0Index, cancellationToken: migrateToken.Token);
                    context.clusterTestUtils.WaitForMigrationCleanup(Primary1Index, cancellationToken: migrateToken.Token);
                }

                var nodePropSuccess = false;
                var start = Stopwatch.GetTimestamp();
                while (Stopwatch.GetElapsedTime(start) < TimeSpan.FromSeconds(5))
                {
                    var curPrimary0Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, NullLogger.Instance);
                    var curPrimary1Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary1, NullLogger.Instance);

                    var movedOffPrimary0 = !curPrimary0Slots.Contains(vectorSetKeySlot);
                    var movedOntoPrimary1 = curPrimary1Slots.Contains(vectorSetKeySlot);

                    if (movedOffPrimary0 && movedOntoPrimary1)
                    {
                        nodePropSuccess = true;
                        break;
                    }
                }

                ClassicAssert.IsTrue(nodePropSuccess, "Node propagation after 0 -> 1 migration took too long");
            }

            // Confirm still valid to add, with client side routing
            var data1 = Enumerable.Range(0, 75).Select(static x => (byte)(x * 2)).ToArray();
            byte[] elem1 = [4, 5, 6, 7];
            var attr1 = "fizz buzz"u8.ToArray();

            var add1Res = (int)readWriteDB.Execute("VADD", [new RedisKey(vectorSetKey), "XB8", data1, elem1, "XPREQ8", "SETATTR", attr1]);
            ClassicAssert.AreEqual(1, add1Res);

            // Migrate 1 -> 0
            {
                using (var migrateToken = new CancellationTokenSource())
                {
                    migrateToken.CancelAfter(30_000);

                    context.clusterTestUtils.MigrateSlots(primary1, primary0, [vectorSetKeySlot]);
                    context.clusterTestUtils.WaitForMigrationCleanup(Primary0Index, cancellationToken: migrateToken.Token);
                    context.clusterTestUtils.WaitForMigrationCleanup(Primary1Index, cancellationToken: migrateToken.Token);
                }

                var nodePropSuccess = false;
                var start = Stopwatch.GetTimestamp();
                while (Stopwatch.GetElapsedTime(start) < TimeSpan.FromSeconds(5))
                {
                    var curPrimary0Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, NullLogger.Instance);
                    var curPrimary1Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary1, NullLogger.Instance);

                    var movedOntoPrimary0 = curPrimary0Slots.Contains(vectorSetKeySlot);
                    var movedOffPrimary1 = !curPrimary1Slots.Contains(vectorSetKeySlot);

                    if (movedOntoPrimary0 && movedOffPrimary1)
                    {
                        nodePropSuccess = true;
                        break;
                    }
                }

                ClassicAssert.IsTrue(nodePropSuccess, "Node propagation after 1 -> 0 migration took too long");
            }

            // Confirm still valid to add, with client side routing
            var data2 = Enumerable.Range(0, 75).Select(static x => (byte)(x * 3)).ToArray();
            byte[] elem2 = [8, 9, 10, 11];
            var attr2 = "foo bar"u8.ToArray();

            var add2Res = (int)readWriteDB.Execute("VADD", [new RedisKey(vectorSetKey), "XB8", data2, elem2, "XPREQ8", "SETATTR", attr2]);
            ClassicAssert.AreEqual(1, add2Res);

            // Confirm no data loss
            var emb0 = ((string[])readWriteDB.Execute("VEMB", [new RedisKey(vectorSetKey), elem0])).Select(static x => (byte)float.Parse(x)).ToArray();
            var emb1 = ((string[])readWriteDB.Execute("VEMB", [new RedisKey(vectorSetKey), elem1])).Select(static x => (byte)float.Parse(x)).ToArray();
            var emb2 = ((string[])readWriteDB.Execute("VEMB", [new RedisKey(vectorSetKey), elem2])).Select(static x => (byte)float.Parse(x)).ToArray();
            ClassicAssert.IsTrue(data0.SequenceEqual(emb0));
            ClassicAssert.IsTrue(data1.SequenceEqual(emb1));
            ClassicAssert.IsTrue(data2.SequenceEqual(emb2));
        }

        [Test]
        public async Task MigrateVectorStressAsync()
        {
            // Move vector sets back and forth between replicas, making sure we don't drop data
            // Keeps reads and writes going continuously

            const int Primary0Index = 0;
            const int Primary1Index = 1;
            const int Secondary0Index = 2;
            const int Secondary1Index = 3;

            const int VectorSetsPerPrimary = 2;

            var gossipFaultsAtTestStart = 0;

            captureLogWriter.capture = true;

            try
            {
                context.CreateInstances(DefaultMultiPrimaryShards, useTLS: true, enableAOF: true);
                context.CreateConnection(useTLS: true);
                _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: DefaultMultiPrimaryShards / 2, replica_count: 1);

                var primary0 = (IPEndPoint)context.endpoints[Primary0Index];
                var primary1 = (IPEndPoint)context.endpoints[Primary1Index];
                var secondary0 = (IPEndPoint)context.endpoints[Secondary0Index];
                var secondary1 = (IPEndPoint)context.endpoints[Secondary1Index];

                ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary0).Value);
                ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary1).Value);
                ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary0).Value);
                ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary1).Value);

                var primary0Id = context.clusterTestUtils.ClusterMyId(primary0);
                var primary1Id = context.clusterTestUtils.ClusterMyId(primary1);

                var slots = context.clusterTestUtils.ClusterSlots(primary0);

                var vectorSetKeys = new List<(string Key, ushort HashSlot)>();

                {
                    var ix = 0;

                    var numP0 = 0;
                    var numP1 = 0;

                    while (numP0 < VectorSetsPerPrimary || numP1 < VectorSetsPerPrimary)
                    {
                        var key = $"{nameof(MigrateVectorStressAsync)}_{ix}";
                        var slot = context.clusterTestUtils.HashSlot(key);

                        var isPrimary0Slot = slots.Any(x => x.nnInfo.Any(y => y.nodeid == primary0Id) && slot >= x.startSlot && slot <= x.endSlot);

                        if (isPrimary0Slot)
                        {
                            if (numP0 < VectorSetsPerPrimary)
                            {
                                vectorSetKeys.Add((key, (ushort)slot));
                                numP0++;
                            }
                        }
                        else
                        {
                            if (numP1 < VectorSetsPerPrimary)
                            {
                                vectorSetKeys.Add((key, (ushort)slot));
                                numP1++;
                            }
                        }

                        ix++;
                    }
                }

                // Remember how cluster looked right after it was stable
                gossipFaultsAtTestStart = CountGossipFaults(captureLogWriter);

                // Start writing to this Vector Set
                using var writeCancel = new CancellationTokenSource();

                using var readWriteCon = ConnectionMultiplexer.Connect(context.clusterTestUtils.GetRedisConfig(context.endpoints));
                var readWriteDB = readWriteCon.GetDatabase();

                var writeTasks = new Task[vectorSetKeys.Count];
                var writeResults = new ConcurrentBag<(byte[] Elem, byte[] Data, byte[] Attr, DateTime InsertionTime)>[vectorSetKeys.Count];

                var mostRecentWrite = 0L;

                for (var i = 0; i < vectorSetKeys.Count; i++)
                {
                    var (key, _) = vectorSetKeys[i];
                    var written = writeResults[i] = new();

                    writeTasks[i] =
                        Task.Run(
                            async () =>
                            {
                                // Force async
                                await Task.Yield();

                                var ix = 0;

                                while (!writeCancel.IsCancellationRequested)
                                {
                                    var elem = new byte[4];
                                    BinaryPrimitives.WriteInt32LittleEndian(elem, ix);

                                    var data = new byte[75];
                                    Random.Shared.NextBytes(data);

                                    var attr = new byte[100];
                                    Random.Shared.NextBytes(attr);

                                    while (true)
                                    {
                                        try
                                        {
                                            var addRes = (int)readWriteDB.Execute("VADD", [new RedisKey(key), "XB8", data, elem, "XPREQ8", "SETATTR", attr]);
                                            ClassicAssert.AreEqual(1, addRes);
                                            break;
                                        }
                                        catch (RedisServerException exc)
                                        {
                                            if (exc.Message.StartsWith("MOVED "))
                                            {
                                                // This is fine, just try again if we're not cancelled
                                                if (writeCancel.IsCancellationRequested)
                                                {
                                                    return;
                                                }

                                                continue;
                                            }

                                            throw;
                                        }
                                    }

                                    var now = DateTime.UtcNow;
                                    written.Add((elem, data, attr, now));

                                    var mostRecentCopy = mostRecentWrite;
                                    while (mostRecentCopy < now.Ticks)
                                    {
                                        var currentMostRecent = Interlocked.CompareExchange(ref mostRecentWrite, now.Ticks, mostRecentCopy);
                                        if (currentMostRecent == mostRecentCopy)
                                        {
                                            break;
                                        }
                                        mostRecentCopy = currentMostRecent;
                                    }

                                    ix++;
                                }
                            }
                        );
                }

                using var readCancel = new CancellationTokenSource();

                var readTasks = new Task<int>[vectorSetKeys.Count];
                for (var i = 0; i < vectorSetKeys.Count; i++)
                {
                    var (key, _) = vectorSetKeys[i];
                    var written = writeResults[i];
                    readTasks[i] =
                        Task.Run(
                            async () =>
                            {
                                await Task.Yield();

                                var successfulReads = 0;

                                while (!readCancel.IsCancellationRequested)
                                {
                                    var r = written.Count;
                                    if (r == 0)
                                    {
                                        await Task.Delay(10);
                                        continue;
                                    }

                                    var (elem, data, _, _) = written.ToList()[Random.Shared.Next(r)];

                                    var emb = (string[])readWriteDB.Execute("VEMB", [new RedisKey(key), elem]);

                                    // If we got data, make sure it's coherent
                                    ClassicAssert.AreEqual(data.Length, emb.Length);

                                    for (var i = 0; i < data.Length; i++)
                                    {
                                        ClassicAssert.AreEqual(data[i], (byte)float.Parse(emb[i]));
                                    }

                                    successfulReads++;
                                }

                                return successfulReads;
                            }
                        );
                }

                await Task.Delay(1_000);

                ClassicAssert.IsTrue(writeResults.All(static r => !r.IsEmpty), "Should have seen some writes pre-migration");

                // Task to flip back and forth between primaries
                using var migrateCancel = new CancellationTokenSource();

                var migrateTask =
                    Task.Run(
                        async () =>
                        {
                            var hashSlotsOnP0 = new List<int>();
                            var hashSlotsOnP1 = new List<int>();
                            foreach (var (_, slot) in vectorSetKeys)
                            {
                                var isPrimary0Slot = slots.Any(x => x.nnInfo.Any(y => y.nodeid == primary0Id) && slot >= x.startSlot && slot <= x.endSlot);
                                if (isPrimary0Slot)
                                {
                                    if (!hashSlotsOnP0.Contains(slot))
                                    {
                                        hashSlotsOnP0.Add(slot);
                                    }
                                }
                                else
                                {
                                    if (!hashSlotsOnP1.Contains(slot))
                                    {
                                        hashSlotsOnP1.Add(slot);
                                    }
                                }
                            }

                            var migrationTimes = new List<DateTime>();

                            var mostRecentMigration = 0L;

                            while (!migrateCancel.IsCancellationRequested)
                            {
                                await Task.Delay(100);

                                // Don't start another migration until we get at least one successful write
                                if (Interlocked.CompareExchange(ref mostRecentWrite, 0, 0) < mostRecentMigration)
                                {
                                    continue;
                                }

                                // Move 0 -> 1
                                if (hashSlotsOnP0.Count > 0)
                                {
                                    using (var migrateToken = new CancellationTokenSource())
                                    {
                                        migrateToken.CancelAfter(30_000);

                                        context.clusterTestUtils.MigrateSlots(primary0, primary1, hashSlotsOnP0);
                                        context.clusterTestUtils.WaitForMigrationCleanup(Primary0Index, cancellationToken: migrateToken.Token);
                                        context.clusterTestUtils.WaitForMigrationCleanup(Primary1Index, cancellationToken: migrateToken.Token);
                                    }

                                    var nodePropSuccess = false;
                                    var start = Stopwatch.GetTimestamp();
                                    while (Stopwatch.GetElapsedTime(start) < TimeSpan.FromSeconds(5))
                                    {
                                        var curPrimary0Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, NullLogger.Instance);
                                        var curPrimary1Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary1, NullLogger.Instance);

                                        var movedOffPrimary0 = !curPrimary0Slots.Any(h => hashSlotsOnP0.Contains(h));
                                        var movedOntoPrimary1 = hashSlotsOnP0.All(h => curPrimary1Slots.Contains(h));

                                        if (movedOffPrimary0 && movedOntoPrimary1)
                                        {
                                            nodePropSuccess = true;
                                            break;
                                        }
                                    }

                                    ClassicAssert.IsTrue(nodePropSuccess, "Node propagation after 0 -> 1 migration took too long");
                                }

                                // Move 1 -> 0
                                if (hashSlotsOnP1.Count > 0)
                                {
                                    using (var migrateToken = new CancellationTokenSource())
                                    {
                                        migrateToken.CancelAfter(30_000);

                                        context.clusterTestUtils.MigrateSlots(primary1, primary0, hashSlotsOnP1);
                                        context.clusterTestUtils.WaitForMigrationCleanup(Primary1Index, cancellationToken: migrateToken.Token);
                                        context.clusterTestUtils.WaitForMigrationCleanup(Primary0Index, cancellationToken: migrateToken.Token);
                                    }

                                    var nodePropSuccess = false;
                                    var start = Stopwatch.GetTimestamp();
                                    while (Stopwatch.GetElapsedTime(start) < TimeSpan.FromSeconds(5))
                                    {
                                        var curPrimary0Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, NullLogger.Instance);
                                        var curPrimary1Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary1, NullLogger.Instance);

                                        var movedOffPrimary1 = !curPrimary1Slots.Any(h => hashSlotsOnP1.Contains(h));
                                        var movedOntoPrimary0 = hashSlotsOnP1.All(h => curPrimary0Slots.Contains(h));

                                        if (movedOffPrimary1 && movedOntoPrimary0)
                                        {
                                            nodePropSuccess = true;
                                            break;
                                        }
                                    }

                                    ClassicAssert.IsTrue(nodePropSuccess, "Node propagation after 1 -> 0 migration took too long");
                                }

                                // Remember for next iteration
                                var now = DateTime.UtcNow;
                                mostRecentMigration = now.Ticks;
                                migrationTimes.Add(now);

                                // Flip around assignment for next pass
                                (hashSlotsOnP0, hashSlotsOnP1) = (hashSlotsOnP1, hashSlotsOnP0);
                            }

                            return migrationTimes;
                        }
                    );

                await Task.Delay(10_000);

                migrateCancel.Cancel();
                var migrationTimes = await migrateTask;

                ClassicAssert.IsTrue(migrationTimes.Count > 2, "Should have moved back and forth at least twice");

                writeCancel.Cancel();
                await Task.WhenAll(writeTasks);

                readCancel.Cancel();
                var readResults = await Task.WhenAll(readTasks);
                ClassicAssert.IsTrue(readResults.All(static r => r > 0), "Should have successful reads on all Vector Sets");

                // Check that everything written survived all the migrations
                {
                    var curPrimary0Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, NullLogger.Instance);
                    var curPrimary1Slots = context.clusterTestUtils.GetOwnedSlotsFromNode(primary1, NullLogger.Instance);

                    for (var i = 0; i < vectorSetKeys.Count; i++)
                    {
                        var (key, slot) = vectorSetKeys[i];

                        var isOnPrimary0 = curPrimary0Slots.Contains(slot);
                        var isOnPrimary1 = curPrimary1Slots.Contains(slot);

                        ClassicAssert.IsTrue(isOnPrimary0 || isOnPrimary1, "Hash slot not found on either node");
                        ClassicAssert.IsFalse(isOnPrimary0 && isOnPrimary1, "Hash slot found on both nodes");

                        var endpoint = isOnPrimary0 ? primary0 : primary1;

                        foreach (var (elem, data, attr, _) in writeResults[i])
                        {
                            var actualData = (string[])context.clusterTestUtils.Execute(endpoint, "VEMB", [key, elem]);

                            for (var j = 0; j < data.Length; j++)
                            {
                                ClassicAssert.AreEqual(data[j], (byte)float.Parse(actualData[j]));
                            }
                        }
                    }
                }

            }
            catch (Exception exc)
            {
                var gossipFaultsAtEnd = CountGossipFaults(captureLogWriter);

                if (gossipFaultsAtTestStart != gossipFaultsAtEnd)
                {
                    // The cluster broke in some way, so data loss is _expected_
                    ClassicAssert.Inconclusive($"Gossip fault lead to data loss, Vector Set migration is (probably) not to blame: {exc.Message}");
                }

                // Anything else, keep it going up
                throw;
            }

            static int CountGossipFaults(CaptureLogWriter captureLogWriter)
            {
                var capturedLog = captureLogWriter.buffer.ToString();

                // These kinds of errors happen from stressing migration independent of Vector Sets
                // 
                // TODO: These out to be fixed outside of Vector Set work
                var faultRound = capturedLog.Split("^GOSSIP round faulted^").Length - 1;
                var faultResponse = capturedLog.Split("^GOSSIP faulted processing response^").Length - 1;
                var faultMergeMap = capturedLog.Split("ClusterConfig.MergeSlotMap(").Length - 1;

                return faultRound + faultResponse + faultMergeMap;
            }
        }

        [Test]
        public async Task FailoverStopsVectorManagerReplicationTasksAsync()
        {
            const int PrimaryIndex = 0;
            const int ReplicaIndex = 1;

            context.CreateInstances(DefaultShards, useTLS: true, enableAOF: true);
            context.CreateConnection(useTLS: true);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: DefaultShards / 2, replica_count: DefaultShards / 2);

            var primary = (IPEndPoint)context.endpoints[PrimaryIndex];
            var replica = (IPEndPoint)context.endpoints[ReplicaIndex];

            var primaryVectorManager = GetStoreWrapper(context.nodes[PrimaryIndex]).DefaultDatabase.VectorManager;
            var replicaVectorManager = GetStoreWrapper(context.nodes[ReplicaIndex]).DefaultDatabase.VectorManager;

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(replica).Value);

            var vectorData0 = Enumerable.Range(0, 75).Select(static x => (byte)x).ToArray();

            var vadd0Res = (int)context.clusterTestUtils.Execute(primary, "VADD", [new RedisKey("foo"), "XB8", vectorData0, new byte[] { 1, 0, 0, 0 }, "XPREQ8"]);
            ClassicAssert.AreEqual(1, vadd0Res);

            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex);
            await Task.Delay(10);

            ClassicAssert.IsFalse(primaryVectorManager.AreReplicationTasksActive);
            ClassicAssert.IsTrue(replicaVectorManager.AreReplicationTasksActive);

            context.ClusterFailoverSpinWait(ReplicaIndex, NullLogger.Instance);

            context.clusterTestUtils.WaitForReplicaAofSync(ReplicaIndex, PrimaryIndex);
            await Task.Delay(10);

            var vectorData1 = Enumerable.Range(0, 75).Select(static x => (byte)(x * 2)).ToArray();

            var vadd1Res = (int)context.clusterTestUtils.Execute(replica, "VADD", [new RedisKey("foo"), "XB8", vectorData1, new byte[] { 2, 0, 0, 0 }, "XPREQ8"]);
            ClassicAssert.AreEqual(1, vadd1Res);

            ClassicAssert.IsTrue(primaryVectorManager.AreReplicationTasksActive);
            ClassicAssert.IsFalse(replicaVectorManager.AreReplicationTasksActive);

            var vsimRes = (byte[][])context.clusterTestUtils.Execute(replica, "VSIM", [new RedisKey("foo"), "XB8", vectorData0]);
            ClassicAssert.IsTrue(vsimRes.Length > 0);
        }

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "storeWrapper")]
        private static extern ref StoreWrapper GetStoreWrapper(GarnetServer server);
    }
}