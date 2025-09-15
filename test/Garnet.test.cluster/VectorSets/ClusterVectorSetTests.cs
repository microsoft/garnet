// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterVectorSetTests
    {
        private const int DefaultShards = 2;

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
        public void BasicVADDReplicates()
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

            var addRes = (int)context.clusterTestUtils.Execute(primary, "VADD", ["foo", "XB8", new byte[] { 1, 2, 3, 4 }, new byte[] { 0, 0, 0, 0 }, "XPREQ8"]);
            ClassicAssert.AreEqual(1, addRes);

            var simRes = (byte[][])context.clusterTestUtils.Execute(primary, "VSIM", ["foo", "XB8", new byte[] { 2, 3, 4, 5 }]);
            ClassicAssert.IsTrue(simRes.Length > 0);

            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, SecondaryIndex);

            var readonlyOnReplica = (string)context.clusterTestUtils.Execute(secondary, "READONLY", []);
            ClassicAssert.AreEqual("OK", readonlyOnReplica);

            var simOnReplica = context.clusterTestUtils.Execute(secondary, "VSIM", ["foo", "XB8", new byte[] { 2, 3, 4, 5 }]);
            ClassicAssert.IsTrue(simOnReplica.Length > 0);
        }

        [Test]
        public async Task ConcurrentVADDReplicatedVSimsAsync()
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
                    vectors[i] = new byte[64];
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

            var readTask =
                Task.Run(
                    async () =>
                    {
                        var r = new Random(2025_09_15_01);

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

            _ = sync.Release(2);
            await writeTask;

            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, SecondaryIndex);

            cts.CancelAfter(TimeSpan.FromSeconds(1));

            var searchesWithNonZeroResults = await readTask;

            ClassicAssert.IsTrue(searchesWithNonZeroResults > 0);
        }
    }
}
