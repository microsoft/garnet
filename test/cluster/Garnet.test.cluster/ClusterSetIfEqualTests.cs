// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterSetIfEqualTests : TestBase
    {
        private ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown() => context?.TearDown();

        [Test]
        [Category("CLUSTER")]
        public async Task SetIfEqualReplicatesAndSurvivesFailover()
        {
            CreateCluster(1, 1);
            const string Key = "ifeq-replication";
            Assert.That((string)Execute(0, "SET", Key, "old", "EX", 300), Is.EqualTo("OK"));
            long expiration = (long)Execute(0, "PEXPIRETIME", Key);
            Assert.That((string)Execute(0, "SET", Key, "new", "IFEQ", "old", "KEEPTTL"), Is.EqualTo("OK"));
            Assert.That(Execute(0, "SET", Key, "rejected", "IFEQ", "old").IsNull, Is.True);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger, context.cts.Token);

            using (GarnetClientSession replica = context.clusterTestUtils.CreateGarnetClientSession(1))
            {
                replica.Connect();
                Assert.That(await replica.ExecuteAsync("READONLY"), Is.EqualTo("OK"));
                Assert.That(await replica.ExecuteAsync("GET", Key), Is.EqualTo("new"));
                int slot = ClusterTestUtils.HashSlot(Encoding.ASCII.GetBytes(Key));
                Exception exception = Assert.ThrowsAsync<Exception>(async () =>
                    await replica.ExecuteAsync("SET", Key, "rejected", "IFEQ", "new").WaitAsync(context.cts.Token));
                Assert.That(exception.Message, Is.EqualTo($"MOVED {slot} {context.clusterTestUtils.GetEndPoint(0)}"));
            }

            Assert.That(context.clusterTestUtils.ClusterFailover(1, logger: context.logger), Is.EqualTo("OK"));
            context.clusterTestUtils.WaitForNoFailover(1, context.logger);
            context.clusterTestUtils.WaitForFailoverCompleted(1, context.logger);
            context.clusterTestUtils.WaitForReplicaRecovery(0, context.logger);
            context.clusterTestUtils.WaitForPrimaryRole(1, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(1, 0, context.logger, context.cts.Token);
            string primaryId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            context.clusterTestUtils.WaitForSlotOwnership(0, primaryId, [0, 16383], context.logger);

            Assert.That((string)Execute(1, "GET", Key), Is.EqualTo("new"));
            Assert.That((long)Execute(1, "PEXPIRETIME", Key), Is.EqualTo(expiration));
            Assert.That(Execute(1, "SET", Key, "rejected", "IFEQ", "old").IsNull, Is.True);
            Assert.That((string)Execute(1, "SET", Key, "final", "IFEQ", "new", "GET"), Is.EqualTo("new"));
            Assert.That((string)Execute(1, "GET", Key), Is.EqualTo("final"));
            Assert.That((long)Execute(1, "PTTL", Key), Is.EqualTo(-1));
        }

        [Test]
        [Category("CLUSTER")]
        public async Task SetIfEqualRoutesAndMigrates()
        {
            CreateCluster(2);
            const string Key = "{b}ifeq";
            const string Missing = "{b}missing";
            int slot = ClusterTestUtils.HashSlot(Encoding.ASCII.GetBytes(Key));
            Assert.That(slot, Is.LessThan(8192));
            Assert.That((string)Execute(0, "SET", Key, "old", "EX", 300), Is.EqualTo("OK"));
            long expiration = (long)Execute(0, "PEXPIRETIME", Key);
            string sourceId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            string targetId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            string moved = $"MOVED {slot} {context.clusterTestUtils.GetEndPoint(0)}";
            string ask = $"ASK {slot} {context.clusterTestUtils.GetEndPoint(1)}";

            AssertRedirect(1, Key, "old", moved);
            Assert.That(context.clusterTestUtils.SetSlot(1, slot, "IMPORTING", sourceId), Is.EqualTo("OK"));
            Assert.That(context.clusterTestUtils.SetSlot(0, slot, "MIGRATING", targetId), Is.EqualTo("OK"));
            Assert.That((string)Execute(0, "SET", Key, "new", "GET", "IFEQ", "old", "KEEPTTL"), Is.EqualTo("old"));
            AssertRedirect(0, Missing, "", ask);
            AssertRedirect(1, Missing, "", moved);

            using (GarnetClientSession importer = context.clusterTestUtils.CreateGarnetClientSession(1))
            {
                importer.Connect();
                Assert.That(await importer.ExecuteAsync("ASKING"), Is.EqualTo("OK"));
                Assert.That(await importer.ExecuteAsync("SET", Missing, "rejected", "IFEQ", "").WaitAsync(context.cts.Token), Is.Null);
                Exception exception = Assert.ThrowsAsync<Exception>(async () =>
                    await importer.ExecuteAsync("SET", Missing, "rejected", "IFEQ", "").WaitAsync(context.cts.Token));
                Assert.That(exception.Message, Is.EqualTo(moved));
            }

            context.clusterTestUtils.MigrateKeys(context.clusterTestUtils.GetEndPoint(0), context.clusterTestUtils.GetEndPoint(1),
                [Encoding.ASCII.GetBytes(Key)], context.logger);
            context.clusterTestUtils.WaitForMigrationCleanup(0, context.cts.Token, context.logger);
            Assert.That(context.clusterTestUtils.SetSlot(1, slot, "NODE", targetId), Is.EqualTo("OK"));
            Assert.That(context.clusterTestUtils.SetSlot(0, slot, "NODE", targetId), Is.EqualTo("OK"));
            Assert.That((string)Execute(1, "GET", Key), Is.EqualTo("new"));
            Assert.That((long)Execute(1, "PEXPIRETIME", Key), Is.EqualTo(expiration));
            Assert.That(Execute(1, "SET", Key, "rejected", "IFEQ", "old").IsNull, Is.True);
            Assert.That((string)Execute(1, "SET", Key, "final", "IFEQ", "new", "KEEPTTL"), Is.EqualTo("OK"));
            Assert.That((string)Execute(1, "GET", Key), Is.EqualTo("final"));
            Assert.That((long)Execute(1, "PEXPIRETIME", Key), Is.EqualTo(expiration));
            AssertRedirect(0, Key, "final", $"MOVED {slot} {context.clusterTestUtils.GetEndPoint(1)}");
        }

        private void CreateCluster(int primaryCount, int replicaCount = 0)
        {
            context.CreateInstances(primaryCount * (replicaCount + 1), enableAOF: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(primaryCount, replicaCount, logger: context.logger);
        }

        private RedisResult Execute(int nodeIndex, string command, params object[] arguments)
            => context.clusterTestUtils.GetServer(nodeIndex).Execute(0, command, arguments, CommandFlags.NoRedirect);

        private void AssertRedirect(int nodeIndex, string key, string expected, string redirect)
        {
            using GarnetClientSession client = context.clusterTestUtils.CreateGarnetClientSession(nodeIndex);
            client.Connect();
            Exception exception = Assert.ThrowsAsync<Exception>(async () =>
                await client.ExecuteAsync("SET", key, "rejected", "IFEQ", expected).WaitAsync(context.cts.Token));
            Assert.That(exception.Message, Is.EqualTo(redirect));
        }
    }
}