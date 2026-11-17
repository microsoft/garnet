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
        public async Task SetValueConditionalReplicatesAndSurvivesFailover([Values("IFEQ", "IFNE", "IFDEQ", "IFDNE")] string condition)
        {
            CreateCluster(1, 1);
            const string Key = "ifeq-replication";
            Assert.That((string)Execute(0, "SET", Key, "old", "EX", 300), Is.EqualTo("OK"));
            long expiration = (long)Execute(0, "PEXPIRETIME", Key);
            Assert.That((string)Execute(0, "SET", Key, "new", condition, ComparisonFor(condition, condition is "IFEQ" or "IFDEQ" ? "old" : "different"), "KEEPTTL"), Is.EqualTo("OK"));
            Assert.That(Execute(0, "SET", Key, "rejected", condition, ComparisonFor(condition, condition is "IFEQ" or "IFDEQ" ? "old" : "new")).IsNull, Is.True);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger, context.cts.Token);

            using (GarnetClientSession replica = context.clusterTestUtils.CreateGarnetClientSession(1))
            {
                replica.Connect();
                Assert.That(await replica.ExecuteAsync("READONLY"), Is.EqualTo("OK"));
                Assert.That(await replica.ExecuteAsync("GET", Key), Is.EqualTo("new"));
                Assert.That(await replica.ExecuteAsync("DIGEST", Key), Is.EqualTo(ComparisonFor("IFDEQ", "new")));
                int slot = ClusterTestUtils.HashSlot(Encoding.ASCII.GetBytes(Key));
                Exception exception = Assert.ThrowsAsync<Exception>(async () =>
                    await replica.ExecuteAsync("SET", Key, "rejected", condition, "new").WaitAsync(context.cts.Token));
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
            Assert.That(Execute(1, "SET", Key, "rejected", condition, ComparisonFor(condition, condition is "IFEQ" or "IFDEQ" ? "old" : "new")).IsNull, Is.True);
            Assert.That((string)Execute(1, "SET", Key, "final", condition, ComparisonFor(condition, condition is "IFEQ" or "IFDEQ" ? "new" : "old"), "GET"), Is.EqualTo("new"));
            Assert.That((string)Execute(1, "GET", Key), Is.EqualTo("final"));
            Assert.That((long)Execute(1, "PTTL", Key), Is.EqualTo(-1));
        }

        [Test]
        [Category("CLUSTER")]
        public async Task SetValueConditionalRoutesAndMigrates([Values("IFEQ", "IFNE", "IFDEQ", "IFDNE")] string condition)
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
            AssertDigestRedirect(1, Key, moved);
            Assert.That(context.clusterTestUtils.SetSlot(1, slot, "IMPORTING", sourceId), Is.EqualTo("OK"));
            Assert.That(context.clusterTestUtils.SetSlot(0, slot, "MIGRATING", targetId), Is.EqualTo("OK"));
            Assert.That((string)Execute(0, "SET", Key, "new", "GET", condition, ComparisonFor(condition, condition is "IFEQ" or "IFDEQ" ? "old" : "different"), "KEEPTTL"), Is.EqualTo("old"));
            AssertRedirect(0, Missing, "", ask);
            AssertRedirect(1, Missing, "", moved);
            AssertDigestRedirect(0, Missing, ask);

            using (GarnetClientSession importer = context.clusterTestUtils.CreateGarnetClientSession(1))
            {
                importer.Connect();
                Assert.That(await importer.ExecuteAsync("ASKING"), Is.EqualTo("OK"));
                Assert.That(await importer.ExecuteAsync("SET", Missing, "rejected", "IFEQ", "").WaitAsync(context.cts.Token), Is.Null);
                Exception exception = Assert.ThrowsAsync<Exception>(async () =>
                    await importer.ExecuteAsync("SET", Missing, "rejected", "IFEQ", "").WaitAsync(context.cts.Token));
                Assert.That(exception.Message, Is.EqualTo(moved));
                Assert.That(await importer.ExecuteAsync("ASKING"), Is.EqualTo("OK"));
                Assert.That(await importer.ExecuteAsync("SET", "{b}created", "new", "IFNE", ""), Is.EqualTo("OK"));
                Assert.That(await importer.ExecuteAsync("ASKING"), Is.EqualTo("OK"));
                Assert.That(await importer.ExecuteAsync("DIGEST", Missing), Is.Null);
            }

            context.clusterTestUtils.MigrateKeys(context.clusterTestUtils.GetEndPoint(0), context.clusterTestUtils.GetEndPoint(1),
                [Encoding.ASCII.GetBytes(Key)], context.logger);
            context.clusterTestUtils.WaitForMigrationCleanup(0, context.cts.Token, context.logger);
            Assert.That(context.clusterTestUtils.SetSlot(1, slot, "NODE", targetId), Is.EqualTo("OK"));
            Assert.That(context.clusterTestUtils.SetSlot(0, slot, "NODE", targetId), Is.EqualTo("OK"));
            Assert.That((string)Execute(1, "GET", Key), Is.EqualTo("new"));
            Assert.That((string)Execute(1, "DIGEST", Key), Is.EqualTo(ComparisonFor("IFDEQ", "new")));
            Assert.That((long)Execute(1, "PEXPIRETIME", Key), Is.EqualTo(expiration));
            Assert.That(Execute(1, "SET", Key, "rejected", condition, ComparisonFor(condition, condition is "IFEQ" or "IFDEQ" ? "old" : "new")).IsNull, Is.True);
            Assert.That((string)Execute(1, "SET", Key, "final", condition, ComparisonFor(condition, condition is "IFEQ" or "IFDEQ" ? "new" : "old"), "KEEPTTL"), Is.EqualTo("OK"));
            Assert.That((string)Execute(1, "GET", Key), Is.EqualTo("final"));
            Assert.That((long)Execute(1, "PEXPIRETIME", Key), Is.EqualTo(expiration));
            AssertRedirect(0, Key, "final", $"MOVED {slot} {context.clusterTestUtils.GetEndPoint(1)}");
            AssertDigestRedirect(0, Key, $"MOVED {slot} {context.clusterTestUtils.GetEndPoint(1)}");
        }

        [Test]
        [Category("CLUSTER")]
        public async Task DeleteValueConditionalReplicatesAndSurvivesFailover([Values("DELIFEQ", "IFEQ", "IFNE", "IFDEQ", "IFDNE")] string condition)
        {
            CreateCluster(1, 1);
            Execute(0, "SET", "deleted", "old");
            Execute(0, "SET", "kept", "old");
            Assert.That(Delete(0, "deleted", condition, condition is "IFNE" or "IFDNE" ? "different" : "old"), Is.EqualTo(1));
            Assert.That(Delete(0, "kept", condition, condition is "IFNE" or "IFDNE" ? "old" : "different"), Is.Zero);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger, context.cts.Token);
            using (GarnetClientSession replica = context.clusterTestUtils.CreateGarnetClientSession(1))
            {
                replica.Connect();
                await replica.ExecuteAsync("READONLY");
                Assert.That(await replica.ExecuteAsync("GET", "deleted"), Is.Null);
                Assert.That(await replica.ExecuteAsync("GET", "kept"), Is.EqualTo("old"));
                int slot = ClusterTestUtils.HashSlot(Encoding.ASCII.GetBytes("kept"));
                Exception exception = Assert.ThrowsAsync<Exception>(async () =>
                    await replica.ExecuteAsync(DeleteRequest("kept", condition, "old")).WaitAsync(context.cts.Token));
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
            Assert.That(Execute(1, "GET", "deleted").IsNull, Is.True);
            Assert.That((string)Execute(1, "GET", "kept"), Is.EqualTo("old"));
            Assert.That(Delete(1, "kept", condition, condition is "IFNE" or "IFDNE" ? "different" : "old"), Is.EqualTo(1));
            Assert.That(Execute(1, "GET", "kept").IsNull, Is.True);
        }

        [Test]
        [Category("CLUSTER")]
        public async Task DeleteValueConditionalRoutesAndMigrates([Values("DELIFEQ", "IFEQ", "IFNE", "IFDEQ", "IFDNE")] string condition)
        {
            CreateCluster(2);
            const string Key = "{b}delete";
            const string Missing = "{b}absent";
            int slot = ClusterTestUtils.HashSlot(Encoding.ASCII.GetBytes(Key));
            Execute(0, "SET", Key, "old", "EX", 300);
            long expiration = (long)Execute(0, "PEXPIRETIME", Key);
            string sourceId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            string targetId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            string moved = $"MOVED {slot} {context.clusterTestUtils.GetEndPoint(0)}";
            AssertDeleteRedirect(1, Key, moved);
            Assert.That(context.clusterTestUtils.SetSlot(1, slot, "IMPORTING", sourceId), Is.EqualTo("OK"));
            Assert.That(context.clusterTestUtils.SetSlot(0, slot, "MIGRATING", targetId), Is.EqualTo("OK"));
            Assert.That(Delete(0, Key, condition, condition is "IFNE" or "IFDNE" ? "old" : "different"), Is.Zero);
            Assert.That((long)Execute(0, "PEXPIRETIME", Key), Is.EqualTo(expiration));
            AssertDeleteRedirect(0, Missing, $"ASK {slot} {context.clusterTestUtils.GetEndPoint(1)}");
            using (GarnetClientSession importer = context.clusterTestUtils.CreateGarnetClientSession(1))
            {
                importer.Connect();
                await importer.ExecuteAsync("ASKING");
                Assert.That(await importer.ExecuteAsync(DeleteRequest(Missing, condition, "old")), Is.EqualTo("0"));
                Exception exception = Assert.ThrowsAsync<Exception>(async () =>
                    await importer.ExecuteAsync(DeleteRequest(Missing, condition, "old")).WaitAsync(context.cts.Token));
                Assert.That(exception.Message, Is.EqualTo(moved));
            }

            context.clusterTestUtils.MigrateKeys(context.clusterTestUtils.GetEndPoint(0), context.clusterTestUtils.GetEndPoint(1),
                [Encoding.ASCII.GetBytes(Key)], context.logger);
            context.clusterTestUtils.WaitForMigrationCleanup(0, context.cts.Token, context.logger);
            Assert.That(context.clusterTestUtils.SetSlot(1, slot, "NODE", targetId), Is.EqualTo("OK"));
            Assert.That(context.clusterTestUtils.SetSlot(0, slot, "NODE", targetId), Is.EqualTo("OK"));
            Assert.That((long)Execute(1, "PEXPIRETIME", Key), Is.EqualTo(expiration));
            Assert.That(Delete(1, Key, condition, condition is "IFNE" or "IFDNE" ? "different" : "old"), Is.EqualTo(1));
            Assert.That(Execute(1, "GET", Key).IsNull, Is.True);
            AssertDeleteRedirect(0, Key, $"MOVED {slot} {context.clusterTestUtils.GetEndPoint(1)}");

            void AssertDeleteRedirect(int node, string key, string redirect)
            {
                using GarnetClientSession client = context.clusterTestUtils.CreateGarnetClientSession(node);
                client.Connect();
                Exception exception = Assert.ThrowsAsync<Exception>(async () =>
                    await client.ExecuteAsync(DeleteRequest(key, condition, "old")).WaitAsync(context.cts.Token));
                Assert.That(exception.Message, Is.EqualTo(redirect));
            }
        }

        private long Delete(int nodeIndex, string key, string condition, string expected)
            => (long)Execute(nodeIndex, condition == "DELIFEQ" ? "DELIFEQ" : "DELEX",
                condition == "DELIFEQ" ? [key, expected] : [key, condition, ComparisonFor(condition, expected)]);

        private static string[] DeleteRequest(string key, string condition, string expected)
            => condition == "DELIFEQ" ? ["DELIFEQ", key, expected] : ["DELEX", key, condition, ComparisonFor(condition, expected)];

        private static string ComparisonFor(string condition, string value)
            => condition is "IFDEQ" or "IFDNE"
                ? System.IO.Hashing.XxHash3.HashToUInt64(Encoding.UTF8.GetBytes(value)).ToString("x16")
                : value;

        private void AssertDigestRedirect(int nodeIndex, string key, string redirect)
        {
            using GarnetClientSession client = context.clusterTestUtils.CreateGarnetClientSession(nodeIndex);
            client.Connect();
            Exception exception = Assert.ThrowsAsync<Exception>(async () =>
                await client.ExecuteAsync("DIGEST", key).WaitAsync(context.cts.Token));
            Assert.That(exception.Message, Is.EqualTo(redirect));
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