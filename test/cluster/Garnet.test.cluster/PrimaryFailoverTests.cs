// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Garnet.cluster;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;
using ClusterNodeRole = Garnet.cluster.NodeRole;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class PrimaryFailoverTests
    {
        ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown() => context.TearDown();

        ClusterProvider Provider(int index)
            => (ClusterProvider)context.nodes[index].Provider.StoreWrapper.clusterProvider;

        void SetupReplica(int sublogCount)
        {
            context.CreateInstances(2, disableObjects: true, enableAOF: true, sublogCount: sublogCount);
            context.CreateConnection();
            context.SimplePrimaryReplicaSetup();
            context.kvPairs = [];
            context.PopulatePrimary(ref context.kvPairs, keyLength: 16, kvpairCount: 100, primaryIndex: 0);
            var primaryId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            Assert.That(context.clusterTestUtils.ClusterReplicate(1, primaryId, async: true, logger: context.logger), Is.EqualTo("OK"));
            context.clusterTestUtils.BumpEpoch(1, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            context.clusterTestUtils.WaitForSlotOwnership(1, primaryId, [0, 16383], context.logger);
            context.clusterTestUtils.WaitForConfigPropagation(1, [0], context.logger);
        }

        [TestCase(1)]
        [TestCase(2)]
        [TestCase(4)]
        [CancelAfter(120_000)]
        public void PrimaryFailoverUsesLogTailAfterDemotion(int sublogCount)
        {
            SetupReplica(sublogCount);
            var primary = Provider(0);
            var tail = primary.storeWrapper.appendOnlyFile.Log.TailAddress;
            var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            Assert.That(context.clusterTestUtils.GetServer(0).Execute(
                "FAILOVER", "TO", endpoint.Address.ToString(), endpoint.Port, "TIMEOUT", 30_000).ToString(), Is.EqualTo("OK"));
            context.clusterTestUtils.WaitForNoFailover(0, context.logger);

            Assert.That(primary.failoverManager.IsWritePaused, Is.False);
            Assert.That(primary.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.REPLICA));
            Assert.That(primary.replicationManager.ReplicationOffset.EqualsAll(tail), Is.False,
                "The demoted primary's replica-tracking offset must differ from its final log tail.");
            Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY),
                "The synchronized replica must be promoted using the final log tail, not the stale replica-tracking offset.");
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1, 1);
        }

        [TestCase(1)]
        [TestCase(2)]
        [TestCase(4)]
        [CancelAfter(120_000)]
        public void FailStopWritesReturnsLogTailAfterDemotion(int sublogCount)
        {
            SetupReplica(sublogCount);
            var primary = Provider(0);
            var tail = primary.storeWrapper.appendOnlyFile.Log.TailAddress;
            var replicaId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            var reply = context.clusterTestUtils.GetServer(0).Execute("CLUSTER", "FAILSTOPWRITES", replicaId).ToString();

            Assert.That(primary.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.REPLICA));
            Assert.That(primary.replicationManager.ReplicationOffset.EqualsAll(tail), Is.False,
                "The demoted primary's replica-tracking offset must differ from its final log tail.");
            Assert.That(reply, Is.EqualTo(tail.ToString()));
        }

        [Test, CancelAfter(120_000)]
        public void PausedPrimaryRejectsWritesButServesReads()
        {
            SetupReplica(1);
            var primary = Provider(0);
            var config = primary.clusterManager.CurrentConfig;
            var db = context.clusterTestUtils.GetDatabase();
            var key = context.kvPairs.Keys.First();
            var value = db.StringGet(key);
            primary.failoverManager.PauseWrites(true);
            try
            {
                Assert.That(primary.clusterManager.CurrentConfig, Is.SameAs(config));
                Assert.That(db.StringGet(key), Is.EqualTo(value));
                var writeError = Assert.Throws<RedisServerException>(() => db.StringSet(key, "changed"));
                Assert.That(writeError.Message, Does.StartWith("CLUSTERDOWN"));
                var multiWriteError = Assert.Throws<RedisServerException>(() => db.StringSet(
                    [new KeyValuePair<RedisKey, RedisValue>(key, "changed")]));
                Assert.That(multiWriteError.Message, Does.StartWith("CLUSTERDOWN"));
                Assert.That(db.StringGet(key), Is.EqualTo(value));
            }
            finally
            {
                primary.failoverManager.PauseWrites(false);
            }
            Assert.That(db.StringSet(key, value), Is.True);
        }

        [TestCase(false)]
        [TestCase(true)]
        [CancelAfter(120_000)]
        public void PausedPrimaryRejectsDatabaseFlush(bool allDatabases)
        {
            SetupReplica(1);
            var primary = Provider(0);
            primary.failoverManager.PauseWrites(true);
            try
            {
                var server = context.clusterTestUtils.GetServer(0);
                var error = Assert.Throws<RedisServerException>(() =>
                {
                    if (allDatabases)
                        server.FlushAllDatabases();
                    else
                        server.FlushDatabase();
                });
                Assert.That(error.Message, Does.StartWith("CLUSTERDOWN"));
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 0, 0);
            }
            finally
            {
                primary.failoverManager.PauseWrites(false);
            }
        }

        [Test, CancelAfter(120_000)]
        public async Task PrimaryFailoverWaitsForEpochBeforeChangingRole()
        {
            SetupReplica(1);
            var primary = Provider(0);
            var server = (GarnetServerTcp)primary.storeWrapper.Servers[0];
            var existing = server.ActiveClusterSessions().ToHashSet();
            using var connection = new TcpClient();
            await connection.ConnectAsync((IPEndPoint)context.clusterTestUtils.GetEndPoint(0));
            var stream = connection.GetStream();
            await stream.WriteAsync("*1\r\n$4\r\nPING\r\n"u8.ToArray());
            await stream.ReadExactlyAsync(new byte[7]);
            var reader = server.ActiveClusterSessions().OfType<ClusterSession>().Single(s => !existing.Contains(s));
            Assert.That(() => reader.LocalCurrentEpoch, Is.Zero.After(5_000, 10));
            reader.AcquireCurrentEpoch();
            try
            {
                var config = primary.clusterManager.CurrentConfig;
                var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
                Assert.That(context.clusterTestUtils.GetServer(0).Execute(
                    "FAILOVER", "TO", endpoint.Address.ToString(), endpoint.Port, "TIMEOUT", 30_000).ToString(), Is.EqualTo("OK"));
                Assert.That(() => primary.failoverManager.IsWritePaused, Is.True.After(5_000, 10));
                Assert.That(primary.clusterManager.CurrentConfig, Is.SameAs(config),
                    "Pausing and draining clients must not advertise a role or slot handoff.");
                Assert.That(primary.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
                Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.REPLICA));
            }
            finally
            {
                reader.ReleaseCurrentEpoch();
            }
            context.clusterTestUtils.WaitForNoFailover(0, context.logger);
            Assert.That(primary.failoverManager.IsWritePaused, Is.False);
            Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
        }
    }
}