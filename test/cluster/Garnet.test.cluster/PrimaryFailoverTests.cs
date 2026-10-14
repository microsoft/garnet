// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using Garnet.cluster;
using NUnit.Framework;
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
    }
}