// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    /// <summary>
    /// End-to-end tests that drive the internal server-to-server operations performed over
    /// <c>GarnetClient</c>: periodic gossip and MEET (<c>GarnetServerNode</c>), cross-node PUBLISH
    /// forwarding (<c>ClusterPublishNoResponse</c>), and the failover handshake
    /// (<c>PrimaryFailoverSession.Failover</c>, <c>ReplicaFailoverSession.GossipAsync</c>/<c>ReplicaOf</c>).
    ///
    /// Assertions are intentionally outcome-based (cluster converges, data is retained, roles swap)
    /// rather than tied to precise gossip timing, and run over the default (non-TLS) transport so
    /// they validate deterministically.
    /// </summary>
    [TestFixture, NonParallelizable]
    public class ClusterServerNodeOpsTests
    {
        ClusterTestContext context;
        readonly Dictionary<string, LogLevel> monitorTests = [];

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests);
        }

        [TearDown]
        public void TearDown()
        {
            context.TearDown();
        }

        /// <summary>
        /// A multi-primary cluster must fully converge through gossip. Every node sending its config
        /// to every peer exercises the gossip path in <c>GarnetServerNode</c>; convergence proves
        /// the gossip client delivers and merges configs correctly across the cluster.
        /// </summary>
        [Test, Order(1), CancelAfter(120_000)]
        public void GossipConvergesAcrossMultiNodeCluster()
        {
            const int nodeCount = 4;

            context.CreateInstances(nodeCount);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: nodeCount, replica_count: 0, logger: context.logger);

            // Convergence: every node must learn about every other node purely through gossip.
            for (var i = 0; i < nodeCount; i++)
                context.clusterTestUtils.WaitUntilNodeIsKnownByAllNodes(i, context.logger);
        }

        /// <summary>
        /// After a peer node is bounced, the surviving nodes keep gossiping to it and the gossip
        /// client must transparently reconnect once it is back up, re-converging the cluster. This
        /// exercises the exponential-backoff reconnect path of the gossip client.
        /// </summary>
        [Test, Order(2), CancelAfter(120_000)]
        public void GossipReconvergesAfterPeerRestart()
        {
            const int nodeCount = 3;
            const int bouncedIndex = 2;

            context.CreateInstances(nodeCount);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: nodeCount, replica_count: 0, logger: context.logger);

            for (var i = 0; i < nodeCount; i++)
                context.clusterTestUtils.WaitUntilNodeIsKnownByAllNodes(i, context.logger);

            // Bounce one node; it keeps its cluster config across the restart and must rejoin.
            context.RestartNode(bouncedIndex);

            // The gossip clients on the surviving nodes must reconnect and re-converge the cluster.
            for (var i = 0; i < nodeCount; i++)
                context.clusterTestUtils.WaitUntilNodeIsKnownByAllNodes(i, context.logger);
        }

        /// <summary>
        /// A PUBLISH whose payload is larger than one pub/sub log page cannot be enqueued and is
        /// instead delivered synchronously. Delivering such a message to a subscriber on a different
        /// node proves cross-node publish forwarding handles payloads larger than the AOF page.
        /// </summary>
        [Test, Order(3), CancelAfter(120_000)]
        public void LargePublishForwardedAcrossNodes()
        {
            const int nodeCount = 2;
            const int publisherIndex = 0;
            const int peerIndex = 1;

            context.CreateInstances(nodeCount, disablePubSub: false);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 2, replica_count: 0, logger: context.logger);

            context.clusterTestUtils.WaitUntilNodeIsKnown(publisherIndex, peerIndex, context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(peerIndex, publisherIndex, context.logger);

            var publisherEndpoint = context.clusterTestUtils.GetEndPoint(publisherIndex);
            var peerEndpoint = context.clusterTestUtils.GetEndPoint(peerIndex);

            const string channelName = "large-forward-channel";
            var channel = RedisChannel.Literal(channelName);

            var message = new string('x', 4 * 1024);

            // The cluster test multiplexer disables PUBLISH/SUBSCRIBE, so use dedicated connections.
            using var pubRedis = ConnectionMultiplexer.Connect(SingleNodeConfig(publisherEndpoint));
            using var subRedis = ConnectionMultiplexer.Connect(SingleNodeConfig(peerEndpoint));

            using var delivered = new ManualResetEventSlim(false);
            var subscriber = subRedis.GetSubscriber();
            subscriber.Subscribe(channel, (_, value) =>
            {
                if (value == message)
                    delivered.Set();
            });

            // SUBSCRIBE can return before the subscription is active on the server, so retry the
            // publish until the large message is delivered (or we time out).
            var deadline = Stopwatch.StartNew();
            var wasDelivered = false;
            while (deadline.Elapsed < TimeSpan.FromSeconds(30))
            {
                Assert.DoesNotThrow(
                    () => pubRedis.GetServer(publisherEndpoint).Execute(0, "PUBLISH", [channelName, message]),
                    "Large publish threw while forwarding across nodes");
                if (delivered.Wait(TimeSpan.FromMilliseconds(500)))
                {
                    wasDelivered = true;
                    break;
                }
            }

            ClassicAssert.IsTrue(wasDelivered, "Large publish was not forwarded/delivered across nodes");
            subscriber.Unsubscribe(channel);
        }

        /// <summary>
        /// A coordinated (DEFAULT) failover drives the full failover handshake: the primary asks the
        /// replica to take over (<c>PrimaryFailoverSession.Failover(TAKEOVER)</c>), then the new
        /// primary gossips its config and reattaches the demoted node
        /// (<c>ReplicaFailoverSession.GossipAsync</c>/<c>ReplicaOf</c>). The roles must swap and all
        /// data must survive on the new primary.
        /// </summary>
        [Test, Order(4), CancelAfter(120_000)]
        public void CoordinatedFailoverPromotesReplica()
        {
            const int primaryIndex = 0;
            const int replicaIndex = 1;

            SetupPrimaryReplica(out var primaryId);
            context.PopulatePrimary(ref context.kvPairs, keyLength: 16, kvpairCount: 100, primaryIndex: primaryIndex);

            _ = context.clusterTestUtils.ClusterReplicate(replicaIndex, primaryId, async: true, logger: context.logger);
            context.clusterTestUtils.BumpEpoch(replicaIndex, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, logger: context.logger);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex, primaryIndex);

            // Coordinated failover: asserts the roles swapped and the demoted node resynced.
            context.FailoverTo(replicaIndex, primaryIndex);

            // All data must still be served by the newly promoted primary.
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex, replicaIndex);
        }

        /// <summary>
        /// A forced failover while the primary is down drives the replica self-promotion path
        /// (<c>ReplicaFailoverSession</c>). The replica promotes itself and still attempts the
        /// gossip/attach handshake toward the (now unreachable) old primary, which must fail
        /// gracefully while the promotion succeeds and the data remains available.
        /// </summary>
        [Test, Order(5), CancelAfter(120_000)]
        public void ForceFailoverPromotesReplica()
        {
            const int primaryIndex = 0;
            const int replicaIndex = 1;

            SetupPrimaryReplica(out var primaryId);
            context.PopulatePrimary(ref context.kvPairs, keyLength: 16, kvpairCount: 100, primaryIndex: primaryIndex);

            _ = context.clusterTestUtils.ClusterReplicate(replicaIndex, primaryId, async: true, logger: context.logger);
            context.clusterTestUtils.BumpEpoch(replicaIndex, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, logger: context.logger);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex, primaryIndex);

            // Take the primary down, then force the replica to promote itself without coordination.
            context.ShutdownNode(primaryIndex, ensureAofFlush: true);
            _ = context.clusterTestUtils.ClusterFailover(replicaIndex, "FORCE", context.logger);
            context.clusterTestUtils.WaitForPrimaryRole(replicaIndex, context.logger);
            context.clusterTestUtils.AssertRole(replicaIndex, "master", context.logger);

            // The promoted node must still serve all data written before the failover.
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex, replicaIndex);
        }

        /// <summary>
        /// Builds a single-primary / single-replica cluster (node 0 primary, node 1 replica) with AOF
        /// enabled so replication offsets can be synchronized, and returns the primary node id.
        /// </summary>
        private void SetupPrimaryReplica(out string primaryId)
        {
            const int primaryIndex = 0;

            context.CreateInstances(2, disableObjects: true, enableAOF: true);
            context.CreateConnection();
            context.SimplePrimaryReplicaSetup();
            context.kvPairs = [];
            primaryId = context.clusterTestUtils.GetNodeIdFromNode(primaryIndex, context.logger);
        }

        private static ConfigurationOptions SingleNodeConfig(EndPoint endpoint)
        {
            var config = new ConfigurationOptions
            {
                AbortOnConnectFail = false,
                ConnectRetry = 5,
                ConnectTimeout = 5000,
            };
            config.EndPoints.Add(endpoint);
            return config;
        }
    }
}
