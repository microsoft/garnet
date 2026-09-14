// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Garnet.cluster;
using Garnet.common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    internal class ClusterConfigTests : TestBase
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

        [Test, Order(1)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterConfigInitializesUnassignedWorkerTest()
        {
            var config = new ClusterConfig().InitializeLocalWorker(
                Generator.CreateHexId(),
                "127.0.0.1",
                ClusterTestContext.Port + 1,
                configEpoch: 0,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "");

            (string address, int port) = config.GetWorkerAddress(0);
            Assert.That(address == "unassigned");
            Assert.That(port == 0);
            Assert.That(Garnet.cluster.NodeRole.UNASSIGNED == config.GetNodeRoleFromNodeId("asdasdqwe"));

            var configBytes = config.ToByteArray();
            var restoredConfig = ClusterConfig.FromByteArray(configBytes);

            (address, port) = restoredConfig.GetWorkerAddress(0);
            Assert.That(address == "unassigned");
            Assert.That(port == 0);
            Assert.That(Garnet.cluster.NodeRole.UNASSIGNED == restoredConfig.GetNodeRoleFromNodeId("asdasdqwe"));
        }

        [Test, Order(2)]
        [Category("CLUSTER-CONFIG")]
        public void ClusterForgetAfterNodeRestartTest()
        {
            int nbInstances = 4;
            context.CreateInstances(nbInstances);
            context.CreateConnection();
            var (shards, slots) = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            // Restart node with new ACL file
            context.nodes[0].Dispose(false);
            context.nodes[0] = context.CreateInstance(context.clusterTestUtils.GetEndPoint(0), useAcl: true, cleanClusterConfig: false);
            context.nodes[0].Start();
            context.CreateConnection();

            var firstNode = context.nodes[0];
            var nodesResult = context.clusterTestUtils.ClusterNodes(0);
            Assert.That(nodesResult.Nodes.Count == nbInstances);

            var server = context.clusterTestUtils.GetServer(context.endpoints[0].ToIPEndPoint());
            var args = new List<object>() {
                    "forget",
                    Encoding.ASCII.GetBytes("1ip23j89123no"),
                    Encoding.ASCII.GetBytes("0")
                };
            var ex = Assert.Throws<RedisServerException>(() => server.Execute("cluster", args),
                "Cluster forget call shouldn't have succeeded for an invalid node id.");

            Assert.That(ex.Message, Is.EqualTo("ERR I don't know about node 1ip23j89123no."));

            nodesResult = context.clusterTestUtils.ClusterNodes(0);
            Assert.That(nodesResult.Nodes.Count == nbInstances, "No node should've been removed from the cluster after an invalid id was passed.");
            Assert.That(nodesResult.Nodes.ElementAt(0).IsMyself);
            Assert.That(nodesResult.Nodes.ElementAt(0).EndPoint.ToIPEndPoint().Port == ClusterTestContext.Port, $"Expected the node to be replying to be the one with ClusterTestContext.Port {ClusterTestContext.Port} pt 1.");

            context.clusterTestUtils.ClusterForget(0, nodesResult.Nodes.Last().NodeId, 0);
            nodesResult = context.clusterTestUtils.ClusterNodes(0);
            Assert.That(nodesResult.Nodes.Count == nbInstances - 1, "A node should've been removed from the cluster.");
            Assert.That(nodesResult.Nodes.ElementAt(0).IsMyself);
            Assert.That(nodesResult.Nodes.ElementAt(0).EndPoint.ToIPEndPoint().Port == ClusterTestContext.Port, $"Expected the node to be replying to be the one with ClusterTestContext.Port {ClusterTestContext.Port} pt 2.");
        }

        [Test, Order(2)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterAnnounceRecoverTest()
        {
            context.CreateInstances(1);
            context.CreateConnection();

            var config = context.clusterTestUtils.ClusterNodes(0, logger: context.logger);
            var origin = config.Origin;

            var clusterNodesEndpoint = origin.ToIPEndPoint();
            ClassicAssert.AreEqual("127.0.0.1", clusterNodesEndpoint.Address.ToString());
            ClassicAssert.AreEqual(ClusterTestContext.Port, clusterNodesEndpoint.Port);

            ClassicAssert.IsTrue(IPAddress.TryParse("127.0.0.2", out var ipAddress));
            var announcePort = clusterNodesEndpoint.Port + 10000;
            var clusterAnnounceEndpoint = new IPEndPoint(ipAddress, announcePort);
            context.nodes[0].Dispose(false);
            context.nodes[0] = context.CreateInstance(context.clusterTestUtils.GetEndPoint(0), cleanClusterConfig: false, tryRecover: true, clusterAnnounceEndpoint: clusterAnnounceEndpoint);
            context.nodes[0].Start();
            context.CreateConnection();

            config = context.clusterTestUtils.ClusterNodes(0, logger: context.logger);
            origin = config.Origin;
            clusterNodesEndpoint = origin.ToIPEndPoint();
            ClassicAssert.AreEqual(clusterAnnounceEndpoint.Address.ToString(), clusterNodesEndpoint.Address.ToString());
            ClassicAssert.AreEqual(clusterAnnounceEndpoint.Port, clusterNodesEndpoint.Port);
        }

        [Test, Order(3)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterAnyIPAnnounce()
        {
            context.nodes = new GarnetServer[1];
            context.nodes[0] = context.CreateInstance(new IPEndPoint(IPAddress.Any, ClusterTestContext.Port));
            context.nodes[0].Start();

            context.endpoints = TestUtils.GetShardEndPoints(1, IPAddress.Loopback, ClusterTestContext.Port);
            context.CreateConnection();

            var config = context.clusterTestUtils.ClusterNodes(0, logger: context.logger);
            var origin = config.Origin;

            var endpoint = origin.ToIPEndPoint();
            ClassicAssert.AreEqual(ClusterTestContext.Port, endpoint.Port);

            using var client = TestUtils.GetGarnetClient(config.Origin);
            client.Connect();
            var resp = client.PingAsync().GetAwaiter().GetResult();
            ClassicAssert.AreEqual("PONG", resp);
            resp = client.QuitAsync().GetAwaiter().GetResult();
            ClassicAssert.AreEqual("OK", resp);
        }

        [Test, Order(4)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterConfigVersionRoundTripTest()
        {
            var config = new ClusterConfig().InitializeLocalWorker(
                Generator.CreateHexId(),
                "127.0.0.1",
                ClusterTestContext.Port + 1,
                configEpoch: 1,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "");

            var configBytes = config.ToByteArray();

            // Verify version byte at start of payload
            Assert.That(ClusterConfig.TryPeekVersion(configBytes, out var version), Is.True);
            Assert.That(version, Is.EqualTo(ClusterConfig.ClusterConfigVersion));

            // Round-trip should succeed
            var restored = ClusterConfig.FromByteArray(configBytes);
            Assert.That(restored.LocalNodeId, Is.EqualTo(config.LocalNodeId));
        }

        [Test, Order(5)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterConfigVersionMismatchThrowsTest()
        {
            var config = new ClusterConfig().InitializeLocalWorker(
                Generator.CreateHexId(),
                "127.0.0.1",
                ClusterTestContext.Port + 1,
                configEpoch: 1,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "");

            var configBytes = config.ToByteArray();

            // Corrupt the version byte (at index 0)
            configBytes[0] = (byte)(ClusterConfig.ClusterConfigVersion + 1);

            // Deserialization should throw
            Assert.Throws<System.IO.InvalidDataException>(() => ClusterConfig.FromByteArray(configBytes));
        }

        [Test, Order(6)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterConfigTryPeekVersionEmptyDataTest()
        {
            Assert.That(ClusterConfig.TryPeekVersion([], out _), Is.False);
        }

        [Test, Order(7)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ReplicationHistoryVersionRoundTripTest()
        {
            var history = new ReplicationHistory(1);
            var bytes = history.ToByteArray();

            // Verify version byte at start of payload
            Assert.That(bytes[0], Is.EqualTo(ReplicationHistory.ReplicationHistoryVersion));

            // Round-trip should succeed and preserve fields
            var restored = ReplicationHistory.FromByteArray(bytes);
            Assert.That(restored.PrimaryReplId, Is.EqualTo(history.PrimaryReplId));
            Assert.That(restored.PrimaryReplId2, Is.EqualTo(history.PrimaryReplId2));
        }

        [Test, Order(9)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ReplicationHistoryVersionMismatchThrowsTest()
        {
            var history = new ReplicationHistory(1);
            var bytes = history.ToByteArray();

            // Corrupt the version byte (at index 0)
            bytes[0] = (byte)(ReplicationHistory.ReplicationHistoryVersion + 1);

            // Deserialization should throw
            Assert.Throws<System.IO.InvalidDataException>(() => ReplicationHistory.FromByteArray(bytes));
        }

        /// <summary>
        /// Verifies that a merge whose only effect is resetting stale slot attributions is retained.
        /// When a sender no longer claims a slot the receiver attributes to it, MergeSlotMap resets
        /// that slot to OFFLINE so the real owner can reclaim it.
        /// </summary>
        [Test, Order(10)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterConfigMergeSlotMapRetainsStaleOwnershipResetTest()
        {
            const int StaleSlot = 100;   // receiver wrongly thinks sender owns this
            const int SenderSlot = 200;  // sender genuinely owns this, receiver agrees

            var senderId = Generator.CreateHexId();
            var thirdPartyId = Generator.CreateHexId();

            var thirdParty = new ClusterConfig().InitializeLocalWorker(
                thirdPartyId, "127.0.0.1", ClusterTestContext.Port + 3,
                configEpoch: 5, Garnet.cluster.NodeRole.PRIMARY, null, "");

            // Sender knows the third party, owns SenderSlot, and attributes StaleSlot to the
            // third party — i.e. it does NOT claim StaleSlot.
            var sender = new ClusterConfig()
                .InitializeLocalWorker(
                    senderId, "127.0.0.1", ClusterTestContext.Port + 1,
                    configEpoch: 20, Garnet.cluster.NodeRole.PRIMARY, null, "")
                .Merge(thirdParty, []);
            sender = sender
                .UpdateSlotState(SenderSlot, ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE)
                .UpdateSlotState(StaleSlot, sender.GetWorkerIdFromNodeId(thirdPartyId), SlotState.STABLE);

            // Receiver believes the sender owns BOTH slots (StaleSlot is the stale belief).
            var receiver = new ClusterConfig()
                .InitializeLocalWorker(
                    Generator.CreateHexId(), "127.0.0.1", ClusterTestContext.Port + 2,
                    configEpoch: 1, Garnet.cluster.NodeRole.PRIMARY, null, "")
                .Merge(sender, []);
            var senderWorkerId = receiver.GetWorkerIdFromNodeId(senderId);
            Assert.That(senderWorkerId, Is.Not.Zero);
            receiver = receiver
                .UpdateSlotState(StaleSlot, senderWorkerId, SlotState.STABLE)
                .UpdateSlotState(SenderSlot, senderWorkerId, SlotState.STABLE);

            Assert.That(receiver.GetNodeIdFromSlot(StaleSlot), Is.EqualTo(senderId),
                "precondition: receiver starts out wrongly attributing StaleSlot to the sender");

            // Gossip from the sender. Its only effect is the stale-ownership reset, since the
            // sender's genuine slot is already correct on the receiver.
            var merged = receiver.Merge(sender, []);

            Assert.That(merged.GetNodeIdFromSlot(StaleSlot), Is.Not.EqualTo(senderId),
                "stale attribution should be cleared");
            Assert.That(merged.GetState((ushort)StaleSlot), Is.EqualTo(SlotState.OFFLINE),
                "cleared slot should be OFFLINE so the true owner can claim it");
            Assert.That(merged.GetNodeIdFromSlot(SenderSlot), Is.EqualTo(senderId),
                "the sender's genuine slot must be unaffected");
        }

        /// <summary>
        /// Verifies that MergeSlotMap accumulates its updated flag across slots, so a later slot that
        /// assigns no change cannot discard an earlier stale-ownership reset.
        /// </summary>
        /// <remarks>
        /// The sender's config epoch is zero, which is what lets the slot it genuinely owns bypass the
        /// epoch guard and reach the ownership-assignment path. The receiver already agrees about that
        /// slot, so the assignment produces no change; because it sits at a higher slot index than the
        /// reset, a plain assignment there would clear the flag and the whole merge would be discarded.
        /// </remarks>
        [Test, Order(11)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterConfigMergeSlotMapAccumulatesUpdatedAcrossSlotsTest()
        {
            const int StaleSlot = 100;   // reset happens here, visited first
            const int SenderSlot = 200;  // no-change assignment, visited after the reset

            var senderId = Generator.CreateHexId();
            var thirdPartyId = Generator.CreateHexId();

            var thirdParty = new ClusterConfig().InitializeLocalWorker(
                thirdPartyId, "127.0.0.1", ClusterTestContext.Port + 3,
                configEpoch: 5, Garnet.cluster.NodeRole.PRIMARY, null, "");

            // Config epoch zero keeps SenderSlot from being short-circuited by the epoch guard,
            // so it reaches the ownership-assignment path with nothing to change.
            var sender = new ClusterConfig()
                .InitializeLocalWorker(
                    senderId, "127.0.0.1", ClusterTestContext.Port + 1,
                    configEpoch: 0, Garnet.cluster.NodeRole.PRIMARY, null, "")
                .Merge(thirdParty, []);
            sender = sender
                .UpdateSlotState(SenderSlot, ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE)
                .UpdateSlotState(StaleSlot, sender.GetWorkerIdFromNodeId(thirdPartyId), SlotState.STABLE);

            var receiver = new ClusterConfig()
                .InitializeLocalWorker(
                    Generator.CreateHexId(), "127.0.0.1", ClusterTestContext.Port + 2,
                    configEpoch: 1, Garnet.cluster.NodeRole.PRIMARY, null, "")
                .Merge(sender, []);
            var senderWorkerId = receiver.GetWorkerIdFromNodeId(senderId);
            Assert.That(senderWorkerId, Is.Not.Zero);
            receiver = receiver
                .UpdateSlotState(StaleSlot, senderWorkerId, SlotState.STABLE)
                .UpdateSlotState(SenderSlot, senderWorkerId, SlotState.STABLE);

            Assert.That(receiver.GetNodeIdFromSlot(StaleSlot), Is.EqualTo(senderId),
                "precondition: receiver starts out wrongly attributing StaleSlot to the sender");
            Assert.That(receiver.GetNodeIdFromSlot(SenderSlot), Is.EqualTo(senderId),
                "precondition: SenderSlot already matches the sender, so its assignment changes nothing");

            var merged = receiver.Merge(sender, []);

            Assert.That(merged.GetState((ushort)StaleSlot), Is.EqualTo(SlotState.OFFLINE),
                "the reset must survive the no-change assignment at the higher slot index");
            Assert.That(merged.GetNodeIdFromSlot(StaleSlot), Is.Not.EqualTo(senderId),
                "stale attribution should be cleared");
        }

        /// <summary>
        /// Verifies that gossip from a replica cannot make that replica the owner of a slot the receiver
        /// has no owner for. A replica only relays its primary's view, so treating it as the claimant hands
        /// the slot to a node that never owned it, and the real owner can then never take it back.
        /// </summary>
        [Test, Order(12)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterConfigMergeSlotMapIgnoresUnownedSlotFromReplicaTest()
        {
            const int Slot = 8192;

            var ownerId = Generator.CreateHexId();         // primary that genuinely owns Slot
            var otherPrimaryId = Generator.CreateHexId();  // the replica's own primary
            var replicaId = Generator.CreateHexId();

            // The genuine owner of Slot, with a deliberately low config epoch.
            var owner = new ClusterConfig()
                .InitializeLocalWorker(
                    ownerId, "127.0.0.1", ClusterTestContext.Port + 1,
                    configEpoch: 2, Garnet.cluster.NodeRole.PRIMARY, null, "")
                .UpdateSlotState(Slot, ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);

            var otherPrimary = new ClusterConfig().InitializeLocalWorker(
                otherPrimaryId, "127.0.0.1", ClusterTestContext.Port + 2,
                configEpoch: 3, Garnet.cluster.NodeRole.PRIMARY, null, "");

            // A replica of some other primary. It does not own Slot, it merely relays that the owner does.
            // Its config epoch is higher than the owner's, as happens after an epoch collision is resolved.
            var replica = new ClusterConfig()
                .InitializeLocalWorker(
                    replicaId, "127.0.0.1", ClusterTestContext.Port + 3,
                    configEpoch: 5, Garnet.cluster.NodeRole.REPLICA, otherPrimaryId, "")
                .Merge(otherPrimary, [])
                .Merge(owner, []);

            Assert.That(replica.GetNodeIdFromSlot(Slot), Is.EqualTo(ownerId),
                "precondition: the replica relays that the owner owns the slot");

            // Receiver knows the owner as a worker but has not learned who owns Slot yet, which is the
            // normal state of a freshly met node.
            var ownerWithoutSlots = new ClusterConfig().InitializeLocalWorker(
                ownerId, "127.0.0.1", ClusterTestContext.Port + 1,
                configEpoch: 2, Garnet.cluster.NodeRole.PRIMARY, null, "");
            var receiver = new ClusterConfig()
                .InitializeLocalWorker(
                    Generator.CreateHexId(), "127.0.0.1", ClusterTestContext.Port + 4,
                    configEpoch: 1, Garnet.cluster.NodeRole.PRIMARY, null, "")
                .Merge(ownerWithoutSlots, []);

            Assert.That(receiver.GetState((ushort)Slot), Is.Not.EqualTo(SlotState.STABLE),
                "precondition: the receiver has no owner for the slot");

            var merged = receiver.Merge(replica, []);

            Assert.That(merged.GetNodeIdFromSlot(Slot), Is.Not.EqualTo(replicaId),
                "a replica must never become the owner of a slot it only relays");

            // The owner must still be able to claim the slot afterwards. Without the guard above the
            // replica holds it at a higher config epoch and this claim is rejected forever.
            var claimed = merged.Merge(owner, []);
            Assert.That(claimed.GetNodeIdFromSlot(Slot), Is.EqualTo(ownerId),
                "the genuine owner must still be able to claim the slot");
        }

        /// <summary>
        /// Models one node of a gossiping cluster: its published configuration plus the two caches that
        /// decide whether a configuration is actually put on the wire.
        /// </summary>
        private sealed class GossipNode
        {
            public string NodeId;

            /// <summary>
            /// ClusterManager.currentConfig.
            /// </summary>
            public ClusterConfig Config;

            /// <summary>
            /// GarnetServerNode.lastConfig, one per outgoing gossip connection.
            /// </summary>
            public readonly Dictionary<string, ClusterConfig> LastConfigSentTo = [];

            /// <summary>
            /// ClusterCommands.lastSentConfig, one per inbound gossip session.
            /// </summary>
            public readonly Dictionary<string, ClusterConfig> LastConfigRepliedTo = [];
        }

        private static readonly ConcurrentDictionary<string, long> EmptyBanList = new();

        /// <summary>
        /// Models ClusterManager.TryMerge.
        /// </summary>
        private static void GossipTryMerge(GossipNode node, ClusterConfig senderConfig)
        {
            var currentCopy = node.Config.Copy();
            var next = currentCopy.Merge(senderConfig, EmptyBanList).HandleConfigEpochCollision(senderConfig);
            if (currentCopy != next)
                node.Config = next;
        }

        /// <summary>
        /// Models one gossip exchange: GarnetServerNode.TryGossip sends a configuration only when the local
        /// configuration object changed since the last send on that connection, the receiver merges it and
        /// replies with its own pre-merge configuration under the same rule, and the sender merges the reply.
        /// </summary>
        private static void GossipExchange(GossipNode from, GossipNode to, bool withMeet = false)
        {
            byte[] request;
            if (!from.LastConfigSentTo.TryGetValue(to.NodeId, out var lastSent) || lastSent != from.Config)
            {
                from.LastConfigSentTo[to.NodeId] = from.Config;
                request = from.Config.ToByteArray();
            }
            else
            {
                request = [];
            }

            var receiverConfig = to.Config;
            if (request.Length > 0)
            {
                var senderConfig = ClusterConfig.FromByteArray(request);
                if (withMeet || receiverConfig.IsKnown(senderConfig.LocalNodeId))
                    GossipTryMerge(to, senderConfig);
            }

            byte[] reply;
            if (withMeet || !to.LastConfigRepliedTo.TryGetValue(from.NodeId, out var lastReplied) || lastReplied != receiverConfig)
            {
                to.LastConfigRepliedTo[from.NodeId] = receiverConfig;
                reply = receiverConfig.ToByteArray();
            }
            else
            {
                reply = [];
            }

            if (reply.Length > 0)
            {
                var replyConfig = ClusterConfig.FromByteArray(reply);

                // A MEET response is merged unconditionally, the peer is trusted because an admin issued the meet.
                if (withMeet || from.Config.IsKnown(replyConfig.LocalNodeId))
                    GossipTryMerge(from, replyConfig);
            }
        }

        /// <summary>
        /// Replays what SimpleSetupCluster does: assign slots to the two primaries, set config epochs, then
        /// issue every CLUSTER MEET from the first node.
        /// </summary>
        private static List<GossipNode> BuildGossipCluster()
        {
            var nodes = new List<GossipNode>();
            for (var i = 0; i < 4; i++)
            {
                var nodeId = Generator.CreateHexId();
                nodes.Add(new GossipNode
                {
                    NodeId = nodeId,
                    Config = new ClusterConfig().InitializeLocalWorker(
                        nodeId, "127.0.0.1", ClusterTestContext.Port + i, configEpoch: 0,
                        Garnet.cluster.NodeRole.PRIMARY, null, "")
                });
            }

            nodes[0].Config = nodes[0].Config.AssignSlots([.. Enumerable.Range(0, 8192)], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);
            nodes[1].Config = nodes[1].Config.AssignSlots([.. Enumerable.Range(8192, 8192)], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);

            for (var i = 0; i < nodes.Count; i++)
                nodes[i].Config = nodes[i].Config.SetLocalWorkerConfigEpoch(i + 1);

            for (var i = 1; i < nodes.Count; i++)
                GossipExchange(nodes[0], nodes[i], withMeet: true);

            return nodes;
        }

        /// <summary>
        /// The state WaitForSyncAsync waits for: every node agrees that the two primaries own their halves of
        /// the slot space and that the two replicas own nothing and are flagged as replicas.
        /// </summary>
        private static bool GossipClusterConverged(List<GossipNode> nodes)
        {
            foreach (var node in nodes)
            {
                for (var slot = 0; slot < 8192; slot++)
                    if (node.Config.GetNodeIdFromSlot((ushort)slot) != nodes[0].NodeId) return false;

                for (var slot = 8192; slot < 16384; slot++)
                    if (node.Config.GetNodeIdFromSlot((ushort)slot) != nodes[1].NodeId) return false;

                if (node.Config.GetNodeRoleFromNodeId(nodes[2].NodeId) != Garnet.cluster.NodeRole.REPLICA) return false;
                if (node.Config.GetNodeRoleFromNodeId(nodes[3].NodeId) != Garnet.cluster.NodeRole.REPLICA) return false;
            }
            return true;
        }

        private static string DescribeGossipCluster(List<GossipNode> nodes)
        {
            var sb = new StringBuilder();
            foreach (var node in nodes)
            {
                var info = node.Config.GetClusterInfo(null);
                for (var i = 0; i < nodes.Count; i++)
                    info = info.Replace(nodes[i].NodeId, $"n{i}");
                sb.Append(info).Append('\n');
            }
            return sb.ToString();
        }

        /// <summary>
        /// Drives the real merge code through the cluster layout SimpleSetupCluster builds, over many gossip
        /// orderings. A configuration is only put on the wire when the sender's configuration object changed
        /// since its last send on that connection, so a view that a merge refuses to take is never offered
        /// again and any divergence the merge rules allow is permanent rather than transient.
        /// </summary>
        [Test, Order(13)]
        [Category("CLUSTER-CONFIG"), CancelAfter(60_000)]
        public void ClusterConfigGossipConvergesForEveryOrderingTest()
        {
            const int Trials = 200;
            const int RoundsPerTrial = 60;

            var pairs = new List<(int From, int To)>();
            for (var from = 0; from < 4; from++)
                for (var to = 0; to < 4; to++)
                    if (from != to) pairs.Add((from, to));

            var random = new Random(12345);
            for (var trial = 0; trial < Trials; trial++)
            {
                var nodes = BuildGossipCluster();

                // CLUSTER REPLICATE bumps the local config epoch so the role change can propagate. Gossip runs
                // between the two calls because the test waits for the first primary to see its replica.
                nodes[2].Config = nodes[2].Config.MakeReplicaOf(nodes[0].NodeId).BumpLocalNodeConfigEpoch();
                var interleaved = random.Next(0, 8);
                for (var i = 0; i < interleaved; i++)
                {
                    var (from, to) = pairs[random.Next(pairs.Count)];
                    GossipExchange(nodes[from], nodes[to]);
                }
                nodes[3].Config = nodes[3].Config.MakeReplicaOf(nodes[1].NodeId).BumpLocalNodeConfigEpoch();

                for (var round = 0; round < RoundsPerTrial && !GossipClusterConverged(nodes); round++)
                {
                    foreach (var (from, to) in pairs.OrderBy(_ => random.Next()))
                        GossipExchange(nodes[from], nodes[to]);
                }

                Assert.That(GossipClusterConverged(nodes), Is.True,
                    $"cluster did not converge after {RoundsPerTrial} gossip rounds in trial {trial}:\n{DescribeGossipCluster(nodes)}");
            }
        }
    }
}