// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Collections.Generic;
using System.IO;
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
            Assert.That(version, Is.EqualTo(1));

            // Round-trip should succeed
            var restored = ClusterConfig.FromByteArray(configBytes);
            Assert.That(restored.LocalNodeId, Is.EqualTo(config.LocalNodeId));
        }

        [TestCase(1)]
        [TestCase(2)]
        [Category("CLUSTER-CONFIG")]
        public void ClusterConfigWorkerFormatTest(byte version)
        {
            var workers = CreateFormatWorkers();
            var expected = SerializeFormatFixture(version, workers);
            var config = new ClusterConfig(new HashSlot[ClusterConfig.MAX_HASH_SLOT_VALUE], workers);

            Assert.That(config.ToByteArray(version), Is.EqualTo(expected));
            Assert.That(config.ToByteArray(), Is.EqualTo(SerializeFormatFixture(1, workers)));

            var restored = ClusterConfig.FromByteArray(expected);
            if (version == 1)
            {
                for (var i = 1; i < workers.Length; i++)
                {
                    workers[i].ClusterAddress = workers[i].Address;
                    workers[i].ClusterPort = workers[i].Port;
                }
            }

            Assert.That(restored.ToByteArray(2), Is.EqualTo(SerializeFormatFixture(2, workers)));
            Assert.That(restored.Copy().ToByteArray(2), Is.EqualTo(restored.ToByteArray(2)));
            Assert.That(restored.ToByteArray(), Is.EqualTo(SerializeFormatFixture(1, workers)));

            var initialized = restored.InitializeLocalWorker(workers[1].Nodeid, workers[1].Address,
                workers[1].Port, workers[1].ConfigEpoch, workers[1].Role, null, workers[1].hostname);
            workers[1].ReplicationOffset = 0;
            Assert.That(initialized.ToByteArray(2), Is.EqualTo(SerializeFormatFixture(2, workers)));
        }

        [Test]
        [Category("CLUSTER-CONFIG")]
        public void ClusterConfigClusterFieldsDoNotChangeRoutingTest()
        {
            var config = ClusterConfig.FromByteArray(SerializeFormatFixture(2, CreateFormatWorkers()));
            config = config.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);

            Assert.That(config.GetWorkerAddress(1), Is.EqualTo(("203.0.113.1", 17001)));
            Assert.That(config.GetEndpointFromSlot(0, Garnet.server.ClusterPreferredEndpointType.Ip), Is.EqualTo(("203.0.113.1", 17001)));
            Assert.That(config.AskEndpointFromSlot(0, Garnet.server.ClusterPreferredEndpointType.Ip), Is.EqualTo(("203.0.113.1", 17001)));
            Assert.That(config.GetEndpointFromSlot(0, Garnet.server.ClusterPreferredEndpointType.Hostname), Is.EqualTo(("node.example.com", 17001)));
            Assert.That(config.GetReplicaEndpoints(config.LocalNodeId).Single(), Is.EqualTo(("203.0.113.2", 17002)));
            Assert.That(config.GetWorkerInfoForGossip().Single(), Is.EqualTo((new string('b', 40), "203.0.113.2", 17002)));
            Assert.That(config.GetClusterInfo(null), Does.Contain("203.0.113.1:17001@27001,node.example.com"));
        }

        [Test]
        [Category("CLUSTER-CONFIG")]
        public void ClusterConfigMergePreservesClusterFieldsTest()
        {
            var workers = CreateFormatWorkers();
            var receiver = new ClusterConfig().InitializeLocalWorker(workers[1].Nodeid, workers[1].Address,
                workers[1].Port, workers[1].ConfigEpoch, workers[1].Role, null, workers[1].hostname);
            var senderWorkers = new Worker[] { default, workers[2] };
            var sender = ClusterConfig.FromByteArray(SerializeFormatFixture(2, senderWorkers));

            receiver = receiver.Merge(sender, []);
            workers[1].ClusterAddress = workers[1].Address;
            workers[1].ClusterPort = workers[1].Port;
            workers[1].ReplicationOffset = 0;
            workers[2].ReplicationOffset = 0;
            Assert.That(receiver.ToByteArray(2), Is.EqualTo(SerializeFormatFixture(2, workers)));

            workers[2].ConfigEpoch++;
            workers[2].ClusterAddress = "127.0.0.3";
            workers[2].ClusterPort = 7003;
            senderWorkers[1] = workers[2];
            sender = ClusterConfig.FromByteArray(SerializeFormatFixture(2, senderWorkers));
            receiver = receiver.Merge(sender, []);
            Assert.That(receiver.ToByteArray(2), Is.EqualTo(SerializeFormatFixture(2, workers)));
        }

        [TestCase(0)]
        [TestCase(3)]
        [TestCase(255)]
        [Category("CLUSTER-CONFIG")]
        public void ClusterConfigUnsupportedVersionTest(byte version)
        {
            var config = new ClusterConfig();
            Assert.That(ClusterConfig.IsSupportedVersion(version), Is.False);
            Assert.Throws<ArgumentOutOfRangeException>(() => config.ToByteArray(version));
            Assert.Throws<InvalidDataException>(() => ClusterConfig.FromByteArray([version]));
        }

        [Test]
        [Category("CLUSTER-CONFIG")]
        public void ClusterConfigTruncatedVersion2WorkerTest()
        {
            var bytes = SerializeFormatFixture(2, CreateFormatWorkers());
            Assert.Throws<EndOfStreamException>(() => ClusterConfig.FromByteArray(bytes[..^1]));
        }

        [TestCase(1, true)]
        [TestCase(2, true)]
        [TestCase(0, false)]
        [TestCase(3, false)]
        [TestCase(255, false)]
        [Category("CLUSTER-CONFIG")]
        public void ClusterConfigGossipVersionTest(byte version, bool supported)
        {
            context.CreateInstances(1);
            context.CreateConnection();
            var workers = CreateFormatWorkers();
            var server = context.clusterTestUtils.GetServer(context.endpoints[0].ToIPEndPoint());
            var response = (byte[])server.Execute("CLUSTER", "GOSSIP", "WITHMEET", SerializeFormatFixture(version, workers));

            Assert.That(ClusterConfig.IsSupportedVersion(version), Is.EqualTo(supported));
            Assert.That(response[0], Is.EqualTo(1));
            var nodes = context.clusterTestUtils.ClusterNodes(0);
            Assert.That(nodes.Nodes.Any(node => node.NodeId == workers[1].Nodeid), Is.EqualTo(supported));
        }

        [TestCase(1)]
        [TestCase(2)]
        [Category("CLUSTER-CONFIG")]
        public void ClusterConfigDiskVersionTest(byte version)
        {
            context.CreateInstances(1);
            context.CreateConnection();
            var nodeId = context.clusterTestUtils.ClusterNodes(0).Nodes.Single(node => node.IsMyself).NodeId;
            context.nodes[0].Dispose(false);
            context.nodes[0] = null;

            var workers = CreateFormatWorkers();
            workers[1].Nodeid = nodeId;
            workers[1].Address = "127.0.0.1";
            workers[1].Port = ClusterTestContext.Port;
            var bytes = SerializeFormatFixture(version, [default, workers[1]]);
            var configPath = Directory.GetFiles(context.TestFolder, "nodes.conf*", SearchOption.AllDirectories).Single();
            using (var stream = new FileStream(configPath, FileMode.Open, FileAccess.Write))
            using (var writer = new BinaryWriter(stream))
            {
                writer.Write(bytes.Length);
                writer.Write(bytes);
            }

            context.nodes[0] = context.CreateInstance(context.clusterTestUtils.GetEndPoint(0), cleanClusterConfig: false);
            context.nodes[0].Start();
            context.CreateConnection();
            Assert.That(context.clusterTestUtils.ClusterNodes(0).Nodes.Single(node => node.IsMyself).NodeId, Is.EqualTo(nodeId));

            var server = context.clusterTestUtils.GetServer(context.endpoints[0].ToIPEndPoint());
            var response = (byte[])server.Execute("CLUSTER", "GOSSIP", Array.Empty<byte>());
            Assert.That(response[0], Is.EqualTo(1));
            Assert.That(server.Execute("CLUSTER", "BUMPEPOCH").ToString(), Is.EqualTo("OK"));
            context.nodes[0].Dispose(false);
            context.nodes[0] = null;
            Assert.That(File.ReadAllBytes(configPath)[sizeof(int)], Is.EqualTo(1));
        }

        private static Worker[] CreateFormatWorkers()
        {
            var primaryId = new string('a', 40);
            return
            [
                default,
                new Worker
                {
                    Nodeid = primaryId,
                    Address = "203.0.113.1",
                    Port = 17001,
                    ClusterAddress = "127.0.0.1",
                    ClusterPort = 7001,
                    ConfigEpoch = 1,
                    Role = Garnet.cluster.NodeRole.PRIMARY,
                    ReplicationOffset = 100,
                    hostname = "node.example.com"
                },
                new Worker
                {
                    Nodeid = new string('b', 40),
                    Address = "203.0.113.2",
                    Port = 17002,
                    ClusterAddress = "127.0.0.2",
                    ClusterPort = 7002,
                    ConfigEpoch = 2,
                    Role = Garnet.cluster.NodeRole.REPLICA,
                    ReplicaOfNodeId = primaryId,
                    ReplicationOffset = 90
                }
            ];
        }

        private static byte[] SerializeFormatFixture(byte version, Worker[] workers)
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);
            writer.Write(version);
            writer.Write((ushort)1);
            writer.Write((ushort)ClusterConfig.MAX_HASH_SLOT_VALUE);
            writer.Write((ushort)0);
            writer.Write((byte)SlotState.OFFLINE);
            writer.Write(workers.Length);
            foreach (var worker in workers.Skip(1))
            {
                writer.Write(worker.Nodeid);
                writer.Write(worker.Address);
                writer.Write(worker.Port);
                writer.Write(worker.ConfigEpoch);
                writer.Write((byte)worker.Role);
                writer.Write(worker.ReplicaOfNodeId != null);
                if (worker.ReplicaOfNodeId != null)
                    writer.Write(worker.ReplicaOfNodeId);
                writer.Write(worker.ReplicationOffset);
                writer.Write(worker.hostname != null);
                if (worker.hostname != null)
                    writer.Write(worker.hostname);
                if (version == 2)
                {
                    writer.Write(worker.ClusterAddress);
                    writer.Write(worker.ClusterPort);
                }
            }
            return stream.ToArray();
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
        /// Builds a replica whose slot map credits its own primary with <paramref name="slots"/>, which is what a
        /// replica gossips in a healthy cluster, and returns it alongside the two node-ids involved.
        /// </summary>
        private static (ClusterConfig replica, string replicaId, string primaryId) CreateReplicaSender(
            long primaryEpoch, long replicaEpoch, params int[] slots)
        {
            var primaryId = Generator.CreateHexId();
            var replicaId = Generator.CreateHexId();

            var primary = new ClusterConfig().InitializeLocalWorker(
                primaryId, "127.0.0.1", ClusterTestContext.Port + 1,
                primaryEpoch, Garnet.cluster.NodeRole.PRIMARY, null, "");
            foreach (var slot in slots)
                primary = primary.UpdateSlotState(slot, ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);

            // The replica learns the slots from its primary, so its own map has them STABLE under the primary.
            var replica = new ClusterConfig()
                .InitializeLocalWorker(
                    replicaId, "127.0.0.1", ClusterTestContext.Port + 2,
                    replicaEpoch, Garnet.cluster.NodeRole.REPLICA, primaryId, "")
                .Merge(primary, []);

            Assert.That(replica.GetNodeIdFromSlot((ushort)slots[0]), Is.EqualTo(primaryId),
                "precondition: the replica's map should credit its primary with the slot");

            return (replica, replicaId, primaryId);
        }

        /// <summary>
        /// Creates a primary that knows the replica and its primary, with every supplied slot left unowned.
        /// </summary>
        private static ClusterConfig CreateReceiverAwareOf(ClusterConfig other, params int[] unownedSlots)
        {
            var receiver = new ClusterConfig()
                .InitializeLocalWorker(
                    Generator.CreateHexId(), "127.0.0.1", ClusterTestContext.Port + 3,
                    configEpoch: 1, Garnet.cluster.NodeRole.PRIMARY, null, "")
                .Merge(other, []);

            // A node that just started with CleanClusterConfig, or one whose stale attribution was reset by
            // MergeSlotMap, knows the other workers but holds the slot unowned.
            foreach (var slot in unownedSlots)
                receiver = receiver.UpdateSlotState(slot, ClusterConfig.RESERVED_WORKER_ID, SlotState.OFFLINE);

            return receiver;
        }

        /// <summary>
        /// A replica sender must not be credited with a slot the receiver holds unowned. Doing so blocks the
        /// true owner, whose claim loses the config epoch comparison against the bogus owner until gossip
        /// from that owner resets the slot back to unowned.
        /// Also guards the null dereference of workers[RESERVED_WORKER_ID].Nodeid fixed by #1435.
        /// </summary>
        [Test, Order(12)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterConfigMergeSlotMapReplicaSenderCannotClaimUnownedSlotTest()
        {
            const int UnownedSlot = 300;

            // The replica's epoch exceeds its primary's, which is what makes a wrong assignment unrecoverable.
            var (replica, replicaId, primaryId) = CreateReplicaSender(primaryEpoch: 10, replicaEpoch: 30, UnownedSlot);
            var receiver = CreateReceiverAwareOf(replica, UnownedSlot);

            Assert.That(receiver.GetWorkerIdFromSlot(UnownedSlot), Is.EqualTo(ClusterConfig.RESERVED_WORKER_ID),
                "precondition: the receiver holds the slot unowned");

            ClusterConfig merged = null;
            Assert.DoesNotThrow(() => merged = receiver.Merge(replica, []),
                "workers[RESERVED_WORKER_ID].Nodeid is null, and dereferencing it was the failure fixed by #1435");

            Assert.That(merged.GetNodeIdFromSlot(UnownedSlot), Is.Not.EqualTo(replicaId),
                "a replica must never be recorded as the owner of a slot");
            Assert.That(merged.GetWorkerIdFromSlot(UnownedSlot), Is.EqualTo(ClusterConfig.RESERVED_WORKER_ID),
                "the slot must stay unowned so its real primary can still claim it");
            Assert.That(merged.GetNodeIdFromSlot(UnownedSlot), Is.Not.EqualTo(primaryId),
                "the replica's primary has no claim to the slot through a replica's gossip either");
        }

        /// <summary>
        /// A replica sender must not take a slot away from a node the receiver already credits with it.
        /// </summary>
        [Test, Order(13)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterConfigMergeSlotMapReplicaSenderCannotStealOwnedSlotTest()
        {
            const int OwnedSlot = 400;

            var (replica, replicaId, primaryId) = CreateReplicaSender(primaryEpoch: 10, replicaEpoch: 30, OwnedSlot);
            var receiver = CreateReceiverAwareOf(replica, OwnedSlot);

            // The receiver correctly credits the real primary with the slot.
            receiver = receiver.UpdateSlotState(OwnedSlot, receiver.GetWorkerIdFromNodeId(primaryId), SlotState.STABLE);

            var merged = receiver.Merge(replica, []);

            Assert.That(merged.GetNodeIdFromSlot(OwnedSlot), Is.EqualTo(primaryId),
                "ownership by the real primary must be left untouched by a replica's gossip");
            Assert.That(merged.GetNodeIdFromSlot(OwnedSlot), Is.Not.EqualTo(replicaId));
        }

        /// <summary>
        /// The planned-failover hand-off must keep working: when the receiver still credits the sender with a slot
        /// and the sender has since been demoted to a replica, the slot moves to the sender's new primary.
        /// </summary>
        [Test, Order(14)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterConfigMergeSlotMapReplicaSenderHandsOffOwnedSlotToPrimaryTest()
        {
            const int HandoffSlot = 500;

            var (replica, replicaId, primaryId) = CreateReplicaSender(primaryEpoch: 10, replicaEpoch: 30, HandoffSlot);
            var receiver = CreateReceiverAwareOf(replica, HandoffSlot);

            // The receiver still believes the demoted sender owns the slot.
            receiver = receiver.UpdateSlotState(HandoffSlot, receiver.GetWorkerIdFromNodeId(replicaId), SlotState.STABLE);

            var merged = receiver.Merge(replica, []);

            Assert.That(merged.GetNodeIdFromSlot(HandoffSlot), Is.EqualTo(primaryId),
                "the slot should be handed off to the node that took over from the sender");
            Assert.That(merged.GetState((ushort)HandoffSlot), Is.EqualTo(SlotState.STABLE));
        }

        /// <summary>
        /// The hand-off correction applies to the slot that earned it and must not carry over to later slots in
        /// the same merge.
        /// </summary>
        [Test, Order(15)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterConfigMergeSlotMapReplicaHandoffDoesNotLeakToLaterSlotsTest()
        {
            const int HandoffSlot = 600;  // visited first, takes the hand-off correction
            const int UnownedSlot = 700;  // visited later, must be left alone

            var (replica, replicaId, primaryId) =
                CreateReplicaSender(primaryEpoch: 10, replicaEpoch: 30, HandoffSlot, UnownedSlot);
            var receiver = CreateReceiverAwareOf(replica, HandoffSlot, UnownedSlot);

            receiver = receiver.UpdateSlotState(HandoffSlot, receiver.GetWorkerIdFromNodeId(replicaId), SlotState.STABLE);

            var merged = receiver.Merge(replica, []);

            Assert.That(merged.GetNodeIdFromSlot(HandoffSlot), Is.EqualTo(primaryId),
                "precondition: the earlier slot takes the hand-off correction");
            Assert.That(merged.GetWorkerIdFromSlot(UnownedSlot), Is.EqualTo(ClusterConfig.RESERVED_WORKER_ID),
                "the later unowned slot must not inherit the correction computed for the earlier slot");
        }
    }
}