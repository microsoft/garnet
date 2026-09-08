// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Garnet.cluster;
using Garnet.common;
using Garnet.server;
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

        [Test, Order(2)]
        [Category("CLUSTER-CONFIG"), CancelAfter(5000)]
        public void ClusterClientEndpointRecoverTest()
        {
            const string clientAddress = "203.0.113.10";
            const int clientPort = 17000;
            const string clientHostname = "node.example.com";

            context.CreateInstances(
                1,
                clusterPreferredEndpointType: ClusterPreferredEndpointType.Hostname,
                clusterClientAnnounceIp: clientAddress,
                clusterClientAnnouncePortBase: clientPort,
                clusterClientAnnounceHostname: clientHostname);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            AssertClientEndpoint();

            context.nodes[0].Dispose(false);
            context.nodes[0] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(0),
                cleanClusterConfig: false,
                tryRecover: true,
                clusterPreferredEndpointType: ClusterPreferredEndpointType.Hostname,
                clusterClientAnnounceIp: clientAddress,
                clusterClientAnnouncePort: clientPort,
                clusterClientAnnounceHostname: clientHostname);
            context.nodes[0].Start();
            context.CreateConnection();

            AssertClientEndpoint();

            void AssertClientEndpoint()
            {
                var slots = context.clusterTestUtils.ClusterSlots(0, context.logger);
                var endpoint = slots.SelectMany(slot => slot.nnInfo).Single();
                ClassicAssert.AreEqual(clientHostname, endpoint.endpoint);
                ClassicAssert.AreEqual(clientAddress, endpoint.ip);
                ClassicAssert.AreEqual(clientPort, endpoint.port);
            }
        }

        [Test, Order(3)]
        [TestCase(-1)]
        [TestCase(int.MaxValue)]
        public void InvalidReplicationHistoryLengthRecoversTest(int length)
        {
            context.CreateInstances(1, enableAOF: true);
            string checkpointDirectory = context.nodeOptions[0].CheckpointDir;
            context.ShutdownNode(0, ensureAofFlush: true);
            string[] files = Directory.GetFiles(checkpointDirectory, "replication.conf*", SearchOption.AllDirectories);
            Assert.That(files, Has.Length.EqualTo(1));
            using (FileStream file = File.OpenWrite(files[0]))
                file.Write(BitConverter.GetBytes(length));

            Assert.DoesNotThrow(() => context.RestartNode(0));
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

        [Test]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterClientEndpointRoundTripTest()
        {
            const string transportAddress = "127.0.0.1";
            const int transportPort = 7001;
            const string clientAddress = "203.0.113.10";
            const int clientPort = 17001;
            const string clientHostname = "node.example.com";

            var config = new ClusterConfig().InitializeLocalWorker(
                Generator.CreateHexId(),
                transportAddress,
                transportPort,
                configEpoch: 1,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "internal.example.com",
                clientAddress,
                clientPort,
                clientHostname);
            config = config.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);

            Assert.That(config.GetWorkerAddress(ClusterConfig.LOCAL_WORKER_ID), Is.EqualTo((transportAddress, transportPort)));
            Assert.That(config.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Ip), Is.EqualTo((clientAddress, clientPort)));
            Assert.That(config.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Hostname), Is.EqualTo((clientHostname, clientPort)));
            Assert.That(config.AskEndpointFromSlot(0, ClusterPreferredEndpointType.Ip), Is.EqualTo((clientAddress, clientPort)));
            Assert.That(config.AskEndpointFromSlot(0, ClusterPreferredEndpointType.Hostname), Is.EqualTo((clientHostname, clientPort)));

            var restored = ClusterConfig.FromByteArray(config.ToByteArray());
            Assert.That(restored.GetWorkerAddress(ClusterConfig.LOCAL_WORKER_ID), Is.EqualTo((transportAddress, transportPort)));
            Assert.That(restored.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Ip), Is.EqualTo((clientAddress, clientPort)));
            Assert.That(restored.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Hostname), Is.EqualTo((clientHostname, clientPort)));
            Assert.That(restored.AskEndpointFromSlot(0, ClusterPreferredEndpointType.Ip), Is.EqualTo((clientAddress, clientPort)));
            Assert.That(restored.AskEndpointFromSlot(0, ClusterPreferredEndpointType.Hostname), Is.EqualTo((clientHostname, clientPort)));
        }

        [Test]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void LegacyClusterConfigUsesTransportEndpointTest()
        {
            const string transportAddress = "127.0.0.1";
            const int transportPort = 7001;
            var nodeId = Generator.CreateHexId();

            var config = new ClusterConfig().InitializeLocalWorker(
                nodeId,
                transportAddress,
                transportPort,
                configEpoch: 1,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "internal.example.com");
            config = config.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);

            var legacyBytes = config.ToByteArray();
            var extendedConfig = new ClusterConfig().InitializeLocalWorker(
                nodeId,
                transportAddress,
                transportPort,
                configEpoch: 1,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "internal.example.com",
                "203.0.113.10",
                17001,
                "node.example.com");
            extendedConfig = extendedConfig.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);
            var extendedBytes = extendedConfig.ToByteArray();

            Assert.That(extendedBytes.Length, Is.GreaterThan(legacyBytes.Length));
            Assert.That(extendedBytes.AsSpan(0, legacyBytes.Length).SequenceEqual(legacyBytes), Is.True);

            var restored = ClusterConfig.FromByteArray(legacyBytes);
            Assert.That(restored.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Ip), Is.EqualTo((transportAddress, transportPort)));
            Assert.That(restored.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Hostname), Is.EqualTo(("internal.example.com", transportPort)));
        }

        [Test]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void FutureClientEndpointExtensionFallsBackToTransportEndpointTest()
        {
            const string transportAddress = "127.0.0.1";
            const int transportPort = 7001;
            var nodeId = Generator.CreateHexId();

            var legacyConfig = new ClusterConfig().InitializeLocalWorker(
                nodeId,
                transportAddress,
                transportPort,
                configEpoch: 1,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "internal.example.com");
            legacyConfig = legacyConfig.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);

            var extendedConfig = new ClusterConfig().InitializeLocalWorker(
                nodeId,
                transportAddress,
                transportPort,
                configEpoch: 1,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "internal.example.com",
                "203.0.113.10",
                17001,
                "node.example.com");
            extendedConfig = extendedConfig.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);

            var legacyBytes = legacyConfig.ToByteArray();
            var futureBytes = extendedConfig.ToByteArray();
            Assert.That(futureBytes.Length, Is.GreaterThan(legacyBytes.Length));
            futureBytes[legacyBytes.Length] = 2;

            var restored = ClusterConfig.FromByteArray(futureBytes);
            Assert.That(restored.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Ip), Is.EqualTo((transportAddress, transportPort)));
            Assert.That(restored.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Hostname), Is.EqualTo(("internal.example.com", transportPort)));
        }

        [Test]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void TruncatedClientEndpointExtensionThrowsTest()
        {
            var config = new ClusterConfig().InitializeLocalWorker(
                Generator.CreateHexId(),
                "127.0.0.1",
                7001,
                configEpoch: 1,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "internal.example.com",
                "203.0.113.10",
                17001,
                "node.example.com");

            var serialized = config.ToByteArray();
            var truncated = serialized.AsSpan(0, serialized.Length - 1).ToArray();

            Assert.Throws<InvalidDataException>(() => ClusterConfig.FromByteArray(truncated));
        }

        [Test]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClientEndpointUpdateAtSameConfigEpochMergesFromOwnerTest()
        {
            const string remoteNodeAddress = "127.0.0.2";
            const int remoteNodePort = 7002;
            const long configEpoch = 2;
            var localNodeId = Generator.CreateHexId();
            var remoteNodeId = Generator.CreateHexId();
            ConcurrentDictionary<string, long> workerBanList = new();

            var localConfig = new ClusterConfig().InitializeLocalWorker(
                localNodeId,
                "127.0.0.1",
                7001,
                configEpoch: 1,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "local.internal.example.com");

            var remoteConfig = new ClusterConfig().InitializeLocalWorker(
                remoteNodeId,
                remoteNodeAddress,
                remoteNodePort,
                configEpoch,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "remote.internal.example.com",
                "203.0.113.10",
                17002,
                "remote.example.com");
            remoteConfig = remoteConfig.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);

            localConfig = localConfig.Merge(remoteConfig, workerBanList);
            Assert.That(localConfig.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Hostname), Is.EqualTo(("remote.example.com", 17002)));

            var updatedRemoteConfig = new ClusterConfig().InitializeLocalWorker(
                remoteNodeId,
                remoteNodeAddress,
                remoteNodePort,
                configEpoch,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "remote.internal.example.com",
                "203.0.113.11",
                18002,
                "updated.example.com");
            updatedRemoteConfig = updatedRemoteConfig.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);

            localConfig = localConfig.Merge(updatedRemoteConfig, workerBanList);

            Assert.That(localConfig.GetWorkerAddressFromNodeId(remoteNodeId), Is.EqualTo((remoteNodeAddress, remoteNodePort)));
            Assert.That(localConfig.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Ip), Is.EqualTo(("203.0.113.11", 18002)));
            Assert.That(localConfig.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Hostname), Is.EqualTo(("updated.example.com", 18002)));

            var clearedRemoteConfig = new ClusterConfig().InitializeLocalWorker(
                remoteNodeId,
                remoteNodeAddress,
                remoteNodePort,
                configEpoch,
                Garnet.cluster.NodeRole.PRIMARY,
                null,
                "remote.internal.example.com",
                clientAddress: null,
                clientPort: 0,
                clientHostname: null);
            clearedRemoteConfig = clearedRemoteConfig.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);

            localConfig = localConfig.Merge(clearedRemoteConfig, workerBanList);

            Assert.That(localConfig.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Ip), Is.EqualTo((remoteNodeAddress, remoteNodePort)));
            Assert.That(localConfig.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Hostname), Is.EqualTo(("remote.internal.example.com", remoteNodePort)));
        }

        [Test, Order(5)]
        [TestCase("invalid", 17001, "node.example.com")]
        [TestCase("203.0.113.10", -1, "node.example.com")]
        [TestCase("203.0.113.10", 65536, "node.example.com")]
        [TestCase("203.0.113.10", 17001, "bad\r\nhostname")]
        public void InvalidSerializedClientEndpointTest(string address, int port, string hostname)
        {
            ClusterConfig config = new ClusterConfig().InitializeLocalWorker(
                Generator.CreateHexId(), "127.0.0.1", 7001, 1, Garnet.cluster.NodeRole.PRIMARY,
                null, "peer.example.com", address, port, hostname);
            Assert.Throws<InvalidDataException>(() => ClusterConfig.FromByteArray(config.ToByteArray()));
        }

        [Test]
        public void ClientEndpointDoesNotReplacePeerEndpointsTest()
        {
            string primaryId = Generator.CreateHexId();
            string replicaId = Generator.CreateHexId();
            ClusterConfig primary = new ClusterConfig().InitializeLocalWorker(
                primaryId, "127.0.0.1", 7001, 1, Garnet.cluster.NodeRole.PRIMARY,
                null, "primary.peer.example", "203.0.113.1", 65535, "primary.example.com");
            primary = primary.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);
            ClusterConfig replica = new ClusterConfig().InitializeLocalWorker(
                replicaId, "127.0.0.2", 7002, 2, Garnet.cluster.NodeRole.REPLICA,
                primaryId, "replica.peer.example", "203.0.113.2", 17002, "replica.example.com");

            primary = ClusterConfig.FromByteArray(primary.Merge(replica, []).ToByteArray());
            replica = ClusterConfig.FromByteArray(replica.Merge(primary, []).ToByteArray());
            ushort replicaWorkerId = primary.GetWorkerIdFromNodeId(replicaId);

            Assert.That(primary.GetWorkerAddress(replicaWorkerId), Is.EqualTo(("127.0.0.2", 7002)));
            Assert.That(primary.GetWorkerAddressFromNodeId(replicaId), Is.EqualTo(("127.0.0.2", 7002)));
            Assert.That(primary.GetReplicaEndpoints(primaryId).Single(), Is.EqualTo(("127.0.0.2", 7002)));
            Assert.That(primary.GetWorkerInfoForGossip().Single(), Is.EqualTo((replicaId, "127.0.0.2", 7002)));
            Assert.That(primary.GetWorkerNodeIdFromAddress("203.0.113.2", 17002), Is.Null);
            Assert.That(primary.GetWorkerNodeIdFromAddressOrHostname("replica.example.com", 17002), Is.Null);
            Assert.That(primary.GetWorkerNodeIdFromAddressOrHostname("replica.peer.example", 7002), Is.EqualTo(replicaId));
            Assert.That(replica.GetLocalNodePrimaryAddress(), Is.EqualTo(("127.0.0.1", 7001)));
            Assert.That(primary.GetClusterInfo(null), Does.Contain("203.0.113.1:65535@17001,primary.example.com"));
            Assert.That(primary.GetNodeInfo(replicaWorkerId, default), Does.Contain("203.0.113.2:17002@17002,replica.example.com"));
        }

        [Test]
        public void OlderRelayPreservesClientEndpointUntilOwnerClearsItTest()
        {
            string ownerId = Generator.CreateHexId();
            ClusterConfig receiver = new ClusterConfig().InitializeLocalWorker(
                Generator.CreateHexId(), "127.0.0.1", 7001, 1, Garnet.cluster.NodeRole.PRIMARY, null, "");
            ClusterConfig owner = new ClusterConfig().InitializeLocalWorker(
                ownerId, "127.0.0.2", 7002, 2, Garnet.cluster.NodeRole.PRIMARY, null, "",
                "203.0.113.2", 17002, "owner.example.com");
            owner = owner.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);
            receiver = receiver.Merge(owner, []);

            ClusterConfig legacyOwner = new ClusterConfig().InitializeLocalWorker(
                ownerId, "127.0.0.2", 7002, 3, Garnet.cluster.NodeRole.PRIMARY, null, "");
            legacyOwner = legacyOwner.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);
            ClusterConfig relay = new ClusterConfig().InitializeLocalWorker(
                Generator.CreateHexId(), "127.0.0.3", 7003, 4, Garnet.cluster.NodeRole.PRIMARY, null, "");
            relay = relay.Merge(legacyOwner, []);
            receiver = receiver.Merge(ClusterConfig.FromByteArray(relay.ToByteArray()), []);
            Assert.That(receiver.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Ip), Is.EqualTo(("203.0.113.2", 17002)));

            receiver = receiver.Merge(legacyOwner, []);
            Assert.That(receiver.GetEndpointFromSlot(0, ClusterPreferredEndpointType.Ip), Is.EqualTo(("127.0.0.2", 7002)));
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
    }
}