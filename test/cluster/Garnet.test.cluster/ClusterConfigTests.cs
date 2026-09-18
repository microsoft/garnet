// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
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
            Assert.That(version, Is.EqualTo(2));

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
            Assert.That(config.ToByteArray(), Is.EqualTo(SerializeFormatFixture(2, workers)));

            var restored = ClusterConfig.FromByteArray(expected);
            if (version == 1)
            {
                for (var i = 1; i < workers.Length; i++)
                {
                    workers[i].ClusterAddress = null;
                    workers[i].ClusterPort = 0;
                }
            }

            Assert.That(restored.ToByteArray(2), Is.EqualTo(SerializeFormatFixture(2, workers)));
            Assert.That(restored.Copy().ToByteArray(2), Is.EqualTo(restored.ToByteArray(2)));
            Assert.That(restored.ToByteArray(), Is.EqualTo(SerializeFormatFixture(2, workers)));

            var initialized = restored.InitializeLocalWorker(workers[1].Nodeid, workers[1].Address,
                workers[1].Port, workers[1].ConfigEpoch, workers[1].Role, null, workers[1].hostname);
            workers[1].ReplicationOffset = 0;
            workers[1].ClusterAddress = workers[1].Address;
            workers[1].ClusterPort = workers[1].Port;
            Assert.That(initialized.ToByteArray(2), Is.EqualTo(SerializeFormatFixture(2, workers)));
        }

        [Test]
        [Category("CLUSTER-CONFIG")]
        public void ClusterConfigRoutesPeersSeparatelyFromClientsTest()
        {
            var config = ClusterConfig.FromByteArray(SerializeFormatFixture(2, CreateFormatWorkers()));
            config = config.AssignSlots([0], ClusterConfig.LOCAL_WORKER_ID, SlotState.STABLE);

            Assert.That(config.GetWorkerAddress(1), Is.EqualTo(("127.0.0.1", 7001)));
            Assert.That(config.GetWorkerAddressFromNodeId(new string('b', 40)), Is.EqualTo(("127.0.0.2", 7002)));
            Assert.That(config.GetEndpointFromNodeId(new string('b', 40)), Is.EqualTo(new IPEndPoint(IPAddress.Parse("127.0.0.2"), 7002)));
            Assert.That(config.GetLocalNodeReplicaEndpoints().Single(), Is.EqualTo(new IPEndPoint(IPAddress.Parse("127.0.0.2"), 7002)));
            Assert.That(config.GetEndpointFromSlot(0, Garnet.server.ClusterPreferredEndpointType.Ip), Is.EqualTo(("203.0.113.1", 17001)));
            Assert.That(config.AskEndpointFromSlot(0, Garnet.server.ClusterPreferredEndpointType.Ip), Is.EqualTo(("203.0.113.1", 17001)));
            Assert.That(config.GetEndpointFromSlot(0, Garnet.server.ClusterPreferredEndpointType.Hostname), Is.EqualTo(("node.example.com", 17001)));
            Assert.That(config.GetReplicaEndpoints(config.LocalNodeId).Single(), Is.EqualTo(("127.0.0.2", 7002)));
            Assert.That(config.GetWorkerInfoForGossip().Single(), Is.EqualTo((new string('b', 40), "127.0.0.2", 7002)));
            Assert.That(config.GetEndpointFromNodeId(config.LocalNodeId), Is.EqualTo(new IPEndPoint(IPAddress.Loopback, 7001)));
            config.GetAllNodeIds(out var allNodes);
            config.GetNodeIdsForShard(out var shardNodes);
            Assert.That(allNodes.Single().EndPoint.Port, Is.EqualTo(7002));
            Assert.That(shardNodes.Single().EndPoint.Port, Is.EqualTo(7002));
            Assert.That(config.GetWorkerNodeIdFromAddress("127.0.0.2", 7002), Is.EqualTo(new string('b', 40)));
            Assert.That(config.GetWorkerNodeIdFromAddressOrHostname("203.0.113.2", 17002), Is.EqualTo(new string('b', 40)));
            Assert.That(config.GetWorkerNodeIdFromAddressOrHostname("127.0.0.2", 7002), Is.EqualTo(new string('b', 40)));
            Assert.That(config.GetClusterInfo(null), Does.Contain("203.0.113.1:17001@17001,node.example.com"));
        }

        [Test]
        [Category("CLUSTER-CONFIG")]
        public void ClusterConfigLegacyRelayPreservesKnownEndpointsTest()
        {
            var workers = CreateFormatWorkers();
            var owner = ClusterConfig.FromByteArray(SerializeFormatFixture(2, [default, workers[1]]));
            var receiver = new ClusterConfig().InitializeLocalWorker(new string('c', 40), "127.0.0.3", 7003,
                3, Garnet.cluster.NodeRole.PRIMARY, null, "");
            receiver = receiver.Merge(owner, []);
            var snapshot = receiver;

            workers[1].ConfigEpoch++;
            var legacyRelay = ClusterConfig.FromByteArray(SerializeFormatFixture(1, [default, workers[2], workers[1]]));
            var upgradedRelay = ClusterConfig.FromByteArray(legacyRelay.ToByteArray());
            Assert.That(upgradedRelay.GetWorkerFromNodeId(owner.LocalNodeId).ClusterAddress, Is.Null);
            Assert.That(upgradedRelay.GetWorkerAddressFromNodeId(owner.LocalNodeId), Is.EqualTo(("203.0.113.1", 17001)));

            receiver = receiver.Merge(upgradedRelay, []);
            Assert.That(receiver.GetWorkerAddressFromNodeId(owner.LocalNodeId), Is.EqualTo(("127.0.0.1", 7001)));
            Assert.That(receiver.GetWorkerFromNodeId(owner.LocalNodeId).ConfigEpoch, Is.EqualTo(2));
            Assert.That(snapshot.GetWorkerFromNodeId(owner.LocalNodeId).ConfigEpoch, Is.EqualTo(1));

            receiver = ClusterConfig.FromByteArray(receiver.ToByteArray());
            Assert.That(receiver.GetWorkerAddressFromNodeId(owner.LocalNodeId), Is.EqualTo(("127.0.0.1", 7001)));
            workers[1].ClusterPort = 7004;
            var changedOwner = ClusterConfig.FromByteArray(SerializeFormatFixture(2, [default, workers[1]]));
            receiver = receiver.Merge(changedOwner, []);
            receiver = receiver.Merge(upgradedRelay, []);
            receiver = receiver.Merge(owner, []);
            Assert.That(receiver.GetWorkerAddressFromNodeId(owner.LocalNodeId), Is.EqualTo(("127.0.0.1", 7004)));
        }

        [Test]
        [Category("CLUSTER-CONFIG")]
        public void ClusterConfigFillsMissingRelayedEndpointAtSameEpochTest()
        {
            var workers = CreateFormatWorkers();
            var staleRelay = ClusterConfig.FromByteArray(SerializeFormatFixture(1, [default, workers[2], workers[1]]));
            var receiver = new ClusterConfig().InitializeLocalWorker(new string('c', 40), "127.0.0.3", 7003,
                3, Garnet.cluster.NodeRole.PRIMARY, null, "").Merge(staleRelay, []);
            receiver = ClusterConfig.FromByteArray(receiver.ToByteArray());
            Assert.That(receiver.GetWorkerFromNodeId(workers[1].Nodeid).ClusterAddress, Is.Null);

            var relay = ClusterConfig.FromByteArray(SerializeFormatFixture(2, [default, workers[2], workers[1]]));
            receiver = receiver.Merge(relay, []);
            Assert.That(receiver.GetWorkerAddressFromNodeId(workers[1].Nodeid), Is.EqualTo(("127.0.0.1", 7001)));

            workers[1].ClusterPort = 7004;
            var owner = ClusterConfig.FromByteArray(SerializeFormatFixture(2, [default, workers[1]]));
            receiver = receiver.Merge(owner, []).Merge(relay, []).Merge(staleRelay, []);
            Assert.That(receiver.GetWorkerAddressFromNodeId(workers[1].Nodeid), Is.EqualTo(("127.0.0.1", 7004)));

            var legacyOwner = ClusterConfig.FromByteArray(owner.ToByteArray(1));
            receiver = receiver.Merge(legacyOwner, []);
            Assert.That(receiver.GetWorkerAddressFromNodeId(workers[1].Nodeid), Is.EqualTo(("203.0.113.1", 17001)));
        }

        [Test]
        [Category("CLUSTER-CONFIG")]
        public void ClusterConfigGossipNegotiatesPerConnectionTest()
        {
            context.CreateInstances(1);
            context.CreateConnection();
            var server = context.clusterTestUtils.GetServer(context.endpoints[0].ToIPEndPoint());
            Assert.That(((byte[])server.Execute("CLUSTER", "GOSSIP", Array.Empty<byte>()))[0], Is.EqualTo(1));
            Assert.That((int)server.Execute("CLUSTER", "GOSSIP"), Is.EqualTo(2));
            Assert.That(((byte[])server.Execute("CLUSTER", "GOSSIP", Array.Empty<byte>()))[0], Is.EqualTo(2));

            using var client = TestUtils.GetGarnetClient(context.endpoints[0]);
            client.Connect();
            using var response = client.GossipAsync(Array.Empty<byte>()).GetAwaiter().GetResult();
            Assert.That(response.Span[0], Is.EqualTo(1));
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
            Assert.That(response[0], Is.EqualTo(version == 2 ? 2 : 1));
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
            Assert.That(File.ReadAllBytes(configPath)[sizeof(int)], Is.EqualTo(2));
        }

        [Test]
        public void ClusterConfigOwnerRefreshesEndpointsAtSameEpochTest()
        {
            var workers = CreateFormatWorkers();
            var receiver = ClusterConfig.FromByteArray(SerializeFormatFixture(2, workers));
            var original = receiver.ToByteArray();
            var owner = workers[2];
            owner.Address = "203.0.113.3";
            owner.Port = 18003;
            owner.ClusterAddress = "127.0.0.3";
            owner.ClusterPort = 7003;
            owner.hostname = "changed.example.com";
            owner.Role = Garnet.cluster.NodeRole.PRIMARY;
            owner.ReplicaOfNodeId = null;
            var sender = new ClusterConfig(new HashSlot[ClusterConfig.MAX_HASH_SLOT_VALUE], [default, owner]);

            var refreshed = receiver.Merge(sender, []);
            var actual = refreshed.GetWorkerFromNodeId(owner.Nodeid);
            Assert.That(actual.ClusterAddress, Is.EqualTo(owner.ClusterAddress));
            Assert.That(actual.ClusterPort, Is.EqualTo(owner.ClusterPort));
            Assert.That(actual.Address, Is.EqualTo(owner.Address));
            Assert.That(actual.Port, Is.EqualTo(owner.Port));
            Assert.That(actual.hostname, Is.EqualTo(owner.hostname));
            Assert.That(actual.Role, Is.EqualTo(workers[2].Role));
            Assert.That(actual.ReplicaOfNodeId, Is.EqualTo(workers[2].ReplicaOfNodeId));
            Assert.That(receiver.ToByteArray(), Is.EqualTo(original));

            var intermediary = workers[1];
            intermediary.Nodeid = new string('c', 40);
            var staleGossip = new ClusterConfig(new HashSlot[ClusterConfig.MAX_HASH_SLOT_VALUE], [default, intermediary, workers[2]]);
            refreshed = refreshed.Merge(staleGossip, []);
            Assert.That(refreshed.GetWorkerAddressFromNodeId(owner.Nodeid), Is.EqualTo((owner.ClusterAddress, owner.ClusterPort)));
            owner.ConfigEpoch--;
            sender = new ClusterConfig(new HashSlot[ClusterConfig.MAX_HASH_SLOT_VALUE], [default, owner]);
            Assert.That(refreshed.Merge(sender, []).GetWorkerAddressFromNodeId(owner.Nodeid), Is.EqualTo((actual.ClusterAddress, actual.ClusterPort)));
        }

        [TestCase(1)]
        [TestCase(2)]
        public void ClusterEndpointRecoveryOverridesTest(byte version)
        {
            context.CreateInstances(1);
            context.CreateConnection();
            var config = ReadLiveConfig(0);
            var nodeId = config.LocalNodeId;
            context.ShutdownNode(0);
            var bytes = config.ToByteArray(version);
            var path = Directory.GetFiles(context.TestFolder, "nodes.conf*", SearchOption.AllDirectories).Single();
            using (var stream = new FileStream(path, FileMode.Open, FileAccess.Write))
            using (var writer = new BinaryWriter(stream))
            {
                writer.Write(bytes.Length);
                writer.Write(bytes);
            }
            context.nodeOptions[0].ClusterAnnounceEndpoint = new IPEndPoint(IPAddress.Parse("203.0.113.10"), 17001);
            context.nodeOptions[0].ClusterAnnounceHostname = "public.example.com";
            context.nodeOptions[0].ClusterAddress = "127.0.0.2";
            context.nodeOptions[0].ClusterPort = 7001;
            context.RestartNode(0);
            context.CreateConnection();
            var recovered = ReadLiveConfig(0);
            var worker = recovered.GetWorkerFromNodeId(nodeId);
            Assert.That(recovered.LocalNodeId, Is.EqualTo(nodeId));
            Assert.That(worker.Address, Is.EqualTo("203.0.113.10"));
            Assert.That(worker.Port, Is.EqualTo(17001));
            Assert.That(worker.hostname, Is.EqualTo("public.example.com"));
            Assert.That(worker.ClusterAddress, Is.EqualTo("127.0.0.2"));
            Assert.That(worker.ClusterPort, Is.EqualTo(7001));

            context.nodeOptions[0].ClusterAddress = null;
            context.nodeOptions[0].ClusterPort = 0;
            context.RestartNode(0);
            context.CreateConnection();
            worker = ReadLiveConfig(0).GetWorkerFromNodeId(nodeId);
            Assert.That(worker.ClusterAddress, Is.EqualTo(worker.Address));
            Assert.That(worker.ClusterPort, Is.EqualTo(worker.Port));
            context.ShutdownNode(0);
            Assert.That(File.ReadAllBytes(path)[sizeof(int)], Is.EqualTo(2));
        }

        [Test]
        public void SeparateEndpointsMeetReplicationAndFailoverTest()
        {
            CreateSeparateEndpointCluster(3);
            var primaryId = ReadLiveConfig(0).LocalNodeId;
            const string key = "{peer}key";
            var slot = HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(key));
            Assert.That(ExecuteNode(0, "CLUSTER", "ADDSLOTS", (int)slot).ToString(), Is.EqualTo("OK"));
            Assert.That(ExecuteNode(0, "SET", key, "value").ToString(), Is.EqualTo("OK"));
            Assert.That(context.clusterTestUtils.ClusterReplicate(1, primaryId), Is.EqualTo("OK"));
            Assert.That(context.clusterTestUtils.ClusterReplicate(2, primaryId), Is.EqualTo("OK"));
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, cancellation: context.cts.Token);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 2, cancellation: context.cts.Token);
            WaitForConfig(0, config => config.GetReplicaIds(primaryId).Count == 2);
            WaitForConfig(1, config => config.GetReplicaIds(primaryId).Count == 2);

            var slots = context.clusterTestUtils.ClusterSlots(0, context.logger);
            Assert.That(slots.Single().nnInfo.Select(info => info.port), Is.EquivalentTo([17001, 17002, 17003]));
            Assert.That(context.clusterTestUtils.ClusterFailover(1), Is.EqualTo("OK"));
            WaitForConfig(1, config => config.IsPrimary);
            var newPrimaryId = ReadLiveConfig(1).LocalNodeId;
            WaitForConfig(2, config => config.LocalNodePrimaryId == newPrimaryId);
            context.clusterTestUtils.WaitForReplicaAofSync(1, 2, cancellation: context.cts.Token);
            Assert.That(ExecuteNode(1, "GET", key).ToString(), Is.EqualTo("value"));
        }

        [TestCase(false)]
        [TestCase(true)]
        public unsafe void SeparateEndpointsRedirectAndMigrationTest(bool useHostname)
        {
            CreateSeparateEndpointCluster(2, useHostname);
            const string key = "{peer}key";
            var slot = HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(key));
            var sourceId = ReadLiveConfig(0).LocalNodeId;
            var targetId = ReadLiveConfig(1).LocalNodeId;
            Assert.That(ExecuteNode(0, "CLUSTER", "ADDSLOTS", (int)slot).ToString(), Is.EqualTo("OK"));
            Assert.That(ExecuteNode(0, "SET", key, "value").ToString(), Is.EqualTo("OK"));
            WaitForConfig(1, config => config.GetNodeIdFromSlot(slot) == sourceId);
            using var target = new LightClientRequest(context.endpoints[1], 0, countResponseType: CountResponseType.Newlines);
            var moved = Encoding.ASCII.GetString(target.SendCommand($"GET {key}"));
            Assert.That(moved, Does.StartWith($"-MOVED {slot} {(useHostname ? "node0.example.com" : "203.0.113.1")}:17001\r\n"));
            Assert.That(ExecuteNode(1, "CLUSTER", "SETSLOT", (int)slot, "IMPORTING", sourceId).ToString(), Is.EqualTo("OK"));
            Assert.That(ExecuteNode(0, "CLUSTER", "SETSLOT", (int)slot, "MIGRATING", targetId).ToString(), Is.EqualTo("OK"));
            using var source = new LightClientRequest(context.endpoints[0], 0, countResponseType: CountResponseType.Newlines);
            var ask = Encoding.ASCII.GetString(source.SendCommand("GET {peer}missing"));
            Assert.That(ask, Does.StartWith($"-ASK {slot} {(useHostname ? "node1.example.com" : "203.0.113.2")}:17002\r\n"));
            Assert.That(ExecuteNode(0, "MIGRATE", useHostname ? "node1.example.com" : "203.0.113.2", 17002, "", 0, 10000, "KEYS", key).ToString(), Is.EqualTo("OK"));
            Assert.That(ExecuteNode(1, "CLUSTER", "SETSLOT", (int)slot, "NODE", targetId).ToString(), Is.EqualTo("OK"));
            Assert.That(ExecuteNode(0, "CLUSTER", "SETSLOT", (int)slot, "NODE", targetId).ToString(), Is.EqualTo("OK"));
            Assert.That(ExecuteNode(1, "GET", key).ToString(), Is.EqualTo("value"));
            var shards = context.clusterTestUtils.ClusterShards(1, context.logger);
            Assert.That(shards.SelectMany(shard => shard.nodes).Select(node => node.ip),
                Is.EquivalentTo(["203.0.113.1", "203.0.113.2"]));
        }

        [Test]
        public void SeparateEndpointsRefreshAfterOwnerRestartTest()
        {
            CreateSeparateEndpointCluster(2);
            var original = ReadLiveConfig(1);
            var peerEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.2"), context.endpoints[1].ToIPEndPoint().Port + 20);
            context.nodeOptions[1].EndPoints = [peerEndpoint];
            context.nodeOptions[1].ClusterAddress = peerEndpoint.Address.ToString();
            context.nodeOptions[1].ClusterPort = peerEndpoint.Port;
            context.nodeOptions[1].ClusterAnnounceEndpoint = new IPEndPoint(IPAddress.Parse("203.0.113.20"), 18020);
            context.nodeOptions[1].ClusterAnnounceHostname = "restarted.example.com";
            context.RestartNode(1);
            WaitForConfig(0, config => config.GetWorkerAddressFromNodeId(original.LocalNodeId) == ("127.0.0.2", peerEndpoint.Port));
            var worker = ReadLiveConfig(0).GetWorkerFromNodeId(original.LocalNodeId);
            Assert.That(worker.ConfigEpoch, Is.EqualTo(original.LocalNodeConfigEpoch));
            Assert.That(worker.Address, Is.EqualTo("203.0.113.20"));
            Assert.That(worker.Port, Is.EqualTo(18020));
            Assert.That(worker.hostname, Is.EqualTo("restarted.example.com"));

            while (true)
            {
                context.cts.Token.ThrowIfCancellationRequested();
                var nodes = ExecuteNode(0, "CLUSTER", "NODES").ToString();
                var remote = nodes.Split('\n').Single(line => line.StartsWith(original.LocalNodeId, StringComparison.Ordinal));
                if (remote.Split(' ')[7] == "connected")
                    break;
                Thread.Sleep(20);
            }
        }

        [TestCase(null, 0, "203.0.113.1", 17001)]
        [TestCase("127.0.0.2", 0, "127.0.0.2", 17001)]
        [TestCase(null, 7001, "203.0.113.1", 7001)]
        public void ClusterPeerOverridesDefaultIndependentlyTest(string address, int port, string expectedAddress, int expectedPort)
        {
            context.CreateInstances(1);
            context.nodeOptions[0].ClusterAnnounceEndpoint = new IPEndPoint(IPAddress.Parse("203.0.113.1"), 17001);
            context.nodeOptions[0].ClusterAddress = address;
            context.nodeOptions[0].ClusterPort = port;
            context.RestartNode(0);
            context.CreateConnection();
            Assert.That(ReadLiveConfig(0).GetWorkerAddress(1), Is.EqualTo((expectedAddress, expectedPort)));
            Assert.That(ExecuteNode(0, "CLUSTER", "RESET", "HARD").ToString(), Is.EqualTo("OK"));
            Assert.That(ReadLiveConfig(0).GetWorkerAddress(1), Is.EqualTo((expectedAddress, expectedPort)));
        }

        private void CreateSeparateEndpointCluster(int count, bool useHostname = false)
        {
            context.CreateInstances(count, enableAOF: true);
            for (var i = 0; i < count; i++)
            {
                context.nodeOptions[i].ClusterAnnounceEndpoint = new IPEndPoint(IPAddress.Parse($"203.0.113.{i + 1}"), 17001 + i);
                context.nodeOptions[i].ClusterAnnounceHostname = $"node{i}.example.com";
                context.nodeOptions[i].ClusterPreferredEndpointType = useHostname
                    ? Garnet.server.ClusterPreferredEndpointType.Hostname : Garnet.server.ClusterPreferredEndpointType.Ip;
                context.nodeOptions[i].ClusterAddress = "127.0.0.1";
                context.nodeOptions[i].ClusterPort = context.endpoints[i].ToIPEndPoint().Port;
                context.RestartNode(i);
            }
            context.CreateConnection();
            for (var i = 1; i < count; i++)
                Assert.That(ExecuteNode(0, "CLUSTER", "MEET", "127.0.0.1", context.endpoints[i].ToIPEndPoint().Port).ToString(), Is.EqualTo("OK"));
            for (var i = 0; i < count; i++)
                WaitForConfig(i, config => config.NumWorkers == count);
        }

        private RedisResult ExecuteNode(int index, string command, params object[] args)
            => context.clusterTestUtils.GetServer(context.endpoints[index].ToIPEndPoint()).Execute(command, args, CommandFlags.NoRedirect);

        private ClusterConfig ReadLiveConfig(int index)
        {
            Assert.That((int)ExecuteNode(index, "CLUSTER", "GOSSIP"), Is.EqualTo(2));
            return ClusterConfig.FromByteArray((byte[])ExecuteNode(index, "CLUSTER", "GOSSIP", "WITHMEET", Array.Empty<byte>()));
        }

        private void WaitForConfig(int index, Func<ClusterConfig, bool> predicate)
        {
            while (!predicate(ReadLiveConfig(index)))
            {
                context.cts.Token.ThrowIfCancellationRequested();
                Thread.Sleep(20);
            }
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
                    writer.Write(worker.ClusterAddress ?? "");
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
    }
}