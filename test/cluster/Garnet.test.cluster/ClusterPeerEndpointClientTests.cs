// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterPeerEndpointClientTests : TestBase
    {
        private ClusterTestContext context;
        private IPEndPoint[] peerEndpoints;
        private string[] nodeIds;
        private const string Key = "{peer-client}key";
        private const string OtherKey = "{peer-client-other}key";

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown() => context.TearDown();

        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        public void ClientDiscoversAndRoutesToAdvertisedEndpoints(bool separateEndpoints, bool useHostname)
        {
            CreateCluster(separateEndpoints, useHostname);
            int slot = GetSlot(Key);
            int otherSlot = GetSlot(OtherKey);
            Assert.That(otherSlot, Is.Not.EqualTo(slot));
            AssignSlot(0, slot);
            AssignSlot(1, otherSlot);

            using ConnectionMultiplexer client = CreateClient();
            IDatabase database = client.GetDatabase(0);
            Assert.That(database.StringSet(Key, "first"), Is.True);
            Assert.That(database.StringSet(OtherKey, "second"), Is.True);
            Assert.That(database.StringGet(Key).ToString(), Is.EqualTo("first"));
            Assert.That(database.StringGet(OtherKey).ToString(), Is.EqualTo("second"));
            Assert.That(ExecuteNode(0, "GET", Key).ToString(), Is.EqualTo("first"));
            Assert.That(ExecuteNode(1, "GET", OtherKey).ToString(), Is.EqualTo("second"));

            RedisServerException moved = Assert.Throws<RedisServerException>(() => ExecuteNode(0, "GET", OtherKey));
            Assert.That(moved.Message, Does.StartWith("Key has MOVED").And.Contain($"{GetAdvertisedAddress(useHostname)}:{GetClientEndpoint(1).Port}").And.Contain($"hashslot {otherSlot}"));
            for (int index = 0; index < 2; index++)
            {
                Assert.That(ExecuteNode(0, "CLUSTER", "ENDPOINT", nodeIds[index]).ToString(), Is.EqualTo(GetClientEndpoint(index).ToString()));
                string nodes = ExecuteNode(index, "CLUSTER", "NODES").ToString();
                Assert.That(nodes, Does.Contain(GetClientEndpoint(0).ToString()));
                Assert.That(nodes, Does.Contain(GetClientEndpoint(1).ToString()));
                if (separateEndpoints)
                    Assert.That(nodes, Does.Not.Contain("127.0.0.2"));
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void HostnameClientEndpointsAcceptIpv4AndIpv6(bool separateEndpoints)
        {
            CreateCluster(separateEndpoints, useHostname: true);
            for (int index = 0; index < 2; index++)
            {
                foreach (IPAddress address in new[] { IPAddress.Loopback, IPAddress.IPv6Loopback })
                {
                    IPEndPoint endpoint = new(address, GetClientEndpoint(index).Port);
                    using ConnectionMultiplexer client = ConnectionMultiplexer.Connect(TestUtils.GetConfig([endpoint], allowAdmin: true));
                    Assert.That(client.GetServer(endpoint).Execute("PING").ToString(), Is.EqualTo("PONG"));
                }
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ClientFollowsAskAndMovedDuringMigration(bool useHostname)
        {
            CreateCluster(separateEndpoints: true, useHostname);
            int slot = GetSlot(Key);
            AssignSlot(0, slot);
            AssignSlot(1, GetSlot(OtherKey));
            using ConnectionMultiplexer client = CreateClient();
            IDatabase database = client.GetDatabase(0);
            Assert.That(database.StringSet(OtherKey, "other"), Is.True);
            Assert.That(database.StringSet(Key, "before"), Is.True);

            Assert.That(ExecuteNode(1, "CLUSTER", "SETSLOT", slot, "IMPORTING", nodeIds[0]).ToString(), Is.EqualTo("OK"));
            Assert.That(ExecuteNode(0, "CLUSTER", "SETSLOT", slot, "MIGRATING", nodeIds[1]).ToString(), Is.EqualTo("OK"));
            Assert.That(ExecuteNode(0, "MIGRATE", GetAdvertisedAddress(useHostname), GetClientEndpoint(1).Port, "", 0, 10000, "KEYS", Key).ToString(), Is.EqualTo("OK"));

            RedisConnectionException ask = Assert.Throws<RedisConnectionException>(() => ExecuteNode(0, "GET", Key));
            Assert.That(ask.Message, Does.StartWith("Endpoint").And.Contain($"{GetAdvertisedAddress(useHostname)}:{GetClientEndpoint(1).Port}").And.Contain($"hashslot {slot} "));
            Assert.That(database.StringGet(Key).ToString(), Is.EqualTo("before"));
            Assert.That(database.StringSet(Key, "during"), Is.True);
            Assert.That(database.StringGet(Key).ToString(), Is.EqualTo("during"));

            Assert.That(ExecuteNode(1, "CLUSTER", "SETSLOT", slot, "NODE", nodeIds[1]).ToString(), Is.EqualTo("OK"));
            Assert.That(ExecuteNode(0, "CLUSTER", "SETSLOT", slot, "NODE", nodeIds[1]).ToString(), Is.EqualTo("OK"));
            WaitForSlotOwnership(slot, nodeIds[1]);
            RedisServerException moved = Assert.Throws<RedisServerException>(() => ExecuteNode(0, "GET", Key));
            Assert.That(moved.Message, Does.StartWith("Key has MOVED").And.Contain($"{GetAdvertisedAddress(useHostname)}:{GetClientEndpoint(1).Port}").And.Contain($"hashslot {slot}"));
            Assert.That(database.StringSet(Key, "after"), Is.True);
            Assert.That(database.StringGet(Key).ToString(), Is.EqualTo("after"));
            Assert.That(ExecuteNode(1, "GET", Key).ToString(), Is.EqualTo("after"));
            Assert.That(database.StringGet(OtherKey).ToString(), Is.EqualTo("other"));
        }

        [TestCase(false, "client")]
        [TestCase(true, "client")]
        [TestCase(true, "hostname")]
        [TestCase(true, "peer")]
        public void ReplicaSideFailoverPreservesClientAccess(bool separateEndpoints, string replicaOfTarget)
        {
            CreateCluster(separateEndpoints);
            int slot = GetSlot(Key);
            AssignSlot(0, slot);
            using ConnectionMultiplexer client = CreateClient();
            IDatabase database = client.GetDatabase(0);
            Assert.That(database.StringSet(Key, "before"), Is.True);
            ConfigureReplica(replicaOfTarget);
            WaitForReplicaData("before");

            Assert.That(ExecuteNode(1, "CLUSTER", "FAILOVER").ToString(), Is.EqualTo("OK"));
            context.clusterTestUtils.WaitForFailoverCompleted(1);
            WaitUntil(() => ((RedisResult[])ExecuteNode(1, "ROLE"))[0].ToString() == "master");
            WaitForSlotOwnership(slot, nodeIds[1]);
            Assert.That(database.StringGet(Key).ToString(), Is.EqualTo("before"));
            Assert.That(database.StringSet(Key, "after"), Is.True);
            Assert.That(database.StringGet(Key).ToString(), Is.EqualTo("after"));
            Assert.That(ExecuteNode(1, "GET", Key).ToString(), Is.EqualTo("after"));
            Assert.That(ExecuteNode(0, "CLUSTER", "ENDPOINT", nodeIds[1]).ToString(), Is.EqualTo(GetClientEndpoint(1).ToString()));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ReplicaReconnectsAfterPeerEndpointChanges(bool useHostname)
        {
            CreateCluster(separateEndpoints: true, useHostname);
            AssignSlot(0, GetSlot(Key));
            using ConnectionMultiplexer client = CreateClient();
            IDatabase database = client.GetDatabase(0);
            Assert.That(database.StringSet(Key, "before"), Is.True);
            ConfigureReplica("client");
            WaitForReplicaData("before");

            IPEndPoint newPeerEndpoint = new(IPAddress.Parse("127.0.0.2"), ClusterTestContext.Port + 4);
            context.nodeOptions[1].EndPoints = useHostname
                ? [GetClientEndpoint(1), new IPEndPoint(IPAddress.IPv6Loopback, GetClientEndpoint(1).Port), newPeerEndpoint]
                : [GetClientEndpoint(1), newPeerEndpoint];
            context.nodeOptions[1].ClusterPort = newPeerEndpoint.Port;
            context.nodeOptions[1].Recover = true;
            context.ShutdownNode(1, ensureAofFlush: true);
            context.clusterTestUtils.WaitForAofSyncDriverDipose(0);
            context.RestartNode(1);
            context.CreateConnection();
            WaitUntil(() => ((RedisResult[])ExecuteNode(1, "ROLE"))[0].ToString() == "slave");
            Assert.That(ExecuteNode(1, "CLUSTER", "MYID").ToString(), Is.EqualTo(nodeIds[1]));
            Assert.That(ExecuteNode(0, "CLUSTER", "ENDPOINT", nodeIds[1]).ToString(), Is.EqualTo(GetClientEndpoint(1).ToString()));
            Assert.That(database.StringSet(Key, "after"), Is.True);
            WaitForReplicaData("after");
            Assert.That(database.StringGet(Key).ToString(), Is.EqualTo("after"));
        }

        private void CreateCluster(bool separateEndpoints, bool useHostname = false)
        {
            context.CreateInstances(2, enableAOF: true, clusterReplicationReestablishmentTimeout: 1);
            peerEndpoints = new IPEndPoint[2];
            nodeIds = new string[2];
            for (int index = 0; index < 2; index++)
            {
                IPEndPoint clientEndpoint = GetClientEndpoint(index);
                IPEndPoint peerEndpoint = separateEndpoints
                    ? new IPEndPoint(IPAddress.Parse("127.0.0.2"), ClusterTestContext.Port + 2 + index)
                    : clientEndpoint;
                peerEndpoints[index] = peerEndpoint;
                GarnetServerOptions options = context.nodeOptions[index];
                options.EndPoints = separateEndpoints ? [clientEndpoint, peerEndpoint] : [clientEndpoint];
                if (useHostname)
                    options.EndPoints = [.. options.EndPoints, new IPEndPoint(IPAddress.IPv6Loopback, clientEndpoint.Port)];
                options.ClusterAnnounceEndpoint = clientEndpoint;
                options.ClusterAnnounceHostname = "localhost";
                options.ClusterPreferredEndpointType = useHostname ? ClusterPreferredEndpointType.Hostname : ClusterPreferredEndpointType.Ip;
                options.ClusterAddress = separateEndpoints ? peerEndpoint.Address.ToString() : null;
                options.ClusterPort = separateEndpoints ? peerEndpoint.Port : null;
                context.RestartNode(index);
            }

            context.CreateConnection();
            for (int index = 0; index < 2; index++)
            {
                nodeIds[index] = ExecuteNode(index, "CLUSTER", "MYID").ToString();
                Assert.That(ExecuteNode(index, "CLUSTER", "SET-CONFIG-EPOCH", index + 1).ToString(), Is.EqualTo("OK"));
            }
            Assert.That(ExecuteNode(0, "CLUSTER", "MEET", peerEndpoints[1].Address.ToString(), peerEndpoints[1].Port).ToString(), Is.EqualTo("OK"));
            for (int index = 0; index < 2; index++)
            {
                int nodeIndex = index;
                WaitUntil(() => context.clusterTestUtils.GetServer(nodeIndex).ClusterNodes().Nodes.Count() == 2);
            }
        }

        private void ConfigureReplica(string target)
        {
            string address = target == "peer" ? peerEndpoints[0].Address.ToString()
                : target == "hostname" ? "localhost" : GetClientEndpoint(0).Address.ToString();
            int port = target == "peer" ? peerEndpoints[0].Port : GetClientEndpoint(0).Port;
            Assert.That(ExecuteNode(1, "REPLICAOF", address, port).ToString(), Is.EqualTo("OK"));
            WaitForSlotOwnership(GetSlot(Key), nodeIds[0]);
            WaitUntil(() => ((RedisResult[])ExecuteNode(0, "CLUSTER", "REPLICAS", nodeIds[0]))
                .Any(replica => replica.ToString().StartsWith(nodeIds[1], StringComparison.Ordinal)));
        }

        private void WaitForReplicaData(string expected)
        {
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, cancellation: context.cts.Token);
            Assert.That(ExecuteNode(1, "READONLY").ToString(), Is.EqualTo("OK"));
            Assert.That(ExecuteNode(1, "GET", Key).ToString(), Is.EqualTo(expected));
        }

        private void AssignSlot(int index, int slot)
        {
            Assert.That(ExecuteNode(index, "CLUSTER", "ADDSLOTS", slot).ToString(), Is.EqualTo("OK"));
            WaitForSlotOwnership(slot, nodeIds[index]);
        }

        private void WaitForSlotOwnership(int slot, string ownerId)
        {
            for (int index = 0; index < 2; index++)
                context.clusterTestUtils.WaitForSlotOwnership(index, ownerId, [slot, slot]);
        }

        private void WaitUntil(Func<bool> predicate)
        {
            while (!predicate())
            {
                if (context.cts.IsCancellationRequested)
                {
                    for (int index = 0; index < 2; index++)
                        TestContext.Progress.WriteLine($"Node {index}: {ExecuteNode(index, "CLUSTER", "NODES")}");
                }
                context.cts.Token.ThrowIfCancellationRequested();
                Thread.Sleep(20);
            }
        }

        private ConnectionMultiplexer CreateClient()
            => ConnectionMultiplexer.Connect(TestUtils.GetConfig([GetClientEndpoint(0)], allowAdmin: true));

        private IPEndPoint GetClientEndpoint(int index) => context.endpoints[index].ToIPEndPoint();

        private RedisResult ExecuteNode(int index, string command, params object[] args)
            => context.clusterTestUtils.GetServer(index).Execute(0, command, args, CommandFlags.NoRedirect);

        private static int GetSlot(string key) => HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(key));

        private static string GetAdvertisedAddress(bool useHostname) => useHostname ? "localhost" : "127.0.0.1";
    }
}