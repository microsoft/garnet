// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Linq;
using System.Net;
using Garnet.client;
using Garnet.cluster;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    internal class GossipVersionNegotiationTests : TestBase
    {
        private ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown() => context.TearDown();

        [TestCase(false)]
        [TestCase(true)]
        public void LatestVersionMeetSucceedsWithoutCapabilityDiscovery(bool separateEndpoints)
        {
            CreateServer(separateEndpoints);
            using GarnetClient client = TestUtils.GetGarnetClient(context.endpoints[0]);
            client.Connect();
            ClusterProvider provider = (ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider;
            GossipVersionNegotiation negotiation = new(client, provider.clusterManager);
            ClusterConfig config = CreatePeerConfig();
            ClusterConfig response = negotiation.ExchangeAsync(config, withMeet: true, expectedNodeId: null, context.cts.Token).GetAwaiter().GetResult();
            Assert.That(response.LocalNodeId, Is.EqualTo(provider.clusterManager.CurrentConfig.LocalNodeId));
            Assert.That(context.clusterTestUtils.GetServer(0).ClusterNodes().Nodes.Count(), Is.EqualTo(2));
        }

        [Test]
        public void SharedEndpointsAcceptLegacyGossip()
        {
            CreateServer(separateEndpoints: false);
            ClusterConfig config = CreatePeerConfig();
            byte[] response = (byte[])context.clusterTestUtils.GetServer(0).Execute("CLUSTER", "GOSSIP", "WITHMEET", config.ToByteArray(1));
            Assert.That(response[0], Is.EqualTo(1));
            Assert.That(context.clusterTestUtils.GetServer(0).ClusterNodes().Nodes.Count(), Is.EqualTo(2));
        }

        [Test]
        public void SplitEndpointsRejectLegacyMeetBeforeMerging()
        {
            CreateServer(separateEndpoints: true);
            ClusterConfig config = CreatePeerConfig();
            RedisServerException exception = Assert.Throws<RedisServerException>(() =>
                context.clusterTestUtils.GetServer(0).Execute("CLUSTER", "GOSSIP", "WITHMEET", config.ToByteArray(1)));
            Assert.That(exception.Message, Is.EqualTo("ERR Separate client and peer endpoints require gossip version two"));
            Assert.That(context.clusterTestUtils.GetServer(0).ClusterNodes().Nodes.Count(), Is.EqualTo(1));
        }

        [Test]
        public void SplitEndpointsRejectVersionOneHeartbeat()
        {
            CreateServer(separateEndpoints: true);
            RedisServerException exception = Assert.Throws<RedisServerException>(() =>
                context.clusterTestUtils.GetServer(0).Execute("CLUSTER", "GOSSIP", "WITHMEET", System.Array.Empty<byte>()));
            Assert.That(exception.Message, Is.EqualTo("ERR Separate client and peer endpoints require gossip version two"));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void VersionOneSerializationRequiresSharedEndpoints(bool separateEndpoints)
        {
            ClusterConfig config = CreatePeerConfig(separateEndpoints);
            Assert.That(config.CanUseGossipVersion(1), Is.EqualTo(!separateEndpoints));
            Assert.That(config.CanUseGossipVersion(2), Is.True);
            Assert.That(config.ToGossipByteArray(2), Is.EqualTo(config.ToByteArray(2)));
            if (separateEndpoints)
                Assert.Throws<InvalidDataException>(() => config.ToGossipByteArray(1));
            else
                Assert.That(config.ToGossipByteArray(1), Is.EqualTo(config.ToByteArray(1)));
        }

        [Test]
        public void SplitEndpointMetadataInAnotherWorkerPreventsVersionOneSerialization()
        {
            ClusterConfig config = CreatePeerConfig().Merge(CreatePeerConfig(separateEndpoints: true), []);
            Assert.That(config.CanUseGossipVersion(1), Is.False);
            Assert.Throws<InvalidDataException>(() => config.ToGossipByteArray(1));
        }

        private void CreateServer(bool separateEndpoints)
        {
            context.CreateInstances(1);
            if (separateEndpoints)
            {
                IPEndPoint clientEndpoint = context.endpoints[0].ToIPEndPoint();
                IPEndPoint peerEndpoint = new(IPAddress.Parse("127.0.0.2"), ClusterTestContext.Port + 1);
                GarnetServerOptions options = context.nodeOptions[0];
                options.EndPoints = [clientEndpoint, peerEndpoint];
                options.ClusterAddress = peerEndpoint.Address.ToString();
                options.ClusterPort = peerEndpoint.Port;
                context.RestartNode(0);
            }
            context.CreateConnection();
        }

        private static ClusterConfig CreatePeerConfig(bool separateEndpoints = false)
            => new ClusterConfig().InitializeLocalWorker(Generator.CreateHexId(), "127.0.0.1", ClusterTestContext.Port + 2,
                configEpoch: 1, Garnet.cluster.NodeRole.PRIMARY, null, "localhost",
                clusterAddress: separateEndpoints ? "127.0.0.2" : null,
                clusterPort: separateEndpoints ? ClusterTestContext.Port + 3 : 0);
    }
}