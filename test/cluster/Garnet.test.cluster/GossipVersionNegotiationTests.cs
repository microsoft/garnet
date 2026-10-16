// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Linq;
using System.Net;
using Garnet.cluster;
using Garnet.common;
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
            using var client = TestUtils.GetGarnetLightClient(context.endpoints[0]);
            client.Connect();
            var provider = (ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider;
            GossipVersionNegotiation negotiation = new(client, provider.clusterManager);
            var config = CreatePeerConfig();
            var response = negotiation.ExchangeAsync(config, withMeet: true, expectedNodeId: null, context.cts.Token).GetAwaiter().GetResult();
            Assert.That(response.LocalNodeId, Is.EqualTo(provider.clusterManager.CurrentConfig.LocalNodeId));
            Assert.That(context.clusterTestUtils.GetServer(0).ClusterNodes().Nodes.Count(), Is.EqualTo(2));
        }

        [Test]
        public void SharedEndpointsAcceptLegacyGossip()
        {
            CreateServer(separateEndpoints: false);
            var config = CreatePeerConfig();
            var response = (byte[])context.clusterTestUtils.GetServer(0).Execute("CLUSTER", "GOSSIP", "WITHMEET", config.ToByteArray(1));
            Assert.That(response[0], Is.EqualTo(1));
            Assert.That(context.clusterTestUtils.GetServer(0).ClusterNodes().Nodes.Count(), Is.EqualTo(2));
        }

        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        public void UnsupportedGossipAlwaysReturnsHighestSupportedVersion(bool separateEndpoints, bool withMeet)
        {
            CreateServer(separateEndpoints);
            using var client = TestUtils.GetLightGarnetClient(context.endpoints[0]);
            client.Connect();
            var provider = (ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider;
            var original = provider.clusterManager.CurrentConfig;
            if (!separateEndpoints)
            {
                using var initial = client.GossipAsync(System.Array.Empty<byte>()).GetAwaiter().GetResult();
                Assert.That(initial.Span[0], Is.EqualTo(ClusterConfig.MinimumSupportedClusterConfigVersion));
            }

            // The unreadable version header must be handled without interpreting its payload.
            byte[] unsupported = [(byte)(ClusterConfig.MaximumSupportedClusterConfigVersion + 1)];
            for (var attempt = 0; attempt < 2; attempt++)
            {
                using var response = (withMeet ? client.GossipWithMeetAsync(unsupported) : client.GossipAsync(unsupported))
                    .GetAwaiter().GetResult();
                Assert.That(response.Span[0], Is.EqualTo(ClusterConfig.MaximumSupportedClusterConfigVersion));
                Assert.That(response.Span.ToArray(), Is.EqualTo(original.ToByteArray()));
                Assert.That(provider.clusterManager.CurrentConfig, Is.SameAs(original));
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void FutureVersionRequestCanRetryWithVersionTwo(bool separateEndpoints)
        {
            CreateServer(separateEndpoints);
            using var client = TestUtils.GetLightGarnetClient(context.endpoints[0]);
            client.Connect();
            var provider = (ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider;
            var config = CreatePeerConfig(separateEndpoints);
            var unsupported = config.ToByteArray();
            unsupported[0] = (byte)(ClusterConfig.MaximumSupportedClusterConfigVersion + 1);
            using var offered = client.GossipWithMeetAsync(unsupported).GetAwaiter().GetResult();
            Assert.That(offered.Span[0], Is.EqualTo(2));
            Assert.That(provider.clusterManager.CurrentConfig.IsKnown(config.LocalNodeId), Is.False);

            GossipVersionNegotiation negotiation = new(client, provider.clusterManager);
            var response = negotiation.ExchangeAsync(config, withMeet: true, expectedNodeId: null, context.cts.Token).GetAwaiter().GetResult();
            Assert.That(response.LocalNodeId, Is.EqualTo(provider.clusterManager.CurrentConfig.LocalNodeId));
            Assert.That(negotiation.IsNegotiated, Is.True);
            var accepted = provider.clusterManager.CurrentConfig.GetWorkerFromNodeId(config.LocalNodeId);
            var expected = config.GetWorkerFromNodeId(config.LocalNodeId);
            Assert.That(accepted.PeerAddress, Is.EqualTo(expected.PeerAddress));
            Assert.That(accepted.PeerPort, Is.EqualTo(expected.PeerPort));
            _ = negotiation.ExchangeAsync(config, withMeet: false, response.LocalNodeId, context.cts.Token).GetAwaiter().GetResult();
            Assert.That(negotiation.ExchangeAsync(config, withMeet: false, response.LocalNodeId, context.cts.Token).GetAwaiter().GetResult(), Is.Null);
        }

        [Test]
        public void UnsupportedGossipDoesNotChangeTheSelectedReplyVersion()
        {
            CreateServer(separateEndpoints: false);
            using var client = TestUtils.GetLightGarnetClient(context.endpoints[0]);
            client.Connect();
            using var initialOffer = client.GossipAsync(new byte[] { 3 }).GetAwaiter().GetResult();
            Assert.That(initialOffer.Span[0], Is.EqualTo(2));
            using var initialHeartbeat = client.GossipAsync(System.Array.Empty<byte>()).GetAwaiter().GetResult();
            Assert.That(initialHeartbeat.Span[0], Is.EqualTo(1));
            var config = CreatePeerConfig();
            using var accepted = client.GossipWithMeetAsync(config.ToByteArray(1)).GetAwaiter().GetResult();
            using var offer = client.GossipAsync(new byte[] { 3 }).GetAwaiter().GetResult();
            Assert.That(offer.Span[0], Is.EqualTo(2));
            using var updated = client.GossipAsync(System.Array.Empty<byte>()).GetAwaiter().GetResult();
            Assert.That(updated.Span[0], Is.EqualTo(1));
            using var unchanged = client.GossipAsync(System.Array.Empty<byte>()).GetAwaiter().GetResult();
            Assert.That(unchanged.Length, Is.Zero);
        }

        [Test]
        public void SplitEndpointsRejectLegacyMeetBeforeMerging()
        {
            CreateServer(separateEndpoints: true);
            var config = CreatePeerConfig();
            var exception = Assert.Throws<RedisServerException>(() =>
                context.clusterTestUtils.GetServer(0).Execute("CLUSTER", "GOSSIP", "WITHMEET", config.ToByteArray(1)));
            Assert.That(exception.Message, Is.EqualTo("ERR Separate client and peer endpoints require gossip version two"));
            Assert.That(context.clusterTestUtils.GetServer(0).ClusterNodes().Nodes.Count(), Is.EqualTo(1));
        }

        [Test]
        public void SplitEndpointsRejectVersionOneHeartbeat()
        {
            CreateServer(separateEndpoints: true);
            var exception = Assert.Throws<RedisServerException>(() =>
                context.clusterTestUtils.GetServer(0).Execute("CLUSTER", "GOSSIP", "WITHMEET", System.Array.Empty<byte>()));
            Assert.That(exception.Message, Is.EqualTo("ERR Separate client and peer endpoints require gossip version two"));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void VersionOneSerializationRequiresSharedEndpoints(bool separateEndpoints)
        {
            var config = CreatePeerConfig(separateEndpoints);
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
            var config = CreatePeerConfig().Merge(CreatePeerConfig(separateEndpoints: true), []);
            Assert.That(config.CanUseGossipVersion(1), Is.False);
            Assert.Throws<InvalidDataException>(() => config.ToGossipByteArray(1));
        }

        private void CreateServer(bool separateEndpoints)
        {
            context.CreateInstances(1);
            if (separateEndpoints)
            {
                var clientEndpoint = context.endpoints[0].ToIPEndPoint();
                IPEndPoint peerEndpoint = new(IPAddress.Parse("127.0.0.2"), ClusterTestContext.Port + 1);
                var options = context.nodeOptions[0];
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