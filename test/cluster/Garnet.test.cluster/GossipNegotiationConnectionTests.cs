// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.cluster;
using Garnet.common;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    internal class GossipNegotiationConnectionTests : TestBase
    {
        private ClusterTestContext context;
        private ClusterProvider provider;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
            context.CreateInstances(1);
            provider = (ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider;
        }

        [TearDown]
        public void TearDown() => context.TearDown();

        [TestCase(1)]
        [TestCase(2)]
        public async Task OptimisticMeetRetriesOnlyForOlderPeersAndCachesTheVersion(byte supportedVersion)
        {
            await using TestPeer peer = new(provider.clusterManager.CurrentConfig, supportedVersion);
            GarnetServerNode node = CreateNode(peer);
            try
            {
                Task first = node.InitializeAsync().AsTask();
                Task second = node.InitializeAsync().AsTask();
                Assert.That(first, Is.SameAs(second));
                await Task.WhenAll(first, second).WaitAsync(context.cts.Token);
                ClusterConfig response = await node.TryMeetAsync(provider.clusterManager.CurrentConfig);
                Assert.That(response.LocalNodeId, Is.EqualTo(provider.clusterManager.CurrentConfig.LocalNodeId));
                ClusterConfig nextResponse = await node.TryMeetAsync(provider.clusterManager.CurrentConfig);
                Assert.That(nextResponse.LocalNodeId, Is.EqualTo(response.LocalNodeId));
                byte[] expectedVersions = supportedVersion == 1 ? [2, 1, 1] : [2, 2];
                Assert.That(peer.RequestVersions, Is.EqualTo(expectedVersions));
                Assert.That(peer.RequestLengths, Has.All.GreaterThan(0));
                Assert.That(peer.AcceptedGossipCount, Is.EqualTo(2));
                Assert.That(peer.InfoCount, Is.Zero);
            }
            finally
            {
                node.Dispose();
            }
        }

        [TestCase("permission")]
        [TestCase("truncated")]
        [TestCase("unsupported")]
        [TestCase("empty")]
        [TestCase("identity")]
        public async Task InvalidOrDeniedGossipDoesNotDowngrade(string failure)
        {
            ClusterConfig responseConfig = failure == "identity"
                ? new ClusterConfig().InitializeLocalWorker(Generator.CreateHexId(), "127.0.0.1", ClusterTestContext.Port + 4,
                    0, Garnet.cluster.NodeRole.PRIMARY, null, "localhost")
                : provider.clusterManager.CurrentConfig;
            await using TestPeer peer = new(responseConfig, supportedVersion: 1)
            {
                DenyGossip = failure == "permission",
                ReplyBytes = failure == "truncated" ? [1] : failure == "unsupported" ? [3] : failure == "empty" ? [] : null
            };
            GarnetServerNode node = CreateNode(peer);
            try
            {
                Exception exception = null;
                try
                {
                    _ = await node.TryMeetAsync(provider.clusterManager.CurrentConfig);
                }
                catch (Exception error)
                {
                    exception = error;
                }
                Assert.That(exception, Is.Not.Null);
                if (failure == "permission")
                    Assert.That(exception.Message, Does.StartWith("NOPERM"));
                Assert.That(peer.RequestVersions, Is.EqualTo(new byte[] { 2 }));
                Assert.That(peer.InfoCount, Is.Zero);
            }
            finally
            {
                node.Dispose();
            }
        }

        [Test]
        public async Task InFlightGossipIsNotReportedAsAFailure()
        {
            await using TestPeer peer = new(provider.clusterManager.CurrentConfig, supportedVersion: 2, blockGossip: true);
            GarnetServerNode node = CreateNode(peer);
            try
            {
                await node.InitializeAsync();
                Assert.That(node.TryGossip(), Is.True);
                await peer.GossipRequested.Task.WaitAsync(context.cts.Token);
                Assert.That(node.TryGossip(), Is.True);
                Assert.That(peer.GossipCount, Is.EqualTo(1));
                Assert.That(peer.InfoCount, Is.Zero);
            }
            finally
            {
                node.Dispose();
            }
        }

        [Test]
        public async Task ConfirmedConnectionDoesNotDowngradeAgain()
        {
            await using TestPeer peer = new(provider.clusterManager.CurrentConfig, supportedVersion: 2);
            GarnetServerNode node = CreateNode(peer);
            try
            {
                _ = await node.TryMeetAsync(provider.clusterManager.CurrentConfig);
                peer.ReplyVersion = 1;
                InvalidDataException exception = null;
                try
                {
                    _ = await node.TryMeetAsync(provider.clusterManager.CurrentConfig);
                }
                catch (InvalidDataException error)
                {
                    exception = error;
                }
                Assert.That(exception, Is.Not.Null);
                Assert.That(peer.RequestVersions, Is.EqualTo(new byte[] { 2, 2 }));
                Assert.That(peer.InfoCount, Is.Zero);
            }
            finally
            {
                node.Dispose();
            }
        }

        [Test]
        public async Task GossipTimeoutDoesNotDowngrade()
        {
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig([context.endpoints[0].ToIPEndPoint()], allowAdmin: true));
            Assert.That(redis.GetServer(context.endpoints[0].ToIPEndPoint()).Execute("CONFIG", "SET", "cluster-node-timeout", "1").ToString(), Is.EqualTo("OK"));
            await using TestPeer peer = new(provider.clusterManager.CurrentConfig, supportedVersion: 2, blockGossip: true);
            GarnetServerNode node = CreateNode(peer);
            try
            {
                Task<ClusterConfig> exchange = node.TryMeetAsync(provider.clusterManager.CurrentConfig);
                await peer.GossipRequested.Task.WaitAsync(context.cts.Token);
                OperationCanceledException exception = null;
                try
                {
                    _ = await exchange.WaitAsync(TimeSpan.FromSeconds(10), context.cts.Token);
                }
                catch (OperationCanceledException error)
                {
                    exception = error;
                }
                Assert.That(exception, Is.Not.Null);
                Assert.That(peer.RequestVersions, Is.EqualTo(new byte[] { 2 }));
                Assert.That(peer.InfoCount, Is.Zero);
            }
            finally
            {
                node.Dispose();
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task InvalidGossipReplyFailsTheConnection(bool wrongIdentity)
        {
            ClusterConfig responseConfig = wrongIdentity
                ? new ClusterConfig().InitializeLocalWorker(Generator.CreateHexId(), "127.0.0.1", ClusterTestContext.Port + 4,
                    0, Garnet.cluster.NodeRole.PRIMARY, null, "localhost")
                : provider.clusterManager.CurrentConfig;
            await using TestPeer peer = new(responseConfig, supportedVersion: 2) { ReplyBytes = wrongIdentity ? null : [3] };
            GarnetServerNode node = CreateNode(peer);
            try
            {
                await node.InitializeAsync();
                Assert.That(node.TryGossip(), Is.True);
                await peer.GossipRequested.Task.WaitAsync(context.cts.Token);
                while (node.TryGossip())
                    await Task.Delay(10, context.cts.Token);
                Assert.That(peer.GossipCount, Is.EqualTo(1));
                Assert.That(peer.InfoCount, Is.Zero);
            }
            finally
            {
                node.Dispose();
            }
        }

        [Test]
        public async Task ReplacementConnectionAttemptsTheLatestVersionAgain()
        {
            foreach (byte supportedVersion in new byte[] { 1, 2 })
            {
                await using TestPeer peer = new(provider.clusterManager.CurrentConfig, supportedVersion);
                GarnetServerNode node = CreateNode(peer);
                try
                {
                    _ = await node.TryMeetAsync(provider.clusterManager.CurrentConfig);
                    Assert.That(peer.LastRequestVersion, Is.EqualTo(supportedVersion));
                    Assert.That(peer.RequestVersions[0], Is.EqualTo(2));
                    Assert.That(peer.InfoCount, Is.Zero);
                }
                finally
                {
                    node.Dispose();
                }
            }
        }

        [TestCase(1)]
        [TestCase(2)]
        public async Task ConcurrentMeetsShareTheNegotiatedVersion(byte supportedVersion)
        {
            await using TestPeer peer = new(provider.clusterManager.CurrentConfig, supportedVersion, blockGossip: true);
            GarnetServerNode node = CreateNode(peer);
            try
            {
                Task<ClusterConfig> first = node.TryMeetAsync(provider.clusterManager.CurrentConfig);
                Task<ClusterConfig> second = node.TryMeetAsync(provider.clusterManager.CurrentConfig);
                await peer.GossipRequested.Task.WaitAsync(context.cts.Token);
                Assert.That(peer.GossipCount, Is.EqualTo(1));
                peer.ReleaseGossip.TrySetResult();
                _ = await Task.WhenAll(first, second).WaitAsync(context.cts.Token);
                byte[] expectedVersions = supportedVersion == 1 ? [2, 1, 1] : [2, 2];
                Assert.That(peer.RequestVersions, Is.EqualTo(expectedVersions));
                Assert.That(peer.InfoCount, Is.Zero);
            }
            finally
            {
                node.Dispose();
            }
        }

        [Test]
        public async Task OlderPeerRetryResendsFullConfigurationBeforeHeartbeats()
        {
            await using TestPeer peer = new(provider.clusterManager.CurrentConfig, supportedVersion: 1) { EmptyRetryResponse = true };
            GarnetServerNode node = CreateNode(peer);
            try
            {
                await node.InitializeAsync();
                GossipVersionNegotiation negotiation = new(node.Client, provider.clusterManager);
                ClusterConfig response = await negotiation.ExchangeAsync(provider.clusterManager.CurrentConfig, withMeet: false, node.NodeId, context.cts.Token);
                Assert.That(response.LocalNodeId, Is.EqualTo(node.NodeId));
                Assert.That(negotiation.IsNegotiated, Is.True);
                _ = await negotiation.ExchangeAsync(provider.clusterManager.CurrentConfig, withMeet: false, node.NodeId, context.cts.Token);
                Assert.That(peer.RequestVersions, Is.EqualTo(new byte[] { 2, 1, 1 }));
                Assert.That(peer.RequestLengths[0], Is.GreaterThan(0));
                Assert.That(peer.RequestLengths[1], Is.GreaterThan(0));
                Assert.That(peer.RequestLengths[2], Is.Zero);
                Assert.That(peer.InfoCount, Is.Zero);
            }
            finally
            {
                node.Dispose();
            }
        }

        [Test]
        public async Task MeetCannotConfirmAnOlderVersionWithAnEmptyRetryReply()
        {
            await using TestPeer peer = new(provider.clusterManager.CurrentConfig, supportedVersion: 1) { EmptyRetryResponse = true };
            GarnetServerNode node = CreateNode(peer);
            try
            {
                Assert.ThrowsAsync<InvalidDataException>(async () => _ = await node.TryMeetAsync(provider.clusterManager.CurrentConfig));
                Assert.That(peer.RequestVersions, Is.EqualTo(new byte[] { 2, 1 }));
                Assert.That(peer.InfoCount, Is.Zero);
            }
            finally
            {
                node.Dispose();
            }
        }

        [Test]
        public async Task SplitEndpointsNeverRetryVersionOne()
        {
            await using TestPeer peer = new(provider.clusterManager.CurrentConfig, supportedVersion: 1);
            GarnetServerNode node = CreateNode(peer);
            ClusterConfig splitConfig = new ClusterConfig().InitializeLocalWorker(Generator.CreateHexId(), "127.0.0.1", ClusterTestContext.Port,
                0, Garnet.cluster.NodeRole.PRIMARY, null, "localhost", clusterAddress: "127.0.0.2", clusterPort: ClusterTestContext.Port + 2);
            try
            {
                Assert.ThrowsAsync<InvalidDataException>(async () => _ = await node.TryMeetAsync(splitConfig));
                Assert.That(peer.RequestVersions, Is.EqualTo(new byte[] { 2 }));
                Assert.That(peer.InfoCount, Is.Zero);
            }
            finally
            {
                node.Dispose();
            }
        }

        [Test]
        public async Task RetryMustConfirmTheSelectedVersion()
        {
            await using TestPeer peer = new(provider.clusterManager.CurrentConfig, supportedVersion: 1) { RetryReplyVersion = 2 };
            GarnetServerNode node = CreateNode(peer);
            try
            {
                await node.InitializeAsync();
                GossipVersionNegotiation negotiation = new(node.Client, provider.clusterManager);
                Assert.ThrowsAsync<InvalidDataException>(async () => _ = await negotiation.ExchangeAsync(
                    provider.clusterManager.CurrentConfig, withMeet: true, node.NodeId, context.cts.Token));
                Assert.That(negotiation.IsNegotiated, Is.False);
                Assert.That(peer.RequestVersions, Is.EqualTo(new byte[] { 2, 1 }));
                Assert.That(peer.InfoCount, Is.Zero);
            }
            finally
            {
                node.Dispose();
            }
        }

        [Test]
        public async Task CachedVersionOneRejectsNewSplitEndpointConfiguration()
        {
            await using TestPeer peer = new(provider.clusterManager.CurrentConfig, supportedVersion: 1);
            GarnetServerNode node = CreateNode(peer);
            try
            {
                await node.InitializeAsync();
                GossipVersionNegotiation negotiation = new(node.Client, provider.clusterManager);
                _ = await negotiation.ExchangeAsync(provider.clusterManager.CurrentConfig, withMeet: true, node.NodeId, context.cts.Token);
                ClusterConfig splitConfig = new ClusterConfig().InitializeLocalWorker(Generator.CreateHexId(), "127.0.0.1", ClusterTestContext.Port,
                    0, Garnet.cluster.NodeRole.PRIMARY, null, "localhost", clusterAddress: "127.0.0.2", clusterPort: ClusterTestContext.Port + 2);
                Assert.ThrowsAsync<InvalidDataException>(async () => _ = await negotiation.ExchangeAsync(splitConfig, withMeet: false, node.NodeId, context.cts.Token));
                Assert.That(peer.RequestVersions, Is.EqualTo(new byte[] { 2, 1 }));
                Assert.That(peer.InfoCount, Is.Zero);
            }
            finally
            {
                node.Dispose();
            }
        }

        private GarnetServerNode CreateNode(TestPeer peer)
            => new(provider, peer.Endpoint, null, provider.clusterManager.clusterConnectionStore.Epoch) { NodeId = provider.clusterManager.CurrentConfig.LocalNodeId };

        private sealed class TestPeer : IAsyncDisposable
        {
            private readonly TcpListener listener;
            private readonly CancellationTokenSource cancellation = new();
            private readonly ClusterConfig config;
            private readonly Task serverTask;
            public readonly TaskCompletionSource GossipRequested = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly TaskCompletionSource ReleaseGossip = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly byte supportedVersion;
            public bool DenyGossip;
            public byte? ReplyVersion;
            public byte? RetryReplyVersion;
            public byte[] ReplyBytes;
            public bool EmptyRetryResponse;
            public int InfoCount;
            public int GossipCount;
            public int AcceptedGossipCount;
            public byte LastRequestVersion;
            public readonly List<byte> RequestVersions = [];
            public readonly List<int> RequestLengths = [];
            public IPEndPoint Endpoint => (IPEndPoint)listener.LocalEndpoint;

            public TestPeer(ClusterConfig config, byte supportedVersion, bool blockGossip = false)
            {
                this.config = config;
                this.supportedVersion = supportedVersion;
                if (!blockGossip)
                    ReleaseGossip.SetResult();
                listener = new(IPAddress.Loopback, ClusterTestContext.Port + 4);
                listener.Start();
                serverTask = ServeAsync();
            }

            private async Task ServeAsync()
            {
                try
                {
                    using TcpClient client = await listener.AcceptTcpClientAsync(cancellation.Token);
                    using NetworkStream stream = client.GetStream();
                    while (!cancellation.IsCancellationRequested)
                    {
                        string header = await ReadLineAsync(stream, cancellation.Token);
                        if (header == null)
                            return;
                        int count = int.Parse(header.AsSpan(1), CultureInfo.InvariantCulture);
                        List<byte[]> arguments = new(count);
                        for (int index = 0; index < count; index++)
                        {
                            string bulkHeader = await ReadLineAsync(stream, cancellation.Token);
                            int length = int.Parse(bulkHeader.AsSpan(1), CultureInfo.InvariantCulture);
                            byte[] value = new byte[length];
                            await stream.ReadExactlyAsync(value, cancellation.Token);
                            Assert.That(await ReadLineAsync(stream, cancellation.Token), Is.Empty);
                            arguments.Add(value);
                        }
                        if (Encoding.ASCII.GetString(arguments[0]) == "CLIENT")
                            await stream.WriteAsync("+OK\r\n"u8.ToArray(), cancellation.Token);
                        else if (Encoding.ASCII.GetString(arguments[1]) == "INFO")
                        {
                            Interlocked.Increment(ref InfoCount);
                            await stream.WriteAsync("-NOPERM CLUSTER INFO must not be used\r\n"u8.ToArray(), cancellation.Token);
                        }
                        else
                        {
                            byte[] request = arguments[^1];
                            LastRequestVersion = request.Length == 0 ? LastRequestVersion : request[0];
                            RequestVersions.Add(LastRequestVersion);
                            RequestLengths.Add(request.Length);
                            Interlocked.Increment(ref GossipCount);
                            if (LastRequestVersion <= supportedVersion)
                                Interlocked.Increment(ref AcceptedGossipCount);
                            GossipRequested.TrySetResult();
                            await ReleaseGossip.Task.WaitAsync(cancellation.Token);
                            if (DenyGossip)
                                await stream.WriteAsync("-NOPERM CLUSTER GOSSIP denied\r\n"u8.ToArray(), cancellation.Token);
                            else
                            {
                                byte replyVersion = GossipCount == 2 && RetryReplyVersion.HasValue
                                    ? RetryReplyVersion.Value : ReplyVersion ?? Math.Min(LastRequestVersion, supportedVersion);
                                byte[] response = ReplyBytes ?? (EmptyRetryResponse && GossipCount == 2 ? [] : config.ToByteArray(replyVersion));
                                await WriteBulkAsync(stream, response, cancellation.Token);
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
                {
                }
                catch (IOException error) when (error.InnerException is SocketException { SocketErrorCode: SocketError.ConnectionReset })
                {
                    // GarnetClient closes its socket with a reset during disposal.
                }
            }

            private static async Task<string> ReadLineAsync(NetworkStream stream, CancellationToken token)
            {
                List<byte> bytes = [];
                byte[] buffer = new byte[1];
                while (await stream.ReadAsync(buffer, token) != 0)
                {
                    if (buffer[0] == '\n')
                        return Encoding.ASCII.GetString(bytes.ToArray()).TrimEnd('\r');
                    bytes.Add(buffer[0]);
                }
                return null;
            }

            private static async Task WriteBulkAsync(NetworkStream stream, byte[] value, CancellationToken token)
            {
                await stream.WriteAsync(Encoding.ASCII.GetBytes($"${value.Length}\r\n"), token);
                await stream.WriteAsync(value, token);
                await stream.WriteAsync("\r\n"u8.ToArray(), token);
            }

            public async ValueTask DisposeAsync()
            {
                await cancellation.CancelAsync();
                listener.Stop();
                await serverTask;
                cancellation.Dispose();
            }
        }
    }
}