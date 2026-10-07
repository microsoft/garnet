// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Tests for <see cref="GarnetLightClient"/>, the single-connection client intended for
    /// multi-threaded producers (cluster gossip and pub/sub forwarding). Mirrors the structure of
    /// <see cref="GarnetClientTests"/> while exercising the light client's RESP, GOSSIP and pub/sub surface.
    /// </summary>
    [TestFixture]
    public class GarnetLightClientTests : TestBase
    {
        static readonly byte[] SET = Encoding.ASCII.GetBytes("$3\r\nSET\r\n");
        static readonly byte[] GET = Encoding.ASCII.GetBytes("$3\r\nGET\r\n");
        static readonly byte[] ECHO = Encoding.ASCII.GetBytes("$4\r\nECHO\r\n");
        static readonly byte[] INCR = Encoding.ASCII.GetBytes("$4\r\nINCR\r\n");
        static readonly byte[] INCRBY = Encoding.ASCII.GetBytes("$6\r\nINCRBY\r\n");

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task SimplePingTest([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            using var db = TestUtils.GetGarnetLightClient(useTLS: useTLS);
            await db.ConnectAsync().ConfigureAwait(false);

            var result = await db.PingAsync().ConfigureAwait(false);
            ClassicAssert.AreEqual("PONG", result);
        }

        [Test]
        public async Task SimpleSetGetTest()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetLightClient();
            await db.ConnectAsync().ConfigureAwait(false);

            var origValue = "abcdefg";
            var setResult = await db.ExecuteForStringResultAsync(SET, ["mykey", origValue]).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", setResult);

            var getResult = await db.ExecuteForStringResultAsync(GET, ["mykey"]).ConfigureAwait(false);
            ClassicAssert.AreEqual(origValue, getResult);

            var missing = await db.ExecuteForStringResultAsync(GET, ["nokey"]).ConfigureAwait(false);
            ClassicAssert.IsNull(missing);
        }

        [Test]
        public async Task SimpleIncrTest()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetLightClient();
            await db.ConnectAsync().ConfigureAwait(false);

            var n = await db.ExecuteForLongResultAsync(INCR, ["counter"]).ConfigureAwait(false);
            ClassicAssert.AreEqual(1, n);

            n = await db.ExecuteForLongResultAsync(INCRBY, ["counter", "10"]).ConfigureAwait(false);
            ClassicAssert.AreEqual(11, n);
        }

        [Test]
        public async Task SimpleMemoryResultTest()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetLightClient();
            await db.ConnectAsync().ConfigureAwait(false);

            var payload = new byte[] { 0x00, 0x01, 0x02, 0xFE, 0xFF };
            _ = await db.ExecuteForStringResultAsync(SET, [(Memory<byte>)Encoding.ASCII.GetBytes("binkey"), (Memory<byte>)payload]).ConfigureAwait(false);

            using var result = await db.ExecuteForMemoryResultAsync(GET, [(Memory<byte>)Encoding.ASCII.GetBytes("binkey")]).ConfigureAwait(false);
            CollectionAssert.AreEqual(payload, result.Span.ToArray());
        }

        [Test]
        public async Task ConcurrentProducersIncrTest()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetLightClient();
            await db.ConnectAsync().ConfigureAwait(false);

            const int producers = 8;
            const int perProducer = 200;

            var tasks = new Task[producers];
            for (var p = 0; p < producers; p++)
            {
                tasks[p] = Task.Run(async () =>
                {
                    for (var i = 0; i < perProducer; i++)
                        _ = await db.ExecuteForLongResultAsync(INCR, ["shared"]).ConfigureAwait(false);
                });
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);

            // GET returns the value as a bulk string; ExecuteForLongResultAsync parses that as an integer.
            var final = await db.ExecuteForLongResultAsync(GET, ["shared"]).ConfigureAwait(false);
            ClassicAssert.AreEqual(producers * perProducer, final);
        }

        [Test]
        public async Task CompletionBackpressureStressTest([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            const int completionCapacity = 8;
            var options = new LightNetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingCompletions: completionCapacity,
                maxConcurrentNetworkSends: 8);
            using var db = TestUtils.GetGarnetLightClient(
                useTLS: useTLS,
                networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            const int producers = 8;
            const int perProducer = 200;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            var producerTasks = new Task[producers];

            for (var p = 0; p < producers; p++)
            {
                var producer = p;
                producerTasks[p] = Task.Run(async () =>
                {
                    var pending = new Task<string>[perProducer];
                    var expected = new string[perProducer];

                    for (var i = 0; i < perProducer; i++)
                    {
                        var value = $"{producer}:{i}:{new string((char)('a' + producer), i % 384)}";
                        expected[i] = value;
                        pending[i] = db.ExecuteForStringResultWithCancellationAsync(ECHO, [value], cts.Token);
                    }

                    var actual = await Task.WhenAll(pending).ConfigureAwait(false);
                    for (var i = 0; i < perProducer; i++)
                        ClassicAssert.AreEqual(expected[i], actual[i], $"Completion mismatch for producer {producer}, request {i}.");
                }, cts.Token);
            }

            await Task.WhenAll(producerTasks).ConfigureAwait(false);
        }

        [Test]
        public async Task PeerDisconnectFaultsPublishedCompletion()
        {
            using var listener = new TcpListener(IPAddress.Loopback, TestUtils.TestPort);
            listener.Start();

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var acceptTask = listener.AcceptSocketAsync(cts.Token).AsTask();
            using var db = new GarnetLightClient(new IPEndPoint(IPAddress.Loopback, TestUtils.TestPort));
            var connectTask = db.ConnectAsync(cts.Token);
            using var peer = await acceptTask.ConfigureAwait(false);
            await connectTask.ConfigureAwait(false);

            var pending = db.ExecuteForStringResultWithCancellationAsync(ECHO, ["pending"], cts.Token);
            var receiveBuffer = new byte[256];
            ClassicAssert.Greater(
                await peer.ReceiveAsync(receiveBuffer, SocketFlags.None, cts.Token).ConfigureAwait(false),
                0,
                "The request was not published before the peer disconnected.");

            peer.Dispose();

            Assert.ThrowsAsync<GarnetClientDisposedException>(async () =>
                await pending.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
        }

        [Test]
        public async Task ReconnectFaultsOperationWaitingOnMemoryAdmission()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            const int memoryCapacity = 512;
            var options = new LightNetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: memoryCapacity);
            using var db = TestUtils.GetGarnetLightClient(networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var writer = GetNetworkWriter(db);
            var allocationSize = writer.GetRequestBufferAllocationSize(300);
            ClassicAssert.AreEqual(memoryCapacity, allocationSize);
            ClassicAssert.IsTrue(writer.AdmitOutOfLineRental(allocationSize, CancellationToken.None));

            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                var pending = db.ExecuteForStringResultWithCancellationAsync(ECHO, [new string('x', 300)], cts.Token);
                var memoryThrottle = GetMemoryThrottle(writer);
                ClassicAssert.IsTrue(
                    SpinWait.SpinUntil(() => memoryThrottle.WaiterCount == 1, TimeSpan.FromSeconds(5)),
                    "The operation did not wait for out-of-line memory admission.");

                await AssertReconnectUnblocksPendingOperation(db, pending, cts.Token).ConfigureAwait(false);
            }
            finally
            {
                writer.ReleaseOutOfLineRental(allocationSize);
            }
        }

        [Test]
        public async Task ReconnectFaultsNoResponseOperationWaitingOnMemoryAdmission()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true);
            server.Start();

            const int memoryCapacity = 512;
            var options = new LightNetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: memoryCapacity);
            using var db = TestUtils.GetGarnetLightClient(networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var writer = GetNetworkWriter(db);
            var allocationSize = writer.GetRequestBufferAllocationSize(300);
            ClassicAssert.AreEqual(memoryCapacity, allocationSize);
            ClassicAssert.IsTrue(writer.AdmitOutOfLineRental(allocationSize, CancellationToken.None));

            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                var pending = Task.Run(
                    () => db.ClusterPublishNoResponse("channel"u8.ToArray(), Encoding.ASCII.GetBytes(new string('x', 300))),
                    cts.Token);
                var memoryThrottle = GetMemoryThrottle(writer);
                ClassicAssert.IsTrue(
                    SpinWait.SpinUntil(() => memoryThrottle.WaiterCount == 1, TimeSpan.FromSeconds(5)),
                    "The no-response operation did not wait for out-of-line memory admission.");

                await db.ReconnectAsync(cts.Token).WaitAsync(TimeSpan.FromSeconds(5), cts.Token).ConfigureAwait(false);
                Assert.ThrowsAsync<ObjectDisposedException>(async () =>
                    await pending.WaitAsync(TimeSpan.FromSeconds(5), cts.Token).ConfigureAwait(false));
                ClassicAssert.AreEqual(
                    "PONG",
                    await db.PingAsync(cts.Token).WaitAsync(TimeSpan.FromSeconds(5), cts.Token).ConfigureAwait(false));
            }
            finally
            {
                writer.ReleaseOutOfLineRental(allocationSize);
            }
        }

        [Test]
        public async Task ReconnectFaultsOperationWaitingOnCompletionCapacity()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            var options = new LightNetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingCompletions: 1,
                maxConcurrentNetworkSends: 8);
            using var db = TestUtils.GetGarnetLightClient(networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var writer = GetNetworkWriter(db);
            var recordSize = writer.GetRecordSize(1, out _);
            writer.epoch.Resume();
            try
            {
                ClassicAssert.IsTrue(writer.TryScheduleSend(
                    recordSize,
                    expectsResponse: true,
                    out _,
                    out _));
            }
            finally
            {
                writer.epoch.Suspend();
            }

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var pending = db.PingAsync(cts.Token);
            await Task.Delay(50, cts.Token).ConfigureAwait(false);
            ClassicAssert.IsFalse(pending.IsCompleted, "The operation did not wait for completion-lane capacity.");

            await AssertReconnectUnblocksPendingOperation(db, pending, cts.Token).ConfigureAwait(false);
        }

        [Test]
        public void BufferPoolIdleByteCapDropsExcessEntries()
        {
            using var pool = new LimitedFixedBufferPool(
                minAllocationSize: 64,
                maxEntriesPerLevel: 16,
                numLevels: 4,
                maxPooledBytes: 192);

            var small = pool.Get(64);
            var medium = pool.Get(128);
            var large = pool.Get(256);
            small.Dispose();
            medium.Dispose();
            large.Dispose();

            ClassicAssert.AreEqual(192, pool.PooledBytes);

            var reused = pool.Get(128);
            ClassicAssert.AreEqual(64, pool.PooledBytes);
            reused.Dispose();
            ClassicAssert.AreEqual(192, pool.PooledBytes);

            pool.Purge();
            ClassicAssert.AreEqual(0, pool.PooledBytes);

            Parallel.For(0, 10_000, i =>
            {
                var size = 64 << (i % 3);
                pool.Get(size).Dispose();
            });
            ClassicAssert.LessOrEqual(pool.PooledBytes, 192);
            pool.Purge();
            ClassicAssert.AreEqual(0, pool.PooledBytes);
        }

        [Test]
        public void DefaultNetworkWriterMemoryFootprint()
        {
            var options = LightNetworkWriterOptions.Default;

            ClassicAssert.AreEqual(64L << 20, options.MaxOutOfLineRentedBytes);
            ClassicAssert.AreEqual(14 * 1024, options.MinMemoryFootprint());
            ClassicAssert.AreEqual(30 * 1024, options.MaxMemoryFootprint());
        }

        [Test]
        public async Task ClientMemoryUsageTracksOutOfLineReservations()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            const int maxOutOfLineRentedBytes = 512;
            var options = new LightNetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: maxOutOfLineRentedBytes);
            using var db = TestUtils.GetGarnetLightClient(networkWriterOptions: options);

            ClassicAssert.AreEqual(0, db.ActiveMemoryUsageBytes);
            ClassicAssert.AreEqual(
                options.MaxMemoryFootprint() + maxOutOfLineRentedBytes,
                db.MaxMemoryUsageBytes);

            await db.ConnectAsync().ConfigureAwait(false);
            ClassicAssert.AreEqual(options.MinMemoryFootprint(), db.ActiveMemoryUsageBytes);

            var writer = GetNetworkWriter(db);
            ClassicAssert.IsTrue(writer.AdmitOutOfLineRental(maxOutOfLineRentedBytes, CancellationToken.None));
            try
            {
                ClassicAssert.AreEqual(
                    options.MinMemoryFootprint() + maxOutOfLineRentedBytes,
                    db.ActiveMemoryUsageBytes);
            }
            finally
            {
                writer.ReleaseOutOfLineRental(maxOutOfLineRentedBytes);
            }

            ClassicAssert.AreEqual(options.MinMemoryFootprint(), db.ActiveMemoryUsageBytes);
            ClassicAssert.AreEqual("PONG", await db.PingAsync().ConfigureAwait(false));
            ClassicAssert.Greater(db.ActiveMemoryUsageBytes, options.MinMemoryFootprint());
            ClassicAssert.LessOrEqual(db.ActiveMemoryUsageBytes, options.MaxMemoryFootprint());
        }

        [Test]
        public void RequestPageSizeMustFitRecordHeader()
        {
            var exception = Assert.Throws<ArgumentOutOfRangeException>(() => new LightNetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: DuplexRingRecordFormat.HeaderSize - 1,
                requestPageCount: 2,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8));

            ClassicAssert.AreEqual("requestPageSizeBytes", exception.ParamName);

            var options = new LightNetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: DuplexRingRecordFormat.HeaderSize,
                requestPageCount: 2,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8);
            ClassicAssert.AreEqual(DuplexRingRecordFormat.HeaderSize, options.RequestPageSizeBytes);
        }

        [Test]
        public async Task RequestMemoryThrottleRejectsOversizedOutOfLineRequestWithoutLeak()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            var options = new LightNetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 512);
            using var db = TestUtils.GetGarnetLightClient(networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var exception = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await db.ExecuteForStringResultAsync(ECHO, [new string('x', 700)]).ConfigureAwait(false));

            StringAssert.Contains("1024 bytes exceeds the configured maximum of 512 bytes", exception.Message);
            ClassicAssert.AreEqual("PONG", await db.PingAsync().ConfigureAwait(false));
        }

        [Test]
        public async Task RequestMemoryThrottleStabilizesConcurrentOutOfLineRequests()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            const int capacityBytes = 2048;
            var options = new LightNetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingCompletions: 16,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: capacityBytes);
            using var db = TestUtils.GetGarnetLightClient(networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            const int producers = 8;
            const int requestsPerProducer = 50;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            var tasks = new Task[producers];
            for (var producer = 0; producer < producers; producer++)
            {
                var id = producer;
                tasks[producer] = Task.Run(async () =>
                {
                    for (var request = 0; request < requestsPerProducer; request++)
                    {
                        var value = $"{id}:{request}:{new string((char)('a' + id), 700)}";
                        var result = await db.ExecuteForStringResultWithCancellationAsync(ECHO, [value], cts.Token).ConfigureAwait(false);
                        ClassicAssert.AreEqual(value, result);
                    }
                }, cts.Token);
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);

            var finalValue = new string('z', 700);
            ClassicAssert.AreEqual(
                finalValue,
                await db.ExecuteForStringResultWithCancellationAsync(ECHO, [finalValue], cts.Token).ConfigureAwait(false));
        }

        [Test]
        public async Task ChunkedLargePayloadTest()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            // Force small pages so the payload spans several chunks/pages.
            var options = new LightNetworkWriterOptions(
                networkBufferSizeBytes: 1 << 17,
                requestPageSizeBytes: 1 << 12,
                requestPageCount: 2,
                maxOutstandingCompletions: 1 << 19,
                maxConcurrentNetworkSends: 8);
            using var db = new GarnetLightClient(TestUtils.EndPoint, networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var value = new string('x', 1 << 16);
            var setResult = await db.ExecuteForStringResultAsync(SET, ["bigkey", value]).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", setResult);

            var getResult = await db.ExecuteForStringResultAsync(GET, ["bigkey"]).ConfigureAwait(false);
            ClassicAssert.AreEqual(value, getResult);
        }

        [Test]
        public async Task PublishNoSubscribersReturnsZero()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetLightClient();
            await db.ConnectAsync().ConfigureAwait(false);

            var received = await db.PublishAsync(Encoding.ASCII.GetBytes("channel"), Encoding.ASCII.GetBytes("hello")).ConfigureAwait(false);
            ClassicAssert.AreEqual(0, received);
        }

        [Test]
        public async Task PublishDeliversToSubscriber()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var subscriber = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var messages = new BlockingCollection<string>();
            await subscriber.GetSubscriber().SubscribeAsync(RedisChannel.Literal("news"),
                (_, message) => messages.Add(message)).ConfigureAwait(false);

            using var db = TestUtils.GetGarnetLightClient();
            await db.ConnectAsync().ConfigureAwait(false);

            long received = 0;
            // The subscription registration on the server may momentarily lag the SUBSCRIBE round-trip.
            for (var attempt = 0; attempt < 50 && received == 0; attempt++)
            {
                received = await db.PublishAsync(Encoding.ASCII.GetBytes("news"), Encoding.ASCII.GetBytes("breaking")).ConfigureAwait(false);
                if (received == 0) await Task.Delay(20).ConfigureAwait(false);
            }

            ClassicAssert.AreEqual(1, received);
            ClassicAssert.IsTrue(messages.TryTake(out var delivered, TimeSpan.FromSeconds(5)));
            ClassicAssert.AreEqual("breaking", delivered);
        }

        [TestCase(16, TestName = "NoResponseInlinePreservesCompletionAlignment")]
        [TestCase(512, TestName = "NoResponseOutOfLinePreservesCompletionAlignment")]
        public async Task NoResponsePreservesCompletionAlignment(int messageLength)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true);
            server.Start();

            using var subscriber = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var messages = new BlockingCollection<string>();
            await subscriber.GetSubscriber().SubscribeAsync(RedisChannel.Literal("no-response-news"),
                (_, message) => messages.Add(message)).ConfigureAwait(false);

            var options = new LightNetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingCompletions: 1 << 12,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 1024);
            using var db = TestUtils.GetGarnetLightClient(networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var channel = Encoding.ASCII.GetBytes("no-response-news");
            var sent = new HashSet<string>();
            string delivered = null;
            for (var attempt = 0; attempt < 10 && delivered == null; attempt++)
            {
                var message = $"{attempt:D2}-{new string('x', messageLength)}";
                sent.Add(message);
                db.ClusterPublishNoResponse(channel, Encoding.ASCII.GetBytes(message));

                // PING is ordered after the no-response command, so PONG confirms both that the publish was
                // processed and that it did not consume or misalign a completion ticket.
                ClassicAssert.AreEqual("PONG", await db.PingAsync().ConfigureAwait(false));
                if (messages.TryTake(out var candidate, TimeSpan.FromMilliseconds(100)) && sent.Contains(candidate))
                    delivered = candidate;
            }

            ClassicAssert.IsNotNull(delivered, "No fire-and-forget publish reached the subscriber.");
        }

        [Test]
        public async Task DisabledClusterPublishProducesNoResponse()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true, disablePubSub: true);
            server.Start();

            using var db = TestUtils.GetGarnetLightClient();
            await db.ConnectAsync().ConfigureAwait(false);

            db.ClusterPublishNoResponse("channel"u8.ToArray(), "message"u8.ToArray());
            ClassicAssert.AreEqual("PONG", await db.PingAsync().ConfigureAwait(false));
        }

        [Test]
        public async Task GossipWithMeetReturnsConfig()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true);
            server.Start();

            using var db = TestUtils.GetGarnetLightClient();
            await db.ConnectAsync().ConfigureAwait(false);

            // WITHMEET always elicits a response carrying the node's serialized configuration, exercising the
            // binary-safe (MemoryResult) reply path even with an empty outbound payload.
            using var response = await db.GossipWithMeetAsync(Memory<byte>.Empty).ConfigureAwait(false);
            ClassicAssert.Greater(response.Length, 0);
        }

        static LightNetworkWriter GetNetworkWriter(GarnetLightClient client)
            => (LightNetworkWriter)typeof(GarnetLightClient)
                .GetField("networkWriter", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(client);

        static WaiterQueue<MemoryThrottle, int> GetMemoryThrottle(LightNetworkWriter writer)
            => (WaiterQueue<MemoryThrottle, int>)typeof(LightNetworkWriter)
                .GetField("memoryThrottle", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(writer);

        static async Task AssertReconnectUnblocksPendingOperation(
            GarnetLightClient client,
            Task<string> pending,
            CancellationToken token)
        {
            await client.ReconnectAsync(token).WaitAsync(TimeSpan.FromSeconds(5), token).ConfigureAwait(false);

            try
            {
                _ = await pending.WaitAsync(TimeSpan.FromSeconds(5), token).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
            }

            ClassicAssert.AreEqual(
                "PONG",
                await client.PingAsync(token).WaitAsync(TimeSpan.FromSeconds(5), token).ConfigureAwait(false));
        }
    }
}