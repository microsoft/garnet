// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

            ClassicAssert.AreEqual(12 * 1024, options.MinMemoryFootprint());
            ClassicAssert.AreEqual(26 * 1024, options.MaxMemoryFootprint());
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
                maxConcurrentNetworkSends: 8);
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
    }
}