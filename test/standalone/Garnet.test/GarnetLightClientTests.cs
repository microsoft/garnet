// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Tests for <see cref="GarnetLightClient"/>, the single-connection out-of-line client intended for
    /// multi-threaded producers (cluster gossip and pub/sub forwarding). Mirrors the structure of
    /// <see cref="GarnetClientTests"/> while exercising the light client's RESP, GOSSIP and pub/sub surface.
    /// </summary>
    [TestFixture]
    public class GarnetLightClientTests : TestBase
    {
        static readonly byte[] SET = Encoding.ASCII.GetBytes("$3\r\nSET\r\n");
        static readonly byte[] GET = Encoding.ASCII.GetBytes("$3\r\nGET\r\n");
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
        public async Task ChunkedLargePayloadTest()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            // Force small pages so the payload spans several chunks/pages.
            using var db = new GarnetLightClient(TestUtils.EndPoint, sendPageSize: 1 << 12);
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