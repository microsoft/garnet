// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    class RespPubSubTests : AllureTestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, pubSubPageSize: "256k");
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void BasicSUBSCRIBE()
        {
            using var subRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var sub = subRedis.GetSubscriber();
            var db = redis.GetDatabase(0);
            string value = "published message";

            ManualResetEvent evt = new(false);

            SubscribeAndPublish(sub, db, RedisChannel.Literal("messages"), RedisChannel.Literal("messages"), value, onSubscribe: (channel, message) =>
            {
                ClassicAssert.AreEqual("messages", (string)channel);
                ClassicAssert.AreEqual(value, (string)message);
                evt.Set();
            });

            sub.Unsubscribe(RedisChannel.Literal("messages"));
        }

        [Test]
        public void LargeSUBSCRIBE()
        {
            using var subRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var sub = subRedis.GetSubscriber();
            var db = redis.GetDatabase(0);
            RedisValue value = RandomNumberGenerator.GetBytes(140 * 1024);

            ManualResetEvent evt = new(false);

            SubscribeAndPublish(sub, db, RedisChannel.Literal("messages"), RedisChannel.Literal("messages"), value, onSubscribe: (channel, message) =>
            {
                ClassicAssert.AreEqual("messages", (string)channel);
                ClassicAssert.AreEqual(value, (string)message);
                evt.Set();
            });

            sub.Unsubscribe(RedisChannel.Literal("messages"));
        }

        [Test]
        public void BasicPSUBSCRIBE()
        {
            using var subRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var sub = subRedis.GetSubscriber();
            var db = redis.GetDatabase(0);

            string glob = "messagesA*";
            string actual = "messagesAtest";
            string value = "published message";

            var channel = new RedisChannel(glob, RedisChannel.PatternMode.Pattern);

            ManualResetEvent evt = new(false);

            SubscribeAndPublish(sub, db, channel, RedisChannel.Pattern(actual), value, (receivedChannel, message) =>
            {
                ClassicAssert.AreEqual(glob, (string)channel);
                ClassicAssert.AreEqual(actual, (string)receivedChannel);
                ClassicAssert.AreEqual(value, (string)message);
                evt.Set();
            });

            sub.Unsubscribe(channel);
        }

        [Test]
        public void BasicPUBSUB_CHANNELS()
        {
            using var subRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var sub = subRedis.GetSubscriber();
            var db = redis.GetDatabase(0);
            var server = redis.GetServers()[0];

            var channelA = "messagesAtest";
            var channelB = "messagesB";

            SubscribeAndPublish(sub, db, RedisChannel.Literal(channelA), RedisChannel.Pattern(channelA));
            SubscribeAndPublish(sub, db, RedisChannel.Literal(channelB), RedisChannel.Pattern(channelB));

            var result = server.SubscriptionChannels();
            string[] expectedResult = [channelA, channelB];
            CollectionAssert.IsSubsetOf(expectedResult, result.Select(x => x.ToString()));

            result = server.SubscriptionChannels(RedisChannel.Pattern("messages*"));
            expectedResult = [channelA, channelB];
            CollectionAssert.AreEquivalent(expectedResult, result.Select(x => x.ToString()));

            result = server.SubscriptionChannels(RedisChannel.Pattern("messages?test"));
            expectedResult = [channelA];
            CollectionAssert.AreEquivalent(expectedResult, result.Select(x => x.ToString()));

            result = server.SubscriptionChannels(RedisChannel.Pattern("messagesC*"));
            ClassicAssert.AreEqual(0, result.Length);

            sub.Unsubscribe(RedisChannel.Literal(channelA));
            sub.Unsubscribe(RedisChannel.Literal(channelB));
        }

        [Test]
        public void BasicPUBSUB_NUMPAT()
        {
            using var subRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var sub = subRedis.GetSubscriber();
            var db = redis.GetDatabase(0);
            var server = redis.GetServers()[0];

            string glob = "com.messages.*";
            string globB = "com.messagesB.*";
            string actual = "com.messages.testmessage";
            string actualB = "com.messagesB.testmessage";
            string value = "published message";

            var channel = new RedisChannel(glob, RedisChannel.PatternMode.Pattern);
            var channelB = new RedisChannel(globB, RedisChannel.PatternMode.Pattern);

            var result = server.SubscriptionPatternCount();
            ClassicAssert.AreEqual(0, result);

            SubscribeAndPublish(sub, db, channel, RedisChannel.Literal(actual), value);
            SubscribeAndPublish(sub, db, channelB, RedisChannel.Literal(actualB), value);

            result = server.SubscriptionPatternCount();
            ClassicAssert.AreEqual(2, result);

            sub.Unsubscribe(channel);
            sub.Unsubscribe(channelB);
        }

        [Test]
        public void BasicPUBSUB_NUMSUB()
        {
            using var subRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var sub = subRedis.GetSubscriber();
            var db = redis.GetDatabase(0);
            var server = redis.GetServers()[0];

            var multiChannelResult = server.Execute("PUBSUB", ["NUMSUB"]);
            ClassicAssert.AreEqual(0, multiChannelResult.Length);

            multiChannelResult = server.Execute("PUBSUB", ["NUMSUB", "messagesA", "messagesB"]);
            ClassicAssert.AreEqual(4, multiChannelResult.Length);
            ClassicAssert.AreEqual("messagesA", multiChannelResult[0].ToString());
            ClassicAssert.AreEqual("0", multiChannelResult[1].ToString());
            ClassicAssert.AreEqual("messagesB", multiChannelResult[2].ToString());
            ClassicAssert.AreEqual("0", multiChannelResult[3].ToString());

            SubscribeAndPublish(sub, db, RedisChannel.Literal("messagesA"));
            SubscribeAndPublish(sub, db, RedisChannel.Literal("messagesB"));

            var result = server.SubscriptionSubscriberCount(RedisChannel.Literal("messagesA"));
            ClassicAssert.AreEqual(1, result);

            multiChannelResult = server.Execute("PUBSUB", ["NUMSUB", "messagesA", "messagesB"]);
            ClassicAssert.AreEqual(4, multiChannelResult.Length);
            ClassicAssert.AreEqual("messagesA", multiChannelResult[0].ToString());
            ClassicAssert.AreEqual("1", multiChannelResult[1].ToString());
            ClassicAssert.AreEqual("messagesB", multiChannelResult[2].ToString());
            ClassicAssert.AreEqual("1", multiChannelResult[3].ToString());

            sub.Unsubscribe(RedisChannel.Literal("messagesA"));
            sub.Unsubscribe(RedisChannel.Literal("messagesB"));
        }

        private void SubscribeAndPublish(ISubscriber sub, IDatabase db, RedisChannel channel, RedisChannel? publishChannel = null, RedisValue? message = null, Action<RedisChannel, RedisValue> onSubscribe = null)
        {
            if (!message.HasValue)
            {
                message = "published message";
            }
            publishChannel ??= channel;
            ManualResetEvent evt = new(false);
            sub.Subscribe(channel, (receivedChannel, receivedMessage) =>
            {
                onSubscribe?.Invoke(receivedChannel, receivedMessage);
                evt.Set();
            });

            // Doing publish to make sure the channel is subscribed
            // Repeat to work-around bug in StackExchange.Redis subscribe behavior
            // where it returns before the SUBSCRIBE call is processed.
            int repeat = 5;
            while (true)
            {
                db.Publish(publishChannel.Value, message.Value);
                var ret = evt.WaitOne(TimeSpan.FromSeconds(1));
                if (ret) break;
                repeat--;
                ClassicAssert.IsTrue(repeat != 0, "Timeout waiting for subscription receive");
            }
        }
    }
}