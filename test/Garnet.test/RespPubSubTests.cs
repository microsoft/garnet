// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    class RespPubSubTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
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

            ManualResetEvent evt = new(false);

            sub.Subscribe(RedisChannel.Literal("messages"), (channel, message) =>
            {
                ClassicAssert.AreEqual("messages", (string)channel);
                ClassicAssert.AreEqual("published message", (string)message);
                evt.Set();
            });

            // Repeat to work-around bug in StackExchange.Redis subscribe behavior
            // where it returns before the SUBSCRIBE call is processed.
            int repeat = 5;
            while (true)
            {
                db.Publish(RedisChannel.Pattern("messages"), "published message");
                var ret = evt.WaitOne(TimeSpan.FromSeconds(1));
                if (ret) break;
                repeat--;
                ClassicAssert.IsTrue(repeat != 0, "Timeout waiting for subsciption receive");
            }
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

            sub.Subscribe(channel, (receivedChannel, message) =>
            {
                ClassicAssert.AreEqual(glob, (string)channel);
                ClassicAssert.AreEqual(actual, (string)receivedChannel);
                ClassicAssert.AreEqual(value, (string)message);
                evt.Set();
            });

            // Repeat to work-around bug in StackExchange.Redis subscribe behavior
            // where it returns before the SUBSCRIBE call is processed.
            int repeat = 5;
            while (true)
            {
                db.Publish(RedisChannel.Pattern(actual), value);
                var ret = evt.WaitOne(TimeSpan.FromSeconds(1));
                if (ret) break;
                repeat--;
                ClassicAssert.IsTrue(repeat != 0, "Timeout waiting for subsciption receive");
            }
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

            ManualResetEvent evt = new(false);
            var isMessagesASubed = false;
            var isMessagesBSubed = false;
            var channelA = "messagesAtest";
            var channelB = "messagesB";

            sub.Subscribe(RedisChannel.Literal(channelA), (channel, message) =>
            {
                isMessagesASubed = true;
                if (isMessagesBSubed)
                    evt.Set();
            });

            sub.Subscribe(RedisChannel.Literal(channelB), (channel, message) =>
            {
                isMessagesBSubed = true;
                if (isMessagesASubed)
                    evt.Set();
            });

            // Doing publish to make sure the channel is subscribed
            // Repeat to work-around bug in StackExchange.Redis subscribe behavior
            // where it returns before the SUBSCRIBE call is processed.
            int repeat = 5;
            while (true)
            {
                if (!isMessagesASubed)
                    db.Publish(RedisChannel.Pattern(channelA), "published message");
                if (!isMessagesBSubed)
                    db.Publish(RedisChannel.Pattern(channelB), "published message");
                var ret = evt.WaitOne(TimeSpan.FromSeconds(1));
                if (ret) break;
                repeat--;
                ClassicAssert.IsTrue(repeat != 0, "Timeout waiting for subsciption receive");
            }

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

            var isMessagesASubed = false;
            var isMessagesBSubed = false;

            ManualResetEvent evt = new(false);

            sub.Subscribe(channel, (receivedChannel, message) =>
            {
                isMessagesASubed = true;
                if (isMessagesASubed && isMessagesBSubed)
                    evt.Set();
            });

            sub.Subscribe(channelB, (receivedChannel, message) =>
            {
                isMessagesBSubed = true;
                if (isMessagesASubed && isMessagesBSubed)
                    evt.Set();
            });

            // Repeat to work-around bug in StackExchange.Redis subscribe behavior
            // where it returns before the SUBSCRIBE call is processed.
            int repeat = 5;
            while (true)
            {
                if (!isMessagesASubed)
                    db.Publish(RedisChannel.Literal(actual), value);
                if (!isMessagesBSubed)
                    db.Publish(RedisChannel.Literal(actualB), value);
                var ret = evt.WaitOne(TimeSpan.FromSeconds(1));
                if (ret) break;
                repeat--;
                ClassicAssert.IsTrue(repeat != 0, "Timeout waiting for subsciption receive");
            }

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

            ManualResetEvent evt = new(false);
            var isMessagesASubed = false; 
            var isMessagesBSubed = false;

            var multiChannelResult = server.Execute("PUBSUB", ["NUMSUB"]);
            ClassicAssert.AreEqual(0, multiChannelResult.Length);

            multiChannelResult = server.Execute("PUBSUB", ["NUMSUB", "messagesA", "messagesB"]);
            ClassicAssert.AreEqual(4, multiChannelResult.Length);
            ClassicAssert.AreEqual("messagesA", multiChannelResult[0].ToString());
            ClassicAssert.AreEqual("0", multiChannelResult[1].ToString());
            ClassicAssert.AreEqual("messagesB", multiChannelResult[2].ToString());
            ClassicAssert.AreEqual("0", multiChannelResult[3].ToString());

            sub.Subscribe(RedisChannel.Literal("messagesA"), (channel, message) =>
            {
                isMessagesASubed = true;
                if (isMessagesASubed && isMessagesBSubed)
                    evt.Set();
            });

            sub.Subscribe(RedisChannel.Literal("messagesB"), (channel, message) =>
            {
                isMessagesBSubed = true;
                if (isMessagesASubed && isMessagesBSubed)
                    evt.Set();
            });

            // Doing publish to make sure the channel is subscribed
            // Repeat to work-around bug in StackExchange.Redis subscribe behavior
            // where it returns before the SUBSCRIBE call is processed.
            int repeat = 5;
            while (true)
            {
                if(!isMessagesASubed)
                    db.Publish(RedisChannel.Pattern("messagesA"), "published message");
                if (!isMessagesBSubed)
                    db.Publish(RedisChannel.Pattern("messagesB"), "published message");
                var ret = evt.WaitOne(TimeSpan.FromSeconds(1));
                if (ret) break;
                repeat--;
                ClassicAssert.IsTrue(repeat != 0, "Timeout waiting for subsciption receive");
            }

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
    }
}