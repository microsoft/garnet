// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using NUnit.Framework;
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

            sub.Subscribe("messages", (channel, message) =>
            {
                Assert.AreEqual("messages", (string)channel);
                Assert.AreEqual("published message", (string)message);
                evt.Set();
            });

            // Repeat to work-around bug in StackExchange.Redis subscribe behavior
            // where it returns before the SUBSCRIBE call is processed.
            int repeat = 5;
            while (true)
            {
                db.Publish("messages", "published message");
                var ret = evt.WaitOne(TimeSpan.FromSeconds(1));
                if (ret) break;
                repeat--;
                Assert.IsTrue(repeat != 0, "Timeout waiting for subsciption receive");
            }
            sub.Unsubscribe("messages");
        }

        [Test]
        public void BasicPSUBSCRIBE()
        {
            using var subRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var sub = subRedis.GetSubscriber();
            var db = redis.GetDatabase(0);

            string glob = "com.messages.*";
            string actual = "com.messages.testmessage";
            string value = "published message";

            var channel = new RedisChannel(glob, RedisChannel.PatternMode.Pattern);

            ManualResetEvent evt = new(false);

            sub.Subscribe(channel, (receivedChannel, message) =>
            {
                Assert.AreEqual(glob, (string)channel);
                Assert.AreEqual(actual, (string)receivedChannel);
                Assert.AreEqual(value, (string)message);
                evt.Set();
            });

            // Repeat to work-around bug in StackExchange.Redis subscribe behavior
            // where it returns before the SUBSCRIBE call is processed.
            int repeat = 5;
            while (true)
            {
                db.Publish(actual, value);
                var ret = evt.WaitOne(TimeSpan.FromSeconds(1));
                if (ret) break;
                repeat--;
                Assert.IsTrue(repeat != 0, "Timeout waiting for subsciption receive");
            }
            sub.Unsubscribe(channel);
        }
    }
}