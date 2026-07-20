// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    class RespPubSubTests : TestBase
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
            TestUtils.OnTearDown();
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

        /// <summary>
        /// Regression test for https://github.com/microsoft/garnet/issues/1615 (network-lock-error part).
        ///
        /// A client that PUBLISHes to a channel it is itself subscribed to receives its own message via a
        /// reentrant delivery on the same connection: SubscribeBroker.Broadcast calls back into this exact
        /// connection's Publish() while that connection's TryConsumeMessages is still on the stack and still
        /// holding the network sender's (non-reentrant) SpinLock. Before the fix, the reentrant Enter failed
        /// but the subsequent finally still unconditionally called Exit, releasing a lock this call never
        /// actually held. That caused the outer TryConsumeMessages to later throw a
        /// SynchronizationLockException when it tried to release the lock it did hold, tearing down the
        /// connection. This test would previously fail because the connection died and the trailing PING
        /// got no response (or the socket read timed out / the connection was reset).
        /// </summary>
        [Test]
        public void SelfPublishOnSubscribedChannelDoesNotCorruptConnection()
        {
            const string channel = "self-publish-channel";

            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(TestUtils.EndPoint);
            socket.ReceiveTimeout = 10_000;
            socket.SendTimeout = 10_000;

            // Subscribe on this connection, and wait for the subscribe confirmation so we know the broker
            // has registered this session as a subscriber before we publish to the same channel below.
            SendCommand(socket, "SUBSCRIBE", channel);
            var subscribeResponse = ReadAvailable(socket);
            StringAssert.Contains("subscribe", subscribeResponse);
            StringAssert.Contains(channel, subscribeResponse);

            // Publish to the channel we're subscribed to, on the very same connection - this is what
            // triggers the reentrant delivery into this connection's own Publish() path.
            SendCommand(socket, "PUBLISH", channel, "self-published-message");
            var publishResponse = ReadAvailable(socket);
            // The PUBLISH command itself must still complete and report the one (self-)subscriber,
            // regardless of whether the self-delivered "message" push made it through.
            StringAssert.Contains(":1", publishResponse);

            // The critical assertion: the connection must still be alive and the session must still be
            // usable afterwards. Before the fix, the SynchronizationLockException thrown while unwinding
            // TryConsumeMessages tore down this connection, so this PING would get no reply at all.
            // (The reply is the RESP2 "subscribe-mode" PONG form - *2\r\n$4\r\npong\r\n$0\r\n\r\n - since this
            // connection is still subscribed; that's expected and orthogonal to what we're checking here.)
            SendCommand(socket, "PING");
            var pingResponse = ReadAvailable(socket);
            ClassicAssert.AreEqual("*2\r\n$4\r\npong\r\n$0\r\n\r\n", pingResponse,
                "Connection did not survive a self-publish on a subscribed channel - PING got no normal reply");
        }

        /// <summary>
        /// Same regression as <see cref="SelfPublishOnSubscribedChannelDoesNotCorruptConnection"/>, but for
        /// the PSUBSCRIBE/PatternPublish path, which received the identical entered-guard fix in
        /// PatternPublish() but had no direct coverage. A client that is pattern-subscribed to a channel and
        /// then PUBLISHes a matching key on that same connection triggers the same reentrant delivery, this
        /// time through SubscribeBroker's pattern-matching broadcast into PatternPublish().
        /// </summary>
        [Test]
        public void SelfPublishOnPatternSubscribedChannelDoesNotCorruptConnection()
        {
            const string pattern = "self-publish-pattern-*";
            const string channel = "self-publish-pattern-channel";

            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(TestUtils.EndPoint);
            socket.ReceiveTimeout = 10_000;
            socket.SendTimeout = 10_000;

            // Pattern-subscribe on this connection, and wait for the confirmation so we know the broker has
            // registered this session as a pattern subscriber before we publish a matching key below.
            SendCommand(socket, "PSUBSCRIBE", pattern);
            var subscribeResponse = ReadAvailable(socket);
            StringAssert.Contains("psubscribe", subscribeResponse);
            StringAssert.Contains(pattern, subscribeResponse);

            // Publish to a channel matching our own pattern subscription, on the very same connection - this
            // triggers the reentrant delivery into this connection's own PatternPublish() path.
            SendCommand(socket, "PUBLISH", channel, "self-published-message");
            var publishResponse = ReadAvailable(socket);
            // The PUBLISH command itself must still complete and report the one (self-)subscriber,
            // regardless of whether the self-delivered "pmessage" push made it through.
            StringAssert.Contains(":1", publishResponse);

            // The critical assertion: the connection must still be alive and the session must still be
            // usable afterwards. Before the fix, the SynchronizationLockException thrown while unwinding
            // TryConsumeMessages tore down this connection, so this PING would get no reply at all.
            SendCommand(socket, "PING");
            var pingResponse = ReadAvailable(socket);
            ClassicAssert.AreEqual("*2\r\n$4\r\npong\r\n$0\r\n\r\n", pingResponse,
                "Connection did not survive a self-publish on a pattern-subscribed channel - PING got no normal reply");
        }

        private static void SendCommand(Socket socket, params string[] args)
        {
            var sb = new StringBuilder();
            sb.Append('*').Append(args.Length).Append("\r\n");
            foreach (var arg in args)
            {
                var argBytes = Encoding.UTF8.GetByteCount(arg);
                sb.Append('$').Append(argBytes).Append("\r\n").Append(arg).Append("\r\n");
            }
            socket.Send(Encoding.UTF8.GetBytes(sb.ToString()));
        }

        /// <summary>
        /// Reads whatever the server sends back within a short window, returning once the socket has been
        /// quiet for a bit. Used instead of parsing exact RESP token counts, since a self-publish can cause
        /// zero or more independent flushes (push message and/or command reply) on the same connection.
        /// </summary>
        private static string ReadAvailable(Socket socket, int overallTimeoutMs = 5_000, int quietPeriodMs = 200)
        {
            var buffer = new byte[4096];
            var sb = new StringBuilder();
            var deadline = DateTime.UtcNow.AddMilliseconds(overallTimeoutMs);

            while (DateTime.UtcNow < deadline)
            {
                if (socket.Poll(quietPeriodMs * 1000, SelectMode.SelectRead))
                {
                    int read;
                    try
                    {
                        read = socket.Receive(buffer);
                    }
                    catch (SocketException)
                    {
                        break;
                    }

                    if (read == 0)
                    {
                        // Peer closed the connection - stop trying to read.
                        break;
                    }
                    sb.Append(Encoding.UTF8.GetString(buffer, 0, read));
                }
                else if (sb.Length > 0)
                {
                    // We received some data and it has now been quiet for quietPeriodMs - assume the
                    // server is done flushing for this round-trip.
                    break;
                }
            }

            return sb.ToString();
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