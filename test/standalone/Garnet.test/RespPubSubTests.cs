// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Garnet.common;
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

        /// <summary>
        /// Verifies that disallowed commands (GET, SET, PUBLISH, MULTI) are rejected
        /// with an appropriate error when a RESP2 session is in subscription mode.
        /// </summary>
        [Test]
        public void PubSubModeRejectsDisallowedCommandsInResp2()
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

            // Subscribe to enter subscription mode
            var subscribeResp = "*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n";
            var response = lightClientRequest.Execute("SUBSCRIBE foo", subscribeResp.Length);
            ClassicAssert.AreEqual(subscribeResp, response);

            // GET should be rejected
            var errorResp = "-ERR Can't execute 'GET': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n";
            response = lightClientRequest.Execute("GET bar", errorResp.Length);
            ClassicAssert.AreEqual(errorResp, response);

            // SET should be rejected
            errorResp = "-ERR Can't execute 'SET': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n";
            response = lightClientRequest.Execute("SET bar value", errorResp.Length);
            ClassicAssert.AreEqual(errorResp, response);

            // PUBLISH should be rejected
            errorResp = "-ERR Can't execute 'PUBLISH': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n";
            response = lightClientRequest.Execute("PUBLISH foo bar", errorResp.Length);
            ClassicAssert.AreEqual(errorResp, response);

            // MULTI should be rejected
            errorResp = "-ERR Can't execute 'MULTI': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n";
            response = lightClientRequest.Execute("MULTI", errorResp.Length);
            ClassicAssert.AreEqual(errorResp, response);
        }

        /// <summary>
        /// Verifies that allowed commands (PING, SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE,
        /// PUNSUBSCRIBE, QUIT) work correctly in RESP2 subscription mode.
        /// </summary>
        [Test]
        public void PubSubModeAllowsValidCommandsInResp2()
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

            // Enter subscription mode
            var subscribeResp = "*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n";
            var response = lightClientRequest.Execute("SUBSCRIBE foo", subscribeResp.Length);
            ClassicAssert.AreEqual(subscribeResp, response);

            // PING should return subscription-mode PONG
            var pongResp = "*2\r\n$4\r\npong\r\n$0\r\n\r\n";
            response = lightClientRequest.Execute("PING", pongResp.Length);
            ClassicAssert.AreEqual(pongResp, response);

            // Another SUBSCRIBE should work (channel count goes to 2)
            subscribeResp = "*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n";
            response = lightClientRequest.Execute("SUBSCRIBE bar", subscribeResp.Length);
            ClassicAssert.AreEqual(subscribeResp, response);

            // PSUBSCRIBE should work (channel count goes to 3)
            var psubResp = "*3\r\n$10\r\npsubscribe\r\n$4\r\nbaz*\r\n:3\r\n";
            response = lightClientRequest.Execute("PSUBSCRIBE baz*", psubResp.Length);
            ClassicAssert.AreEqual(psubResp, response);

            // UNSUBSCRIBE should work (channel count goes to 2)
            var unsubResp = "*3\r\n$11\r\nunsubscribe\r\n$3\r\nbar\r\n:2\r\n";
            response = lightClientRequest.Execute("UNSUBSCRIBE bar", unsubResp.Length);
            ClassicAssert.AreEqual(unsubResp, response);

            // PUNSUBSCRIBE should work (channel count goes to 1)
            var punsubResp = "*3\r\n$12\r\npunsubscribe\r\n$4\r\nbaz*\r\n:1\r\n";
            response = lightClientRequest.Execute("PUNSUBSCRIBE baz*", punsubResp.Length);
            ClassicAssert.AreEqual(punsubResp, response);

            // Still in subscription mode - GET should be rejected
            var errorResp = "-ERR Can't execute 'GET': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n";
            response = lightClientRequest.Execute("GET bar", errorResp.Length);
            ClassicAssert.AreEqual(errorResp, response);

            // UNSUBSCRIBE last channel to exit subscription mode (channel count goes to 0)
            unsubResp = "*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:0\r\n";
            response = lightClientRequest.Execute("UNSUBSCRIBE foo", unsubResp.Length);
            ClassicAssert.AreEqual(unsubResp, response);

            // No longer in subscription mode - GET should work (key doesn't exist = null)
            var getResp = "$-1\r\n";
            response = lightClientRequest.Execute("GET bar", getResp.Length);
            ClassicAssert.AreEqual(getResp, response);
        }

        /// <summary>
        /// Verifies that a RESP3 session in subscription mode can execute PUBLISH (including self-publish
        /// to its own subscribed channel), delivering the self-message without a lock error.
        /// </summary>
        [Test]
        public void PubSubSelfPublishResp3NoLockError()
        {
            // Use Newlines counting for HELLO 3 (variable-length map response)
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Newlines);

            // Switch to RESP3
            var response = lightClientRequest.Execute("HELLO 3", 30);
            ClassicAssert.IsTrue(response.Contains("proto"));

            // Switch to Bytes counting for precise response control
            lightClientRequest.countResponseType = CountResponseType.Bytes;

            // Subscribe to a channel
            var subscribeResp = "*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n";
            response = lightClientRequest.Execute("SUBSCRIBE foo", subscribeResp.Length);
            ClassicAssert.AreEqual(subscribeResp, response);

            // Self-publish triggers the reentrant Publish() callback on the same session.
            // Expected response consists of:
            //   1. Push message (self-notification): >3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
            //   2. PUBLISH response: :1\r\n
            var pushMsg = ">3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
            var publishResp = ":1\r\n";
            var expectedPublishTotal = pushMsg + publishResp;
            response = lightClientRequest.Execute("PUBLISH foo bar", expectedPublishTotal.Length);
            ClassicAssert.AreEqual(expectedPublishTotal, response);

            // Unsubscribe and verify server is still responsive
            var unsubResp = "*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:0\r\n";
            response = lightClientRequest.Execute("UNSUBSCRIBE foo", unsubResp.Length);
            ClassicAssert.AreEqual(unsubResp, response);

            // PING to confirm server health
            response = lightClientRequest.Execute("PING", "+PONG\r\n".Length);
            ClassicAssert.AreEqual("+PONG\r\n", response);
        }

        /// <summary>
        /// Verifies that the PatternPublish reentrant path works correctly in RESP3.
        /// When a session has a pattern subscription (PSUBSCRIBE) and publishes to a
        /// matching channel, the PatternPublish() callback is invoked on the same thread.
        /// </summary>
        [Test]
        public void PubSubSelfPatternPublishResp3NoLockError()
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Newlines);

            // Switch to RESP3
            var response = lightClientRequest.Execute("HELLO 3", 30);
            ClassicAssert.IsTrue(response.Contains("proto"));

            lightClientRequest.countResponseType = CountResponseType.Bytes;

            // Pattern subscribe
            var psubResp = "*3\r\n$10\r\npsubscribe\r\n$4\r\nfoo*\r\n:1\r\n";
            response = lightClientRequest.Execute("PSUBSCRIBE foo*", psubResp.Length);
            ClassicAssert.AreEqual(psubResp, response);

            // Self-publish to a matching channel — triggers reentrant PatternPublish() callback.
            // Expected response:
            //   1. Push pmessage: >4\r\n$8\r\npmessage\r\n$4\r\nfoo*\r\n$6\r\nfoobar\r\n$3\r\nbaz\r\n
            //   2. PUBLISH response: :1\r\n
            var pushMsg = ">4\r\n$8\r\npmessage\r\n$4\r\nfoo*\r\n$6\r\nfoobar\r\n$3\r\nbaz\r\n";
            var publishResp = ":1\r\n";
            var expectedTotal = pushMsg + publishResp;
            response = lightClientRequest.Execute("PUBLISH foobar baz", expectedTotal.Length);
            ClassicAssert.AreEqual(expectedTotal, response);

            // Clean up and verify server health
            var punsubResp = "*3\r\n$12\r\npunsubscribe\r\n$4\r\nfoo*\r\n:0\r\n";
            response = lightClientRequest.Execute("PUNSUBSCRIBE foo*", punsubResp.Length);
            ClassicAssert.AreEqual(punsubResp, response);

            response = lightClientRequest.Execute("PING", "+PONG\r\n".Length);
            ClassicAssert.AreEqual("+PONG\r\n", response);
        }

        /// <summary>
        /// Verifies that in RESP3 subscription mode, regular commands (GET, SET)
        /// are allowed and execute correctly alongside active subscriptions.
        /// </summary>
        [Test]
        public void PubSubModeAllowsRegularCommandsInResp3()
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Newlines);

            // Switch to RESP3
            var response = lightClientRequest.Execute("HELLO 3", 30);
            ClassicAssert.IsTrue(response.Contains("proto"));

            lightClientRequest.countResponseType = CountResponseType.Bytes;

            // Subscribe to a channel
            var subscribeResp = "*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n";
            response = lightClientRequest.Execute("SUBSCRIBE foo", subscribeResp.Length);
            ClassicAssert.AreEqual(subscribeResp, response);

            // SET should work in RESP3 subscription mode
            response = lightClientRequest.Execute("SET mykey myval", "+OK\r\n".Length);
            ClassicAssert.AreEqual("+OK\r\n", response);

            // GET should work in RESP3 subscription mode
            var getResp = "$5\r\nmyval\r\n";
            response = lightClientRequest.Execute("GET mykey", getResp.Length);
            ClassicAssert.AreEqual(getResp, response);

            // Clean up
            var unsubResp = "*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:0\r\n";
            response = lightClientRequest.Execute("UNSUBSCRIBE foo", unsubResp.Length);
            ClassicAssert.AreEqual(unsubResp, response);
        }

        /// <summary>
        /// Verifies that entering subscription mode via PSUBSCRIBE alone (without SUBSCRIBE)
        /// also correctly restricts commands in RESP2.
        /// </summary>
        [Test]
        public void PubSubModeViaPsubscribeRejectsCommandsInResp2()
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

            // Enter subscription mode via PSUBSCRIBE only
            var psubResp = "*3\r\n$10\r\npsubscribe\r\n$4\r\nfoo*\r\n:1\r\n";
            var response = lightClientRequest.Execute("PSUBSCRIBE foo*", psubResp.Length);
            ClassicAssert.AreEqual(psubResp, response);

            // GET should be rejected
            var errorResp = "-ERR Can't execute 'GET': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n";
            response = lightClientRequest.Execute("GET bar", errorResp.Length);
            ClassicAssert.AreEqual(errorResp, response);

            // PUNSUBSCRIBE to exit subscription mode
            var punsubResp = "*3\r\n$12\r\npunsubscribe\r\n$4\r\nfoo*\r\n:0\r\n";
            response = lightClientRequest.Execute("PUNSUBSCRIBE foo*", punsubResp.Length);
            ClassicAssert.AreEqual(punsubResp, response);

            // GET should work again
            response = lightClientRequest.Execute("GET bar", "$-1\r\n".Length);
            ClassicAssert.AreEqual("$-1\r\n", response);
        }

        /// <summary>
        /// A client that PUBLISHes to a channel it is subscribed to receives its own message via a reentrant
        /// delivery on the same connection. Exercised over RESP3 (RESP2 rejects PUBLISH while subscribed);
        /// verifies the self-published message is delivered and the connection stays usable.
        /// </summary>
        [Test]
        public void SelfPublishOnSubscribedChannelDoesNotCorruptConnection()
        {
            const string channel = "self-publish-channel";
            const string message = "self-published-message";

            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(TestUtils.EndPoint);
            socket.ReceiveTimeout = 10_000;
            socket.SendTimeout = 10_000;

            // Switch to RESP3 so PUBLISH is permitted while subscribed (RESP2 rejects it).
            SendCommand(socket, "HELLO", "3");
            StringAssert.Contains("proto", ReadAvailable(socket));

            // Subscribe on this connection, and wait for the confirmation so we know the broker has
            // registered this session as a subscriber before we publish to the same channel below.
            SendCommand(socket, "SUBSCRIBE", channel);
            var subscribeResponse = ReadAvailable(socket);
            StringAssert.Contains("subscribe", subscribeResponse);
            StringAssert.Contains(channel, subscribeResponse);

            // Publish to the channel we're subscribed to, on the very same connection - this is what
            // triggers the reentrant delivery into this connection's own Publish() path.
            SendCommand(socket, "PUBLISH", channel, message);
            var publishResponse = ReadAvailable(socket);
            // The self-published message must be delivered back to this connection (the value only appears
            // in the response via the delivered push), and the PUBLISH reply must report the one subscriber.
            StringAssert.Contains(message, publishResponse);
            StringAssert.Contains(":1", publishResponse);

            // The connection must still be alive and usable afterwards.
            SendCommand(socket, "PING");
            StringAssert.Contains("+PONG", ReadAvailable(socket));
        }

        /// <summary>
        /// Pattern-subscription variant of <see cref="SelfPublishOnSubscribedChannelDoesNotCorruptConnection"/>:
        /// a client PSUBSCRIBEd to a pattern that PUBLISHes a matching channel on the same connection triggers
        /// the reentrant delivery through PatternPublish(). Verifies the pmessage is delivered and the
        /// connection stays usable.
        /// </summary>
        [Test]
        public void SelfPublishOnPatternSubscribedChannelDoesNotCorruptConnection()
        {
            const string pattern = "self-publish-pattern-*";
            const string channel = "self-publish-pattern-channel";
            const string message = "self-published-message";

            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(TestUtils.EndPoint);
            socket.ReceiveTimeout = 10_000;
            socket.SendTimeout = 10_000;

            // Switch to RESP3 so PUBLISH is permitted while subscribed (RESP2 rejects it).
            SendCommand(socket, "HELLO", "3");
            StringAssert.Contains("proto", ReadAvailable(socket));

            // Pattern-subscribe on this connection, and wait for the confirmation so we know the broker has
            // registered this session as a pattern subscriber before we publish a matching key below.
            SendCommand(socket, "PSUBSCRIBE", pattern);
            var subscribeResponse = ReadAvailable(socket);
            StringAssert.Contains("psubscribe", subscribeResponse);
            StringAssert.Contains(pattern, subscribeResponse);

            // Publish to a channel matching our own pattern subscription, on the very same connection - this
            // triggers the reentrant delivery into this connection's own PatternPublish() path.
            SendCommand(socket, "PUBLISH", channel, message);
            var publishResponse = ReadAvailable(socket);
            // The self-published message must be delivered back to this connection via the pmessage push,
            // and the PUBLISH reply must report the one subscriber.
            StringAssert.Contains(message, publishResponse);
            StringAssert.Contains(":1", publishResponse);

            // The connection must still be alive and usable afterwards.
            SendCommand(socket, "PING");
            StringAssert.Contains("+PONG", ReadAvailable(socket));
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
    }
}