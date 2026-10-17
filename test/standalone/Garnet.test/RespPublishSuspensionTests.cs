// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.networking;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// A publisher that can park delivers to every subscriber whose connection has room, and then parks for
    /// the ones whose connection has none, so a subscriber that is slow to read holds back the PUBLISH that is
    /// waiting for it without holding a thread.
    /// </summary>
    /// <remarks>
    /// The sessions are driven directly, against senders whose fullness the test decides, so that what a
    /// publisher does while a connection is full is observed with no network in the way.
    /// <see cref="RespSendBackpressureTests"/> covers the same through sockets.
    /// </remarks>
    [TestFixture]
    public class RespPublishSuspensionTests : TestBase
    {
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

        static readonly FieldInfo DeferredField = typeof(RespServerSession).GetField("deferredDeliveries",
            BindingFlags.Instance | BindingFlags.NonPublic);

        GarnetServer server;

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task APublisherParksForASubscriberWhoseConnectionIsFullAndDeliversOnceItHasRoom(bool pattern)
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 4096) { WaitingSendsBlock = true };
            var publisher = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var publisherSession = NewSession(publisher);
            try
            {
                await ConsumeAsync(subscriberSession, pattern ? Frame("PSUBSCRIBE", "c*") : Frame("SUBSCRIBE", "c"));
                var subscribed = subscriber.Sent;
                var message = pattern ? PatternMessage("c*", "c", "hello") : Message("c", "hello");
                var publish = Pinned(Frame("PUBLISH", "c", "hello"));

                // With room on the subscriber's connection the message is written inside the call, and nothing is held back
                var consume = await PublishAsync(publisherSession, publish);
                ClassicAssert.IsTrue(consume.IsCompleted, "the publisher parked although the subscriber had room");
                ClassicAssert.AreEqual(publish.Length, await consume);
                ClassicAssert.AreEqual(subscribed + message, subscriber.Sent);
                ClassicAssert.AreEqual(":1\r\n", publisher.Sent);
                ClassicAssert.AreEqual(0, subscriber.Registrations);
                ClassicAssert.AreEqual(0, DeferredCount(publisherSession));

                // Without room it parks: nothing is written to the subscriber, no thread waits inside the send,
                // and the reply is held back
                subscriber.Full = true;
                consume = await PublishAsync(publisherSession, publish);
                ClassicAssert.IsFalse(consume.IsCompleted, "the publisher did not wait for the subscriber");
                ClassicAssert.AreEqual(subscribed + message, subscriber.Sent);
                ClassicAssert.AreEqual(0, subscriber.WaitingSends, "the publisher waited for room inside the send");
                ClassicAssert.AreEqual(1, subscriber.Registrations);
                ClassicAssert.AreEqual(1, subscriber.Waiting);
                ClassicAssert.AreEqual(":1\r\n", publisher.Sent);

                // A wake that finds the connection full again parks the publisher again, and writes nothing
                subscriber.SignalCapacity();
                await WaitUntilAsync(() => subscriber.Registrations == 2, "the publisher waited a second time");
                ClassicAssert.IsFalse(consume.IsCompleted, "the publisher delivered although the connection was still full");
                ClassicAssert.AreEqual(subscribed + message, subscriber.Sent);
                ClassicAssert.AreEqual(1, subscriber.Waiting);

                // Room at last
                subscriber.Full = false;
                subscriber.SignalCapacity();
                ClassicAssert.AreEqual(publish.Length, await consume.AsTask().WaitAsync(Bound));
                ClassicAssert.AreEqual(subscribed + message + message, subscriber.Sent);
                ClassicAssert.AreEqual(":1\r\n:1\r\n", publisher.Sent);
                ClassicAssert.AreEqual(0, subscriber.Waiting);
                ClassicAssert.AreEqual(0, DeferredCount(publisherSession), "the publisher kept the subscribers it had delivered to");
            }
            finally
            {
                subscriber.ReleaseWaitingSends();
                subscriberSession.Dispose();
                publisherSession.Dispose();
            }
        }

        [Test]
        public async Task AFullSubscriberDoesNotHoldBackTheOthers()
        {
            StartServer();
            var full = new ControlledSender(bufferSize: 4096) { WaitingSendsBlock = true };
            var open = new ControlledSender(bufferSize: 4096);
            var publisher = new ControlledSender(bufferSize: 4096);
            var fullSession = NewSession(full);
            var openSession = NewSession(open);
            var publisherSession = NewSession(publisher);
            try
            {
                await ConsumeAsync(fullSession, Frame("SUBSCRIBE", "c"));
                await ConsumeAsync(openSession, Frame("SUBSCRIBE", "c"));
                var fullSubscribed = full.Sent;
                var openSubscribed = open.Sent;
                full.Full = true;

                var publish = Pinned(Frame("PUBLISH", "c", "hello"));
                var consume = await PublishAsync(publisherSession, publish);

                // Whichever of the two the broadcast reached first, the one with room has the message while the publisher waits
                ClassicAssert.IsFalse(consume.IsCompleted, "the publisher did not wait for the full subscriber");
                ClassicAssert.AreEqual(openSubscribed + Message("c", "hello"), open.Sent);
                ClassicAssert.AreEqual(fullSubscribed, full.Sent);
                ClassicAssert.AreEqual(0, full.WaitingSends);
                ClassicAssert.AreEqual(1, full.Waiting);
                ClassicAssert.AreEqual(0, open.Registrations);

                full.Full = false;
                full.SignalCapacity();
                ClassicAssert.AreEqual(publish.Length, await consume.AsTask().WaitAsync(Bound));
                ClassicAssert.AreEqual(fullSubscribed + Message("c", "hello"), full.Sent);
                ClassicAssert.AreEqual(openSubscribed + Message("c", "hello"), open.Sent);
                ClassicAssert.AreEqual(":2\r\n", publisher.Sent);
            }
            finally
            {
                full.ReleaseWaitingSends();
                fullSession.Dispose();
                openSession.Dispose();
                publisherSession.Dispose();
            }
        }

        [Test]
        public async Task APublisherTornDownWhileParkedLeavesNoRegistrationOnTheSubscriber()
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 4096) { WaitingSendsBlock = true };
            var subscriberSession = NewSession(subscriber);
            var publisherSession = NewSession(new ControlledSender(bufferSize: 4096));
            var publisherTornDown = false;
            try
            {
                await ConsumeAsync(subscriberSession, Frame("SUBSCRIBE", "c"));
                subscriber.Full = true;

                var consume = await PublishAsync(publisherSession, Pinned(Frame("PUBLISH", "c", "hello")));
                ClassicAssert.IsFalse(consume.IsCompleted, "the publisher did not wait for the subscriber");
                ClassicAssert.AreEqual(1, subscriber.Waiting);

                publisherTornDown = true;
                publisherSession.Dispose();

                // A body that is torn down is never resumed, so the teardown has to take back what it registered
                ClassicAssert.AreEqual(0, subscriber.Waiting, "the publisher's registration outlived its session");
                _ = Assert.ThrowsAsync<Garnet.common.SessionTornDownException>(async () => await consume.AsTask().WaitAsync(Bound));
            }
            finally
            {
                subscriber.ReleaseWaitingSends();
                subscriberSession.Dispose();
                if (!publisherTornDown)
                    publisherSession.Dispose();
            }
        }

        [Test]
        public void AWaitCancelledBeforeItRegistersWithAPeerLeavesNothingRegistered()
        {
            var peer = new ControlledSender(bufferSize: 64) { Full = true };
            var wait = new SendCapacityWait();

            // A teardown that runs while the body is on its way to registering finds nothing to take back
            wait.Cancel();
            ClassicAssert.IsTrue(wait.TryRegisterWithPeer(peer));

            ClassicAssert.AreEqual(0, peer.Waiting, "a registration made after the teardown stays on the peer");
            ClassicAssert.IsTrue(wait.WaitAsync().IsCanceled, "the wait was left to wait for a signal that cannot come");
        }

        [Test]
        public async Task APublisherSubscribedToItsOwnChannelWritesItsMessageWhileItsConnectionIsFull()
        {
            StartServer();
            var sender = new ControlledSender(bufferSize: 4096);
            var session = NewSession(sender);
            try
            {
                // A push can be told from a reply only under RESP3
                await ConsumeAsync(session, Frame("HELLO", "3"), Frame("SUBSCRIBE", "c"));
                var before = sender.Sent;

                sender.Full = true;
                var publish = Pinned(Frame("PUBLISH", "c", "x"));
                var consume = await PublishAsync(session, publish);

                // The message goes into the batch the session is building, ahead of the reply, rather than waiting
                // for a connection that is the publisher's own
                ClassicAssert.AreEqual(before + Message("c", "x", resp3: true) + ":1\r\n", sender.Sent);

                // The session then parks for its connection, as after any command that leaves it full
                sender.Full = false;
                if (!consume.IsCompleted)
                    sender.SignalCapacity();

                ClassicAssert.AreEqual(publish.Length, await consume.AsTask().WaitAsync(Bound));
            }
            finally
            {
                session.Dispose();
            }
        }

        [Test]
        public async Task AMessageForAChannelAndAPatternOfOneSubscriberArrivesInTheOrderItWasMade()
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 4096);
            var publisher = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var publisherSession = NewSession(publisher);
            try
            {
                await ConsumeAsync(subscriberSession, Frame("SUBSCRIBE", "c"), Frame("PSUBSCRIBE", "c*"));
                var subscribed = subscriber.Sent;

                // Full when the publisher first looks at the connection and with room at every look after, as
                // when a send completes between two of them. The message for the channel is held back, and the
                // one for the pattern must be held back behind it though the connection has room by then.
                var looks = 0;
                subscriber.Saturation = () => Interlocked.Increment(ref looks) == 1;

                var publish = Pinned(Frame("PUBLISH", "c", "hello"));
                var consume = await PublishAsync(publisherSession, publish);

                ClassicAssert.IsTrue(consume.IsCompleted, "the publisher parked although the connection had room when it looked again");
                ClassicAssert.AreEqual(publish.Length, await consume);
                ClassicAssert.AreEqual(subscribed + Message("c", "hello") + PatternMessage("c*", "c", "hello"), subscriber.Sent);
                ClassicAssert.AreEqual(":2\r\n", publisher.Sent);
            }
            finally
            {
                subscriberSession.Dispose();
                publisherSession.Dispose();
            }
        }

        [Test]
        public async Task APublisherOnAClusterNodeStillMakesTheDeliveriesItHeldBack()
        {
            StartServer(cluster: true);
            var subscriber = new ControlledSender(bufferSize: 4096) { WaitingSendsBlock = true };
            var publisher = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var publisherSession = NewSession(publisher);
            try
            {
                await ConsumeAsync(subscriberSession, Frame("SUBSCRIBE", "c"));
                var subscribed = subscriber.Sent;
                subscriber.Full = true;

                var publish = Pinned(Frame("PUBLISH", "c", "hello"));
                var consume = await PublishAsync(publisherSession, publish);
                ClassicAssert.IsFalse(consume.IsCompleted, "the publisher replied without delivering to the full subscriber");
                ClassicAssert.AreEqual(subscribed, subscriber.Sent);
                ClassicAssert.AreEqual("", publisher.Sent);

                subscriber.Full = false;
                subscriber.SignalCapacity();
                ClassicAssert.AreEqual(publish.Length, await consume.AsTask().WaitAsync(Bound));
                ClassicAssert.AreEqual(subscribed + Message("c", "hello"), subscriber.Sent);
                ClassicAssert.AreEqual(":1\r\n", publisher.Sent);
            }
            finally
            {
                subscriber.ReleaseWaitingSends();
                subscriberSession.Dispose();
                publisherSession.Dispose();
            }
        }

        [Test]
        public async Task APublisherThatCannotParkWaitsForRoomInsideTheBroadcast()
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 4096) { WaitingSendsBlock = true };
            var publisher = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var publisherSession = NewSession(publisher);
            try
            {
                await ConsumeAsync(subscriberSession, Frame("SUBSCRIBE", "c"));
                var subscribed = subscriber.Sent;
                subscriber.Full = true;

                // A consume with no receive loop to unwind to, as an embedded transport drives, cannot park, so a
                // publisher there waits for the room where it stands
                var publish = Pinned(Frame("PUBLISH", "c", "hello"));
                var consume = Task.Run(() => ConsumeSync(publisherSession, publish));

                await WaitUntilAsync(() => subscriber.WaitingSends == 1, "the publisher waited for room inside the send");
                ClassicAssert.IsFalse(consume.IsCompleted);
                ClassicAssert.AreEqual(0, subscriber.Registrations);
                ClassicAssert.AreEqual(subscribed, subscriber.Sent);

                subscriber.ReleaseWaitingSends();
                ClassicAssert.AreEqual(publish.Length, await consume.WaitAsync(Bound));
                ClassicAssert.AreEqual(subscribed + Message("c", "hello"), subscriber.Sent);
                ClassicAssert.AreEqual(":1\r\n", publisher.Sent);
            }
            finally
            {
                subscriber.ReleaseWaitingSends();
                subscriberSession.Dispose();
                publisherSession.Dispose();
            }
        }

        /// <summary>
        /// A transaction holds its key locks until it ends, so a publisher inside one neither parks nor waits for a
        /// subscriber whose connection is full: it sends the message past that connection's limit.
        /// </summary>
        [TestCase(false)]
        [TestCase(true)]
        public async Task APublisherInsideATransactionSendsToAFullSubscriberWithoutWaiting(bool pattern)
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 4096) { WaitingSendsBlock = true };
            var publisher = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var publisherSession = NewSession(publisher);
            try
            {
                await ConsumeAsync(subscriberSession, pattern ? Frame("PSUBSCRIBE", "c*") : Frame("SUBSCRIBE", "c"));
                var subscribed = subscriber.Sent;
                subscriber.Full = true;

                var transaction = Pinned(Frame("MULTI"), Frame("PUBLISH", "c", "hello"), Frame("EXEC"));
                var consume = await PublishAsync(publisherSession, transaction);

                ClassicAssert.IsTrue(consume.IsCompleted, "the publisher parked inside its transaction");
                ClassicAssert.AreEqual(transaction.Length, await consume);
                ClassicAssert.AreEqual(0, subscriber.WaitingSends, "the publisher waited for room inside the send");
                ClassicAssert.AreEqual(0, subscriber.Registrations, "the publisher waited for the subscriber");
                ClassicAssert.AreEqual(subscribed + (pattern ? PatternMessage("c*", "c", "hello") : Message("c", "hello")), subscriber.Sent);
                ClassicAssert.AreEqual("+OK\r\n+QUEUED\r\n*1\r\n:1\r\n", publisher.Sent);
                ClassicAssert.AreEqual(0, DeferredCount(publisherSession));
            }
            finally
            {
                subscriber.ReleaseWaitingSends();
                subscriberSession.Dispose();
                publisherSession.Dispose();
            }
        }

        /// <summary>
        /// A script runs to completion on the thread that called it, so a publisher inside one neither parks nor
        /// waits for a subscriber whose connection is full: it sends the message past that connection's limit.
        /// </summary>
        [Test]
        public async Task APublisherInsideAScriptSendsToAFullSubscriberWithoutWaiting()
        {
            StartServer(lua: true);
            var subscriber = new ControlledSender(bufferSize: 4096) { WaitingSendsBlock = true };
            var publisher = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var publisherSession = NewSession(publisher);
            try
            {
                await ConsumeAsync(subscriberSession, Frame("SUBSCRIBE", "c"));
                var subscribed = subscriber.Sent;
                subscriber.Full = true;

                var script = Pinned(Frame("EVAL", "return redis.call('PUBLISH', 'c', 'hello')", "0"));
                var consume = await PublishAsync(publisherSession, script);

                ClassicAssert.IsTrue(consume.IsCompleted, "the script waited for the subscriber");
                ClassicAssert.AreEqual(script.Length, await consume);
                ClassicAssert.AreEqual(0, subscriber.WaitingSends, "the script waited for room inside the send");
                ClassicAssert.AreEqual(0, subscriber.Registrations, "the script waited for the subscriber");
                ClassicAssert.AreEqual(subscribed + Message("c", "hello"), subscriber.Sent);
                ClassicAssert.AreEqual(":1\r\n", publisher.Sent);
            }
            finally
            {
                subscriber.ReleaseWaitingSends();
                subscriberSession.Dispose();
                publisherSession.Dispose();
            }
        }

        void StartServer(bool cluster = false, bool lua = false)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: cluster, enableLua: lua);
            server.Start();
        }

        RespServerSession NewSession(ControlledSender sender)
            => (RespServerSession)server.Provider.GetSession(WireFormat.ASCII, sender);

        static unsafe ValueTask<int> Consume(RespServerSession session, byte[] pinnedInput)
            => session.TryConsumeMessagesAsync((byte*)Unsafe.AsPointer(ref pinnedInput[0]), pinnedInput.Length);

        static unsafe int ConsumeSync(RespServerSession session, byte[] pinnedInput)
            => session.TryConsumeMessages((byte*)Unsafe.AsPointer(ref pinnedInput[0]), pinnedInput.Length);

        // Runs the call on a pool thread, so that a publisher that waits inside it fails the test by timing out
        // and does not hang it
        static async Task<ValueTask<int>> PublishAsync(RespServerSession publisher, byte[] pinnedInput)
            => await Task.Run(() => Consume(publisher, pinnedInput)).WaitAsync(Bound);

        static async Task ConsumeAsync(RespServerSession session, params string[] frames)
        {
            var input = Pinned(frames);
            ClassicAssert.AreEqual(input.Length, await Consume(session, input).AsTask().WaitAsync(Bound));
        }

        static int DeferredCount(RespServerSession session)
        {
            ClassicAssert.IsNotNull(DeferredField, "RespServerSession should still keep the deliveries it holds back in 'deferredDeliveries'");
            return ((DeferredDeliveries)DeferredField.GetValue(session))?.Count ?? 0;
        }

        internal static string Frame(params string[] tokens)
        {
            var frame = new StringBuilder().Append('*').Append(tokens.Length).Append("\r\n");
            foreach (var token in tokens)
                _ = frame.Append('$').Append(token.Length).Append("\r\n").Append(token).Append("\r\n");
            return frame.ToString();
        }

        internal static byte[] Pinned(params string[] frames)
        {
            var bytes = Encoding.ASCII.GetBytes(string.Concat(frames));
            var pinned = GC.AllocateArray<byte>(bytes.Length, pinned: true);
            bytes.CopyTo(pinned, 0);
            return pinned;
        }

        static string Message(string channel, string payload, bool resp3 = false)
            => $"{(resp3 ? '>' : '*')}3\r\n$7\r\nmessage\r\n${channel.Length}\r\n{channel}\r\n${payload.Length}\r\n{payload}\r\n";

        static string PatternMessage(string pattern, string channel, string payload, bool resp3 = false)
            => $"{(resp3 ? '>' : '*')}4\r\n$8\r\npmessage\r\n${pattern.Length}\r\n{pattern}\r\n${channel.Length}\r\n{channel}\r\n${payload.Length}\r\n{payload}\r\n";

        static async Task WaitUntilAsync(Func<bool> condition, string what)
        {
            var stopwatch = Stopwatch.StartNew();
            while (!condition())
            {
                if (stopwatch.Elapsed > Bound)
                    Assert.Fail($"Timed out waiting until {what}.");
                await Task.Delay(1);
            }
        }
    }
}