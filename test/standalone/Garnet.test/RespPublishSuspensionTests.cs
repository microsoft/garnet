// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.networking;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

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

        /// <summary>
        /// More messages than the broker's log keeps in memory at the default page size, many times over.
        /// </summary>
        const int Many = 2000;

        /// <summary>
        /// How long a subscriber stays suspended holding its response object: long enough for a publisher to reach
        /// it, by a wide margin.
        /// </summary>
        const double SuspendSeconds = 3;

        const string Ok = "+OK\r\n";

        static readonly FieldInfo DeferredField = typeof(RespServerSession).GetField("deferredDeliveries",
            BindingFlags.Instance | BindingFlags.NonPublic);

        static readonly FieldInfo SenderWaitersField = typeof(RespServerSession).GetField("senderWaiters",
            BindingFlags.Instance | BindingFlags.NonPublic);

        /// <summary>What is waiting for the session to let go of its response object, messages left included.</summary>
        static int SenderWaiters(RespServerSession session) => (int)SenderWaitersField.GetValue(session);

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
            StartServer();
            var peer = new ControlledSender(bufferSize: 64) { Full = true };
            var peerSession = NewSession(peer);
            var wait = new SendCapacityWait();
            try
            {
                // A teardown that runs while the body is on its way to registering finds nothing to take back
                wait.Cancel();
                ClassicAssert.IsTrue(wait.TryRegisterWithPeer(peerSession));

                ClassicAssert.AreEqual(0, peer.Waiting, "a registration made after the teardown stays on the peer");
                ClassicAssert.IsTrue(wait.WaitAsync().IsCanceled, "the wait was left to wait for a signal that cannot come");
            }
            finally
            {
                peerSession.Dispose();
            }
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
        public async Task APublisherThatCannotParkWaitsForRoomOutsideTheSend()
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

                // A consume with no receive loop to unwind to, as an embedded transport drives, cannot park. The
                // publisher there still holds its message back and waits for room on the subscriber's connection,
                // not inside a send to it.
                var publish = Pinned(Frame("PUBLISH", "c", "hello"));
                var consume = Task.Run(() => ConsumeSync(publisherSession, publish));

                await WaitUntilAsync(() => subscriber.Waiting == 1, "the publisher waited for the subscriber's connection");
                ClassicAssert.IsFalse(consume.IsCompleted);
                ClassicAssert.AreEqual(0, subscriber.WaitingSends, "the publisher waited for room inside the send");
                ClassicAssert.AreEqual(1, subscriber.Registrations);
                ClassicAssert.AreEqual(subscribed, subscriber.Sent);

                subscriber.Full = false;
                subscriber.SignalCapacity();
                ClassicAssert.AreEqual(publish.Length, await consume.WaitAsync(Bound));
                ClassicAssert.AreEqual(subscribed + Message("c", "hello"), subscriber.Sent);
                ClassicAssert.AreEqual(":1\r\n", publisher.Sent);
                ClassicAssert.AreEqual(0, subscriber.WaitingSends);
            }
            finally
            {
                subscriber.ReleaseWaitingSends();
                subscriberSession.Dispose();
                publisherSession.Dispose();
            }
        }

        /// <summary>
        /// A message forwarded from another cluster node goes through the broker's log, whose consumer holds back
        /// what a full subscriber cannot take and waits for its connection without a thread, and without moving on
        /// to the next message.
        /// </summary>
        [TestCase(false)]
        [TestCase(true)]
        public async Task TheBrokerWaitsForAFullSubscriberWithoutAThreadAndDeliversInOrder(bool pattern)
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 4096) { WaitingSendsBlock = true };
            var open = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var openSession = NewSession(open);
            try
            {
                await ConsumeAsync(subscriberSession, pattern ? Frame("PSUBSCRIBE", "c*") : Frame("SUBSCRIBE", "c"));
                await ConsumeAsync(openSession, Frame("SUBSCRIBE", "c"));
                var subscribed = subscriber.Sent;
                var openSubscribed = open.Sent;
                subscriber.Full = true;

                var broker = Broker();
                AssertEnqueued(Publish(broker, "c", "one"));
                AssertEnqueued(Publish(broker, "c", "two"));

                await WaitUntilAsync(() => subscriber.Waiting == 1, "the broker waited for the full subscriber");
                await WaitUntilAsync(() => open.Sent == openSubscribed + Message("c", "one"), "the open subscriber had the first message");
                ClassicAssert.AreEqual(0, subscriber.WaitingSends, "the broker waited for room inside the send");
                ClassicAssert.AreEqual(subscribed, subscriber.Sent);
                ClassicAssert.AreEqual(openSubscribed + Message("c", "one"), open.Sent, "the broker moved past a message it had not delivered");

                // A wake that finds the connection full again waits again, and writes nothing
                subscriber.SignalCapacity();
                await WaitUntilAsync(() => subscriber.Registrations == 2, "the broker waited a second time");
                ClassicAssert.AreEqual(subscribed, subscriber.Sent);

                subscriber.Full = false;
                subscriber.SignalCapacity();
                string Delivered(string payload) => pattern ? PatternMessage("c*", "c", payload) : Message("c", payload);
                await WaitUntilAsync(() => subscriber.Sent == subscribed + Delivered("one") + Delivered("two"), "the subscriber had both messages, in order");
                await WaitUntilAsync(() => open.Sent == openSubscribed + Message("c", "one") + Message("c", "two"), "the open subscriber had both messages");
                ClassicAssert.AreEqual(0, subscriber.WaitingSends);
                ClassicAssert.AreEqual(0, subscriber.Waiting);
            }
            finally
            {
                subscriber.ReleaseWaitingSends();
                subscriberSession.Dispose();
                openSession.Dispose();
            }
        }

        /// <summary>
        /// The broker's log keeps only a few pages in memory, so a producer that would take it further ahead of a
        /// consumer held up by a full subscriber waits for room instead, and no message is lost.
        /// </summary>
        [Test]
        public async Task AProducerWaitsForRoomInTheBrokersLogAndNothingIsLost()
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 1 << 16) { WaitingSendsBlock = true };
            var subscriberSession = NewSession(subscriber);
            try
            {
                await ConsumeAsync(subscriberSession, Frame("SUBSCRIBE", "c"));
                var subscribed = subscriber.Sent;
                subscriber.Full = true;

                var broker = Broker();
                var payload = new string('x', 100);
                var expected = new StringBuilder(subscribed);
                ValueTask parked = default;
                var published = 0;
                for (; published < Many; published++)
                {
                    var message = $"{published:D4}{payload}";
                    _ = expected.Append(Message("c", message));
                    var enqueue = Publish(broker, "c", message);
                    if (published == 0)
                        await WaitUntilAsync(() => subscriber.Waiting == 1, "the broker waited for the full subscriber");
                    if (!enqueue.IsCompleted)
                    {
                        parked = enqueue;
                        published++;
                        break;
                    }
                }

                // The four pages of the log the default page size gives hold some 150 of these
                ClassicAssert.IsFalse(parked.IsCompleted, "the producer took the log further ahead of the consumer than it keeps");
                ClassicAssert.Less(published, 200);
                await Task.Delay(50);
                ClassicAssert.IsFalse(parked.IsCompleted, "the producer appended although the consumer had not moved");
                ClassicAssert.AreEqual(subscribed, subscriber.Sent);

                subscriber.Full = false;
                subscriber.SignalCapacity();
                await parked.AsTask().WaitAsync(Bound);

                // As the session that published it would, the next goes in only once the last is in
                for (; published < Many; published++)
                {
                    var message = $"{published:D4}{payload}";
                    _ = expected.Append(Message("c", message));
                    await Publish(broker, "c", message).AsTask().WaitAsync(Bound);
                }

                await WaitUntilAsync(() => subscriber.Sent.Length >= expected.Length, "the subscriber had every message");
                ClassicAssert.AreEqual(expected.ToString(), subscriber.Sent);
                ClassicAssert.AreEqual(0, subscriber.WaitingSends);
            }
            finally
            {
                subscriber.ReleaseWaitingSends();
                subscriberSession.Dispose();
            }
        }

        /// <summary>
        /// A message too large for a page of the broker's log waits beside it, one at a time, and is delivered in
        /// its place among the others.
        /// </summary>
        [Test]
        public async Task AMessageTooLargeForTheBrokersLogArrivesInItsPlace()
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 1 << 16) { WaitingSendsBlock = true };
            var subscriberSession = NewSession(subscriber);
            try
            {
                await ConsumeAsync(subscriberSession, Frame("SUBSCRIBE", "c"));
                var subscribed = subscriber.Sent;
                subscriber.Full = true;

                var broker = Broker();
                var large1 = new string('1', 5000);
                var large2 = new string('2', 5000);
                AssertEnqueued(Publish(broker, "c", "small"));
                await WaitUntilAsync(() => subscriber.Waiting == 1, "the broker waited for the full subscriber");
                AssertEnqueued(Publish(broker, "c", large1));

                // The first large message has not been taken yet, so the second waits for it to be
                var second = Publish(broker, "c", large2);
                await Task.Delay(50);
                ClassicAssert.IsFalse(second.IsCompleted, "two large messages were placed beside the log at once");
                ClassicAssert.AreEqual(subscribed, subscriber.Sent, "a large message overtook the one held back");

                subscriber.Full = false;
                subscriber.SignalCapacity();
                await second.AsTask().WaitAsync(Bound);
                AssertEnqueued(Publish(broker, "c", "last"));

                var expected = subscribed + Message("c", "small") + Message("c", large1) + Message("c", large2) + Message("c", "last");
                await WaitUntilAsync(() => subscriber.Sent.Length >= expected.Length, "the subscriber had every message");
                ClassicAssert.AreEqual(expected, subscriber.Sent);
            }
            finally
            {
                subscriber.ReleaseWaitingSends();
                subscriberSession.Dispose();
            }
        }

        /// <summary>
        /// Disposing the broker stops its consumer while it waits for a full subscriber, and releases a producer
        /// waiting for room in its log.
        /// </summary>
        [Test]
        public async Task DisposingTheBrokerReleasesItsWaits()
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 1 << 16) { WaitingSendsBlock = true };
            var subscriberSession = NewSession(subscriber);
            await ConsumeAsync(subscriberSession, Frame("SUBSCRIBE", "c"));
            subscriber.Full = true;

            var broker = Broker();
            var message = new string('x', 100);
            AssertEnqueued(Publish(broker, "c", message));
            await WaitUntilAsync(() => subscriber.Waiting == 1, "the broker waited for the full subscriber");
            ValueTask parked = default;
            for (var i = 0; i < Many && parked.IsCompleted; i++)
                parked = Publish(broker, "c", message);
            ClassicAssert.IsFalse(parked.IsCompleted, "the producer did not wait for room in the log");

            subscriberSession.Dispose();
            var disposing = server;
            server = null;
            await Task.Run(disposing.Dispose).WaitAsync(Bound);
            await parked.AsTask().WaitAsync(Bound);
            ClassicAssert.AreEqual(0, subscriber.Waiting, "the broker left its wait registered with the subscriber");
            ClassicAssert.AreEqual(0, subscriber.WaitingSends);
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

        /// <summary>
        /// A session delivers to its subscribers while it holds its own response object for its batch, so two
        /// sessions each subscribed to what the other publishes deliver to each other at the same time. Neither
        /// waits on a thread for the other's response object: a message that finds it taken is left for its holder
        /// to write out, in order.
        /// </summary>
        [Test]
        public async Task TwoSessionsPublishingToEachOtherAtOnceBothFinish()
        {
            const int Messages = 20_000;

            StartServer();
            var senders = new[] { new ControlledSender(bufferSize: 4096), new ControlledSender(bufferSize: 4096) };
            var sessions = new[] { NewSession(senders[0]), NewSession(senders[1]) };
            try
            {
                await ConsumeAsync(sessions[0], Frame("HELLO", "3"), Frame("SUBSCRIBE", "a"));
                await ConsumeAsync(sessions[1], Frame("HELLO", "3"), Frame("SUBSCRIBE", "b"));

                var batches = new byte[2][];
                for (var s = 0; s < 2; s++)
                {
                    var frames = new string[Messages];
                    for (var i = 0; i < Messages; i++)
                        frames[i] = Frame("PUBLISH", s == 0 ? "b" : "a", i.ToString(CultureInfo.InvariantCulture));
                    batches[s] = Pinned(frames);
                }

                using var start = new Barrier(2);
                var publishing = new Task<int>[2];
                for (var s = 0; s < 2; s++)
                {
                    var session = sessions[s];
                    var batch = batches[s];
                    publishing[s] = Task.Factory.StartNew(() =>
                    {
                        start.SignalAndWait();
                        return Consume(session, batch).AsTask().GetAwaiter().GetResult();
                    }, TaskCreationOptions.LongRunning);
                }

                var consumed = await Task.WhenAll(publishing).WaitAsync(Bound);
                ClassicAssert.AreEqual(batches[0].Length, consumed[0]);
                ClassicAssert.AreEqual(batches[1].Length, consumed[1]);

                for (var s = 0; s < 2; s++)
                {
                    var sent = senders[s].Sent;
                    var channel = s == 0 ? "a" : "b";
                    var at = 0;
                    for (var i = 0; i < Messages; i++)
                    {
                        at = sent.IndexOf(Message(channel, i.ToString(CultureInfo.InvariantCulture), resp3: true), at, StringComparison.Ordinal);
                        ClassicAssert.GreaterOrEqual(at, 0, $"Subscriber {s} did not receive message {i} in order.");
                    }
                }
            }
            finally
            {
                sessions[0].Dispose();
                sessions[1].Dispose();
            }
        }

        /// <summary>
        /// A message left as the subscriber's response object is let go, after the publisher found it held and before
        /// the message was in place for the holder to write, is written by the publisher that left it.
        /// </summary>
        [Test]
        public async Task AMessageLeftAsTheResponseObjectIsLetGoIsWrittenByItsPublisher()
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 4096);
            var publisher = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var publisherSession = NewSession(publisher);
            try
            {
                await ConsumeAsync(subscriberSession, Frame("SUBSCRIBE", "c"));
                var subscribed = subscriber.Sent;

                // The publisher finds the response object held until its message is in place, and free from then on
                subscriber.FailTryEnterUntil = () => SenderWaiters(subscriberSession) != 0;

                var publish = Pinned(Frame("PUBLISH", "c", "hello"));
                var consume = Task.Run(() => ConsumeSync(publisherSession, publish));
                ClassicAssert.IsTrue(consume.Wait(Bound), "the publisher did not finish");
                ClassicAssert.AreEqual(publish.Length, consume.Result);
                ClassicAssert.IsFalse(subscriber.FailingTryEnter, "the publisher did not leave its message");

                ClassicAssert.AreEqual(subscribed + Message("c", "hello"), subscriber.Sent);
                ClassicAssert.AreEqual(":1\r\n", publisher.Sent);
                ClassicAssert.AreEqual(0, SenderWaiters(subscriberSession));
                ClassicAssert.IsFalse(subscriber.Entered);
            }
            finally
            {
                subscriber.FailTryEnterUntil = null;
                subscriberSession.Dispose();
                publisherSession.Dispose();
            }
        }

        /// <summary>
        /// A publisher that takes the subscriber's response object writes the messages left there before its own, so
        /// a publisher's messages arrive in the order it published them even when it takes the response object between
        /// its holder letting go and the holder writing them out.
        /// </summary>
        [Test]
        public async Task APublisherWritesTheMessagesLeftBeforeItsOwn()
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 4096);
            var first = new ControlledSender(bufferSize: 4096);
            var second = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var firstSession = NewSession(first);
            var secondSession = NewSession(second);
            try
            {
                await ConsumeAsync(subscriberSession, Frame("SUBSCRIBE", "c"));
                var subscribed = subscriber.Sent;

                var publishTwo = Pinned(Frame("PUBLISH", "c", "two"));
                var publishThree = Pinned(Frame("PUBLISH", "c", "three"));
                var left = false;
                var tookOver = false;
                subscriber.OnNextSend = () =>
                {
                    // The first publisher holds the response object, so the second leaves its message
                    var consume = Task.Run(() => ConsumeSync(secondSession, publishTwo));
                    left = consume.Wait(Bound) && consume.Result == publishTwo.Length;

                    subscriber.OnNextExit = () =>
                    {
                        // The first publisher has let go and not yet written what was left; the second takes over
                        var again = Task.Run(() => ConsumeSync(secondSession, publishThree));
                        tookOver = again.Wait(Bound) && again.Result == publishThree.Length;
                    };
                };

                var publishOne = Pinned(Frame("PUBLISH", "c", "one"));
                var consume = await PublishAsync(firstSession, publishOne);
                ClassicAssert.IsTrue(consume.IsCompleted, "the first publisher parked");
                ClassicAssert.AreEqual(publishOne.Length, await consume);
                ClassicAssert.IsTrue(left, "the second publisher did not leave its message");
                ClassicAssert.IsTrue(tookOver, "the second publisher did not publish again");

                ClassicAssert.AreEqual(subscribed + Message("c", "one") + Message("c", "two") + Message("c", "three"), subscriber.Sent);
                ClassicAssert.AreEqual(":1\r\n", first.Sent);
                ClassicAssert.AreEqual(":1\r\n:1\r\n", second.Sent);
                ClassicAssert.AreEqual(0, SenderWaiters(subscriberSession));
                ClassicAssert.IsFalse(subscriber.Entered);
            }
            finally
            {
                subscriberSession.Dispose();
                firstSession.Dispose();
                secondSession.Dispose();
            }
        }

        /// <summary>
        /// A message that finds the subscriber's response object held by another publisher is left for that
        /// publisher, which writes it out as it lets go, so it does not wait for whatever the subscriber does next.
        /// </summary>
        [Test]
        public async Task AMessageLeftWhileAnotherPublisherWritesIsWrittenByThatPublisher()
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 4096);
            var first = new ControlledSender(bufferSize: 4096);
            var second = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var firstSession = NewSession(first);
            var secondSession = NewSession(second);
            try
            {
                await ConsumeAsync(subscriberSession, Frame("SUBSCRIBE", "c"));
                var subscribed = subscriber.Sent;

                var publishTwo = Pinned(Frame("PUBLISH", "c", "two"));
                var leftWhileHeld = false;
                subscriber.OnNextSend = () =>
                {
                    // The first publisher is inside its send to the subscriber, holding the response object
                    var consume = Task.Run(() => ConsumeSync(secondSession, publishTwo));
                    leftWhileHeld = consume.Wait(Bound) && consume.Result == publishTwo.Length
                        && subscriber.Sent == subscribed + Message("c", "one");
                };

                var publishOne = Pinned(Frame("PUBLISH", "c", "one"));
                var consume = await PublishAsync(firstSession, publishOne);
                ClassicAssert.IsTrue(consume.IsCompleted, "the first publisher parked");
                ClassicAssert.AreEqual(publishOne.Length, await consume);
                ClassicAssert.IsTrue(leftWhileHeld, "the second publisher did not leave its message while the first held the response object");

                ClassicAssert.AreEqual(subscribed + Message("c", "one") + Message("c", "two"), subscriber.Sent);
                ClassicAssert.AreEqual(":1\r\n", first.Sent);
                ClassicAssert.AreEqual(":1\r\n", second.Sent);
                ClassicAssert.IsFalse(subscriber.Entered);
            }
            finally
            {
                subscriberSession.Dispose();
                firstSession.Dispose();
                secondSession.Dispose();
            }
        }

        /// <summary>
        /// A subscriber suspended with an operation in flight holds its response object until the operation
        /// completes. A publisher that can park parks for it, and delivers once it resumes, rather than wait for it
        /// on a thread.
        /// </summary>
        [TestCase(false)]
        [TestCase(true)]
        public async Task APublisherParksForASubscriberSuspendedHoldingItsResponseObject(bool pattern)
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 4096);
            var publisher = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var publisherSession = NewSession(publisher);
            try
            {
                var (subscribed, park) = await SubscribeAndSuspendAsync(subscriberSession, subscriber, pattern);

                var publish = Pinned(Frame("PUBLISH", "c", "hello"));
                var consume = await PublishAsync(publisherSession, publish);
                ClassicAssert.IsFalse(consume.IsCompleted, "the publisher did not park for the subscriber");
                ClassicAssert.IsTrue(subscriber.Entered, "the subscriber resumed before the publisher parked");
                ClassicAssert.AreEqual(1, subscriber.Waiting);
                ClassicAssert.AreEqual(subscribed, subscriber.Sent);
                ClassicAssert.AreEqual("", publisher.Sent);

                _ = await park.AsTask().WaitAsync(Bound);
                ClassicAssert.AreEqual(publish.Length, await consume.AsTask().WaitAsync(Bound));
                ClassicAssert.AreEqual(subscribed + Ok + Delivered(pattern, "hello"), subscriber.Sent);
                ClassicAssert.AreEqual(":1\r\n", publisher.Sent);
                ClassicAssert.AreEqual(0, subscriber.Waiting);
                ClassicAssert.AreEqual(0, DeferredCount(publisherSession));
            }
            finally
            {
                subscriberSession.Dispose();
                publisherSession.Dispose();
            }
        }

        /// <summary>
        /// A publisher inside a transaction or a script may not park, and does not wait on a thread for a subscriber
        /// suspended holding its response object either: it leaves its messages for the subscriber, which writes
        /// them out, in order, with the replies of the batch it resumes.
        /// </summary>
        [TestCase(false)]
        [TestCase(true)]
        public async Task APublisherThatCannotParkLeavesItsMessagesForASuspendedSubscriber(bool script)
        {
            StartServer(lua: script);
            var subscriber = new ControlledSender(bufferSize: 4096);
            var publisher = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var publisherSession = NewSession(publisher);
            try
            {
                var (subscribed, park) = await SubscribeAndSuspendAsync(subscriberSession, subscriber, pattern: false);

                var input = script
                    ? Pinned(Frame("EVAL", "redis.call('PUBLISH', 'c', 'one') return redis.call('PUBLISH', 'c', 'two')", "0"))
                    : Pinned(Frame("MULTI"), Frame("PUBLISH", "c", "one"), Frame("PUBLISH", "c", "two"), Frame("EXEC"));
                var consume = await PublishAsync(publisherSession, input);
                ClassicAssert.IsTrue(consume.IsCompleted, "the publisher parked");
                ClassicAssert.AreEqual(input.Length, await consume);
                ClassicAssert.AreEqual(subscribed, subscriber.Sent, "the publisher waited for the subscriber to resume");
                ClassicAssert.AreEqual(0, subscriber.Registrations);
                ClassicAssert.AreEqual(script ? ":1\r\n" : "+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:1\r\n:1\r\n", publisher.Sent);

                _ = await park.AsTask().WaitAsync(Bound);
                ClassicAssert.AreEqual(subscribed + Ok + Message("c", "one", resp3: true) + Message("c", "two", resp3: true), subscriber.Sent);
            }
            finally
            {
                subscriberSession.Dispose();
                publisherSession.Dispose();
            }
        }

        /// <summary>
        /// A subscriber torn down while suspended holding its response object never resumes, so the teardown is what
        /// releases a publisher parked for it.
        /// </summary>
        [Test]
        public async Task APublisherParkedForASuspendedSubscriberIsReleasedWhenTheSubscriberIsTornDown()
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 4096);
            var publisher = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            var publisherSession = NewSession(publisher);
            var subscriberTornDown = false;
            try
            {
                var (_, park) = await SubscribeAndSuspendAsync(subscriberSession, subscriber, pattern: false);

                var publish = Pinned(Frame("PUBLISH", "c", "hello"));
                var consume = await PublishAsync(publisherSession, publish);
                ClassicAssert.IsFalse(consume.IsCompleted, "the publisher did not park for the subscriber");
                ClassicAssert.AreEqual(1, subscriber.Waiting);

                subscriberTornDown = true;
                subscriberSession.Dispose();

                ClassicAssert.AreEqual(publish.Length, await consume.AsTask().WaitAsync(Bound));
                ClassicAssert.AreEqual(":1\r\n", publisher.Sent);
                ClassicAssert.AreEqual(0, subscriber.Waiting);
                _ = Assert.ThrowsAsync<Garnet.common.SessionTornDownException>(async () => await park.AsTask().WaitAsync(Bound));
            }
            finally
            {
                if (!subscriberTornDown)
                    subscriberSession.Dispose();
                publisherSession.Dispose();
            }
        }

        /// <summary>
        /// The broker waits for a subscriber suspended holding its response object as it waits for one whose
        /// connection is full: once, without a thread, and without moving on to the next message.
        /// </summary>
        [Test]
        public async Task TheBrokerWaitsForASuspendedSubscriberWithoutAThreadAndDeliversInOrder()
        {
            StartServer();
            var subscriber = new ControlledSender(bufferSize: 4096);
            var subscriberSession = NewSession(subscriber);
            try
            {
                var (subscribed, park) = await SubscribeAndSuspendAsync(subscriberSession, subscriber, pattern: false);

                var broker = Broker();
                AssertEnqueued(Publish(broker, "c", "one"));
                AssertEnqueued(Publish(broker, "c", "two"));

                await WaitUntilAsync(() => subscriber.Waiting == 1, "the broker waited for the suspended subscriber");
                ClassicAssert.AreEqual(1, subscriber.Registrations, "the broker tried the subscriber again without waiting");
                ClassicAssert.IsTrue(subscriber.Entered, "the subscriber resumed before the broker waited");
                ClassicAssert.AreEqual(subscribed, subscriber.Sent);

                _ = await park.AsTask().WaitAsync(Bound);
                var delivered = subscribed + Ok + Message("c", "one", resp3: true) + Message("c", "two", resp3: true);
                await WaitUntilAsync(() => subscriber.Sent == delivered, "the subscriber had both messages, in order");
                ClassicAssert.AreEqual(0, subscriber.Waiting);
            }
            finally
            {
                subscriberSession.Dispose();
            }
        }

        /// <summary>
        /// Subscribes <paramref name="session"/> over RESP3, which lets it go on running commands, and then suspends
        /// it the way a storage operation that went to the device does: holding its response object, for
        /// <see cref="SuspendSeconds"/>.
        /// </summary>
        /// <returns>What the subscriber was sent before it suspended, and the call that suspended.</returns>
        static async Task<(string Subscribed, ValueTask<int> Park)> SubscribeAndSuspendAsync(RespServerSession session, ControlledSender sender, bool pattern)
        {
            await ConsumeAsync(session, Frame("HELLO", "3"), pattern ? Frame("PSUBSCRIBE", "c*") : Frame("SUBSCRIBE", "c"));
            var subscribed = sender.Sent;

            var park = await PublishAsync(session, Pinned(Frame("DEBUG", "BLOCKIO", SuspendSeconds.ToString("0.0", CultureInfo.InvariantCulture))));
            ClassicAssert.IsFalse(park.IsCompleted, "the subscriber did not suspend");
            ClassicAssert.IsTrue(sender.Entered, "the subscriber let go of its response object while suspended");
            return (subscribed, park);
        }

        static string Delivered(bool pattern, string payload)
            => pattern ? PatternMessage("c*", "c", payload, resp3: true) : Message("c", payload, resp3: true);

        void StartServer(bool cluster = false, bool lua = false)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: cluster, enableLua: lua);
            server.Start();
        }

        SubscribeBroker Broker() => server.Provider.StoreWrapper.subscribeBroker;

        static void AssertEnqueued(ValueTask enqueue)
            => ClassicAssert.IsTrue(enqueue.IsCompletedSuccessfully, "the producer waited for room in the broker's log");

        /// <summary>Publishes through the broker's log, as a message forwarded from another cluster node is.</summary>
        static ValueTask Publish(SubscribeBroker broker, string channel, string message)
        {
            var key = Pinned(channel);
            var value = Pinned(message);
            var enqueue = broker.PublishAsync(PinnedSpanByte.FromPinnedSpan(key), PinnedSpanByte.FromPinnedSpan(value));
            GC.KeepAlive(key);
            GC.KeepAlive(value);
            return enqueue;
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