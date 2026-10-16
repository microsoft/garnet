// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// A session whose client stops reading must neither buffer the rest of its pipeline nor hold a thread, and
    /// must lose nothing when the client reads again.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Replies are handed to the socket without waiting for it. Once <c>NetworkSendThrottleMax</c> sends are
    /// outstanding the session stops consuming its input and parks until one completes, so what the server holds
    /// for a client is bounded by the throttle rather than by the length of the client's pipeline.
    /// </para>
    /// <para>
    /// Each test has a client send a pipeline whose replies are several times what the kernel will absorb for
    /// a connection, and then not read. Two things are observed from a separate connection while the clients
    /// are stopped: whether it is still served by a pool that has fewer threads than there are stopped
    /// sessions, which it cannot be if a session holds a thread, and how many bytes the server holds for them
    /// (<c>liveBytes</c> in <c>INFO BPSTATS</c>).
    /// </para>
    /// <para>
    /// A subscriber that stops reading is the same case for the publishers that deliver to it: once its
    /// connection is backed up a publisher waits for room, rather than holding a thread, and delivers
    /// its message when there is room.
    /// </para>
    /// </remarks>
    [TestFixture(false)]
    [TestFixture(true)]
    public class RespSendBackpressureTests : TestBase
    {
        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";
        const string NilBulk = "$-1\r\n";

        /// <summary>Large enough that a handful of replies fill what the kernel buffers for a connection.</summary>
        const int ValueLength = 256 * 1024;

        const int ValueCount = 8;

        /// <summary>
        /// Fixed rather than autotuned, so the window a stopped client advertises is small and the same on every
        /// platform.
        /// </summary>
        const int ClientReceiveBufferSize = 256 * 1024;

        /// <summary>A throttle this high is never reached, so a session never sees its sends saturate.</summary>
        const int UnboundedThrottle = 1 << 20;

        const string Channel = "chan";

        /// <summary>Size of each message published to a subscriber that does not read; a few of them fill what the kernel buffers for a connection.</summary>
        const int PushLength = 1 << 20;

        /// <summary>More than the pool is capped to, so publishers that each held a thread would exhaust it.</summary>
        const int PublisherCount = 12;

        /// <summary>Pipelined, so that a publisher continuing with its next message after waiting is observed too.</summary>
        const int PublishesPerPublisher = 2;

        /// <summary>The publisher that backs the subscriber up, numbered after the others.</summary>
        const int FillerId = PublisherCount;

        /// <summary>
        /// Many times what the kernel buffers for a connection and the throttle allows to be outstanding, so that
        /// a server that kept accepting messages for a subscriber that is not reading is seen to.
        /// </summary>
        const int MaxBackUpMessages = 64;

        /// <summary>What a message starts with, "07:01:" for the second message of the eighth publisher.</summary>
        const int PushPrefixLength = 6;

        /// <summary>
        /// What one backed-up session may hold: the sends the throttle allows to be outstanding, the reply being
        /// written when the last of them was issued, and the session's own buffers, with room to spare. A
        /// session that kept consuming would hold the tens of megabytes its pipeline asks for.
        /// </summary>
        const long BufferedLimitPerSession = 4L << 20;

        /// <summary>Bounds every wait on the server, so a regression fails the test instead of hanging the run.</summary>
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

        /// <summary>How long a message goes unanswered before it is taken to be waiting for room on the subscriber's connection.</summary>
        static readonly TimeSpan BackedUpAfter = TimeSpan.FromSeconds(2);

        static readonly byte[][] Values = CreateValues();
        static readonly byte[] BulkHeader = Encoding.ASCII.GetBytes($"${ValueLength}\r\n");
        static readonly byte[] CrLf = "\r\n"u8.ToArray();
        static readonly string PushHeader = $"*3\r\n$7\r\nmessage\r\n${Channel.Length}\r\n{Channel}\r\n${PushLength}\r\n";
        static readonly string PushBody = CreatePushBody();

        readonly bool useTls;
        GarnetServer server;

        public RespSendBackpressureTests(bool useTls) => this.useTls = useTls;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// A client that sends a pipeline and does not read stops the session consuming it once the socket is
        /// backed up, and gets every reply, in order, when it starts reading.
        /// </summary>
        [Test]
        public void AClientThatDoesNotReadStopsTheSessionConsumingItsPipeline()
        {
            const int Pairs = 128;

            StartServer();

            using var pinger = new RawRespSession(useTls);
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));
            var baseline = LiveBytes(pinger);

            using var client = new RawRespSession(useTls, receiveBufferSize: ClientReceiveBufferSize) { ReceiveTimeout = Bound };

            // Every GET is followed by an INCR, so the counter shows how far the server got
            var commands = new List<string[]>();
            for (var j = 0; j < Pairs; j++)
            {
                commands.Add(["GET", BigKey(j)]);
                commands.Add(["INCR", "n"]);
            }

            client.SendPipeline([.. commands]);

            var consumed = AwaitStable(() => Counter(pinger, "n"), above: 0);
            var buffered = LiveBytes(pinger) - baseline;

            ClassicAssert.Less(buffered, BufferedLimitPerSession,
                $"The server holds {buffered >> 10} KB for one client that is not reading, after consuming " +
                $"{consumed} of its {Pairs} GETs.");

            if (consumed == Pairs)
                Assert.Ignore("The kernel absorbed the whole pipeline, so the session was never backed up.");
            ClassicAssert.Greater(consumed, 0, "The session consumed nothing, so it was not the socket that stopped it.");

            for (var j = 0; j < Pairs; j++)
            {
                ExpectGetReply(client, j, $"GET #{j}");
                client.ExpectBytes(Encoding.ASCII.GetBytes($":{j + 1}\r\n"), $"INCR #{j}");
            }

            ClassicAssert.AreEqual(Pong, Ask(client, "PING"), "The session did not recover after being parked.");
            ClassicAssert.AreEqual(Pairs, Counter(pinger, "n"));
        }

        /// <summary>
        /// More sessions are backed up on clients that are not reading than the pool has threads. None of them
        /// holds a thread, so another connection is still served, none buffers its pipeline, and each gets every
        /// reply when its client reads.
        /// </summary>
        /// <remarks>
        /// Scatter-gather is on, so a run of GETs is consumed by one command. That command is the path that
        /// could otherwise write a whole run's replies without ever returning to the point where the session
        /// parks.
        /// </remarks>
        [Test]
        public void MoreBackedUpSessionsThanThePoolHasThreadsStarveNoOneAndLoseNothing()
        {
            const int SessionCount = 12;
            const int CappedWorkers = 4;
            const int GetsPerSession = 128;

            StartServer(getSG: true);

            using var pinger = new RawRespSession(useTls);
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));
            var baseline = LiveBytes(pinger);

            using var cap = ThreadPoolCap.Apply(CappedWorkers);

            var sessions = new RawRespSession[SessionCount];
            try
            {
                for (var i = 0; i < SessionCount; i++)
                {
                    sessions[i] = new RawRespSession(useTls, receiveBufferSize: ClientReceiveBufferSize) { ReceiveTimeout = Bound };

                    // The INCR is last, so it runs only if the session consumes its whole pipeline
                    var commands = new List<string[]>();
                    for (var j = 0; j < GetsPerSession; j++)
                        commands.Add(["GET", BigKey(i + j)]);
                    commands.Add(["INCR", $"parked:{i}"]);
                    sessions[i].SendPipeline([.. commands]);
                }

                var buffered = AwaitStable(() => LiveBytes(pinger), above: baseline) - baseline;

                ExpectServed(pinger, rounds: 50, $"{SessionCount} sessions are backed up and the pool has {CappedWorkers} threads");

                ClassicAssert.Less(buffered, SessionCount * BufferedLimitPerSession,
                    $"The server holds {buffered >> 20} MB for {SessionCount} clients that are not reading.");

                var absorbed = 0;
                for (var i = 0; i < SessionCount; i++)
                    absorbed += Counter(pinger, $"parked:{i}") > 0 ? 1 : 0;
                if (absorbed > 0)
                    Assert.Ignore($"The kernel absorbed the whole pipeline of {absorbed} sessions, so they were never backed up.");

                for (var i = 0; i < SessionCount; i++)
                {
                    for (var j = 0; j < GetsPerSession; j++)
                        ExpectGetReply(sessions[i], i + j, $"session {i} GET #{j}");
                    sessions[i].ExpectBytes(":1\r\n"u8, $"session {i} INCR");
                }

                for (var i = 0; i < SessionCount; i++)
                    ClassicAssert.AreEqual(1, Counter(pinger, $"parked:{i}"), $"session {i} did not finish its pipeline.");
            }
            finally
            {
                foreach (var session in sessions)
                    session?.Dispose();
            }
        }

        /// <summary>
        /// A client that sends <c>QUIT</c> behind a large pipeline and does not read leaves its replies in the
        /// server's socket. Closing waits for them without holding a thread, and every reply still arrives,
        /// followed by the end of the stream.
        /// </summary>
        [Test]
        public void QuittingWithoutReadingHoldsNoThread()
        {
            const int SessionCount = 8;
            const int CappedWorkers = 4;
            const int GetsPerSession = 40;

            // The session is never parked for backpressure, so it reaches QUIT with its replies still unsent
            StartServer(throttleMax: UnboundedThrottle);

            using var pinger = new RawRespSession(useTls);
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));
            var baseline = LiveBytes(pinger);

            using var cap = ThreadPoolCap.Apply(CappedWorkers);

            var sessions = new RawRespSession[SessionCount];
            try
            {
                for (var i = 0; i < SessionCount; i++)
                {
                    sessions[i] = new RawRespSession(useTls, receiveBufferSize: ClientReceiveBufferSize) { ReceiveTimeout = Bound };

                    var commands = new List<string[]>();
                    for (var j = 0; j < GetsPerSession; j++)
                        commands.Add(["GET", BigKey(i + j)]);
                    commands.Add(["QUIT"]);
                    sessions[i].SendPipeline([.. commands]);
                }

                var buffered = AwaitStable(() => LiveBytes(pinger), above: baseline) - baseline;

                ExpectServed(pinger, rounds: 50, $"{SessionCount} sessions are closing with replies unsent and the pool has {CappedWorkers} threads");

                if (buffered < SessionCount << 20)
                    Assert.Ignore("The kernel absorbed the replies, so there was nothing for a close to wait on.");

                for (var i = 0; i < SessionCount; i++)
                {
                    for (var j = 0; j < GetsPerSession; j++)
                        ExpectGetReply(sessions[i], i + j, $"session {i} GET #{j}");
                    sessions[i].ExpectBytes(Encoding.ASCII.GetBytes(Ok), $"session {i} QUIT");
                    sessions[i].ExpectEndOfStream($"session {i}");
                }

                // Everything the closed sessions held has been returned
                var remaining = AwaitStable(() => LiveBytes(pinger), above: 0) - baseline;
                ClassicAssert.LessOrEqual(remaining, 2 << 20, $"{remaining >> 10} KB is still held after every session closed.");
            }
            finally
            {
                foreach (var session in sessions)
                    session?.Dispose();
            }
        }

        /// <summary>
        /// Input that arrives after <c>QUIT</c>, while the close is still waiting for the socket to take the
        /// replies, is discarded rather than run.
        /// </summary>
        [Test]
        public void InputAfterQuitIsDiscardedWhileTheCloseWaits()
        {
            const int GetsBeforeQuit = 64;

            StartServer(throttleMax: UnboundedThrottle);

            using var pinger = new RawRespSession(useTls);
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));
            var baseline = LiveBytes(pinger);

            using var client = new RawRespSession(useTls, receiveBufferSize: ClientReceiveBufferSize) { ReceiveTimeout = Bound };

            var commands = new List<string[]>();
            for (var j = 0; j < GetsBeforeQuit; j++)
                commands.Add(["GET", BigKey(j)]);
            commands.Add(["SET", "before-quit", "1"]);
            commands.Add(["QUIT"]);
            client.SendPipeline([.. commands]);

            // The SET precedes the QUIT in the same batch, so once it is visible the QUIT has been run
            var clock = Stopwatch.StartNew();
            while (Ask(pinger, "GET", "before-quit") == NilBulk)
            {
                ClassicAssert.Less(clock.Elapsed, Bound, "The pipeline was not consumed.");
                Thread.Sleep(20);
            }

            var buffered = AwaitStable(() => LiveBytes(pinger), above: baseline) - baseline;
            if (buffered < 1 << 20)
                Assert.Ignore("The kernel absorbed the replies, so the connection was already closed.");

            client.Send("SET", "after-quit", "1");

            // Receives on a connection are serialized, so the SET above can only have been handled after the batch
            // that ran QUIT. There is nothing to wait on for something that must not happen, so give it time to.
            Thread.Sleep(500);
            ClassicAssert.AreEqual(NilBulk, Ask(pinger, "GET", "after-quit"), "A command sent after QUIT ran.");

            for (var j = 0; j < GetsBeforeQuit; j++)
                ExpectGetReply(client, j, $"GET #{j}");
            client.ExpectBytes(Encoding.ASCII.GetBytes(Ok), "SET before QUIT");
            client.ExpectBytes(Encoding.ASCII.GetBytes(Ok), "QUIT");
            client.ExpectEndOfStream("the stream after QUIT");

            ClassicAssert.AreEqual(NilBulk, Ask(pinger, "GET", "after-quit"), "A command sent after QUIT ran.");
        }

        /// <summary>
        /// Publishers that outnumber the pool's threads, publishing to a subscriber that has stopped reading, wait
        /// without holding a thread, so a connection that has nothing to do with them is still served. When the
        /// subscriber reads again every publisher completes, and it receives every message intact, in the order
        /// its publisher made them.
        /// </summary>
        [Test]
        public void MorePublishersWaitingOnAStoppedSubscriberThanThePoolHasThreadsStarveNoOneAndLoseNothing()
        {
            const int CappedWorkers = 4;

            StartServer();

            using var pinger = new RawRespSession(useTls);
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));
            var baseline = LiveBytes(pinger);

            using var cap = ThreadPoolCap.Apply(CappedWorkers);

            using var subscriber = Subscribe();
            using var filler = BackUp(out var fillerMessages);
            using var publishers = new Publishers(useTls);
            publishers.Start();

            // The server reads each publisher up to the first message that has to wait, and then stops
            _ = AwaitStable(() => LiveBytes(pinger), above: baseline);

            ExpectServed(pinger, rounds: 50, $"{PublisherCount + 1} publishers are waiting on a subscriber that is not reading and the pool has {CappedWorkers} threads");

            ExpectEveryPush(subscriber, fillerMessages);
            filler.ExpectBytes(":1\r\n"u8, "the reply to the last message of the publisher that backed the subscriber up");
            publishers.ReadReplies();
            publishers.ExpectReplies(":1\r\n");
            publishers.AwaitSent();
        }

        /// <summary>
        /// Publishers waiting on a subscriber whose connection is lost are released and answered, rather than
        /// left waiting for room on a connection that is gone.
        /// </summary>
        [Test]
        public void PublishersWaitingOnASubscriberThatDisconnectsAreReleased()
        {
            StartServer();

            using var pinger = new RawRespSession(useTls);
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));
            var baseline = LiveBytes(pinger);

            using var subscriber = Subscribe();
            using var filler = BackUp(out _);
            using var publishers = new Publishers(useTls);
            publishers.Start();

            _ = AwaitStable(() => LiveBytes(pinger), above: baseline);

            subscriber.AbortAndDispose();

            // The message the filler was waiting to deliver counted the subscriber when it was made. A message
            // made by a publisher after that finds the subscriber still registered or not, depending on when the
            // server noticed that it was gone.
            filler.ExpectBytes(":1\r\n"u8, "the reply to the last message of the publisher that backed the subscriber up");
            publishers.ReadReplies();
            publishers.ExpectReplies(":0\r\n", ":1\r\n");
            publishers.AwaitSent();
        }

        /// <summary>Starts the server and loads the large values the tests read back.</summary>
        void StartServer(bool getSG = false, int? throttleMax = null)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTls, getSG: getSG, networkSendThrottleMax: throttleMax);
            server.Start();

            // A value and the command that sets it have to fit in the session's buffers
            using var session = new RawRespSession(useTls, bufferSize: 1 << 20);
            for (var k = 0; k < ValueCount; k++)
                ClassicAssert.AreEqual(Ok, session.Execute("SET", BigKey(k), Encoding.ASCII.GetString(Values[k])));
        }

        static string BigKey(int i) => $"big{i % ValueCount}";

        /// <summary>
        /// Subscribes to the channel and then does not read. The session's buffers hold a whole push, which is
        /// read as one reply, and the receive window is fixed so that the connection backs up after a predictable
        /// amount of data.
        /// </summary>
        RawRespSession Subscribe()
        {
            var subscriber = new RawRespSession(useTls, bufferSize: PushLength + 1024, receiveBufferSize: ClientReceiveBufferSize) { ReceiveTimeout = Bound };
            subscriber.ExecuteExpect("*3\r\n$9\r\nsubscribe\r\n$4\r\nchan\r\n:1\r\n"u8, "SUBSCRIBE", Channel);
            return subscriber;
        }

        /// <summary>
        /// Publishes to the channel, one message at a time, until a message is not answered. That message is
        /// waiting for room on the subscriber's connection, which is backed up with the sends the throttle allows
        /// to be outstanding, so every message made after it has to wait too. Fails if the server never stops
        /// accepting messages for a subscriber that is not reading.
        /// </summary>
        /// <param name="sent">How many messages the publisher has made, the last of which it is waiting on.</param>
        RawRespSession BackUp(out int sent)
        {
            sent = 0;
            var filler = new RawRespSession(useTls, bufferSize: PushLength + 1024) { ReceiveTimeout = Bound };
            while (sent < MaxBackUpMessages)
            {
                filler.Send("PUBLISH", Channel, Payload(FillerId, sent++));
                if (!filler.WaitForReply(BackedUpAfter))
                    return filler;

                ClassicAssert.AreEqual(":1\r\n", filler.ReadReply(), $"The reply to message {sent - 1}.");
            }

            filler.Dispose();
            Assert.Fail($"The server accepted {MaxBackUpMessages} messages of {PushLength >> 10} KB for a subscriber that is not reading.");
            return null;
        }

        /// <summary>
        /// Has each publisher send its pipeline of messages from a thread of its own, and reads their replies. The
        /// server stops reading a publisher that is waiting, and a pipeline this large is more than the kernel
        /// takes, so the write can be made neither from the test's thread nor from one of the pool's, which a
        /// test may cap. Replies are only read once the publishers are expected to have them, because a TLS
        /// connection is readable before it has a reply, for the session tickets it has been sent.
        /// </summary>
        sealed class Publishers : IDisposable
        {
            readonly RawRespSession[] sessions = new RawRespSession[PublisherCount];
            readonly List<string>[] replies = new List<string>[PublisherCount];
            readonly Task[] sends = new Task[PublisherCount];

            internal Publishers(bool useTls)
            {
                for (var i = 0; i < PublisherCount; i++)
                {
                    sessions[i] = new RawRespSession(useTls, bufferSize: PublishesPerPublisher * (PushLength + 1024)) { ReceiveTimeout = Bound };
                    replies[i] = [];
                }
            }

            internal void Start()
            {
                for (var i = 0; i < PublisherCount; i++)
                {
                    var session = sessions[i];
                    var publisher = i;
                    sends[i] = Task.Factory.StartNew(() =>
                    {
                        var commands = new string[PublishesPerPublisher][];
                        for (var s = 0; s < PublishesPerPublisher; s++)
                            commands[s] = ["PUBLISH", Channel, Payload(publisher, s)];
                        session.SendPipeline(commands);
                    }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                }
            }

            /// <summary>Reads every publisher's replies, failing if a publisher is not answered in time.</summary>
            internal void ReadReplies()
            {
                for (var i = 0; i < PublisherCount; i++)
                {
                    while (replies[i].Count < PublishesPerPublisher)
                    {
                        try
                        {
                            replies[i].Add(sessions[i].ReadReply());
                        }
                        catch (IOException e)
                        {
                            Assert.Fail($"Publisher {i} was not answered for message {replies[i].Count} ({e.Message}).");
                        }
                    }
                }
            }

            internal void ExpectReplies(params string[] allowed)
            {
                for (var i = 0; i < PublisherCount; i++)
                {
                    for (var s = 0; s < PublishesPerPublisher; s++)
                        CollectionAssert.Contains(allowed, replies[i][s], $"publisher {i}'s reply to message {s}");
                }
            }

            internal void AwaitSent()
                => ClassicAssert.IsTrue(Task.WaitAll(sends, Bound), "A publisher did not finish sending.");

            public void Dispose()
            {
                foreach (var session in sessions)
                    session?.Dispose();

                // A send that was still writing fails with the connection, which is observed here
                foreach (var send in sends)
                {
                    try { _ = send?.Wait(Bound); } catch (AggregateException) { }
                }
            }
        }

        /// <summary>
        /// Reads every message made to the channel, checking that each is intact and that the messages of each
        /// publisher arrive in the order it made them.
        /// </summary>
        /// <param name="subscriber">The subscriber.</param>
        /// <param name="fillerMessages">How many messages the publisher that backed the subscriber up has made.</param>
        static void ExpectEveryPush(RawRespSession subscriber, int fillerMessages)
        {
            var expected = new int[PublisherCount + 1];
            Array.Fill(expected, PublishesPerPublisher);
            expected[FillerId] = fillerMessages;

            var next = new int[expected.Length];
            for (var n = 0; n < PublisherCount * PublishesPerPublisher + fillerMessages; n++)
            {
                var push = subscriber.ReadReply();
                if (!push.StartsWith(PushHeader, StringComparison.Ordinal))
                    Assert.Fail($"Push #{n} does not start as a message does: '{push[..Math.Min(push.Length, 48)]}'");

                var publisher = int.Parse(push.AsSpan(PushHeader.Length, 2), CultureInfo.InvariantCulture);
                var seq = int.Parse(push.AsSpan(PushHeader.Length + 3, 2), CultureInfo.InvariantCulture);
                if (publisher >= expected.Length || seq >= expected[publisher])
                    Assert.Fail($"Push #{n} claims to be message {seq} of publisher {publisher}, which does not exist.");

                ClassicAssert.AreEqual(next[publisher], seq, $"Publisher {publisher}'s messages did not arrive in the order it made them.");
                next[publisher]++;

                var frame = PushFrame(publisher, seq);
                var same = push.AsSpan().CommonPrefixLength(frame.AsSpan());
                if (same != frame.Length || push.Length != frame.Length)
                    Assert.Fail($"Push #{n}, message {seq} of publisher {publisher}, differs from what was published at char {same} of {frame.Length}.");
            }
        }

        /// <summary>
        /// The prefix names the publisher and the message, so that a push that is truncated, duplicated or
        /// delivered out of order differs from the expected one.
        /// </summary>
        static string Payload(int publisher, int seq) => $"{publisher:D2}:{seq:D2}:{PushBody}";

        static string PushFrame(int publisher, int seq) => $"{PushHeader}{Payload(publisher, seq)}\r\n";

        static string CreatePushBody()
        {
            var bytes = new byte[PushLength - PushPrefixLength];
            new Random(2000).NextBytes(bytes);
            for (var i = 0; i < bytes.Length; i++)
                bytes[i] = (byte)('a' + (bytes[i] % 26));
            return Encoding.ASCII.GetString(bytes);
        }

        /// <summary>
        /// Distinct pseudo-random letters per value, so that a reply that is truncated, duplicated, reordered or
        /// delivered from another key differs from the expected one at some byte.
        /// </summary>
        static byte[][] CreateValues()
        {
            var values = new byte[ValueCount][];
            for (var k = 0; k < ValueCount; k++)
            {
                var bytes = new byte[ValueLength];
                new Random(1000 + k).NextBytes(bytes);
                for (var i = 0; i < bytes.Length; i++)
                    bytes[i] = (byte)('a' + (bytes[i] % 26));
                values[k] = bytes;
            }

            return values;
        }

        static void ExpectGetReply(RawRespSession session, int index, string what)
        {
            session.ExpectBytes(BulkHeader, what);
            session.ExpectBytes(Values[index % ValueCount], what);
            session.ExpectBytes(CrLf, what);
        }

        /// <summary>Sends one command and returns its reply, failing if the server does not answer in time.</summary>
        static string Ask(RawRespSession session, params string[] args)
        {
            session.Send(args);
            if (!session.WaitForReply(Bound))
                Assert.Fail($"No reply to {args[0]} within {Bound.TotalSeconds:F0}s: the server is not serving this connection.");
            return session.ReadReply();
        }

        static void ExpectServed(RawRespSession session, int rounds, string context)
        {
            for (var i = 0; i < rounds; i++)
                ClassicAssert.AreEqual(Pong, Ask(session, "PING"), $"Round trip {i} while {context}.");
        }

        static long Counter(RawRespSession session, string key)
        {
            var reply = Ask(session, "GET", key);
            return reply == NilBulk ? 0 : long.Parse(reply.Split("\r\n")[1], CultureInfo.InvariantCulture);
        }

        /// <summary>Bytes the server has checked out of its network buffer pool.</summary>
        static long LiveBytes(RawRespSession session)
            => NetworkBufferBudgetTests.SocketStatBytes("liveBytes", Ask(session, "INFO", "BPSTATS"));

        /// <summary>
        /// Samples until the value is above <paramref name="above"/> and has stopped changing, so a value read
        /// while the server is still working is not taken for where it stopped.
        /// </summary>
        static long AwaitStable(Func<long> sample, long above)
        {
            const int StableSamples = 5;

            var last = long.MinValue;
            var repeats = 0;
            var clock = Stopwatch.StartNew();
            while (true)
            {
                var value = sample();
                repeats = value == last ? repeats + 1 : 0;
                last = value;

                if ((value > above && repeats >= StableSamples) || clock.Elapsed > Bound)
                    return value;

                Thread.Sleep(100);
            }
        }

        /// <summary>
        /// Caps the worker pool for the life of the scope. Uncapped, a session that held a thread would go
        /// unnoticed on a large host: the pool injects a thread for each one that is held.
        /// </summary>
        sealed class ThreadPoolCap : IDisposable
        {
            readonly int minWorkers, minIo, maxWorkers, maxIo;

            ThreadPoolCap(int workers)
            {
                ThreadPool.GetMinThreads(out minWorkers, out minIo);
                ThreadPool.GetMaxThreads(out maxWorkers, out maxIo);

                if (!ThreadPool.SetMinThreads(Math.Min(minWorkers, workers), minIo) ||
                    !ThreadPool.SetMaxThreads(workers, maxIo))
                {
                    Dispose();
                    Assert.Ignore("Could not cap the thread pool, so a held thread cannot be made to matter.");
                }
            }

            internal static ThreadPoolCap Apply(int workers) => new(workers);

            public void Dispose()
            {
                _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                _ = ThreadPool.SetMinThreads(minWorkers, minIo);
            }
        }
    }
}
