// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Threading;
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

        /// <summary>
        /// What one backed-up session may hold: the sends the throttle allows to be outstanding, the reply being
        /// written when the last of them was issued, and the session's own buffers, with room to spare. A
        /// session that kept consuming would hold the tens of megabytes its pipeline asks for.
        /// </summary>
        const long BufferedLimitPerSession = 4L << 20;

        /// <summary>Bounds every wait on the server, so a regression fails the test instead of hanging the run.</summary>
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

        static readonly byte[][] Values = CreateValues();
        static readonly byte[] BulkHeader = Encoding.ASCII.GetBytes($"${ValueLength}\r\n");
        static readonly byte[] CrLf = "\r\n"u8.ToArray();

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
