// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Exercises the asynchronous command suspension path, in which a RESP command awaits an incomplete
    /// operation, the network receive stack unwinds, and the session resumes where it left off once the
    /// operation completes. <c>DEBUG BLOCK</c> is the probe: its body is an ordinary <c>async</c> method.
    /// </summary>
    [TestFixture(false)]
    [TestFixture(true)]
    public class RespAsyncSuspensionTests : TestBase
    {
        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";
        const string Queued = "+QUEUED\r\n";

        /// <summary>
        /// Whether the server under test terminates TLS. The two transports take entirely separate paths
        /// through the receive stack -- a plain socket loop versus the <c>SslStream</c> reader -- and each has
        /// to unwind and resume correctly, so every test here runs against both.
        /// </summary>
        readonly bool useTls;

        GarnetServer server;

        public RespAsyncSuspensionTests(bool useTls) => this.useTls = useTls;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTls);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        static int CurrentThreadCount()
        {
            using var p = Process.GetCurrentProcess();
            p.Refresh();
            return p.Threads.Count;
        }

        [Test]
        public void DebugBlockRepliesAfterTheDelayElapses()
        {
            using var client = new RawRespSession(useTls);

            var sw = Stopwatch.StartNew();
            ClassicAssert.AreEqual(Ok, client.Execute("DEBUG", "BLOCK", "0.4"));
            sw.Stop();

            ClassicAssert.GreaterOrEqual(sw.Elapsed.TotalMilliseconds, 350,
                "DEBUG BLOCK replied before its delay elapsed, so the body never actually suspended.");
        }

        [Test]
        public void ZeroDelayCompletesWithoutSuspending()
        {
            using var client = new RawRespSession(useTls);

            // Task.Delay(0) completes synchronously, so the builder never boxes a state machine. The session
            // must handle a command whose async body finishes inline exactly like any other command.
            for (var i = 0; i < 50; i++)
                ClassicAssert.AreEqual(Ok, client.Execute("DEBUG", "BLOCK", "0"));

            ClassicAssert.AreEqual(Pong, client.Execute("PING"));
        }

        [Test]
        public void MalformedDelayIsRejectedWithoutSuspending()
        {
            using var client = new RawRespSession(useTls);

            StringAssert.StartsWith("-ERR", client.Execute("DEBUG", "BLOCK", "not-a-number"));
            StringAssert.StartsWith("-ERR", client.Execute("DEBUG", "BLOCK", "-1"));
            StringAssert.StartsWith("-ERR", client.Execute("DEBUG", "BLOCK", "100000"));

            // The session stays usable after each rejection.
            ClassicAssert.AreEqual(Pong, client.Execute("PING"));
        }

        [Test]
        public void ParseStateSurvivesTheSuspension()
        {
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: useTls)))
                redis.GetDatabase(0).StringSet("survives", "value-after-park");

            using var client = new RawRespSession(useTls);

            // The key argument points into the receive buffer. Because the suspension happens before the
            // network stack unwinds past TryConsumeMessagesAsync, that buffer cannot be shifted or returned
            // while the command is parked, so the slice is still valid when the body resumes.
            ClassicAssert.AreEqual("$16\r\nvalue-after-park\r\n",
                client.Execute("DEBUG", "BLOCK", "0.3", "survives"));
        }

        [Test]
        public void PipelinedCommandsAroundABlockKeepTheirOrder()
        {
            using var client = new RawRespSession(useTls);

            // One write, three commands. The reply to the first must be flushed when the second suspends,
            // otherwise a client that waits for it before sending more would deadlock.
            client.SendPipeline(["PING"], ["DEBUG", "BLOCK", "0.5"], ["ECHO", "after"]);

            var sw = Stopwatch.StartNew();
            ClassicAssert.AreEqual(Pong, client.ReadReply());
            var firstReplyAt = sw.Elapsed;

            ClassicAssert.AreEqual(Ok, client.ReadReply());
            ClassicAssert.AreEqual("$5\r\nafter\r\n", client.ReadReply());
            sw.Stop();

            ClassicAssert.Less(firstReplyAt.TotalMilliseconds, 250,
                "The reply preceding a suspending command was not flushed at suspension time.");
            ClassicAssert.GreaterOrEqual(sw.Elapsed.TotalMilliseconds, 450,
                "The pipeline completed before the block elapsed, so the block did not take effect.");
        }

        [Test]
        public void CommandsArrivingDuringTheBlockAreServedAfterIt()
        {
            using var client = new RawRespSession(useTls);

            client.Send("DEBUG", "BLOCK", "0.6");

            // Arrives while the session is parked with no receive outstanding. It must not be processed
            // out of order, and it must not be lost.
            Thread.Sleep(150);
            client.Send("ECHO", "queued");

            var sw = Stopwatch.StartNew();
            ClassicAssert.AreEqual(Ok, client.ReadReply());
            ClassicAssert.AreEqual("$6\r\nqueued\r\n", client.ReadReply());
            sw.Stop();

            ClassicAssert.GreaterOrEqual(sw.Elapsed.TotalMilliseconds, 350,
                "The parked command replied too early to have been parked.");
        }

        [Test]
        public void ASuspendedSessionDoesNotStallOtherSessions()
        {
            using var blocked = new RawRespSession(useTls);
            using var other = new RawRespSession(useTls);

            blocked.Send("DEBUG", "BLOCK", "2");
            Thread.Sleep(100);

            var sw = Stopwatch.StartNew();
            for (var i = 0; i < 20; i++)
                ClassicAssert.AreEqual(Pong, other.Execute("PING"));
            sw.Stop();

            ClassicAssert.Less(sw.Elapsed.TotalMilliseconds, 1500,
                "A parked session delayed an unrelated session, so the receive thread was not released.");
            ClassicAssert.AreEqual(Ok, blocked.ReadReply());
        }

        /// <summary>
        /// A suspension releases the session's per-batch resources -- response object, cluster epoch, scratch
        /// buffers -- but deliberately not its transaction. This pins that: while a command parks inside
        /// <c>MULTI</c>/<c>EXEC</c> the keys the transaction locked stay locked, so another session touching
        /// them waits for the park to finish. The existing blocking list commands do the same, joining the
        /// running transaction rather than declining to block inside one, so this is the pattern's behaviour
        /// and not a property of <c>DEBUG BLOCK</c>. A blocking command built on it must therefore bound its
        /// wait, or refuse to park while <c>txnManager.state == TxnState.Running</c>.
        /// </summary>
        [Test]
        public void ASuspensionInsideATransactionHoldsItsLocks()
        {
            using var txn = new RawRespSession(useTls);
            using var other = new RawRespSession(useTls);

            ClassicAssert.AreEqual(Ok, txn.Execute("SET", "txnkey", "before"));
            ClassicAssert.AreEqual(Ok, txn.Execute("MULTI"));
            ClassicAssert.AreEqual(Queued, txn.Execute("SET", "txnkey", "after"));
            ClassicAssert.AreEqual(Queued, txn.Execute("DEBUG", "BLOCK", "1"));

            txn.Send("EXEC");

            // Long enough that EXEC is certainly inside the park, short enough to leave most of it to wait on.
            Thread.Sleep(250);

            var sw = Stopwatch.StartNew();
            var read = other.Execute("GET", "txnkey");
            sw.Stop();

            ClassicAssert.AreEqual("*2\r\n+OK\r\n+OK\r\n", txn.ReadReply(),
                "The transaction did not complete after its parked command resumed.");

            ClassicAssert.GreaterOrEqual(sw.Elapsed.TotalMilliseconds, 300,
                "A reader of a key locked by the parked transaction was not made to wait, so the suspension " +
                "released the transaction's locks.");

            ClassicAssert.AreEqual("$5\r\nafter\r\n", read,
                "The reader observed a value from inside the transaction.");
        }

        [Test]
        public void RepeatedSuspensionsOnOneSessionReuseTheirState()
        {
            using var client = new RawRespSession(useTls);

            for (var i = 0; i < 20; i++)
            {
                ClassicAssert.AreEqual(Ok, client.Execute("DEBUG", "BLOCK", "0.01"));
                ClassicAssert.AreEqual(Pong, client.Execute("PING"));
            }
        }

        /// <summary>
        /// Hammers the handshake between the thread that suspends a command and the thread that completes it.
        /// Delays this short routinely elapse while the suspending thread is still unwinding out of the
        /// session's resource scope, which is the interleaving the suspension state machine exists to order:
        /// the completion must wait for the scope to be released rather than re-entering it concurrently.
        /// </summary>
        [Test]
        public void ShortBlocksFromManySessionsStayOrdered()
        {
            const int SessionCount = 16;
            const int Iterations = 150;

            using (var setup = new RawRespSession(useTls))
                setup.ExecuteExpect(Encoding.ASCII.GetBytes(Ok), "SET", "stress-key", "stress-value");

            var failures = new List<string>();
            var threads = new Thread[SessionCount];

            for (var t = 0; t < SessionCount; t++)
            {
                threads[t] = new Thread(() =>
                {
                    try
                    {
                        using var client = new RawRespSession(useTls);
                        for (var i = 0; i < Iterations; i++)
                        {
                            // Suspends, resumes, and reads a key on the way out, so a resume that re-entered
                            // the scope early would corrupt either the reply framing or the parsed arguments.
                            var blocked = client.Execute("DEBUG", "BLOCK", "0.001", "stress-key");
                            if (blocked != "$12\r\nstress-value\r\n")
                            {
                                lock (failures) failures.Add($"block reply was {Escape(blocked)}");
                                return;
                            }

                            var pong = client.Execute("PING");
                            if (pong != Pong)
                            {
                                lock (failures) failures.Add($"ping reply was {Escape(pong)}");
                                return;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        lock (failures) failures.Add(ex.ToString());
                    }
                });
                threads[t].Start();
            }

            foreach (var thread in threads)
                ClassicAssert.IsTrue(thread.Join(TimeSpan.FromMinutes(2)), "A stress session did not finish");

            ClassicAssert.IsEmpty(failures, string.Join("\n", failures.Take(3)));
        }

        static string Escape(string reply) => reply?.Replace("\r", "\\r").Replace("\n", "\\n") ?? "<null>";

        /// <summary>
        /// A session parked on a name is released by a different connection, which is how every real blocking
        /// command is woken. Unlike a timed park, the wakeup is produced by another session's command.
        /// </summary>
        [Test]
        public void AParkedSessionIsWokenByAnotherSession()
        {
            using var waiter = new RawRespSession(useTls);
            using var signaller = new RawRespSession(useTls);

            waiter.Send("DEBUG", "BLOCKON", "gate");

            // Retried because the waiter's park and this signal are not ordered with each other.
            SignalUntilWoken(signaller, "gate", 1);

            ClassicAssert.AreEqual(Ok, waiter.ReadReply());
            ClassicAssert.AreEqual(":0\r\n", signaller.Execute("DEBUG", "SIGNAL", "gate"),
                "The waiter should no longer be registered once it has been woken");
        }

        /// <summary>
        /// One signal releases every session parked on the name, and each resumes independently.
        /// </summary>
        [Test]
        public void SignallingReleasesEveryParkedSession()
        {
            const int WaiterCount = 32;
            var waiters = new RawRespSession[WaiterCount];
            try
            {
                for (var i = 0; i < WaiterCount; i++)
                {
                    waiters[i] = new RawRespSession(useTls);
                    waiters[i].Send("DEBUG", "BLOCKON", "fanout");
                }

                using var signaller = new RawRespSession(useTls);
                var woken = 0;
                var deadline = Stopwatch.StartNew();
                while (woken < WaiterCount && deadline.Elapsed < TimeSpan.FromSeconds(30))
                {
                    var reply = signaller.Execute("DEBUG", "SIGNAL", "fanout");
                    woken += int.Parse(reply.AsSpan(1, reply.Length - 3), CultureInfo.InvariantCulture);
                }

                ClassicAssert.AreEqual(WaiterCount, woken);
                foreach (var waiter in waiters)
                    ClassicAssert.AreEqual(Ok, waiter.ReadReply());
            }
            finally
            {
                foreach (var waiter in waiters)
                    waiter?.Dispose();
            }
        }

        /// <summary>
        /// Races a cross-session wakeup against the park it releases, thousands of times. The signal is
        /// produced by a command on another connection and so lands at an arbitrary point of the parking
        /// session's own unwind, including before the park has been published. That interleaving is the one
        /// the session's suspension state machine exists to order, and a resume that ran too early would
        /// re-enter the session scope while the parking thread was still inside it.
        /// </summary>
        [Test]
        public void CrossSessionWakeupsRaceTheParkTheyRelease()
        {
            const int WaiterCount = 8;
            const int Rounds = 200;

            var failures = new List<string>();
            using var stopping = new ManualResetEventSlim(false);

            var signaller = new Thread(() =>
            {
                try
                {
                    using var session = new RawRespSession(useTls);
                    while (!stopping.IsSet)
                    {
                        for (var w = 0; w < WaiterCount; w++)
                            _ = session.Execute("DEBUG", "SIGNAL", "race-" + w);
                    }
                }
                catch (Exception ex)
                {
                    lock (failures) failures.Add(ex.ToString());
                }
            });
            signaller.Start();

            var waiters = new Thread[WaiterCount];
            for (var w = 0; w < WaiterCount; w++)
            {
                var name = "race-" + w;
                waiters[w] = new Thread(() =>
                {
                    try
                    {
                        using var session = new RawRespSession(useTls);
                        for (var i = 0; i < Rounds; i++)
                        {
                            var reply = session.Execute("DEBUG", "BLOCKON", name);
                            if (reply != Ok)
                            {
                                lock (failures) failures.Add($"park reply was {Escape(reply)}");
                                return;
                            }

                            var pong = session.Execute("PING");
                            if (pong != Pong)
                            {
                                lock (failures) failures.Add($"ping after park was {Escape(pong)}");
                                return;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        lock (failures) failures.Add(ex.ToString());
                    }
                });
                waiters[w].Start();
            }

            try
            {
                foreach (var waiter in waiters)
                    ClassicAssert.IsTrue(waiter.Join(TimeSpan.FromMinutes(2)), "A racing session did not finish");
            }
            finally
            {
                stopping.Set();
                signaller.Join(TimeSpan.FromSeconds(30));
            }

            ClassicAssert.IsEmpty(failures, string.Join("\n", failures.Take(3)));
        }

        static void SignalUntilWoken(RawRespSession signaller, string name, int expected)
        {
            var woken = 0;
            var deadline = Stopwatch.StartNew();
            while (woken < expected && deadline.Elapsed < TimeSpan.FromSeconds(30))
            {
                var reply = signaller.Execute("DEBUG", "SIGNAL", name);
                woken += int.Parse(reply.AsSpan(1, reply.Length - 3), CultureInfo.InvariantCulture);
            }

            ClassicAssert.AreEqual(expected, woken, "No session was parked on the name");
        }

        /// <summary>
        /// Tears sessions down while a wakeup for them is already in flight, which is the race between a
        /// resume re-entering the session scope and the teardown that reclaims the receive buffer and
        /// response object that resume would touch. The server has to survive it and keep serving.
        /// </summary>
        [Test]
        public void SessionsTornDownWhileBeingWokenLeaveTheServerUsable()
        {
            const int Rounds = 20;
            const int WaiterCount = 16;

            using var signaller = new RawRespSession(useTls);

            for (var round = 0; round < Rounds; round++)
            {
                var waiters = new RawRespSession[WaiterCount];
                for (var i = 0; i < WaiterCount; i++)
                {
                    waiters[i] = new RawRespSession(useTls);
                    waiters[i].Send("DEBUG", "BLOCKON", "teardown-race");
                }

                // Give the parks time to reach the server, then signal and abort concurrently so some
                // sessions are disposed with a resume already dispatched against them.
                Thread.Sleep(5);
                var aborter = new Thread(() =>
                {
                    foreach (var waiter in waiters)
                        waiter.AbortAndDispose();
                });

                aborter.Start();
                _ = signaller.Execute("DEBUG", "SIGNAL", "teardown-race");
                ClassicAssert.IsTrue(aborter.Join(TimeSpan.FromSeconds(30)), "Aborting the parked sessions hung");
            }

            // The signalling connection shares the server with every session torn down above; a resume that
            // ran against a reclaimed buffer would have corrupted it or taken the server down.
            ClassicAssert.AreEqual(Pong, signaller.Execute("PING"));
            using var fresh = new RawRespSession(useTls);
            ClassicAssert.AreEqual(Ok, fresh.Execute("DEBUG", "BLOCK", "0.01"));
        }

        /// <summary>
        /// The point of the whole design: a parked command holds no thread. Far more sessions block
        /// concurrently than the thread pool could ever supply threads for, and they all finish together.
        /// </summary>
        [Test]
        public void MoreSessionsBlockConcurrentlyThanTheThreadPoolHasThreads()
        {
            ThreadPool.GetMinThreads(out var minWorkers, out _);
            var sessionCount = Math.Max(256, minWorkers * 8);
            const double BlockSeconds = 2.0;

            var clients = new RawRespSession[sessionCount];
            try
            {
                for (var i = 0; i < sessionCount; i++)
                    clients[i] = new RawRespSession(useTls);

                var threadsBefore = CurrentThreadCount();

                var sw = Stopwatch.StartNew();
                foreach (var c in clients)
                    c.Send("DEBUG", "BLOCK", BlockSeconds.ToString(CultureInfo.InvariantCulture));

                // Sampled while every session is parked. A suspension that occupied a thread would show up
                // here one-for-one, because the thread pool must grow to keep serving.
                Thread.Sleep((int)(BlockSeconds * 1000 / 2));
                var threadGrowth = CurrentThreadCount() - threadsBefore;

                foreach (var c in clients)
                    ClassicAssert.AreEqual(Ok, c.ReadReply());
                sw.Stop();

                ClassicAssert.Less(threadGrowth, sessionCount / 4,
                    $"The process grew by {threadGrowth} threads while {sessionCount} sessions were parked, " +
                    "so the suspensions are holding threads rather than releasing them.");

                // Serving these synchronously would need one thread each. The pool injects roughly one or two
                // additional threads per second past its minimum, so a synchronous implementation would take
                // minutes. Finishing inside a small multiple of a single block proves they overlapped.
                ClassicAssert.Less(sw.Elapsed.TotalSeconds, BlockSeconds * 4,
                    $"{sessionCount} concurrent blocks took {sw.Elapsed.TotalSeconds:F1}s, which indicates " +
                    "they were serialized on thread-pool threads rather than parked.");
                ClassicAssert.GreaterOrEqual(sw.Elapsed.TotalSeconds, BlockSeconds * 0.8,
                    "The blocks finished faster than a single block, so they did not take effect.");
            }
            finally
            {
                foreach (var c in clients)
                    c?.Dispose();
            }
        }

        [Test]
        public void DisconnectDuringASuspensionIsCleanedUp()
        {
            var client = new RawRespSession(useTls);
            client.Send("DEBUG", "BLOCK", "3");
            Thread.Sleep(150);

            // Abortive close while the session is parked: no receive is outstanding, so the server does not
            // observe this until the park completes and the next receive is posted.
            client.AbortAndDispose();

            // The server must stay healthy and must not leak the session's buffers.
            using var survivor = new RawRespSession(useTls);
            ClassicAssert.AreEqual(Pong, survivor.Execute("PING"));

            Thread.Sleep(3200);
            ClassicAssert.AreEqual(Pong, survivor.Execute("PING"));
        }

        [Test]
        public void ServerDisposeWhileSessionsAreParkedCompletesPromptly()
        {
            var clients = new List<RawRespSession>();
            try
            {
                for (var i = 0; i < 32; i++)
                {
                    var c = new RawRespSession(useTls);
                    c.Send("DEBUG", "BLOCK", "60");
                    clients.Add(c);
                }

                // Let every session reach the parked state.
                Thread.Sleep(300);

                var sw = Stopwatch.StartNew();
                server.Dispose();
                server = null;
                sw.Stop();

                // Teardown cancels each parked operation rather than waiting for it. Waiting out even one of
                // these 60-second blocks would mean the gate was held across the park.
                ClassicAssert.Less(sw.Elapsed.TotalSeconds, 15,
                    "Disposing the server waited on parked commands instead of cancelling them.");
            }
            finally
            {
                foreach (var c in clients)
                    c.Dispose();
            }
        }

        /// <summary>
        /// A suspension must not allocate once the pools are warm.
        /// </summary>
        /// <remarks>
        /// Measured as the gap between a command that parks and the same command when it does not, so the
        /// client, the parser and the reply path all cancel out and what is left is the cost of suspending
        /// and resuming. That gap is only meaningful because <c>DEBUG BLOCK</c> waits on a pooled
        /// <c>SessionDelay</c> rather than <c>Task.Delay</c>, which would otherwise put its own timer and
        /// promise -- a couple of hundred bytes -- into the measurement and swamp it.
        /// </remarks>
        [Test]
        public void SteadyStateSuspensionsDoNotAllocate()
        {
#if DEBUG
            // Roslyn emits an async state machine as a class in Debug and as a struct in Release, so in Debug
            // every park heap-allocates its state machine at the call site, before the pooled builder runs.
            // That cost is the compiler's, not the suspension path's, and it cannot be pooled away; the
            // Release leg of CI is what holds this invariant.
            Assert.Ignore("Async state machines are classes in DEBUG builds, so a park always allocates one.");
#endif

            // SslStream allocates a buffer and a read state per pass regardless of what the session does, so
            // under TLS this measures the TLS stack rather than the suspension machinery.
            Assume.That(!useTls, "Allocation on the TLS receive path is dominated by SslStream.");

            using var client = new RawRespSession(useTls);

            // Long enough that one-off costs -- thread-pool growth, buffer growth, the JIT -- amortise away.
            // At a few hundred iterations they dominate the per-park figure entirely.
            const int Iterations = 20_000;

            // Warm every pool: the state-machine boxes, the completion source, the timer, the buffers.
            for (var i = 0; i < 1024; i++)
            {
                client.ExecuteExpect("+OK\r\n"u8, "DEBUG", "BLOCK", "0");
                client.ExecuteExpect("+OK\r\n"u8, "DEBUG", "BLOCK", "0.001");
            }

            var withoutPark = MeasurePerOp(Iterations, () => client.ExecuteExpect("+OK\r\n"u8, "DEBUG", "BLOCK", "0"));
            var withPark = MeasurePerOp(Iterations, () => client.ExecuteExpect("+OK\r\n"u8, "DEBUG", "BLOCK", "0.001"));

            var perPark = withPark - withoutPark;
            TestContext.Out.WriteLine($"park={withPark:F0} B  no-park={withoutPark:F0} B  per park={perPark:F0} B");

            // Calibrated by removing [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))] from the command
            // bodies and re-measuring: pooled costs 45 B per park on net8 and under 20 B on net10, unpooled
            // costs 195 B. The floor is not zero because the network handler's own continuation frame
            // (AwaitProcessAsync) uses the default builder, which boxes its state machine on .NET 8; that is
            // one allocation on the park path only, and the no-park path stays at zero. The bound sits
            // between the two so it still catches a lost pool without tracking the runtime's own box churn.
            ClassicAssert.Less(perPark, 128,
                "Parking allocates per command: a pool on the suspension path is being missed.");
        }

        static double MeasurePerOp(int iterations, Action op)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var before = GC.GetTotalAllocatedBytes(precise: true);
            for (var i = 0; i < iterations; i++)
                op();

            return (GC.GetTotalAllocatedBytes(precise: true) - before) / (double)iterations;
        }

        /// <summary>
        /// Minimal synchronous RESP client. Unlike a multiplexer it never reorders or coalesces, so a test can
        /// place commands precisely on either side of a suspension and observe exactly when each reply lands.
        /// It is also allocation-light on the steady-state path, so a test can attribute process-wide
        /// allocation to the server rather than to itself.
        /// </summary>
        sealed class RawRespSession : IDisposable
        {
            readonly Socket socket;
            readonly Stream stream;
            readonly byte[] readBuffer = new byte[64 * 1024];
            readonly byte[] writeBuffer = new byte[64 * 1024];
            readonly byte[] replyBuffer = new byte[64 * 1024];
            int readStart, readEnd;

            internal RawRespSession(bool useTls = false)
            {
                var endpoint = TestUtils.EndPoint;
                socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = true,
                };
                socket.Connect(endpoint);
                socket.ReceiveTimeout = (int)TimeSpan.FromSeconds(90).TotalMilliseconds;

                var netStream = new NetworkStream(socket, ownsSocket: false);
                if (useTls)
                {
                    var ssl = new SslStream(netStream, leaveInnerStreamOpen: false,
                        TestUtils.ValidateServerCertificate);
                    ssl.AuthenticateAsClient(new SslClientAuthenticationOptions
                    {
                        ClientCertificates = [TestUtils.GetClientCertificate()],
                        TargetHost = "GarnetTest",
                        AllowRenegotiation = false,
                        RemoteCertificateValidationCallback = TestUtils.ValidateServerCertificate,
                    });
                    stream = ssl;
                }
                else
                {
                    stream = netStream;
                }
            }

            /// <summary>Sends one command and returns its reply as text.</summary>
            internal string Execute(params string[] args)
            {
                Send(args);
                return ReadReply();
            }

            /// <summary>Sends one command and asserts the reply, without allocating on the steady-state path.</summary>
            internal void ExecuteExpect(ReadOnlySpan<byte> expectedReply, params string[] args)
            {
                Send(args);
                var len = ReadReplyBytes();
                if (!replyBuffer.AsSpan(0, len).SequenceEqual(expectedReply))
                    Assert.Fail($"Expected '{Describe(expectedReply)}' but got '{Describe(replyBuffer.AsSpan(0, len))}'");
            }

            /// <summary>Sends one command, writing the whole frame in a single socket write.</summary>
            internal void Send(params string[] args) => SendBatch(args);

            /// <summary>
            /// Sends several commands in a single socket write, so the server sees them in one receive.
            /// Each element is one command's tokens.
            /// </summary>
            internal void SendPipeline(params string[][] commands)
            {
                var n = 0;
                foreach (var cmd in commands)
                    n = Encode(cmd, n);
                stream.Write(writeBuffer, 0, n);
                stream.Flush();
            }

            void SendBatch(string[] args)
            {
                var n = Encode(args, 0);
                stream.Write(writeBuffer, 0, n);
                stream.Flush();
            }

            int Encode(string[] args, int at)
            {
                writeBuffer[at++] = (byte)'*';
                at += WriteInt(args.Length, at);
                at = WriteCrLf(at);
                foreach (var a in args)
                {
                    writeBuffer[at++] = (byte)'$';
                    at += WriteInt(Encoding.UTF8.GetByteCount(a), at);
                    at = WriteCrLf(at);
                    at += Encoding.UTF8.GetBytes(a, writeBuffer.AsSpan(at));
                    at = WriteCrLf(at);
                }
                return at;
            }

            int WriteInt(int value, int at)
            {
                _ = value.TryFormat(writeBuffer.AsSpan(at), out var written,
                    provider: CultureInfo.InvariantCulture);
                return written;
            }

            int WriteCrLf(int at)
            {
                writeBuffer[at++] = (byte)'\r';
                writeBuffer[at++] = (byte)'\n';
                return at;
            }

            /// <summary>Reads one RESP reply, including its framing, as text.</summary>
            internal string ReadReply()
            {
                var len = ReadReplyBytes();
                return Encoding.UTF8.GetString(replyBuffer, 0, len);
            }

            /// <summary>
            /// Reads one complete RESP reply into the reusable reply buffer and returns its length. The reply
            /// retains its framing, so a bulk string reads as "$5\r\nhello\r\n".
            /// </summary>
            internal int ReadReplyBytes()
            {
                var len = 0;
                ReadOne(ref len);
                return len;
            }

            void ReadOne(ref int len)
            {
                var lineStart = len;
                var lineLen = ReadLineInto(ref len);
                if (lineLen == 0)
                    throw new IOException("Empty RESP reply");

                switch (replyBuffer[lineStart])
                {
                    case (byte)'+':
                    case (byte)'-':
                    case (byte)':':
                    case (byte)'_':
                        return;
                    case (byte)'$':
                    case (byte)'=':
                        var size = ParseLen(lineStart + 1, lineLen - 1);
                        if (size < 0) return;
                        ReadInto(size + 2, ref len);
                        return;
                    case (byte)'*':
                    case (byte)'~':
                    case (byte)'>':
                        var count = ParseLen(lineStart + 1, lineLen - 1);
                        for (var i = 0; i < count; i++)
                            ReadOne(ref len);
                        return;
                    default:
                        throw new IOException($"Unexpected RESP type '{(char)replyBuffer[lineStart]}'");
                }
            }

            int ParseLen(int at, int count)
            {
                var v = 0;
                var neg = false;
                for (var i = 0; i < count; i++)
                {
                    var c = replyBuffer[at + i];
                    if (c == '-') { neg = true; continue; }
                    v = (v * 10) + (c - '0');
                }
                return neg ? -v : v;
            }

            /// <summary>Appends one CRLF-terminated line to the reply buffer; returns the length excluding CRLF.</summary>
            int ReadLineInto(ref int len)
            {
                var start = len;
                while (true)
                {
                    var b = ReadByte();
                    replyBuffer[len++] = b;
                    if (b == '\n' && len - start >= 2 && replyBuffer[len - 2] == '\r')
                        return len - start - 2;
                }
            }

            void ReadInto(int count, ref int len)
            {
                for (var i = 0; i < count; i++)
                    replyBuffer[len++] = ReadByte();
            }

            byte ReadByte()
            {
                if (readStart == readEnd)
                {
                    readStart = 0;
                    readEnd = stream.Read(readBuffer, 0, readBuffer.Length);
                    if (readEnd <= 0)
                        throw new IOException("Connection closed by server");
                }
                return readBuffer[readStart++];
            }

            static string Describe(ReadOnlySpan<byte> bytes) =>
                Encoding.UTF8.GetString(bytes).Replace("\r\n", "\\r\\n");

            /// <summary>Closes the connection with a TCP reset, without a graceful shutdown handshake.</summary>
            internal void AbortAndDispose()
            {
                try { socket.LingerState = new LingerOption(true, 0); } catch { }
                Dispose();
            }

            public void Dispose()
            {
                try { stream?.Dispose(); } catch { }
                try { socket?.Dispose(); } catch { }
            }
        }
    }
}