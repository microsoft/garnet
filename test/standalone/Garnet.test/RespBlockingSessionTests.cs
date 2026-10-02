// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Covers session parking: the mechanism that lets a blocking command wait without holding the thread it
    /// was dispatched on. <c>DEBUG BLOCK</c> is the reference command built on it.
    /// </summary>
    [TestFixture]
    public class RespBlockingSessionTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            BlockingCommandContext.ResetContractViolations();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableLua: true);
            server.Start();
        }

        /// <summary>
        /// Started only by the tests that need it, because TLS costs a handshake per connection and most of
        /// these tests are indifferent to the transport.
        /// </summary>
        GarnetServer tlsServer;

        /// <summary>
        /// Brings up a TLS server on the alternate port and returns its endpoint.
        /// </summary>
        System.Net.EndPoint StartTlsServer(bool lowMemory = false)
        {
            var endPoint = TestUtils.EndPoint;
            var tlsPort = ((System.Net.IPEndPoint)endPoint).Port + 1;
            tlsServer = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir + "_tls", enableTLS: true,
                lowMemory: lowMemory,
                endpoints: [new System.Net.IPEndPoint(((System.Net.IPEndPoint)endPoint).Address, tlsPort)]);
            tlsServer.Start();
            return new System.Net.IPEndPoint(((System.Net.IPEndPoint)endPoint).Address, tlsPort);
        }

        /// <summary>
        /// Started only by the tests that need reads to actually miss memory.
        /// </summary>
        GarnetServer lowMemoryServer;

        /// <summary>
        /// Brings up a plain low-memory server on the alternate port and returns its endpoint, for tests that
        /// need a read to go pending but are indifferent to the transport.
        /// </summary>
        /// <remarks>
        /// Built through <c>GetGarnetServerOptions</c> so the database count can be raised, which
        /// <c>CreateGarnetServer</c> does not expose.
        /// </remarks>
        System.Net.EndPoint StartLowMemoryServer()
        {
            var endPoint = (System.Net.IPEndPoint)TestUtils.EndPoint;
            var lowMemoryEndPoint = new System.Net.IPEndPoint(endPoint.Address, endPoint.Port + 3);

            var options = TestUtils.GetGarnetServerOptions(
                checkpointDir: TestUtils.MethodTestDir + "_lowmem",
                logDir: TestUtils.MethodTestDir + "_lowmem",
                endpoint: lowMemoryEndPoint,
                enableCluster: false,
                lowMemory: true);
            options.MaxDatabases = 16;

            lowMemoryServer = new GarnetServer(options);
            lowMemoryServer.Start();
            return lowMemoryEndPoint;
        }

        /// <summary>
        /// Writes enough large values through <paramref name="endPoint"/> that reading them back misses memory
        /// and goes pending, and returns their keys.
        /// </summary>
        static string[] LoadColdKeys(System.Net.EndPoint endPoint, int keyCount, bool useTls = false)
        {
            var keys = new string[keyCount];
            var value = new string('v', 16 * 1024);
            using var loader = new RawRespClient(endPoint, useTls: useTls);
            for (var i = 0; i < keyCount; i++)
            {
                keys[i] = $"cold-key-{i:D4}";
                loader.Send(RawRespClient.Command("SET", keys[i], value));
                ClassicAssert.AreEqual("+OK", loader.ReadLine());
            }

            return keys;
        }

        [TearDown]
        public void TearDown()
        {
            tlsServer?.Dispose();
            tlsServer = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir + "_tls");
            lowMemoryServer?.Dispose();
            lowMemoryServer = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir + "_lowmem");
            server?.Dispose();
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);

            // Runs after the server is gone, so violations raised during teardown are counted too. The
            // blocking-command contract check lives in OnTearDown alongside the epoch leak check, so it
            // covers every test in the suite rather than just this fixture.
            TestUtils.OnTearDown();
        }

        [Test]
        public void DebugBlockWaitsThenRepliesOk()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            var sw = Stopwatch.StartNew();
            var result = db.Execute("DEBUG", "BLOCK", "0.3");
            sw.Stop();

            ClassicAssert.AreEqual("OK", result.ToString());
            ClassicAssert.GreaterOrEqual(sw.ElapsedMilliseconds, 250);
        }

        [Test]
        public void DebugBlockRejectsBadArguments()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("DEBUG", "BLOCK"));
            ClassicAssert.IsTrue(ex.Message.Contains("wrong number of arguments", StringComparison.OrdinalIgnoreCase));

            ex = Assert.Throws<RedisServerException>(() => db.Execute("DEBUG", "BLOCK", "abc"));
            ClassicAssert.AreEqual("ERR timeout is not a float or out of range", ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute("DEBUG", "BLOCK", "-1"));
            ClassicAssert.AreEqual("ERR timeout is negative", ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute("DEBUG", "BLOCK", "0.1", "NOPE"));
            ClassicAssert.AreEqual("ERR syntax error", ex.Message);
        }

        /// <summary>
        /// Commands pipelined behind a blocking one must not be parsed until it completes, and their replies
        /// must follow its reply. Replies for commands ahead of it are flushed before the session parks.
        /// </summary>
        [Test]
        public void PipelineBehindBlockingCommandStaysOrdered()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            var sw = Stopwatch.StartNew();
            client.Send(
                RawRespClient.Command("PING"),
                RawRespClient.Command("DEBUG", "BLOCK", "1"),
                RawRespClient.Command("ECHO", "after"));

            // The reply for the command ahead of the block is flushed at the park, not held until it ends.
            ClassicAssert.AreEqual("+PONG", client.ReadLine());
            var pongAt = sw.ElapsedMilliseconds;
            ClassicAssert.Less(pongAt, 500, "Reply ahead of the blocking command was held back");

            ClassicAssert.AreEqual("+OK", client.ReadLine());
            ClassicAssert.GreaterOrEqual(sw.ElapsedMilliseconds, 900);

            ClassicAssert.AreEqual("$5", client.ReadLine());
            ClassicAssert.AreEqual("after", client.ReadLine());
        }

        /// <summary>
        /// A resumed session keeps serving its connection normally, including further blocking commands.
        /// </summary>
        [Test]
        public void SessionKeepsWorkingAfterResuming()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            for (var i = 0; i < 3; i++)
            {
                client.Send(RawRespClient.Command("DEBUG", "BLOCK", "0.1"));
                ClassicAssert.AreEqual("+OK", client.ReadLine());

                client.Send(RawRespClient.Command("SET", "k" + i, "v" + i));
                ClassicAssert.AreEqual("+OK", client.ReadLine());

                client.Send(RawRespClient.Command("GET", "k" + i));
                ClassicAssert.AreEqual("$2", client.ReadLine());
                ClassicAssert.AreEqual("v" + i, client.ReadLine());
            }
        }

        /// <summary>
        /// Parking and resuming must not allocate. The machinery is deliberately built from a rendezvous
        /// counter and a pooled work item rather than tasks and continuations, so the only managed garbage a
        /// blocking command may produce is its own context object.
        /// </summary>
        /// <remarks>
        /// A zero-length <c>DEBUG BLOCK</c> completes through the thread pool rather than arming a timer, so
        /// the only allocation left on the path is the command's own context. The budget is that context
        /// plus a small margin, which is tight enough that adding any per-park object -- a task, a
        /// continuation, a closure, a boxed work item -- fails the test rather than hiding under headroom.
        /// <para>
        /// Allocation noise is additive, so the cheapest round is the one least polluted by whatever else the
        /// process was doing, and taking the minimum keeps the measurement stable without a tolerance.
        /// </para>
        /// <para>
        /// A zero-length wait races the two halves of the park rendezvous, the receive stack unwinding and
        /// the operation completing, in both orders. This doubles as a soak test for that hand-off.
        /// </para>
        /// </remarks>
        [Test]
        public void ParkAndResumeDoNotAllocate()
        {
            const int Warmup = 1000;
            const int Rounds = 8;
            const int CommandsPerRound = 2000;
            const int OkReplyLength = 5;
            const int PipelineDepth = 8;

            // sizeof(DebugBlockCommandContext) is 80 in Debug, where it carries a release counter that
            // exists only to trip the double-release assert. The margin covers allocator granularity, not
            // another object.
            const int BudgetBytesPerCommand = 88;

            using var client = new RawRespClient(TestUtils.EndPoint);
            var block = RawRespClient.Command("DEBUG", "BLOCK", "0");
            var pipelined = Repeat(block, PipelineDepth);

            void Run(int commands, byte[] payload, int depth)
            {
                for (var i = 0; i < commands / depth; i++)
                {
                    client.SendRaw(payload);
                    client.Consume(OkReplyLength * depth);
                }
            }

            double Measure(byte[] payload, int depth)
            {
                Run(Warmup, payload, depth);

                // Allocation noise is additive, so the cheapest round is the one least polluted by whatever
                // else the process was doing. Taking the minimum keeps this stable without a tolerance.
                var best = double.MaxValue;
                for (var round = 0; round < Rounds; round++)
                {
                    var before = GC.GetTotalAllocatedBytes(precise: true);
                    Run(CommandsPerRound, payload, depth);
                    var measured = (GC.GetTotalAllocatedBytes(precise: true) - before) / (double)CommandsPerRound;

                    if (measured < best)
                        best = measured;
                }

                return best;
            }

            var single = Measure(block, 1);
            var batched = Measure(pipelined, PipelineDepth);

            TestContext.Out.WriteLine($"park and resume allocated {single:F1} bytes per command, " +
                                      $"{batched:F1} at pipeline depth {PipelineDepth}");

            ClassicAssert.Less(single, BudgetBytesPerCommand,
                $"Each parked command allocated {single:F1} bytes");

            // The absolute number above is the reference command's own context, which is its business. What
            // belongs to the machinery is that the number does not move: a resume that drains seven commands
            // it never parsed must cost the same per command as one that drains none. Anything proportional
            // to buffered bytes, to pipeline depth, or to the number of resumes shows up here even when it
            // is small enough to hide under the budget.
            ClassicAssert.Less(batched, single + 1,
                $"Pipelining raised per-command allocation from {single:F1} to {batched:F1} bytes");
        }

        /// <summary>
        /// Concatenates <paramref name="count"/> copies of a pre-encoded command, so a test can send a
        /// pipelined batch without allocating per send.
        /// </summary>
        static byte[] Repeat(byte[] command, int count)
        {
            var payload = new byte[command.Length * count];
            for (var i = 0; i < count; i++)
                Buffer.BlockCopy(command, 0, payload, i * command.Length, command.Length);
            return payload;
        }

        /// <summary>
        /// A burst of connections completing at the same instant must still allocate nothing per park, and
        /// must resume every session exactly once.
        /// </summary>
        /// <remarks>
        /// Steady-state, one-command-at-a-time traffic never has more than one resume in flight, so it
        /// cannot exercise contention on the thread pool's global queue or on the park rendezvous. This
        /// drives many sessions through the hand-off simultaneously instead, which only means anything if
        /// they are provably all parked at once: a wait short enough to be allocation-free is also short
        /// enough that the first connection can resume before the last one has sent. The gate removes the
        /// timing assumption -- waiters are held until something wakes them, and the count is read back
        /// before the release, so the burst is observed rather than hoped for.
        /// </remarks>
        [Test]
        public void BurstCompletionDoesNotAllocatePerPark()
        {
            const int Rounds = 6;
            const int OkReplyLength = 5;
            const int BudgetBytesPerCommand = 88;
            const int GateAttempts = 30_000;
            const int FewConnections = 8;
            const int ManyConnections = 64;

            var clients = new RawRespClient[ManyConnections];
            using var controlConnection = new RawRespClient(TestUtils.EndPoint);
            try
            {
                for (var i = 0; i < ManyConnections; i++)
                    clients[i] = new RawRespClient(TestUtils.EndPoint);

                var block = RawRespClient.Command("DEBUG", "BLOCK", "0", "GATE");
                var release = RawRespClient.Command("DEBUG", "BLOCK", "0", "RELEASE");
                var gateCount = RawRespClient.Command("DEBUG", "BLOCK", "0", "GATECOUNT");

                // Allocation-free on purpose: this runs inside the measured window, so anything it
                // allocated would be charged to the server.
                int Control(byte[] command)
                {
                    controlConnection.SendRaw(command);
                    return controlConnection.ReadInteger();
                }

                void Round(int connections)
                {
                    for (var i = 0; i < connections; i++)
                        clients[i].SendRaw(block);

                    // Every connection is held parked until this reads them all, so the release below wakes
                    // the whole set at once instead of whatever happened to still be waiting. The polling
                    // runs without asserting, because an assertion that boxes would be charged to the
                    // server by the measurement wrapped around this.
                    var attempts = 0;
                    while (Control(gateCount) < connections && attempts < GateAttempts)
                    {
                        attempts++;
                        Thread.Sleep(1);
                    }

                    var released = Control(release);

                    for (var i = 0; i < connections; i++)
                        clients[i].Consume(OkReplyLength);

                    ClassicAssert.Less(attempts, GateAttempts, "Not every connection reached the gate");
                    ClassicAssert.AreEqual(connections, released, "Release woke the wrong number of waiters");
                }

                // Total for the round, not per connection: a round also pays a fixed cost for the control
                // round-trips that gate it, and dividing that by the connection count would make it look
                // like a per-connection cost that shrinks as concurrency rises. Taking the cheapest round
                // settles both levels on the same fixed cost -- one gate read and one release -- so
                // differencing them below removes it exactly.
                double Measure(int connections)
                {
                    Round(connections);

                    var best = double.MaxValue;
                    for (var round = 0; round < Rounds; round++)
                    {
                        var before = GC.GetTotalAllocatedBytes(precise: true);
                        Round(connections);
                        var measured = GC.GetTotalAllocatedBytes(precise: true) - before;

                        if (measured < best)
                            best = measured;
                    }

                    return best;
                }

                var few = Measure(FewConnections);
                var many = Measure(ManyConnections);
                var marginal = (many - few) / (double)(ManyConnections - FewConnections);

                TestContext.Out.WriteLine($"burst park and resume allocated {marginal:F1} bytes per " +
                                          $"additional simultaneously parked connection ({few:F0} bytes at " +
                                          $"{FewConnections} connections, {many:F0} at {ManyConnections})");

                // Each additional connection parked at the same instant must cost one context and nothing
                // else. A work-item box, a queue node, or a continuation taken only under contention is
                // invisible to the steady-state test -- it has no contention -- and invisible to an absolute
                // per-round budget, which the fixed control cost dominates. It is visible here.
                ClassicAssert.Less(marginal, BudgetBytesPerCommand,
                    $"Each additional simultaneously parked connection allocated {marginal:F1} bytes");
            }
            finally
            {
                foreach (var client in clients)
                    client?.Dispose();
            }
        }

        /// <summary>
        /// An operation that cannot start must unpark its session and report the failure, not leave the
        /// connection waiting for a reply that will never come.
        /// </summary>
        /// <remarks>
        /// The session is already parked by the time the operation is asked to start, so a failure there is
        /// the one error path that cannot be reported by returning. The base class owns both halves -- the
        /// release and the reply -- because a command that failed to start has no state with which to
        /// describe itself. The connection is used again afterwards to show it was left usable, not merely
        /// answered.
        /// </remarks>
        [Test]
        public void FailureToStartUnparksTheSessionAndReportsAnError()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            client.Send(RawRespClient.Command("DEBUG", "BLOCK", "0", "FAILSTART"));
            StringAssert.StartsWith("-ERR", client.ReadLine());

            client.Send(RawRespClient.Command("PING"));
            ClassicAssert.AreEqual("+PONG", client.ReadLine());

            // The same connection can still park successfully afterwards: a failed start leaves no residue.
            client.Send(RawRespClient.Command("DEBUG", "BLOCK", "0"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());
        }

        /// <summary>
        /// Recovery failing on top of a failed start must still unpark the session.
        /// </summary>
        /// <remarks>
        /// This is the second-order failure the obvious implementation gets wrong: calling the command's own
        /// recovery hook and then releasing, so that a throw from the hook skips the release and parks the
        /// connection forever. Releasing is unconditional precisely because recovery is command-supplied
        /// code that the machinery cannot vouch for.
        /// </remarks>
        [Test]
        public void FailureInsideFailedStartRecoveryStillUnparksTheSession()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            client.Send(RawRespClient.Command("DEBUG", "BLOCK", "0", "FAILSTART2"));
            StringAssert.StartsWith("-ERR", client.ReadLine());

            client.Send(RawRespClient.Command("PING"));
            ClassicAssert.AreEqual("+PONG", client.ReadLine());
        }

        /// <summary>
        /// A blocking command whose operation finishes before the receive stack has unwound must still
        /// resume exactly once, and in order, when pipelined behind and ahead of ordinary commands.
        /// </summary>
        /// <remarks>
        /// This is the half of the park rendezvous that a timed wait never exercises: the completion arrives
        /// first and has to wait for the receive path to hand over its state, rather than the other way
        /// round. Pipelining a batch in a single write forces the resume to drain buffered commands it has
        /// never parsed, which is where an ordering mistake would show up.
        /// </remarks>
        [Test]
        public void CompletionBeforeParkHandoffPreservesOrdering()
        {
            const int Iterations = 200;

            using var client = new RawRespClient(TestUtils.EndPoint);

            for (var i = 0; i < Iterations; i++)
            {
                // One write, so the server sees ECHO/DEBUG BLOCK/ECHO already buffered and the resume has to
                // pick up the trailing command without re-reading the socket.
                client.Send(RawRespClient.Command("ECHO", "before" + i),
                            RawRespClient.Command("DEBUG", "BLOCK", "0"),
                            RawRespClient.Command("ECHO", "after" + i));

                var before = "before" + i;
                ClassicAssert.AreEqual("$" + before.Length, client.ReadLine());
                ClassicAssert.AreEqual(before, client.ReadLine());
                ClassicAssert.AreEqual("+OK", client.ReadLine());
                var after = "after" + i;
                ClassicAssert.AreEqual("$" + after.Length, client.ReadLine());
                ClassicAssert.AreEqual(after, client.ReadLine());
            }
        }

        /// <summary>
        /// The point of parking: far more connections can be blocked at once than the server has threads, and
        /// unrelated connections keep being served while they are.
        /// </summary>
        /// <remarks>
        /// Waiting in place instead would hold one thread-pool thread per blocked connection. The pool starts
        /// at <see cref="Environment.ProcessorCount"/> threads and grows by only one or two per second beyond
        /// that, so this many simultaneous waits would take minutes to drain and would stall the unrelated
        /// connection below for most of it.
        /// </remarks>
        [Test]
        public void ManyBlockedSessionsDoNotExhaustThreads()
        {
            var sessionCount = Math.Clamp(Environment.ProcessorCount * 3, 256, 512);
            const int BlockSeconds = 2;

            var clients = new List<RawRespClient>(sessionCount);
            try
            {
                for (var i = 0; i < sessionCount; i++)
                    clients.Add(new RawRespClient(TestUtils.EndPoint));

                using var control = new RawRespClient(TestUtils.EndPoint);
                control.Send(RawRespClient.Command("PING"));
                ClassicAssert.AreEqual("+PONG", control.ReadLine());

                var threadsBefore = Process.GetCurrentProcess().Threads.Count;

                var sw = Stopwatch.StartNew();
                foreach (var client in clients)
                    client.Send(RawRespClient.Command("DEBUG", "BLOCK", BlockSeconds.ToString()));

                // Settle first. The sends only reach socket buffers, so probing immediately measures a
                // server that has not picked the blocking commands up yet and reports healthy either way.
                Thread.Sleep(500);

                // Every session is now blocked. A connection that is not must still be served promptly.
                var controlSw = Stopwatch.StartNew();
                control.Send(RawRespClient.Command("PING"));
                ClassicAssert.AreEqual("+PONG", control.ReadLine());
                controlSw.Stop();
                ClassicAssert.Less(controlSw.ElapsedMilliseconds, 2000,
                    $"Server was unresponsive while {sessionCount} sessions were blocked");

                var threadsDuring = Process.GetCurrentProcess().Threads.Count;

                foreach (var client in clients)
                    ClassicAssert.AreEqual("+OK", client.ReadLine());
                sw.Stop();

                // Two-sided on purpose. The upper bound is the parking claim: if every session is parked
                // they wait concurrently and the drain is one block long, whereas a thread each serializes
                // them into pool-sized waves and the drain becomes a multiple of one. The lower bound is
                // what keeps that claim honest -- a build where the sessions never waited at all drains
                // almost instantly and would satisfy every upper bound here while proving nothing.
                ClassicAssert.GreaterOrEqual(sw.ElapsedMilliseconds, (BlockSeconds * 1000) - 100,
                    $"{sessionCount} sessions drained in {sw.ElapsedMilliseconds}ms, faster than the "
                    + $"{BlockSeconds}s they were told to block: they never waited, so this test was not "
                    + "exercising parking at all");

                ClassicAssert.Less(sw.ElapsedMilliseconds, BlockSeconds * 2 * 1000,
                    $"{sessionCount} blocked sessions took {sw.ElapsedMilliseconds}ms to drain, "
                    + $"which is more than the {BlockSeconds}s they waited for: they did not wait at once");

                ClassicAssert.Less(threadsDuring - threadsBefore, sessionCount / 2,
                    $"Blocking {sessionCount} sessions added {threadsDuring - threadsBefore} threads");
            }
            finally
            {
                foreach (var client in clients)
                    client.Dispose();
            }
        }

        /// <summary>
        /// A client that vanishes mid-block must not leave the connection's receive state stranded, including
        /// when it disconnects in the same instant the session is parking.
        /// </summary>
        [Test]
        public void DisconnectWhileBlockedIsCleanedUp()
        {
            // No delay, so teardown lands anywhere in the park: before the command is dispatched, between
            // parking and the receive stack unwinding, or after.
            for (var i = 0; i < 200; i++)
            {
                var client = new RawRespClient(TestUtils.EndPoint);
                client.Send(RawRespClient.Command("DEBUG", "BLOCK", "30"));
                client.Dispose();
            }

            // And with the park reliably complete first.
            for (var i = 0; i < 10; i++)
            {
                var client = new RawRespClient(TestUtils.EndPoint);
                client.Send(RawRespClient.Command("DEBUG", "BLOCK", "30"));
                Thread.Sleep(20);
                client.Dispose();
            }

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            ClassicAssert.AreEqual("PONG", db.Execute("PING").ToString());
        }

        /// <summary>
        /// CLIENT KILL must take effect while the target is parked, rather than leaving the session alive on
        /// the server until its blocking operation would have finished on its own.
        /// </summary>
        [Test]
        public void ClientKillWhileBlockedTakesEffectImmediately()
        {
            using var victim = new RawRespClient(TestUtils.EndPoint);
            victim.Send(RawRespClient.Command("CLIENT", "ID"));
            var idReply = victim.ReadLine();
            ClassicAssert.IsTrue(idReply.StartsWith(':'), $"Unexpected CLIENT ID reply: {idReply}");
            var victimId = $"id={idReply[1..]} ";

            victim.Send(RawRespClient.Command("DEBUG", "BLOCK", "30"));
            Thread.Sleep(200);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            ClassicAssert.IsTrue(db.Execute("CLIENT", "LIST").ToString().Contains(victimId),
                "Parked session was missing from CLIENT LIST before the kill");
            ClassicAssert.AreEqual(1, (int)db.Execute("CLIENT", "KILL", "ID", idReply[1..]));

            // The session is parked on a 30 second wait, so it can only leave CLIENT LIST this quickly if the
            // kill woke it rather than waiting the operation out.
            var elapsed = Stopwatch.StartNew();
            while (db.Execute("CLIENT", "LIST").ToString().Contains(victimId))
            {
                ClassicAssert.Less(elapsed.Elapsed.TotalSeconds, 10,
                    "Killed session stayed registered; it was not woken from its parked state");
                Thread.Sleep(20);
            }

            ClassicAssert.AreEqual("PONG", db.Execute("PING").ToString());
        }

        /// <summary>
        /// Tearing the server down while resumes are in flight must not reclaim connection state underneath
        /// them.
        /// </summary>
        /// <remarks>
        /// A resume runs on a pool thread and uses the session, the network sender and the pooled receive
        /// buffer. Teardown runs on a different thread and frees exactly those. Checking for disposal at the
        /// top of the resume cannot fix this -- it is check-then-act, and teardown can land immediately
        /// after the check -- so reclamation has to be deferred until the resume lets go. The zero-length
        /// block here is what makes the window wide: every connection is resuming, rather than waiting, at
        /// the moment the server is disposed. Failures surface as the buffer-pool leak check in
        /// <see cref="TearDown"/>, as a crash in the resume, or as a hang.
        /// </remarks>
        [Test]
        public void ServerDisposeRacingInFlightResumes()
        {
            server.Dispose();
            server = null;

            for (var iteration = 0; iteration < 25; iteration++)
            {
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                var racing = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
                racing.Start();

                var clients = new List<RawRespClient>();
                try
                {
                    var block = RawRespClient.Command("DEBUG", "BLOCK", "0");
                    for (var i = 0; i < 32; i++)
                    {
                        var client = new RawRespClient(TestUtils.EndPoint);
                        clients.Add(client);

                        // Keep parking and resuming without reading the replies, so the server always has
                        // resumes in flight when the dispose below lands.
                        for (var j = 0; j < 8; j++)
                            client.SendRaw(block);
                    }
                }
                catch (Exception ex) when (ex is SocketException or IOException)
                {
                    // The server may already be going away; that is the condition under test.
                }

                racing.Dispose();

                foreach (var client in clients)
                    client.Dispose();
            }
        }

        /// <summary>
        /// A kill that lands while a session is still establishing its park must take effect, not be missed.
        /// </summary>
        /// <remarks>
        /// Killing a connection closes its socket but does not run the handler's own teardown, so the park
        /// cannot discover it by checking whether the handler was disposed. If the kill is also not latched
        /// where the park can reconcile it after publishing its state, a kill arriving in that window is lost
        /// and the session waits out its full operation with no receive outstanding to notice the client is
        /// gone -- forever, for a wait with no deadline. Each iteration below fires the kill at a different
        /// point relative to the park so the window is actually hit, and the long block means a missed kill
        /// shows up as a timeout rather than as a slow pass.
        /// </remarks>
        [Test]
        public void ClientKillRacingParkEstablishmentTakesEffect()
        {
            const int Iterations = 60;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            for (var i = 0; i < Iterations; i++)
            {
                using var victim = new RawRespClient(TestUtils.EndPoint);
                victim.Send(RawRespClient.Command("CLIENT", "ID"));
                var victimId = victim.ReadLine()[1..];

                // Blocks for long enough that only the kill can end it.
                victim.Send(RawRespClient.Command("DEBUG", "BLOCK", "30"));

                // Walk the delay across the park window: zero lands while the command is still being
                // dispatched, the larger values after it has parked.
                if (i % 3 == 1)
                    Thread.Yield();
                else if (i % 3 == 2)
                    Thread.Sleep(1);

                ClassicAssert.AreEqual(1, (int)db.Execute("CLIENT", "KILL", "ID", victimId),
                    $"Kill did not match the parked session on iteration {i}");

                var marker = $"id={victimId} ";
                var elapsed = Stopwatch.StartNew();
                while (db.Execute("CLIENT", "LIST").ToString().Contains(marker))
                {
                    ClassicAssert.Less(elapsed.Elapsed.TotalSeconds, 10,
                        $"Killed session stayed registered on iteration {i}; the kill was lost against the park");
                    Thread.Sleep(5);
                }
            }

            ClassicAssert.AreEqual("PONG", db.Execute("PING").ToString());
        }

        /// <summary>
        /// An abort that arrives while the operation is still starting must still leave the session released
        /// exactly once, even when the operation it was trying to cancel completes anyway.
        /// </summary>
        /// <remarks>
        /// <c>SLOWSTART</c> holds the command inside <c>OnStart</c> long enough for the kill below to land
        /// there deterministically, and the completion it then arms is uncancellable -- the shape a broker
        /// handoff has, where cancelling cannot un-produce an item already on its way. That combination is
        /// what makes the deferred-abort path observable: the abort is recorded against a half-built
        /// operation and applied once it is coherent, and the operation's own completion arrives afterwards.
        /// If applying the deferred abort does not also claim the outcome, that later completion claims it
        /// instead, overwrites the published result, and releases the session a second time -- which
        /// double-counts the park rendezvous and would resume the session's <em>next</em> park early. The
        /// assertion in <c>BlockingCommandContext.ReleaseSession</c> fails the run when that happens.
        /// </remarks>
        [Test]
        public void AbortDuringStartIsAppliedExactlyOnceWhenTheOperationCompletesAnyway()
        {
            const int Iterations = 12;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            for (var i = 0; i < Iterations; i++)
            {
                using var victim = new RawRespClient(TestUtils.EndPoint);
                victim.Send(RawRespClient.Command("CLIENT", "ID"));
                var victimId = victim.ReadLine()[1..];

                victim.Send(RawRespClient.Command("DEBUG", "BLOCK", "0", "SLOWSTART"));

                // Inside OnStart's 300ms window, so the abort is deferred rather than applied in place.
                Thread.Sleep(100);

                ClassicAssert.AreEqual(1, (int)db.Execute("CLIENT", "KILL", "ID", victimId),
                    $"Kill did not match the parked session on iteration {i}");

                var marker = $"id={victimId} ";
                var elapsed = Stopwatch.StartNew();
                while (db.Execute("CLIENT", "LIST").ToString().Contains(marker))
                {
                    ClassicAssert.Less(elapsed.Elapsed.TotalSeconds, 10,
                        $"Killed session stayed registered on iteration {i}");
                    Thread.Sleep(5);
                }
            }

            ClassicAssert.AreEqual("PONG", db.Execute("PING").ToString());
        }

        /// <summary>
        /// Disposing the server while sessions are parked must abort them and tear down cleanly, which the
        /// leak check in <see cref="TearDown"/> verifies.
        /// </summary>
        /// <remarks>
        /// Server teardown is the one caller that can dispose a handler on a thread other than the one
        /// running the park, so this runs it both after the park has settled and racing against it.
        /// </remarks>
        [Test]
        public void ServerDisposeWhileSessionsAreBlocked()
        {
            BlockThenDispose(server, settle: true);
            server = null;

            for (var i = 0; i < 20; i++)
            {
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                var racing = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
                racing.Start();
                BlockThenDispose(racing, settle: false);
            }

            static void BlockThenDispose(GarnetServer target, bool settle)
            {
                var clients = new List<RawRespClient>();
                try
                {
                    for (var i = 0; i < 16; i++)
                    {
                        var client = new RawRespClient(TestUtils.EndPoint);
                        client.Send(RawRespClient.Command("DEBUG", "BLOCK", "30"));
                        clients.Add(client);
                    }

                    if (settle)
                        Thread.Sleep(200);

                    target.Dispose();
                }
                finally
                {
                    foreach (var client in clients)
                        client.Dispose();
                }
            }
        }

        /// <summary>
        /// Tearing the server down while an operation is still starting must not release that operation's
        /// resources before it has created them.
        /// </summary>
        /// <remarks>
        /// Server teardown runs the handler's reclamation on the disposing thread, and reclamation disposes
        /// the session, which claims and disposes whatever the session parked on. Nothing stops that from
        /// landing while the parking thread is still inside the operation's start, because the context is
        /// published before the operation is started -- deliberately, so a teardown always finds something
        /// to abort. The ordering fix is for disposal to defer to the end of the start rather than run in
        /// the middle of it; without it a command that registers with a broker in its start registers after
        /// its only unregister, leaking the registration and the dead session it refers to for the lifetime
        /// of the process.
        /// <para>
        /// <c>SLOWSTART</c> widens the start to a window a test can aim at. The tripwire is the Debug
        /// assertion in the context itself rather than anything asserted here, because the damage is
        /// invisible from a client: the connection is being destroyed either way.
        /// </para>
        /// </remarks>
        [Test]
        public void ServerDisposeWhileAnOperationIsStartingDefersDisposal()
        {
            server.Dispose();
            server = null;

            for (var attempt = 0; attempt < 12; attempt++)
            {
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                var racing = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
                racing.Start();

                var clients = new List<RawRespClient>();
                try
                {
                    for (var i = 0; i < 8; i++)
                    {
                        var client = new RawRespClient(TestUtils.EndPoint);
                        client.Send(RawRespClient.Command("DEBUG", "BLOCK", "0", "SLOWSTART"));
                        clients.Add(client);
                    }

                    // Long enough for every session to have reached its start, short enough that none has
                    // left it.
                    Thread.Sleep(100);
                    racing.Dispose();
                }
                finally
                {
                    foreach (var client in clients)
                        client.Dispose();
                }
            }
        }

        /// <summary>
        /// A completion that has won the outcome but has not yet published its result still owns that
        /// result, so disposal must wait for it rather than reclaiming an empty context and leaking what
        /// arrives a moment later.
        /// </summary>
        /// <remarks>
        /// Guarding construction alone is not enough. Winning the claim and publishing the result are two
        /// steps, and between them the context looks finished to anyone reading its state while its result
        /// field is still empty -- so a teardown landing there disposes a context that has nothing to
        /// reclaim, and the result published immediately afterwards is never handed back.
        /// <para>
        /// <c>DEBUG BLOCK</c>'s own reply is a constant, which owns nothing and so cannot show this; the
        /// owned result here stands in for what a real blocking command returns -- <c>BLPOP</c>'s rented
        /// reply buffer, a borrowed object reference. <c>HOLDCLAIM</c> pins the winner in the window so the
        /// race is run rather than hoped for.
        /// </para>
        /// </remarks>
        [Test]
        public void DisposeDuringResultPublicationDoesNotLeakTheResult()
            => AssertDisposeInsidePublicationWindowReclaimsTheResult("HOLDCLAIM", "CLAIMHELD", "CLAIMPROCEED",
                   trigger: null);

        /// <summary>
        /// The same property for the other owner of an outcome. An abort claims and publishes exactly as a
        /// completion does, so it needs the same protection from disposal.
        /// </summary>
        /// <remarks>
        /// This is the half that is easy to leave behind, because the abort path reaches publication by its
        /// own route rather than through the completion's claim. The abort is delivered from the thread pool
        /// rather than from a session or from the thread tearing the server down: either of those would be
        /// pinned inside <c>OnAbort</c> along with everything else it owed, deadlocking the teardown this
        /// test runs underneath it.
        /// </remarks>
        [Test]
        public void DisposeDuringAbortPublicationDoesNotLeakTheResult()
            => AssertDisposeInsidePublicationWindowReclaimsTheResult("HOLDABORT", "ABORTHELD", "ABORTPROCEED",
                   trigger: "ABORTGATED");

        /// <summary>
        /// Pins an outcome owner inside its publication window, disposes the server underneath it, then lets
        /// it publish and asserts the result it produced was still reclaimed.
        /// </summary>
        /// <param name="mode">Start modifier that pins the owner.</param>
        /// <param name="heldControl">Control that reports, and consumes, the pinned signal.</param>
        /// <param name="proceedControl">Control that releases the pinned owner.</param>
        /// <param name="trigger">Control that delivers the outcome, for owners that need one.</param>
        void AssertDisposeInsidePublicationWindowReclaimsTheResult(string mode, string heldControl,
            string proceedControl, string trigger)
        {
            int Poll(RawRespClient client, string control)
            {
                client.Send(RawRespClient.Command("DEBUG", "BLOCK", "0", control));
                return client.ReadInteger();
            }

            void WaitUntil(Func<bool> condition, string message)
            {
                var elapsed = Stopwatch.StartNew();
                while (!condition())
                {
                    ClassicAssert.Less(elapsed.Elapsed.TotalSeconds, 20, message);
                    Thread.Sleep(5);
                }
            }

            // The leak counter is process-wide, so another test's leak would otherwise be read as this
            // one's. Taken once, outside the loop, so a leak in an earlier attempt cannot raise the bar the
            // later attempts have to clear.
            int leaked;
            using (var baseline = new RawRespClient(TestUtils.EndPoint))
                leaked = Poll(baseline, "LEAKCOUNT");

            for (var attempt = 0; attempt < 5; attempt++)
            {
                int publishedBefore;

                using (var victim = new RawRespClient(TestUtils.EndPoint))
                using (var control = new RawRespClient(TestUtils.EndPoint))
                {
                    victim.Send(RawRespClient.Command("DEBUG", "BLOCK", "30", mode));

                    if (trigger != null)
                    {
                        // The owner has to be parked before the outcome is delivered, or the delivery finds
                        // nothing and the window is never entered.
                        WaitUntil(() => Poll(control, "GATECOUNT") == 1, "Victim never parked");
                        ClassicAssert.AreEqual(1, Poll(control, trigger));
                    }

                    WaitUntil(() => Poll(control, heldControl) == 1,
                        "The outcome owner never reached the publication window");

                    // Read after the window is entered and before anything is torn down, so the comparison
                    // below cannot be satisfied by a publication that had already happened.
                    publishedBefore = Poll(control, "ACQCOUNT");

                    // Teardown reaches a parked context only through a path that disposes the session
                    // itself. A closed socket does not: a parked connection has no outstanding receive, so
                    // nothing notices. Nor does CLIENT KILL, whose abort defers to the owner already holding
                    // the outcome and so only disposes once that owner has released the session -- after the
                    // window. Disposing the server is the path that lands inside it.
                    server.Dispose();
                }

                // The counters the assertions below read are process-wide, so a fresh server observes what
                // the disposed one left behind.
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
                server.Start();

                using var prober = new RawRespClient(TestUtils.EndPoint);
                ClassicAssert.AreEqual(1, Poll(prober, proceedControl));

                // Publication has to be observed before reclamation is checked. Checking first would read
                // the zero that was there before the result existed and pass without the race being run.
                WaitUntil(() => Poll(prober, "ACQCOUNT") > publishedBefore,
                    "The pinned owner never published its result");

                WaitUntil(() => Poll(prober, "LEAKCOUNT") == leaked,
                    "A result published after the outcome was claimed was never handed back");
            }
        }

        /// <summary>
        /// Resumes can overlap, and teardown must not reclaim the connection out from under a later one.
        /// </summary>
        /// <remarks>
        /// A resume that hands its buffer to an asynchronous receive still has to drop its lease afterwards,
        /// and that receive can complete, park, and schedule the next resume in between. The lease therefore
        /// counts owners rather than flagging one; with a flag the first resume's release frees a lease the
        /// second is relying on and teardown reclaims underneath it. The window is a few instructions wide,
        /// so this drives traffic shaped to land receives inside it and leans on the leak counter and the
        /// contract tripwires in teardown to report what it hits rather than asserting on the interleaving
        /// directly.
        /// </remarks>
        [Test]
        public void OverlappingResumesSurviveTeardown()
        {
            const int Connections = 24;
            const int CommandsPerConnection = 12;

            using var probe = new RawRespClient(TestUtils.EndPoint);
            probe.Send(RawRespClient.Command("DEBUG", "BLOCK", "0", "LEAKCOUNT"));
            var leakedBefore = int.Parse(probe.ReadLine().TrimStart(':'));

            var threads = new Thread[Connections];
            var failures = new List<string>();

            for (var t = 0; t < Connections; t++)
            {
                var seed = t;
                threads[t] = new Thread(() =>
                {
                    var rng = new Random(seed);
                    try
                    {
                        using var client = new RawRespClient(TestUtils.EndPoint);
                        for (var i = 0; i < CommandsPerConnection; i++)
                        {
                            // Short blocks, sent one at a time, so the next command tends to arrive while
                            // the previous resume is between arming its receive and dropping its lease.
                            client.Send(RawRespClient.Command("DEBUG", "BLOCK", "0.01"));
                            var reply = client.ReadLine();
                            if (reply != "+OK")
                            {
                                lock (failures)
                                    failures.Add($"connection {seed} command {i} replied {reply}");
                                return;
                            }

                            if (rng.Next(4) == 0)
                                Thread.Sleep(1);
                        }
                    }
                    catch (Exception ex)
                    {
                        lock (failures)
                            failures.Add($"connection {seed} threw {ex.GetType().Name}: {ex.Message}");
                    }
                })
                { IsBackground = true };
            }

            foreach (var thread in threads)
                thread.Start();
            foreach (var thread in threads)
                ClassicAssert.IsTrue(thread.Join(TimeSpan.FromSeconds(60)), "A connection thread hung");

            CollectionAssert.IsEmpty(failures, string.Join("; ", failures));

            probe.Send(RawRespClient.Command("DEBUG", "BLOCK", "0", "LEAKCOUNT"));
            ClassicAssert.AreEqual(leakedBefore, int.Parse(probe.ReadLine().TrimStart(':')),
                "Overlapping resumes leaked an operation result");

            probe.Send(RawRespClient.Command("PING"));
            ClassicAssert.AreEqual("+PONG", probe.ReadLine());
        }

        /// <summary>
        /// Shutdown must not end up waiting behind a command that still waits in place, when that command
        /// was drained by a resume.
        /// </summary>
        /// <remarks>
        /// Reclaiming connection state is deferred while a resume is in flight, and a resume that drains a
        /// pipelined command which has not been migrated off in-place waiting is in flight for as long as
        /// that wait lasts. Those waits end when the session is disposed -- which is part of the very
        /// reclamation being deferred -- so delivering that cancellation late deadlocks shutdown outright.
        /// </remarks>
        [Test]
        public void ShutdownIsNotBlockedByAnInPlaceWaitBehindAParkedCommand()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            // One write, so the BLPOP is already buffered behind the blocking command and is drained by the
            // resume rather than by a fresh receive. That is what puts an in-place wait under the lease.
            client.Send(
                RawRespClient.Command("DEBUG", "BLOCK", "0"),
                RawRespClient.Command("BLPOP", "no-such-list-for-shutdown", "0"));

            ClassicAssert.AreEqual("+OK", client.ReadLine());

            // Let the resume reach the BLPOP and settle into its wait.
            Thread.Sleep(500);

            var shuttingDown = server;
            server = null;
            var shutdown = new Thread(shuttingDown.Dispose) { IsBackground = true };
            shutdown.Start();

            ClassicAssert.IsTrue(shutdown.Join(TimeSpan.FromSeconds(30)),
                "Server shutdown hung. Reclamation was deferred behind an in-place wait that only ends when "
                + "the session is disposed, and that disposal is part of the deferred reclamation.");
        }

        /// <summary>
        /// The same cycle again, closed this time not by teardown but by the thing that ends the wait
        /// ceasing to exist.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <c>ASYNC BARRIER</c> waits for the session's async GET processor to report its pending reads
        /// done. That processor is launched fire-and-forget, so a throw out of its drain loop has no-one to
        /// observe it and is swallowed, and the release the barrier is waiting for never happens. Nothing
        /// else in the process can end that wait.
        /// </para>
        /// <para>
        /// The fault is reached without injecting anything. <c>CLIENT KILL</c> closes the socket but does
        /// not reclaim the session -- the resume lease keeps the sender and the store alive, but it cannot
        /// make a closed socket writable -- so the processor's next TLS write throws. The client stops
        /// reading first so that the processor is already blocked mid-send against a full socket when the
        /// kill lands, which is what makes the fault land there rather than racing the drain.
        /// </para>
        /// </remarks>
        [Test]
        public void ShutdownIsNotBlockedByABarrierWhoseAsyncProcessorFaulted()
        {
            var endPoint = StartTlsServer(lowMemory: true);

            // Enough large values that reads miss memory and go pending, and that their replies cannot fit
            // in the socket buffer the reading client below asks for.
            const int KeyCount = 256;
            var keys = LoadColdKeys(endPoint, KeyCount, useTls: true);

            using var client = new RawRespClient(endPoint, useTls: true, receiveBufferSize: 1024);

            // RESP3 is a precondition of ASYNC. PING trails it so the handshake can be drained without
            // depending on the exact shape of the HELLO map.
            client.Send(RawRespClient.Command("HELLO", "3"), RawRespClient.Command("PING"));
            while (client.ReadLine() != "+PONG") { }

            client.Send(RawRespClient.Command("CLIENT", "ID"));
            var clientId = long.Parse(client.ReadLine().AsSpan(1));

            client.Send(RawRespClient.Command("ASYNC", "ON"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            // One write, so the whole batch is drained by the resume rather than by fresh receives: the
            // barrier then runs on the resume thread and holds the lease while it waits.
            var pipeline = new List<byte[]> { RawRespClient.Command("DEBUG", "BLOCK", "0") };
            foreach (var key in keys)
                pipeline.Add(RawRespClient.Command("GET", key));
            pipeline.Add(RawRespClient.Command("ASYNC", "BARRIER"));
            client.Send([.. pipeline]);

            // Deliberately read nothing from here on. The processor fills the socket and blocks in its send.
            Thread.Sleep(2000);

            using (var killer = new RawRespClient(endPoint, useTls: true))
            {
                killer.Send(RawRespClient.Command("CLIENT", "KILL", "ID", clientId.ToString()));
                ClassicAssert.AreEqual(":1", killer.ReadLine());
            }

            var shuttingDown = tlsServer;
            tlsServer = null;
            var shutdown = new Thread(shuttingDown.Dispose) { IsBackground = true };
            shutdown.Start();

            ClassicAssert.IsTrue(shutdown.Join(TimeSpan.FromSeconds(30)),
                "Server shutdown hung. The async GET processor faulted on the closed socket, so the release "
                + "the barrier was waiting for never came, and the resume holding the lease never returned.");
        }

        /// <summary>
        /// SELECT must be refused once a session has started asynchronous operations, rather than silently
        /// detaching the async GET processor from the session it drains.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The processor captures the session's storage API once, when it starts, and polls that capture for
        /// the rest of its life. SELECT replaces the field without telling it. Reads issued after the switch
        /// then complete on a context the processor never looks at, so its completion count can never catch
        /// its started count: it spins at full CPU, and any ASYNC BARRIER behind it waits forever.
        /// </para>
        /// <para>
        /// On a parked session that is worse than a stuck connection. A barrier reached on a resume holds the
        /// resume lease while it waits, and the lease is what teardown waits for, so the spin becomes a server
        /// shutdown that never completes. Refusing the switch costs a combination that does not work today and
        /// removes the hang.
        /// </para>
        /// </remarks>
        [Test]
        public void SelectIsRefusedOnceAsyncOperationsHaveStarted()
        {
            var endPoint = StartLowMemoryServer();

            using var client = new RawRespClient(endPoint);

            client.Send(RawRespClient.Command("HELLO", "3"), RawRespClient.Command("PING"));
            while (client.ReadLine() != "+PONG") { }

            // Written before the bulk below so it is the oldest record in the log, and so certain to have been
            // evicted by the time it is read back. Small, so its pushed reply is easy to drain.
            client.Send(RawRespClient.Command("SET", "select-probe", "v"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());
            _ = LoadColdKeys(endPoint, keyCount: 256);

            // Selecting before anything is async is unaffected.
            client.Send(RawRespClient.Command("SELECT", "1"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());
            client.Send(RawRespClient.Command("SELECT", "0"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            client.Send(RawRespClient.Command("ASYNC", "ON"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            // Cold, so the read goes pending: the token in place of a value is what says the processor has
            // started against this session's storage API. The barrier then drains it.
            client.Send(RawRespClient.Command("GET", "select-probe"));
            ClassicAssert.IsTrue(client.ReadLine().StartsWith("-ASYNC", StringComparison.Ordinal),
                "The read completed from memory, so no async GET processor was started and the test would "
                + "not have exercised the guard.");
            client.Send(RawRespClient.Command("ASYNC", "BARRIER"));
            while (client.ReadLine() != "+OK") { }

            // Re-selecting the database already in use changes nothing, so it stays allowed.
            client.Send(RawRespClient.Command("SELECT", "0"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            client.Send(RawRespClient.Command("SELECT", "1"));
            var reply = client.ReadLine();
            ClassicAssert.IsTrue(reply.StartsWith("-ERR", StringComparison.Ordinal),
                $"SELECT to a different database was accepted on a session with a running async GET processor. "
                + $"The processor would then drain a database the session no longer uses. Reply was: {reply}");

            // The session is still usable; only the switch was refused.
            client.Send(RawRespClient.Command("GET", "select-probe"), RawRespClient.Command("ASYNC", "BARRIER"));
            while (client.ReadLine() != "+OK") { }
        }

        /// <summary>
        /// SWAPDB must be refused once a session has started asynchronous operations, for the same reason
        /// SELECT is, and only when the swap would actually move the database the session is using.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Swapping the database behind the active ID replaces the active database session, which rebinds the
        /// storage API out from under the async GET processor exactly as SELECT does. The processor goes on
        /// polling the capture it took when it started, so reads issued afterwards complete somewhere it never
        /// looks and its completion count can never catch its started count.
        /// </para>
        /// <para>
        /// This guard covers the calling session only, and is not by itself what makes a swap safe:
        /// <c>TrySwapDatabases</c> preflights every <em>published</em> session it can observe before it
        /// mutates anything, because its original walk rebound the first session it reached before the second
        /// could refuse. Published is the operative word -- a connection still under construction is not in
        /// <c>ActiveConsumers()</c> and the preflight cannot see it. The caller-side guard is
        /// kept for the error message, since the manager's refusal surfaces as the catch-all
        /// &quot;multiple clients are connected&quot;. See <see cref="RefusedSwapdbLeavesTheDatabasesAlone"/>.
        /// A swap between two databases this session is not using rebinds nothing and stays allowed.
        /// </para>
        /// </remarks>
        [Test]
        public void SwapdbIsRefusedOnceAsyncOperationsHaveStarted()
        {
            var endPoint = StartLowMemoryServer();

            using var client = new RawRespClient(endPoint);

            client.Send(RawRespClient.Command("HELLO", "3"), RawRespClient.Command("PING"));
            while (client.ReadLine() != "+PONG") { }

            // The server starts with a single-database manager, which refuses every swap, and promotes
            // itself the first time a database other than zero is touched.
            client.Send(RawRespClient.Command("SELECT", "1"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());
            client.Send(RawRespClient.Command("SELECT", "0"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            // Swapping before anything is async is unaffected. Done first, and over this one connection,
            // because SWAPDB refuses outright while a second client is connected.
            SwapDatabases(client, 0, 1);
            SwapDatabases(client, 0, 1);

            // Written before the bulk below so it is the oldest record in the log, and so certain to have been
            // evicted by the time it is read back.
            client.Send(RawRespClient.Command("SET", "swapdb-probe", "v"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            var filler = new string('v', 16 * 1024);
            for (var i = 0; i < 256; i++)
            {
                client.Send(RawRespClient.Command("SET", $"cold-key-{i:D4}", filler));
                ClassicAssert.AreEqual("+OK", client.ReadLine());
            }

            client.Send(RawRespClient.Command("ASYNC", "ON"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            // Cold, so the read goes pending: the token in place of a value is what says the processor has
            // started against this session's storage API. The barrier then drains it.
            client.Send(RawRespClient.Command("GET", "swapdb-probe"));
            ClassicAssert.IsTrue(client.ReadLine().StartsWith("-ASYNC", StringComparison.Ordinal),
                "The read completed from memory, so no async GET processor was started and the test would "
                + "not have exercised the guard.");
            client.Send(RawRespClient.Command("ASYNC", "BARRIER"));
            while (client.ReadLine() != "+OK") { }

            // A swap that leaves the active database alone rebinds nothing, so it stays allowed.
            SwapDatabases(client, 2, 3);

            client.Send(RawRespClient.Command("SWAPDB", "0", "1"));
            var reply = client.ReadLine();
            ClassicAssert.IsTrue(reply.Contains("asynchronous operations", StringComparison.Ordinal),
                "SWAPDB of the database in use was accepted on a session with a running async GET processor. "
                + "The processor would then drain a database the session no longer uses. Reply was: "
                + $"{reply}");

            // The session is still usable; only the swap was refused.
            client.Send(RawRespClient.Command("GET", "swapdb-probe"), RawRespClient.Command("ASYNC", "BARRIER"));
            while (client.ReadLine() != "+OK") { }
        }

        /// <summary>
        /// A SWAPDB that is refused must not have swapped the databases anyway.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The manager used to rewrite both database map entries and only then walk the sessions to decide
        /// whether the swap was allowed, so every refusal still swapped the data -- the client was told the
        /// swap did not happen while its databases had already exchanged contents. The same ordering is what
        /// made guarding the caller insufficient for the async case: the walk rebinds the first session it
        /// reaches before it has seen enough sessions to refuse, and that session need not be the one that
        /// issued the command.
        /// </para>
        /// <para>
        /// A second connection is what triggers the refusal, and a third, opened afterwards, is what reads
        /// the result: a session that was already connected resolves its database sessions through its own
        /// cached map, so only a session created after the swap sees what the global map really holds.
        /// </para>
        /// </remarks>
        [Test]
        public void RefusedSwapdbLeavesTheDatabasesAlone()
        {
            var endPoint = StartLowMemoryServer();

            using var client = new RawRespClient(endPoint);

            // Promote the single-database manager, which refuses every swap, by touching a database other
            // than zero.
            client.Send(RawRespClient.Command("SELECT", "1"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            client.Send(RawRespClient.Command("SET", "swap-witness", "from-db1"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            client.Send(RawRespClient.Command("SELECT", "0"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            client.Send(RawRespClient.Command("SET", "swap-witness", "from-db0"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            // A second connection makes the swap refusable: the manager allows a swap only while one session
            // is attached.
            using (var second = new RawRespClient(endPoint))
            {
                second.Send(RawRespClient.Command("PING"));
                ClassicAssert.AreEqual("+PONG", second.ReadLine());

                client.Send(RawRespClient.Command("SWAPDB", "0", "1"));
                var reply = client.ReadLine();
                ClassicAssert.IsTrue(reply.StartsWith("-ERR", StringComparison.Ordinal),
                    $"SWAPDB was expected to be refused while a second client was connected. Reply was: {reply}");
            }

            // Opened after the refusal, so it resolves its database sessions from the global map rather than
            // from anything cached before the swap was attempted.
            using var observer = new RawRespClient(endPoint);

            observer.Send(RawRespClient.Command("SELECT", "0"));
            ClassicAssert.AreEqual("+OK", observer.ReadLine());
            observer.Send(RawRespClient.Command("GET", "swap-witness"));
            ClassicAssert.AreEqual("$8", observer.ReadLine());
            ClassicAssert.AreEqual("from-db0", observer.ReadLine(),
                "The refused SWAPDB swapped the databases anyway: database 0 is holding database 1's value.");

            observer.Send(RawRespClient.Command("SELECT", "1"));
            ClassicAssert.AreEqual("+OK", observer.ReadLine());
            observer.Send(RawRespClient.Command("GET", "swap-witness"));
            ClassicAssert.AreEqual("$8", observer.ReadLine());
            ClassicAssert.AreEqual("from-db1", observer.ReadLine(),
                "The refused SWAPDB swapped the databases anyway: database 1 is holding database 0's value.");
        }

        /// <summary>
        /// Issues SWAPDB and asserts it succeeded, retrying while the server still lists a recently closed
        /// connection as active, which SWAPDB refuses on.
        /// </summary>
        static void SwapDatabases(RawRespClient client, int index1, int index2)
        {
            var reply = string.Empty;
            for (var attempt = 0; attempt < 50; attempt++)
            {
                client.Send(RawRespClient.Command("SWAPDB", index1.ToString(), index2.ToString()));
                reply = client.ReadLine();
                if (reply == "+OK")
                    return;

                Thread.Sleep(100);
            }

            ClassicAssert.Fail($"SWAPDB {index1} {index2} never succeeded. Last reply was: {reply}");
        }

        /// <summary>
        /// The same cycle, but with a second in-place wait that registers *after* teardown has delivered its
        /// cancellation. Cancelling the waits that exist at that instant is not enough on its own.
        /// </summary>
        /// <remarks>
        /// Teardown notifies once. A resume draining a pipeline wakes from the first wait, carries on
        /// parsing, and registers the next one against a broker that has already been told this session is
        /// going away -- so that registration is never cancelled and the resume never drops its lease.
        /// Closing this needs a latch that teardown publishes before it notifies and that each wait
        /// re-reads after it has registered, so a registration can never fall between the two.
        /// </remarks>
        [Test]
        public void ShutdownIsNotBlockedByAWaitRegisteredAfterTeardownCancelled()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            // Two waits behind the parked command. The first absorbs teardown's single notification; the
            // second is the one that registers afterwards.
            client.Send(
                RawRespClient.Command("DEBUG", "BLOCK", "0"),
                RawRespClient.Command("BLPOP", "no-such-list-first", "0"),
                RawRespClient.Command("BLPOP", "no-such-list-second", "0"));

            ClassicAssert.AreEqual("+OK", client.ReadLine());

            // Let the resume reach the first wait and settle into it.
            Thread.Sleep(500);

            var shuttingDown = server;
            server = null;
            var shutdown = new Thread(shuttingDown.Dispose) { IsBackground = true };
            shutdown.Start();

            ClassicAssert.IsTrue(shutdown.Join(TimeSpan.FromSeconds(30)),
                "Server shutdown hung. The second in-place wait registered after teardown had already "
                + "delivered its cancellation, so nothing ever ended it and the resume never released its "
                + "lease.");
        }

        /// <summary>
        /// TLS is the transport most Garnet deployments actually run, so parking that did not work there
        /// would leave the thread-starvation problem exactly where it was for nearly every real server.
        /// </summary>
        /// <remarks>
        /// The TLS receive path reaches the session through a reader loop driven by unconsumed ciphertext,
        /// not through the synchronous receive, so it parks in a different place and resumes through a
        /// different entry. These tests are the plaintext ones re-run over TLS for that reason: the property
        /// is the same, the machinery underneath it is not.
        /// </remarks>
        [Test]
        public void DebugBlockParksAndResumesUnderTls()
        {
            var endPoint = StartTlsServer();
            using var client = new RawRespClient(endPoint, useTls: true);

            var elapsed = Stopwatch.StartNew();
            client.Send(RawRespClient.Command("DEBUG", "BLOCK", "1"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());
            elapsed.Stop();

            ClassicAssert.GreaterOrEqual(elapsed.ElapsedMilliseconds, 900,
                "The TLS session replied before its block elapsed, so it never actually waited");

            // The connection has to keep working afterwards: the reader loop was stopped mid-stream and
            // re-entered, which is where a mismanaged transport buffer or reader status would show up.
            client.Send(RawRespClient.Command("PING"));
            ClassicAssert.AreEqual("+PONG", client.ReadLine());
            client.Send(RawRespClient.Command("SET", "tls-key", "tls-value"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());
            client.Send(RawRespClient.Command("GET", "tls-key"));
            ClassicAssert.AreEqual("$9", client.ReadLine());
            ClassicAssert.AreEqual("tls-value", client.ReadLine());
        }

        /// <summary>
        /// Commands pipelined behind a blocking one are decrypted into the transport buffer before the park,
        /// so under TLS they are already plaintext sitting behind the parked command.
        /// </summary>
        /// <remarks>
        /// This is the case a reader-loop-only resume gets wrong. The loop is driven by unconsumed
        /// ciphertext, and a batch that arrived inside one TLS record leaves none, so a resume that only
        /// re-entered the reader would never look at the plaintext and the pipelined commands would hang
        /// until the client happened to send something else.
        /// </remarks>
        [Test]
        public void PipelineBehindBlockingCommandStaysOrderedUnderTls()
        {
            var endPoint = StartTlsServer();
            using var client = new RawRespClient(endPoint, useTls: true);

            // One write, so the whole batch is encrypted together and the tail arrives as plaintext with no
            // ciphertext behind it.
            client.Send(
                RawRespClient.Command("DEBUG", "BLOCK", "1"),
                RawRespClient.Command("ECHO", "first"),
                RawRespClient.Command("ECHO", "second"));

            ClassicAssert.AreEqual("+OK", client.ReadLine());
            ClassicAssert.AreEqual("$5", client.ReadLine());
            ClassicAssert.AreEqual("first", client.ReadLine());
            ClassicAssert.AreEqual("$6", client.ReadLine());
            ClassicAssert.AreEqual("second", client.ReadLine());
        }

        /// <summary>
        /// What parking costs in allocation on a TLS connection, measured against the same command not
        /// parking rather than against zero.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The plaintext resume is a pooled work item and allocates nothing, but the TLS resume is an
        /// <see langword="async"/> method that can suspend awaiting more ciphertext, and a suspended async
        /// method boxes its state machine. That is a real cost and this test exists to put a number on it
        /// instead of asserting a zero that TLS cannot deliver: <see cref="SslStream"/> allocates per read
        /// and per write on its own, so a park can only be asked to add little *beyond the transport*.
        /// </para>
        /// <para>
        /// The baseline is <c>DEBUG BLOCK 0 SYNC</c>, which is the same command producing the same reply
        /// over the same TLS records and differing only in that it waits in place instead of parking, so the
        /// transport and the parser are common to both and cancel out. The <c>SYNC</c> path does *not*
        /// construct a context -- it sleeps and replies inline -- so the difference is the context plus the
        /// park, the unpark and the resume-side TLS re-entry. <see cref="ParkAndResumeDoNotAllocate"/>
        /// establishes the context at 80 bytes independently, which is what lets the remainder be
        /// attributed to the TLS resume. <c>PING</c> is measured too, only to report what the transport
        /// costs by itself.
        /// </para>
        /// <para>
        /// Pipelining is then measured the same way: a resume that drains commands it never parsed must not
        /// cost more per command than one that drains none, which is what catches anything proportional to
        /// buffered bytes or to pipeline depth.
        /// </para>
        /// </remarks>
        [Test]
        public void ParkAndResumeUnderTlsCostNoMoreThanTheTransport()
        {
            const int Warmup = 500;
            const int Rounds = 8;
            const int CommandsPerRound = 1000;
            const int OkReplyLength = 5;
            const int PongReplyLength = 7;
            const int PipelineDepth = 8;

            // The TLS resume re-enters an async reader loop, and that is what the park may legitimately cost
            // over the identical non-parking command. Anything beyond this is a per-park object that should
            // not be there.
            //
            // The SYNC baseline sleeps and replies inline without building a context, so this budget covers
            // the context as well as the resume. ContextBytes is sizeof(DebugBlockCommandContext) in Debug,
            // pinned independently by ParkAndResumeDoNotAllocate, and is subtracted when reporting so the
            // TLS-specific remainder is visible rather than conflated with it.
            const int ContextBytes = 80;
            const int ParkBudgetBytesOverSyncWait = 288;

            var endPoint = StartTlsServer();
            using var client = new RawRespClient(endPoint, useTls: true);

            var park = RawRespClient.Command("DEBUG", "BLOCK", "0");
            var inPlace = RawRespClient.Command("DEBUG", "BLOCK", "0", "SYNC");
            var ping = RawRespClient.Command("PING");

            double Measure(byte[] unit, int replyLength, int depth)
            {
                var payload = Repeat(unit, depth);

                void Run(int commands)
                {
                    for (var i = 0; i < commands / depth; i++)
                    {
                        client.SendRaw(payload);
                        client.Consume(replyLength * depth);
                    }
                }

                Run(Warmup);

                // Allocation noise is additive, so the cheapest round is the one least polluted by whatever
                // else the process was doing.
                var best = double.MaxValue;
                for (var round = 0; round < Rounds; round++)
                {
                    var before = GC.GetTotalAllocatedBytes(precise: true);
                    Run(CommandsPerRound);
                    var measured = (GC.GetTotalAllocatedBytes(precise: true) - before) / (double)CommandsPerRound;

                    if (measured < best)
                        best = measured;
                }

                return best;
            }

            var transport = Measure(ping, PongReplyLength, 1);
            var waited = Measure(inPlace, OkReplyLength, 1);
            var parked = Measure(park, OkReplyLength, 1);
            var parkedBatched = Measure(park, OkReplyLength, PipelineDepth);

            var parkCost = parked - waited;

            // The SYNC baseline builds no context, so parkCost carries the context as well as the park. The
            // plaintext test pins the context at ContextBytes, which is what attributes the remainder.
            TestContext.Out.WriteLine(
                $"TLS: PING {transport:F1} bytes per command, DEBUG BLOCK 0 SYNC {waited:F1}, parked " +
                $"{parked:F1} ({parkCost:F1} over the in-place wait, of which {ContextBytes} is the context, " +
                $"leaving {parkCost - ContextBytes:F1} for the TLS resume), {parkedBatched:F1} at pipeline " +
                $"depth {PipelineDepth}");

            ClassicAssert.Less(parkCost, ParkBudgetBytesOverSyncWait,
                $"Parking a TLS session added {parkCost:F1} bytes per command over the {waited:F1} the same " +
                "command costs when it waits in place");

            // The same invariant the plaintext test asserts: draining seven commands the resume never parsed
            // must cost the same per command as draining none.
            ClassicAssert.Less(parkedBatched, parked + 1,
                $"Pipelining raised per-command allocation from {parked:F1} to {parkedBatched:F1} bytes under TLS");
        }

        /// <summary>
        /// A resume emits its replies from a thread-pool thread rather than the IO thread that received the
        /// command, and under TLS that means writing to <see cref="SslStream"/> from a different thread than
        /// the one that last used it. A reply large enough to span many TLS records exercises that across
        /// many separate writes and flushes rather than a single small one.
        /// </summary>
        /// <remarks>
        /// The argument that this is safe -- the receive stack has fully unwound, the reader is at rest, and
        /// a connection processes commands strictly in order, so nothing else can touch the stream -- is
        /// sound but is an argument. This makes it an observation: every element has to arrive intact and in
        /// order. Corruption, interleaving or truncation in the record layer shows up here as a mismatched
        /// element or a short read, neither of which a small reply would reveal.
        /// </remarks>
        [Test]
        public void LargeReplyFromAResumeThreadIsIntactUnderTls()
        {
            const int elements = 20_000;

            var endPoint = StartTlsServer();
            using var client = new RawRespClient(endPoint, useTls: true);

            // Seeded before the block so the whole reply is produced by the resume, not by this thread.
            for (var i = 0; i < elements; i++)
            {
                client.Send(RawRespClient.Command("RPUSH", "tls-big", Element(i)));
                _ = client.ReadLine();
            }

            // One write: the LRANGE is already plaintext behind the parked command when the park happens, so
            // the resume both drains it and writes its reply.
            var elapsed = Stopwatch.StartNew();
            client.Send(
                RawRespClient.Command("DEBUG", "BLOCK", "1"),
                RawRespClient.Command("LRANGE", "tls-big", "0", "-1"));

            ClassicAssert.AreEqual("+OK", client.ReadLine());
            ClassicAssert.AreEqual("*" + elements, client.ReadLine());
            elapsed.Stop();

            ClassicAssert.GreaterOrEqual(elapsed.ElapsedMilliseconds, 900,
                "The reply arrived before the block elapsed, so it was not produced by a resume and this " +
                "test is not exercising a resume-thread TLS write at all");

            for (var i = 0; i < elements; i++)
            {
                var expected = Element(i);
                ClassicAssert.AreEqual("$" + expected.Length, client.ReadLine(),
                    $"Length header for element {i} was wrong, so the record layer lost or reordered bytes");
                ClassicAssert.AreEqual(expected, client.ReadLine(),
                    $"Element {i} came back corrupted from a resume-thread TLS write");
            }

            // The stream is still usable afterwards, which a desynchronised record layer would not be.
            client.Send(RawRespClient.Command("PING"));
            ClassicAssert.AreEqual("+PONG", client.ReadLine());

            static string Element(int i) => $"element-{i:D6}-payload";
        }

        /// <summary>
        /// The headline property, over TLS: far more sessions block at once than the thread pool could
        /// possibly have threads for, and an unrelated connection is still served while they wait.
        /// </summary>
        [Test]
        public void ManyBlockedTlsSessionsDoNotExhaustThreads()
        {
            var endPoint = StartTlsServer();

            // The same count as the plaintext test. It has to be comfortably past what the pool will grow
            // to, and the pool sizes itself from the core count, so a smaller number quietly stops testing
            // anything on a large machine: it passes with parking disabled.
            var sessionCount = Math.Clamp(Environment.ProcessorCount * 3, 256, 512);
            const int BlockSeconds = 5;

            var clients = new List<RawRespClient>(sessionCount);
            try
            {
                for (var i = 0; i < sessionCount; i++)
                    clients.Add(new RawRespClient(endPoint, useTls: true));

                using var control = new RawRespClient(endPoint, useTls: true);
                control.Send(RawRespClient.Command("PING"));
                ClassicAssert.AreEqual("+PONG", control.ReadLine());

                var threadsBefore = Process.GetCurrentProcess().Threads.Count;

                var elapsed = Stopwatch.StartNew();
                foreach (var client in clients)
                    client.Send(RawRespClient.Command("DEBUG", "BLOCK", BlockSeconds.ToString()));

                // Settle first. The sends only reach socket buffers, so probing immediately measures a
                // server that has not picked the blocking commands up yet and reports healthy either way.
                Thread.Sleep(500);

                var controlElapsed = Stopwatch.StartNew();
                control.Send(RawRespClient.Command("PING"));
                ClassicAssert.AreEqual("+PONG", control.ReadLine());
                controlElapsed.Stop();
                ClassicAssert.Less(controlElapsed.ElapsedMilliseconds, 2000,
                    $"Server was unresponsive while {sessionCount} TLS sessions were blocked");


                var threadsDuring = Process.GetCurrentProcess().Threads.Count;

                foreach (var client in clients)
                    ClassicAssert.AreEqual("+OK", client.ReadLine());
                elapsed.Stop();

                // Two-sided, for the reason given on the plaintext twin: the lower bound rules out a build
                // where the sessions never waited, which every upper bound here would otherwise accept.
                ClassicAssert.GreaterOrEqual(elapsed.ElapsedMilliseconds, (BlockSeconds * 1000) - 100,
                    $"{sessionCount} TLS sessions drained in {elapsed.ElapsedMilliseconds}ms, faster than "
                    + $"the {BlockSeconds}s they were told to block: they never waited, so this test was "
                    + "not exercising parking at all");

                // The assertion that actually fails when TLS parking regresses. If every session is parked
                // they all wait concurrently and the drain is one block long; if each holds a thread they
                // serialize into pool-sized waves and it becomes a multiple of one.
                ClassicAssert.Less(elapsed.ElapsedMilliseconds, BlockSeconds * 2 * 1000,
                    $"{sessionCount} blocked TLS sessions took {elapsed.ElapsedMilliseconds}ms to drain, "
                    + $"which is more than the {BlockSeconds}s they waited for: they did not wait at once");

                ClassicAssert.Less(threadsDuring - threadsBefore, sessionCount / 2,
                    $"Blocking {sessionCount} TLS sessions added {threadsDuring - threadsBefore} threads");
            }
            finally
            {
                foreach (var client in clients)
                    client.Dispose();
            }
        }

        /// <summary>
        /// Teardown of a parked TLS connection goes through the same abort and lease machinery as plaintext,
        /// but reaches it from the reader loop rather than the receive loop.
        /// </summary>
        [Test]
        public void DisconnectWhileBlockedUnderTlsIsCleanedUp()
        {
            var endPoint = StartTlsServer();

            for (var i = 0; i < 16; i++)
            {
                var client = new RawRespClient(endPoint, useTls: true);
                client.Send(RawRespClient.Command("DEBUG", "BLOCK", "30"));
                Thread.Sleep(i % 4);
                client.Dispose();
            }

            // The server has to still be serving: a stranded receive state or an undisposed handler would
            // show up here, and the contract check in teardown covers the rest.
            using var survivor = new RawRespClient(endPoint, useTls: true);
            survivor.Send(RawRespClient.Command("PING"));
            ClassicAssert.AreEqual("+PONG", survivor.ReadLine());
        }

        /// <summary>
        /// Parking is only valid for a command the network layer dispatched directly. Inside a transaction
        /// the session is driven by a call stack the network layer knows nothing about, so the command must
        /// fall back rather than park.
        /// </summary>
        [Test]
        public void BlockingFallsBackWhereParkingIsUnsafe()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            // The reply is the one the wait would have produced, and it arrives without the wait: falling
            // back to waiting in place is the behaviour this whole pattern exists to remove, so a command
            // that cannot park must answer rather than pin the thread running the transaction.
            var tran = db.CreateTransaction();
            var queued = tran.ExecuteAsync("DEBUG", "BLOCK", "5");
            var sw = Stopwatch.StartNew();
            ClassicAssert.IsTrue(tran.Execute());
            sw.Stop();
            ClassicAssert.AreEqual("OK", queued.Result.ToString());
            ClassicAssert.Less(sw.ElapsedMilliseconds, 2000,
                $"DEBUG BLOCK waited {sw.ElapsedMilliseconds}ms inside EXEC instead of falling back");

            // DEBUG is noscript, so a script cannot reach the only parkable command there is today, and the
            // script path could not park in any case: SessionScriptCache builds its own RespServerSession
            // over a ScratchBufferNetworkSender, which is not a park host, so CanParkSession is false there
            // by construction. If this stops throwing, the script path needs its own test.
            var ex = Assert.Throws<RedisServerException>(
                () => db.ScriptEvaluate("return redis.call('DEBUG', 'BLOCK', '0.2')"));
            ClassicAssert.IsTrue(ex.Message.Contains("not allowed from script", StringComparison.OrdinalIgnoreCase));

            ClassicAssert.AreEqual("PONG", db.Execute("PING").ToString());
        }

        /// <summary>
        /// The SYNC modifier keeps the thread-consuming wait available for comparison.
        /// </summary>
        [Test]
        public void SyncModifierWaitsInPlace()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            var sw = Stopwatch.StartNew();
            ClassicAssert.AreEqual("OK", db.Execute("DEBUG", "BLOCK", "0.3", "SYNC").ToString());
            sw.Stop();

            ClassicAssert.GreaterOrEqual(sw.ElapsedMilliseconds, 250);
        }

        /// <summary>
        /// A zero-length block is the park machinery with the wait removed, so driving it at the highest
        /// rate a connection sustains measures what a park costs and nothing else. This reports that cost
        /// against two baselines on the same connection -- an ordinary non-parking command, and the same
        /// command taking the in-place wait -- bounds it in absolute terms, and then checks the property
        /// that actually matters for a server: that the cost is per connection and not a shared ceiling.
        /// </summary>
        /// <remarks>
        /// A park is serialized against its own connection by construction: no further bytes are parsed
        /// until it resumes, so back-to-back blocking commands on one connection pay one thread-pool
        /// hand-off each, in sequence. Measured in Release on a 160-core host that is roughly 14us per park
        /// against 1.6us for the in-place wait and 0.9us for <c>PING</c>, and essentially all of the
        /// difference is the hand-off latency the pattern exists to pay.
        /// <para>
        /// That ratio is not asserted on. The in-place baseline is nearly free, so the ratio is really a
        /// measurement of how fast this machine can wake a thread-pool worker, which varies by an order of
        /// magnitude across hosts. The absolute bound catches what a ratio was meant to catch -- a resume
        /// that waits on a timer, a lock or an exhausted pool -- without encoding a host's scheduler into a
        /// test. The scaling check is what would catch a shared bottleneck, which is the failure the design
        /// is actually exposed to.
        /// </para>
        /// </remarks>
        [Test]
        public void ParkedCommandThroughputIsBoundedAndScalesWithConnections()
        {
            // Deep enough that the time to service a batch dominates the round trip that delivers it. At
            // shallower depths every payload measures the same thing -- the socket -- and the comparison
            // says nothing about what a park costs.
            const int PipelineDepth = 512;
            const int Warmup = 2048;
            const int Commands = 10_240;
            const int OkReplyLength = 5;
            const int PongReplyLength = 7;
            const int Rounds = 3;
            const int ScaledConnections = 16;

            // Two orders of magnitude above the measured per-park cost. A park that waits on a timer tick, a
            // lock, or a thread pool it has exhausted lands above this; a slow host does not.
            const double ParkBudgetMicroseconds = 200;

            // Parks on different connections are independent, so aggregate throughput should rise with
            // connection count until something shared saturates. A quarter of linear is far below what is
            // measured and far above what a global lock or a single-threaded resume would allow.
            const double MinimumScaling = 0.25;

            using var client = new RawRespClient(TestUtils.EndPoint);

            var parked = Repeat(RawRespClient.Command("DEBUG", "BLOCK", "0"), PipelineDepth);
            var inPlace = Repeat(RawRespClient.Command("DEBUG", "BLOCK", "0", "SYNC"), PipelineDepth);
            var ping = Repeat(RawRespClient.Command("PING"), PipelineDepth);

            static void Run(RawRespClient connection, byte[] payload, int replyLength, int commands)
            {
                for (var i = 0; i < commands / PipelineDepth; i++)
                {
                    connection.SendRaw(payload);
                    connection.Consume(replyLength * PipelineDepth);
                }
            }

            // Best-of, not mean: every sample is the true cost plus whatever else the machine was doing, so
            // the fastest round is the one least contaminated. The three payloads are measured within each
            // round rather than in three phases, because base socket round-trip time on this kind of machine
            // shifts between phases by more than the effect being measured -- the same reason
            // ParkingAddsLittleToRoundTripLatency alternates.
            var rates = new double[3];
            var payloads = new[] { ping, inPlace, parked };
            var replyLengths = new[] { PongReplyLength, OkReplyLength, OkReplyLength };

            for (var i = 0; i < payloads.Length; i++)
                Run(client, payloads[i], replyLengths[i], Warmup);

            for (var round = 0; round < Rounds; round++)
            {
                for (var i = 0; i < payloads.Length; i++)
                {
                    var sw = Stopwatch.StartNew();
                    Run(client, payloads[i], replyLengths[i], Commands);
                    sw.Stop();

                    var rate = Commands / sw.Elapsed.TotalSeconds;
                    if (rate > rates[i])
                        rates[i] = rate;
                }
            }

            var parkedRate = rates[2];
            var microsecondsPerPark = 1_000_000 / parkedRate;

            TestContext.Out.WriteLine(
                $"one connection, pipelined at depth {PipelineDepth}: PING {rates[0]:N0} ops/s, " +
                $"DEBUG BLOCK 0 SYNC {rates[1]:N0} ops/s, DEBUG BLOCK 0 parked {parkedRate:N0} ops/s " +
                $"({microsecondsPerPark:F1}us per park, {rates[1] / parkedRate:F2}x the in-place wait, " +
                $"{rates[0] / parkedRate:F2}x PING)");

            ClassicAssert.Less(microsecondsPerPark, ParkBudgetMicroseconds,
                $"A park cost {microsecondsPerPark:F0}us ({parkedRate:N0} ops/s on one connection)");

            var connections = new RawRespClient[ScaledConnections];
            var failures = new ConcurrentQueue<string>();
            double scaledRate;
            try
            {
                for (var i = 0; i < ScaledConnections; i++)
                    connections[i] = new RawRespClient(TestUtils.EndPoint);

                var threads = new Thread[ScaledConnections];
                var start = new ManualResetEventSlim(false);
                for (var i = 0; i < ScaledConnections; i++)
                {
                    var connection = connections[i];
                    threads[i] = new Thread(() =>
                    {
                        try
                        {
                            Run(connection, parked, OkReplyLength, Warmup);
                            start.Wait();
                            Run(connection, parked, OkReplyLength, Commands);
                        }
                        catch (Exception e)
                        {
                            failures.Enqueue(e.ToString());
                        }
                    })
                    { IsBackground = true };
                    threads[i].Start();
                }

                // Started together so the measured window is the one in which every connection is loaded,
                // rather than a ramp whose ends are measuring one connection.
                Thread.Sleep(200);
                var sw = Stopwatch.StartNew();
                start.Set();

                foreach (var thread in threads)
                    ClassicAssert.IsTrue(thread.Join(TimeSpan.FromMinutes(2)), "A loaded connection hung");
                sw.Stop();

                ClassicAssert.IsTrue(failures.IsEmpty, string.Join(Environment.NewLine, failures));
                scaledRate = ScaledConnections * Commands / sw.Elapsed.TotalSeconds;
            }
            finally
            {
                foreach (var connection in connections)
                    connection?.Dispose();
            }

            var scaling = scaledRate / parkedRate;
            TestContext.Out.WriteLine(
                $"{ScaledConnections} connections: {scaledRate:N0} parks/s aggregate, " +
                $"{scaling:F1}x the single-connection rate ({scaling / ScaledConnections:P0} of linear)");

            ClassicAssert.Greater(scaling, ScaledConnections * MinimumScaling,
                $"{ScaledConnections} connections reached only {scaling:F1}x the single-connection park rate");
        }

        /// <summary>
        /// Measures what a park adds to the round-trip latency of a single command on an idle connection,
        /// by alternating the parked and in-place forms of the same command on the same connection and
        /// differencing them.
        /// </summary>
        /// <remarks>
        /// Alternating is not a stylistic choice, it is the only way to get a trustworthy number here. Base
        /// round-trip time on a loopback socket is bimodal and depends on machine-wide state -- how many
        /// cores the runtime sees, how deeply idle they are, what else is resident -- and it moves by more
        /// than an order of magnitude between runs, and sometimes within one. Measuring the two forms in
        /// separate phases therefore measures the machine: on this hardware that approach reports 34us for
        /// the in-place form and 575us for the parked one, and alternating them reports 565us and 573us.
        /// The 8us difference is the park; the 530us is the phase boundary.
        /// <para>
        /// The assertion is on the difference for the same reason. An absolute bound would have to be loose
        /// enough to pass in the slow state, which makes it blind in the fast one.
        /// </para>
        /// </remarks>
        [Test]
        public void ParkingAddsLittleToRoundTripLatency()
        {
            const int Warmup = 2000;
            const int Samples = 20_000;
            const int OkReplyLength = 5;

            // A park costs one thread-pool hand-off and resumes on a different core from the one that
            // parked, so some difference is inherent. The budget is two orders of magnitude above the
            // measured 8us: it catches a resume that waits on a timer, a lock or a drained thread pool, and
            // ignores scheduler jitter.
            const double MedianBudgetMicroseconds = 250;

            using var client = new RawRespClient(TestUtils.EndPoint);

            var parked = RawRespClient.Command("DEBUG", "BLOCK", "0");
            var inPlace = RawRespClient.Command("DEBUG", "BLOCK", "0", "SYNC");

            var parkedSamples = new double[Samples];
            var inPlaceSamples = new double[Samples];

            for (var i = 0; i < Warmup; i++)
            {
                client.SendRaw(inPlace);
                client.Consume(OkReplyLength);
                client.SendRaw(parked);
                client.Consume(OkReplyLength);
            }

            for (var i = 0; i < Samples; i++)
            {
                var start = Stopwatch.GetTimestamp();
                client.SendRaw(inPlace);
                client.Consume(OkReplyLength);
                var middle = Stopwatch.GetTimestamp();
                client.SendRaw(parked);
                client.Consume(OkReplyLength);
                var end = Stopwatch.GetTimestamp();

                inPlaceSamples[i] = Stopwatch.GetElapsedTime(start, middle).TotalMicroseconds;
                parkedSamples[i] = Stopwatch.GetElapsedTime(middle, end).TotalMicroseconds;
            }

            Array.Sort(inPlaceSamples);
            Array.Sort(parkedSamples);

            var inPlaceMedian = inPlaceSamples[Samples / 2];
            var parkedMedian = parkedSamples[Samples / 2];
            var inPlaceTail = inPlaceSamples[(int)(Samples * 0.99)];
            var parkedTail = parkedSamples[(int)(Samples * 0.99)];

            TestContext.Out.WriteLine(
                $"round-trip latency, in place: p50 {inPlaceMedian:F1}us p99 {inPlaceTail:F1}us " +
                $"max {inPlaceSamples[Samples - 1]:F1}us");
            TestContext.Out.WriteLine(
                $"round-trip latency, parked:   p50 {parkedMedian:F1}us p99 {parkedTail:F1}us " +
                $"max {parkedSamples[Samples - 1]:F1}us");
            TestContext.Out.WriteLine(
                $"parking cost {parkedMedian - inPlaceMedian:F1}us at the median, " +
                $"{parkedTail - inPlaceTail:F1}us at p99");

            ClassicAssert.Less(parkedMedian - inPlaceMedian, MedianBudgetMicroseconds,
                $"Parking added {parkedMedian - inPlaceMedian:F0}us to the median round trip " +
                $"({parkedMedian:F0}us parked against {inPlaceMedian:F0}us in place)");
        }

        /// <summary>
        /// Drives parks and ordinary commands through the same connections, from many connections at once,
        /// for long enough that every interleaving of the park rendezvous occurs repeatedly -- and checks
        /// that every reply is the right one, in the right order, on every connection.
        /// </summary>
        /// <remarks>
        /// The other ordering tests pin a specific interleaving. This pins none: a zero-length block races
        /// the receive stack unwinding against the operation completing, so across this many iterations both
        /// orders occur on every connection, under thread-pool contention from every other connection doing
        /// the same thing. What makes it an ordering test rather than a smoke test is the payload -- each
        /// batch mixes parking and non-parking commands whose replies have different lengths and contents,
        /// so a reply that is dropped, duplicated, reordered, or written by the wrong thread desynchronises
        /// the stream and is caught on the next command rather than being absorbed.
        /// <para>
        /// <see cref="TestUtils.OnTearDown"/> fails the test if the state machine recorded a contract
        /// violation anywhere in the server during the storm.
        /// </para>
        /// </remarks>
        [Test]
        public void ParkStormPreservesOrderingOnEveryConnection()
        {
            const int Connections = 32;
            const int Iterations = 400;

            var clients = new RawRespClient[Connections];
            var failures = new ConcurrentQueue<string>();

            try
            {
                for (var i = 0; i < Connections; i++)
                    clients[i] = new RawRespClient(TestUtils.EndPoint);

                // Interleaved so that a parked command is followed by commands the resume must drain from a
                // buffer it never parsed, and preceded by commands whose replies must already have been
                // flushed when the park took effect. ECHO carries the iteration number, so a reply from the
                // wrong iteration is caught even if it has the right shape.
                var threads = new Thread[Connections];
                for (var i = 0; i < Connections; i++)
                {
                    var client = clients[i];
                    var connection = i;
                    threads[i] = new Thread(() =>
                    {
                        try
                        {
                            for (var iteration = 0; iteration < Iterations; iteration++)
                            {
                                var tag = $"c{connection}i{iteration}";
                                client.Send(
                                    RawRespClient.Command("PING"),
                                    RawRespClient.Command("DEBUG", "BLOCK", "0"),
                                    RawRespClient.Command("ECHO", tag),
                                    RawRespClient.Command("DEBUG", "BLOCK", "0"),
                                    RawRespClient.Command("DEBUG", "BLOCK", "0", "SYNC"),
                                    RawRespClient.Command("ECHO", tag));

                                Expect(client, "+PONG", connection, iteration, failures);
                                Expect(client, "+OK", connection, iteration, failures);
                                Expect(client, "$" + tag.Length, connection, iteration, failures);
                                Expect(client, tag, connection, iteration, failures);
                                Expect(client, "+OK", connection, iteration, failures);
                                Expect(client, "+OK", connection, iteration, failures);
                                Expect(client, "$" + tag.Length, connection, iteration, failures);
                                Expect(client, tag, connection, iteration, failures);
                            }
                        }
                        catch (Exception e)
                        {
                            failures.Enqueue($"connection {connection} threw: {e}");
                        }
                    })
                    { IsBackground = true };
                }

                foreach (var thread in threads)
                    thread.Start();

                foreach (var thread in threads)
                    ClassicAssert.IsTrue(thread.Join(TimeSpan.FromMinutes(2)), "A storm connection hung");

                ClassicAssert.IsTrue(failures.IsEmpty, string.Join(Environment.NewLine, failures));

                // Still serving after the storm, on a connection that took no part in it.
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                ClassicAssert.AreEqual("PONG", redis.GetDatabase(0).Execute("PING").ToString());
            }
            finally
            {
                foreach (var client in clients)
                    client?.Dispose();
            }
        }

        /// <summary>
        /// Reads one reply line and records a mismatch rather than throwing, so a desynchronised connection
        /// reports the first divergence instead of a cascade from every connection behind it.
        /// </summary>
        static void Expect(RawRespClient client, string expected, int connection, int iteration,
            ConcurrentQueue<string> failures)
        {
            var actual = client.ReadLine();
            if (actual != expected)
                failures.Enqueue($"connection {connection}, iteration {iteration}: expected '{expected}', got '{actual}'");
        }

        /// <summary>
        /// Connections that appear, park, and vanish mid-park, continuously, while other connections are
        /// parking on the same server.
        /// </summary>
        /// <remarks>
        /// <see cref="DisconnectWhileBlockedIsCleanedUp"/> covers one such disconnect in isolation. The
        /// failure mode this adds is accumulation: a park that leaks its context, its
        /// <see cref="System.Net.Sockets.SocketAsyncEventArgs"/>, or its rendezvous contribution does not
        /// fail a single-shot test, because the connection it stranded was going away anyway. Thousands of
        /// them strand the server, which this detects by requiring that steady traffic on long-lived
        /// connections is still being served at the end, and that the churn itself never stalls.
        /// </remarks>
        [Test]
        public void ChurningConnectionsThatDisconnectWhileParkedDoNotStrandTheServer()
        {
            const int SteadyConnections = 8;
            const int ChurnThreads = 8;
            const int ChurnCycles = 60;
            const int OkReplyLength = 5;

            var steady = new RawRespClient[SteadyConnections];
            var failures = new ConcurrentQueue<string>();
            var stop = new ManualResetEventSlim(false);
            var steadyOps = 0L;

            try
            {
                for (var i = 0; i < SteadyConnections; i++)
                    steady[i] = new RawRespClient(TestUtils.EndPoint);

                var block = RawRespClient.Command("DEBUG", "BLOCK", "0");
                var steadyThreads = new Thread[SteadyConnections];
                for (var i = 0; i < SteadyConnections; i++)
                {
                    var client = steady[i];
                    steadyThreads[i] = new Thread(() =>
                    {
                        try
                        {
                            while (!stop.IsSet)
                            {
                                client.SendRaw(block);
                                client.Consume(OkReplyLength);
                                _ = Interlocked.Increment(ref steadyOps);
                            }
                        }
                        catch (Exception e)
                        {
                            failures.Enqueue($"steady connection threw: {e}");
                        }
                    })
                    { IsBackground = true };
                }

                // Long enough that the connection is reliably parked when it is dropped, short enough that
                // sixty cycles on eight threads is a test and not a soak.
                var churnBlock = RawRespClient.Command("DEBUG", "BLOCK", "0.05");
                var churnThreads = new Thread[ChurnThreads];
                for (var i = 0; i < ChurnThreads; i++)
                {
                    churnThreads[i] = new Thread(() =>
                    {
                        try
                        {
                            for (var cycle = 0; cycle < ChurnCycles; cycle++)
                            {
                                using var client = new RawRespClient(TestUtils.EndPoint);
                                client.SendRaw(churnBlock);

                                // Dropped without reading the reply, so the close lands while the session is
                                // parked rather than after it has resumed.
                                Thread.Sleep(10);
                            }
                        }
                        catch (Exception e)
                        {
                            failures.Enqueue($"churn thread threw: {e}");
                        }
                    })
                    { IsBackground = true };
                }

                foreach (var thread in steadyThreads)
                    thread.Start();
                foreach (var thread in churnThreads)
                    thread.Start();

                foreach (var thread in churnThreads)
                    ClassicAssert.IsTrue(thread.Join(TimeSpan.FromMinutes(2)), "A churn thread hung");

                var opsBeforeDrain = Interlocked.Read(ref steadyOps);
                stop.Set();

                foreach (var thread in steadyThreads)
                    ClassicAssert.IsTrue(thread.Join(TimeSpan.FromSeconds(30)),
                        "A steady connection was still stuck after the churn stopped");

                ClassicAssert.IsTrue(failures.IsEmpty, string.Join(Environment.NewLine, failures));

                // The steady connections kept being served throughout, rather than being starved by the
                // churn or stranded behind a park that never completed.
                ClassicAssert.Greater(opsBeforeDrain, SteadyConnections,
                    "Steady traffic stalled while connections churned through parks");

                TestContext.Out.WriteLine(
                    $"{ChurnThreads * ChurnCycles} connections disconnected mid-park while " +
                    $"{SteadyConnections} steady connections completed {opsBeforeDrain:N0} parks");

                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                ClassicAssert.AreEqual("PONG", redis.GetDatabase(0).Execute("PING").ToString());
            }
            finally
            {
                stop.Set();
                foreach (var client in steady)
                    client?.Dispose();
            }
        }

        /// <summary>
        /// A throw between the park and the end of the batch loses the batch while the session is already
        /// parked. That close reaches neither the park's own abort nor the `QUIT` tail, so the disposing
        /// `GarnetException` handler owes the same reconciliation the other failure paths make. Anything that
        /// can fail after a handler parks lands here -- a `Send` on replies staged ahead of the park, cluster
        /// bookkeeping, the slow log -- and `FAILBATCH` is what makes it schedulable. The underlying wait is
        /// gated and never released, so a session left parked here is parked for good.
        /// </summary>
        [Test]
        public void ABatchThatFailsAfterParkingDoesNotStrandTheSession()
        {
            const int Connections = 8;

            int BlockedClients()
            {
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
                var clients = redis.GetServers()[0].Info("clients");
                return int.Parse(clients.SelectMany(section => section)
                    .First(entry => entry.Key == "blocked_clients").Value);
            }

            for (var i = 0; i < Connections; i++)
            {
                using var client = new RawRespClient(TestUtils.EndPoint);
                client.SendRaw(RawRespClient.Command("DEBUG", "BLOCK", "0", "FAILBATCH"));
                client.Consume(5);
            }

            // Nothing releases the gate, so anything still parked is parked forever.
            for (var attempt = 0; attempt < 200 && BlockedClients() != 0; attempt++)
                Thread.Sleep(10);

            ClassicAssert.AreEqual(0, BlockedClients(),
                "A session parked by a batch that then failed was left parked after its connection was closed");

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            ClassicAssert.AreEqual("PONG", redis.GetDatabase(0).Execute("PING").ToString());
        }

        /// <summary>
        /// `QUIT` does not end the batch -- it writes its reply, latches the session for disposal and lets
        /// parsing continue -- so a blocking command written behind it in the same packet still parks. The
        /// batch tail then closes the socket, and a parked session has no outstanding receive to notice. The
        /// park has to be reconciled there the way the failure paths reconcile theirs, or the handler and its
        /// receive state stay held until the operation's own deadline, on a connection the server itself just
        /// closed. `GATE` never reaches a deadline, so a leak here is permanent.
        /// </summary>
        [Test]
        public void QuitFollowedByABlockingCommandDoesNotStrandTheSession()
        {
            const int Connections = 8;

            int BlockedClients()
            {
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
                var clients = redis.GetServers()[0].Info("clients");
                return int.Parse(clients.SelectMany(section => section)
                    .First(entry => entry.Key == "blocked_clients").Value);
            }

            var quitThenBlock = new byte[0];
            using (var builder = new MemoryStream())
            {
                var quit = RawRespClient.Command("QUIT");
                var block = RawRespClient.Command("DEBUG", "BLOCK", "0", "GATE");
                builder.Write(quit, 0, quit.Length);
                builder.Write(block, 0, block.Length);
                quitThenBlock = builder.ToArray();
            }

            for (var i = 0; i < Connections; i++)
            {
                // One write, so both commands land in the same batch and the blocking one is parsed after
                // QUIT has already latched the session for disposal.
                using var client = new RawRespClient(TestUtils.EndPoint);
                client.SendRaw(quitThenBlock);
                client.Consume(5);
            }

            // Nothing releases the gate, so anything still parked is parked forever.
            for (var attempt = 0; attempt < 200 && BlockedClients() != 0; attempt++)
                Thread.Sleep(10);

            ClassicAssert.AreEqual(0, BlockedClients(),
                "A session that parked behind QUIT was left parked after its connection was closed");

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            ClassicAssert.AreEqual("PONG", redis.GetDatabase(0).Execute("PING").ToString());
        }

        /// <summary>
        /// A parked connection looks exactly like a hung one from outside the server: it holds a socket,
        /// issues no receive and sends nothing. <c>INFO clients</c> is the only way an operator can tell the
        /// two apart, so the count has to be exact at both ends -- it must rise with the parks and come all
        /// the way back down, including for the connections that are killed rather than completed.
        /// </summary>
        [Test]
        public void BlockedClientsReportsParkedSessions()
        {
            const int Parked = 12;
            const int Dropped = 4;
            const int Killed = 4;
            const int OkReplyLength = 5;

            int BlockedClients()
            {
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
                var clients = redis.GetServers()[0].Info("clients");
                var value = clients.SelectMany(section => section)
                    .First(entry => entry.Key == "blocked_clients").Value;
                return int.Parse(value);
            }

            void WaitForBlockedClients(int expected, string because)
            {
                for (var attempt = 0; attempt < 200 && BlockedClients() != expected; attempt++)
                    Thread.Sleep(10);

                ClassicAssert.AreEqual(expected, BlockedClients(), because);
            }

            ClassicAssert.AreEqual(0, BlockedClients(), "An idle server reported blocked clients");

            var clients = new RawRespClient[Parked];
            var ids = new int[Parked];
            try
            {
                for (var i = 0; i < Parked; i++)
                {
                    clients[i] = new RawRespClient(TestUtils.EndPoint);
                    clients[i].SendRaw(RawRespClient.Command("CLIENT", "ID"));
                    ids[i] = clients[i].ReadInteger();
                    clients[i].SendRaw(RawRespClient.Command("DEBUG", "BLOCK", "0", "GATE"));
                }

                WaitForBlockedClients(Parked, "Parked sessions were not reported as blocked clients");

                // Three ways out of a park, because they release the context in three different places and a
                // counter can be wrong at any one of them independently.
                //
                // First: killed. The abort resumes the session onto a connection that is already gone, so the
                // context is discarded by the handler rather than replied to.
                using (var admin = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var db = admin.GetDatabase(0);
                    for (var i = 0; i < Killed; i++)
                        _ = db.Execute("CLIENT", "KILL", "ID", ids[i].ToString());
                }

                WaitForBlockedClients(Parked - Killed, "A killed parked session stayed counted as blocked");

                // Second: dropped. The socket closes under a park that still completes normally, so the
                // reply path runs and fails to send.
                for (var i = Killed; i < Killed + Dropped; i++)
                {
                    clients[i].Dispose();
                    clients[i] = null;
                }

                var release = RawRespClient.Command("DEBUG", "BLOCK", "0", "RELEASE");
                using (var controlConnection = new RawRespClient(TestUtils.EndPoint))
                {
                    controlConnection.SendRaw(release);
                    _ = controlConnection.ReadInteger();
                }

                // Third: completed, which is the only one that writes a reply.
                for (var i = Killed + Dropped; i < Parked; i++)
                    clients[i].Consume(OkReplyLength);

                WaitForBlockedClients(0, "Blocked clients did not return to zero after every park ended");
            }
            finally
            {
                foreach (var client in clients)
                    client?.Dispose();
            }
        }

        /// <summary>
        /// Minimal RESP client over a bare socket. Used instead of a multiplexer so a test can hold hundreds
        /// of connections cheaply, and so it can observe reply timing and ordering directly.
        /// </summary>
        sealed class RawRespClient : IDisposable
        {
            readonly Socket socket;

            /// <summary>
            /// Null on a plaintext connection, which keeps that path on the raw socket calls the allocation
            /// measurements depend on.
            /// </summary>
            readonly SslStream tls;
            readonly byte[] buffer = new byte[4096];
            int bufferStart;
            int bufferEnd;

            internal RawRespClient(System.Net.EndPoint endPoint, bool useTls = false, int receiveBufferSize = 0)
            {
                socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = true,
                    ReceiveTimeout = 60_000,
                    SendTimeout = 60_000
                };

                // Shrinking the receive window lets a test stop reading and have the server's sends block
                // against a full socket in far less data than an auto-tuned buffer would need.
                if (receiveBufferSize > 0)
                    socket.ReceiveBufferSize = receiveBufferSize;

                socket.Connect(endPoint);

                if (!useTls)
                    return;

                tls = new SslStream(new NetworkStream(socket, ownsSocket: false), leaveInnerStreamOpen: false,
                    TestUtils.ValidateServerCertificate);
                tls.AuthenticateAsClient(new SslClientAuthenticationOptions
                {
                    ClientCertificates = [TestUtils.GetClientCertificate()],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = TestUtils.ValidateServerCertificate,
                });
            }

            /// <summary>Writes a whole payload, over TLS when this connection is encrypted.</summary>
            void Write(byte[] payload, int offset, int count)
            {
                if (tls != null)
                {
                    tls.Write(payload, offset, count);
                    tls.Flush();
                    return;
                }

                var sent = 0;
                while (sent < count)
                    sent += socket.Send(payload, offset + sent, count - sent, SocketFlags.None);
            }

            /// <summary>Reads whatever is available, over TLS when this connection is encrypted.</summary>
            int Read(byte[] destination, int offset, int count)
                => tls != null
                    ? tls.Read(destination, offset, count)
                    : socket.Receive(destination, offset, count, SocketFlags.None);

            internal static byte[] Command(params string[] args)
            {
                var sb = new StringBuilder();
                _ = sb.Append('*').Append(args.Length).Append("\r\n");
                foreach (var arg in args)
                    _ = sb.Append('$').Append(Encoding.UTF8.GetByteCount(arg)).Append("\r\n").Append(arg).Append("\r\n");
                return Encoding.UTF8.GetBytes(sb.ToString());
            }

            /// <summary>
            /// Writes all commands in a single send, so the server sees them as one pipelined batch.
            /// </summary>
            internal void Send(params byte[][] commands)
            {
                var total = 0;
                foreach (var command in commands)
                    total += command.Length;

                var payload = new byte[total];
                var offset = 0;
                foreach (var command in commands)
                {
                    Buffer.BlockCopy(command, 0, payload, offset, command.Length);
                    offset += command.Length;
                }

                Write(payload, 0, payload.Length);
            }

            /// <summary>
            /// Sends a pre-encoded payload. Allocation-free, unlike <see cref="Send"/>, so a caller can
            /// attribute measured allocation to the server.
            /// </summary>
            internal void SendRaw(byte[] payload) => Write(payload, 0, payload.Length);

            /// <summary>
            /// Discards exactly <paramref name="count"/> reply bytes without allocating.
            /// </summary>
            internal void Consume(int count)
            {
                while (count > 0)
                {
                    var buffered = bufferEnd - bufferStart;
                    if (buffered == 0)
                    {
                        bufferStart = bufferEnd = 0;
                        var read = Read(buffer, 0, buffer.Length);
                        if (read == 0)
                            throw new IOException("Connection closed by server");
                        bufferEnd = read;
                        continue;
                    }

                    var take = Math.Min(buffered, count);
                    bufferStart += take;
                    count -= take;
                }
            }

            /// <summary>
            /// Compacts the read buffer and blocks for more bytes.
            /// </summary>
            void FillBuffer()
            {
                if (bufferStart > 0)
                {
                    Buffer.BlockCopy(buffer, bufferStart, buffer, 0, bufferEnd - bufferStart);
                    bufferEnd -= bufferStart;
                    bufferStart = 0;
                }

                var read = Read(buffer, bufferEnd, buffer.Length - bufferEnd);
                if (read == 0)
                    throw new IOException("Connection closed by server");
                bufferEnd += read;
            }

            /// <summary>
            /// Reads a RESP integer reply without allocating, so a caller can poll the server inside an
            /// allocation measurement without attributing its own garbage to the server.
            /// </summary>
            internal int ReadInteger()
            {
                while (true)
                {
                    for (var i = bufferStart; i < bufferEnd - 1; i++)
                    {
                        if (buffer[i] != (byte)'\r' || buffer[i + 1] != (byte)'\n')
                            continue;

                        if (buffer[bufferStart] != (byte)':')
                            throw new IOException($"Expected an integer reply, got '{(char)buffer[bufferStart]}'");

                        var value = 0;
                        for (var digit = bufferStart + 1; digit < i; digit++)
                            value = (value * 10) + (buffer[digit] - (byte)'0');

                        bufferStart = i + 2;
                        return value;
                    }

                    FillBuffer();
                }
            }

            /// <summary>
            /// Reads one CRLF-terminated protocol line, without its terminator.
            /// </summary>
            internal string ReadLine()
            {
                while (true)
                {
                    for (var i = bufferStart; i < bufferEnd - 1; i++)
                    {
                        if (buffer[i] != (byte)'\r' || buffer[i + 1] != (byte)'\n')
                            continue;

                        var line = Encoding.UTF8.GetString(buffer, bufferStart, i - bufferStart);
                        bufferStart = i + 2;
                        return line;
                    }

                    FillBuffer();
                }
            }

            public void Dispose()
            {
                // Disposed before the shutdown so the TLS close-notify goes out while the socket is still
                // writable; a parked session on the other end has to see a clean close, not a reset.
                try { tls?.Dispose(); } catch { }
                try { socket.Shutdown(SocketShutdown.Both); } catch { }
                socket.Dispose();
            }
        }
    }
}