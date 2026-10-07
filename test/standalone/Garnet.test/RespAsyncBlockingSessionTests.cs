// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Covers blocking commands written against <c>AsyncBlockingCommandContext</c>, where the compiler writes
    /// the suspended state instead of the command. <c>DEBUG BLOCKASYNC</c> and <c>DEBUG BLOCKGETASYNC</c> are
    /// the reference commands, and they are deliberately the same operations as <c>DEBUG BLOCK</c> and
    /// <c>DEBUG BLOCKGET</c> so that the two authoring styles are held to the same behaviour.
    /// </summary>
    /// <remarks>
    /// The park itself is covered by <see cref="RespBlockingSessionTests"/> and is shared by both styles.
    /// What is new here is the adapter: that a compiler-generated state machine parks and resumes correctly,
    /// costs no allocation per park, unwinds promptly when its connection goes away, and may still drive the
    /// parked session's storage from its continuation.
    /// </remarks>
    [TestFixture]
    public class RespAsyncBlockingSessionTests : TestBase
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

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);

            // Runs after the server is gone, so violations raised during teardown are counted too.
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Waits for a condition, so a test does not depend on how quickly a background thread gets there.
        /// </summary>
        static bool SpinUntil(Func<bool> condition, int timeoutMs = 5000)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                if (condition())
                    return true;

                Thread.Sleep(10);
            }

            return condition();
        }

        /// <summary>
        /// The command parks, waits, and replies when the wait ends.
        /// </summary>
        [Test]
        public void BlockAsyncWaitsThenRepliesOk()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            var sw = Stopwatch.StartNew();
            client.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC", "0.5"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());
            sw.Stop();

            ClassicAssert.GreaterOrEqual(sw.ElapsedMilliseconds, 400,
                $"DEBUG BLOCKASYNC 0.5 replied after {sw.ElapsedMilliseconds}ms, so it never waited");
            ClassicAssert.Less(sw.ElapsedMilliseconds, 5000,
                $"DEBUG BLOCKASYNC 0.5 took {sw.ElapsedMilliseconds}ms");
        }

        /// <summary>
        /// The session keeps serving the same connection after a park, rather than being left in a state
        /// that only looked correct for the reply itself.
        /// </summary>
        [Test]
        public void SessionKeepsWorkingAfterAsyncResume()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            for (var i = 0; i < 5; i++)
            {
                client.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC", "0"));
                ClassicAssert.AreEqual("+OK", client.ReadLine());

                client.Send(RawRespClient.Command("SET", $"after-{i}", $"value-{i}"));
                ClassicAssert.AreEqual("+OK", client.ReadLine());

                client.Send(RawRespClient.Command("GET", $"after-{i}"));
                ClassicAssert.AreEqual($"value-{i}", client.ReadBulkString());
            }
        }

        /// <summary>
        /// Commands pipelined behind a parked one are answered after it, in the order they were sent. The
        /// park suspends the connection's parsing rather than letting later commands overtake it.
        /// </summary>
        [Test]
        public void PipelineBehindAsyncBlockingCommandStaysOrdered()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            client.Send(RawRespClient.Command("SET", "ordered", "value"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            // One write, so the three commands are in the receive buffer together and the park has to leave
            // the two behind it unparsed.
            client.SendRaw([.. RawRespClient.Command("DEBUG", "BLOCKASYNC", "0.3"),
                            .. RawRespClient.Command("PING"),
                            .. RawRespClient.Command("GET", "ordered")]);

            ClassicAssert.AreEqual("+OK", client.ReadLine());
            ClassicAssert.AreEqual("+PONG", client.ReadLine());
            ClassicAssert.AreEqual("value", client.ReadBulkString());
        }

        /// <summary>
        /// Bad arguments are rejected without parking.
        /// </summary>
        [Test]
        public void BlockAsyncRejectsBadArguments()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            client.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC"));
            StringAssert.StartsWith("-ERR", client.ReadLine());

            client.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC", "not-a-number"));
            StringAssert.StartsWith("-ERR", client.ReadLine());

            client.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC", "-1"));
            StringAssert.StartsWith("-ERR", client.ReadLine());

            client.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC", "0", "extra"));
            StringAssert.StartsWith("-ERR", client.ReadLine());

            // Still usable: a rejected command must not have parked the session.
            client.Send(RawRespClient.Command("PING"));
            ClassicAssert.AreEqual("+PONG", client.ReadLine());
        }

        /// <summary>
        /// The continuation may drive the parked session's storage. This is what distinguishes the pattern
        /// from one that can only defer a reply it already knows.
        /// </summary>
        [Test]
        public void BlockGetAsyncReadsTheStoreAfterTheWait()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            client.Send(RawRespClient.Command("SET", "async-key", "async-value"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            var sw = Stopwatch.StartNew();
            client.Send(RawRespClient.Command("DEBUG", "BLOCKGETASYNC", "0.4", "async-key"));
            ClassicAssert.AreEqual("async-value", client.ReadBulkString());
            sw.Stop();

            ClassicAssert.GreaterOrEqual(sw.ElapsedMilliseconds, 300,
                $"DEBUG BLOCKGETASYNC 0.4 replied after {sw.ElapsedMilliseconds}ms, so it never waited");
        }

        /// <summary>
        /// The read happens when the wait ends, not when the command was parsed, so a write that lands
        /// during the wait is visible to it.
        /// </summary>
        [Test]
        public void BlockGetAsyncSeesAWriteThatLandsDuringTheWait()
        {
            using var reader = new RawRespClient(TestUtils.EndPoint);
            using var writer = new RawRespClient(TestUtils.EndPoint);

            writer.Send(RawRespClient.Command("SET", "racing", "before"));
            ClassicAssert.AreEqual("+OK", writer.ReadLine());

            reader.Send(RawRespClient.Command("DEBUG", "BLOCKGETASYNC", "1", "racing"));

            Thread.Sleep(300);
            writer.Send(RawRespClient.Command("SET", "racing", "after"));
            ClassicAssert.AreEqual("+OK", writer.ReadLine());

            ClassicAssert.AreEqual("after", reader.ReadBulkString());
        }

        /// <summary>
        /// A miss replies with a null bulk string, and a key of the wrong type with an error, both written
        /// by the body's own tail rather than by a separate publication method.
        /// </summary>
        [Test]
        public void BlockGetAsyncReportsMissesAndWrongTypes()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            client.Send(RawRespClient.Command("DEBUG", "BLOCKGETASYNC", "0", "no-such-key"));
            ClassicAssert.IsNull(client.ReadBulkString());

            client.Send(RawRespClient.Command("LPUSH", "a-list", "item"));
            ClassicAssert.AreEqual(":1", client.ReadLine());

            client.Send(RawRespClient.Command("DEBUG", "BLOCKGETASYNC", "0", "a-list"));
            StringAssert.StartsWith("-WRONGTYPE", client.ReadLine());

            client.Send(RawRespClient.Command("SET", "empty", ""));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            client.Send(RawRespClient.Command("DEBUG", "BLOCKGETASYNC", "0", "empty"));
            ClassicAssert.AreEqual("", client.ReadBulkString());
        }

        /// <summary>
        /// Each parked connection reads its own key, so the state the body carries across the wait belongs
        /// to its own park rather than to whichever one completed last.
        /// </summary>
        [Test]
        public void ManyConcurrentAsyncBlockGetsEachReadTheirOwnKey()
        {
            const int Connections = 64;

            using (var loader = new RawRespClient(TestUtils.EndPoint))
            {
                for (var i = 0; i < Connections; i++)
                {
                    loader.Send(RawRespClient.Command("SET", $"own-{i}", $"value-{i}"));
                    ClassicAssert.AreEqual("+OK", loader.ReadLine());
                }
            }

            var clients = new List<RawRespClient>(Connections);
            try
            {
                for (var i = 0; i < Connections; i++)
                    clients.Add(new RawRespClient(TestUtils.EndPoint));

                for (var i = 0; i < Connections; i++)
                    clients[i].Send(RawRespClient.Command("DEBUG", "BLOCKGETASYNC", "0.5", $"own-{i}"));

                for (var i = 0; i < Connections; i++)
                    ClassicAssert.AreEqual($"value-{i}", clients[i].ReadBulkString());
            }
            finally
            {
                foreach (var client in clients)
                    client.Dispose();
            }
        }

        /// <summary>
        /// The claim on the point of the exercise: far more connections block at once than the process has
        /// threads, and they wait concurrently rather than in pool-sized waves.
        /// </summary>
        /// <remarks>
        /// Two-sided. The upper bound is the parking claim -- parked sessions wait together, so the drain is
        /// one block long, whereas a thread each serializes them into waves and makes it a multiple. The
        /// lower bound keeps that claim honest: a build where the sessions never waited at all would drain
        /// instantly and satisfy every upper bound while proving nothing.
        /// </remarks>
        [Test]
        public void ManyBlockedAsyncSessionsDoNotExhaustThreads()
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
                    client.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC", BlockSeconds.ToString()));

                // Settle first. The sends only reach socket buffers, so probing immediately measures a
                // server that has not picked the blocking commands up yet and reports healthy either way.
                Thread.Sleep(500);

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

                ClassicAssert.GreaterOrEqual(sw.ElapsedMilliseconds, (BlockSeconds * 1000) - 100,
                    $"{sessionCount} sessions drained in {sw.ElapsedMilliseconds}ms, faster than the "
                    + $"{BlockSeconds}s they were told to block: they never waited");

                ClassicAssert.Less(sw.ElapsedMilliseconds, BlockSeconds * 2 * 1000,
                    $"{sessionCount} blocked sessions took {sw.ElapsedMilliseconds}ms to drain, which is "
                    + $"more than the {BlockSeconds}s they waited for: they did not wait at once");

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
        /// A park costs no allocation, so a compiler-generated state machine is as cheap as the hand-written
        /// context it replaces.
        /// </summary>
        /// <remarks>
        /// In an optimized build a park allocates nothing at all -- the context is reused across a
        /// connection's parks, and the body's state machine lives in a box the context owns rather than
        /// being allocated afresh each time it suspends -- so the budget is set far below the 24 bytes of
        /// the smallest object the runtime can allocate. A few bytes per command would mean some fraction
        /// of parks allocated, which is exactly what the stock pooled builder does here: swapping
        /// <c>ParkedValueTaskMethodBuilder</c> for <c>PoolingAsyncValueTaskMethodBuilder</c> measures 6.5
        /// bytes per command, and any per-park closure, task or boxed work item costs far more again.
        /// <para>
        /// An unoptimized build emits the state machine as a class rather than a struct, so each call to the
        /// body allocates one no matter which builder holds it -- 96 bytes for this command, confirmed by
        /// reflecting on its <c>AsyncStateMachineAttribute</c>. That is a property of the build, so the
        /// budget there is loosened just enough to admit it and still catch a second allocation.
        /// </para>
        /// <para>
        /// A zero-length wait completes through the thread pool rather than arming a timer, and races the
        /// two halves of the park rendezvous -- the receive stack unwinding and the operation completing --
        /// in both orders, so this doubles as a soak test for that hand-off.
        /// </para>
        /// </remarks>
        [Test]
        public void AsyncParkAndResumeDoNotAllocate()
        {
            const int Warmup = 1000;
            const int Rounds = 8;
            const int CommandsPerRound = 2000;
            const int OkReplyLength = 5;
            const int PipelineDepth = 8;
#if DEBUG
            const int BudgetBytesPerCommand = 120;
#else
            const int BudgetBytesPerCommand = 4;
#endif

            using var client = new RawRespClient(TestUtils.EndPoint);
            var block = RawRespClient.Command("DEBUG", "BLOCKASYNC", "0");
            var pipelined = Enumerable.Repeat(block, PipelineDepth).SelectMany(b => b).ToArray();

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

            TestContext.Out.WriteLine($"async park and resume allocated {single:F1} bytes per command, " +
                                      $"{batched:F1} at pipeline depth {PipelineDepth}");

            ClassicAssert.Less(single, BudgetBytesPerCommand,
                $"Each parked command allocated {single:F1} bytes");

            // A resume that drains seven commands it never parsed must cost the same per command as one that
            // drains none, so anything proportional to buffered bytes or to pipeline depth shows up here.
            ClassicAssert.Less(batched, single + 1,
                $"Pipelining raised per-command allocation from {single:F1} to {batched:F1} bytes");
        }

        /// <summary>
        /// Parks running concurrently on many connections allocate nothing either.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The single-connection measurement is the easy case for anything that caches the body's state
        /// machine per thread or per core, because one connection parks and resumes on a small, warm set of
        /// threads. Concurrency is what the pattern exists for, and it is also what breaks those caches: a
        /// park is rented on the thread that finished parsing and given back on whichever thread completed
        /// the operation, so with enough connections in flight the two are rarely the same and a shared
        /// cache is found empty. Holding the state machine on the context instead makes the hit rate exact,
        /// and this is where that shows: the stock pooled builder measures 9.3 bytes per command here
        /// against nothing at all, and worsens as connections are added rather than improving.
        /// </para>
        /// <para>
        /// The load threads are started, warmed and left spinning on a volatile counter before anything is
        /// measured, so the harness contributes nothing to the window. Allocation is read process-wide,
        /// which is what makes that necessary.
        /// </para>
        /// </remarks>
        [Test]
        public void ConcurrentAsyncParksDoNotAllocate()
        {
            const int Connections = 16;
            const int Warmup = 200;
            const int Rounds = 6;
            const int CommandsPerConnectionPerRound = 500;
            const int OkReplyLength = 5;
            const int PipelineDepth = 8;
#if DEBUG
            const int BudgetBytesPerCommand = 120;
#else
            const int BudgetBytesPerCommand = 4;
#endif

            var clients = new RawRespClient[Connections];
            var threads = new Thread[Connections];
            var payload = Enumerable.Repeat(RawRespClient.Command("DEBUG", "BLOCKASYNC", "0"), PipelineDepth)
                                    .SelectMany(b => b).ToArray();

            var phase = 0;
            var ready = 0;
            var finished = 0;
            Exception failure = null;

            try
            {
                for (var i = 0; i < Connections; i++)
                    clients[i] = new RawRespClient(TestUtils.EndPoint);

                for (var i = 0; i < Connections; i++)
                {
                    var client = clients[i];
                    var thread = new Thread(() =>
                    {
                        try
                        {
                            void Run(int commands)
                            {
                                for (var n = 0; n < commands / PipelineDepth; n++)
                                {
                                    client.SendRaw(payload);
                                    client.Consume(OkReplyLength * PipelineDepth);
                                }
                            }

                            Run(Warmup);

                            for (var round = 1; round <= Rounds; round++)
                            {
                                _ = Interlocked.Increment(ref ready);
                                while (Volatile.Read(ref phase) < round)
                                    Thread.SpinWait(64);

                                Run(CommandsPerConnectionPerRound);
                                _ = Interlocked.Increment(ref finished);
                            }
                        }
                        catch (Exception e)
                        {
                            _ = Interlocked.CompareExchange(ref failure, e, null);

                            // Unblock the measuring thread rather than letting it wait out its timeout.
                            _ = Interlocked.Add(ref ready, Rounds);
                            _ = Interlocked.Add(ref finished, Rounds);
                        }
                    })
                    { IsBackground = true };

                    threads[i] = thread;
                    thread.Start();
                }

                var best = double.MaxValue;
                for (var round = 1; round <= Rounds; round++)
                {
                    var atStart = round;
                    ClassicAssert.IsTrue(
                        SpinUntil(() => Volatile.Read(ref ready) >= atStart * Connections || failure != null, 60000),
                        "Load threads did not reach the start of a round.");
                    if (failure != null)
                        break;

                    var before = GC.GetTotalAllocatedBytes(precise: true);
                    _ = Interlocked.Increment(ref phase);

                    var atEnd = round;
                    var ran = SpinUntil(() => Volatile.Read(ref finished) >= atEnd * Connections || failure != null,
                                        60000);

                    var elapsed = GC.GetTotalAllocatedBytes(precise: true) - before;

                    ClassicAssert.IsTrue(ran, "Load threads did not finish a round.");
                    if (failure != null)
                        break;

                    var measured = elapsed / (CommandsPerConnectionPerRound * (double)Connections);

                    if (measured < best)
                        best = measured;
                }

                if (failure != null)
                    throw new AssertionException("A load thread failed.", failure);

                TestContext.Out.WriteLine(
                    $"{Connections} concurrent connections allocated {best:F1} bytes per parked command");

                ClassicAssert.Less(best, BudgetBytesPerCommand,
                    $"Each concurrently parked command allocated {best:F1} bytes");
            }
            finally
            {
                _ = Interlocked.Add(ref phase, Rounds);

                foreach (var thread in threads)
                    _ = thread?.Join(TimeSpan.FromSeconds(10));

                foreach (var client in clients)
                    client?.Dispose();
            }
        }

        /// <summary>
        /// Repeated parks on one connection reuse a single context, rather than building one per call.
        /// </summary>
        /// <remarks>
        /// This is what the adapter's callback accounting buys. A context is recycled only once nothing can
        /// still reach it, counted by <c>AddCallback</c> and <c>CallbackCompleted</c> around both the body
        /// and each wait it takes; dropping either half leaves the count permanently positive, reuse is
        /// refused forever, and every park allocates a fresh context. Counted rather than measured in bytes
        /// because a reused context sits at zero bytes either way.
        /// </remarks>
        [Test]
        public void RepeatedAsyncParksOnAConnectionReuseOneContext()
        {
#if !DEBUG
            Assert.Ignore("Contexts are only counted in Debug builds.");
#else
            const int Parks = 200;

            using var client = new RawRespClient(TestUtils.EndPoint);

            // Warm up outside the measurement: the first park on a connection has nothing to reuse.
            client.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC", "0"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            BlockingCommandContext.ResetAllocatedContexts();

            for (var i = 0; i < Parks; i++)
            {
                client.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC", "0"));
                ClassicAssert.AreEqual("+OK", client.ReadLine());
            }

            var built = BlockingCommandContext.AllocatedContexts;
            TestContext.Out.WriteLine($"{Parks} parks built {built} contexts");

            // Not zero: a zero-length wait can complete before the park hand-off finishes, which legitimately
            // refuses reuse. What must not happen is one per park.
            ClassicAssert.Less(built, Parks / 4,
                $"{Parks} parks on one connection built {built} contexts, so they are not being reused");
#endif
        }

        /// <summary>
        /// A connection that goes away mid-block is cleaned up, including when it disconnects in the same
        /// instant the session is parking.
        /// </summary>
        [Test]
        public void DisconnectWhileAsyncBlockedIsCleanedUp()
        {
            // No delay, so teardown lands anywhere in the park: before the command is dispatched, between
            // parking and the receive stack unwinding, or after.
            for (var i = 0; i < 200; i++)
            {
                var client = new RawRespClient(TestUtils.EndPoint);
                client.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC", "30"));
                client.Dispose();
            }

            // And with the park reliably complete first.
            for (var i = 0; i < 10; i++)
            {
                var client = new RawRespClient(TestUtils.EndPoint);
                client.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC", "30"));
                Thread.Sleep(20);
                client.Dispose();
            }

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            ClassicAssert.AreEqual("PONG", db.Execute("PING").ToString());
        }

        /// <summary>
        /// An aborted command unwinds its body straight away, rather than leaving it parked on a wait that
        /// can no longer do anything useful.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The abort releases the wait as well as cancelling the token, which is what makes this prompt:
        /// without it the state machine, its context and everything they reference stay alive until the
        /// original timer fires -- half a minute here, and unbounded in general, because the duration is the
        /// client's to choose. The long wait is the point of the test, so the assertion is that the bodies
        /// are gone in a fraction of it.
        /// </para>
        /// <para>
        /// <c>CLIENT KILL</c> is the abort, not a client-side close. A parked session has no receive
        /// outstanding, so nothing on the server is positioned to see a peer close until the session resumes
        /// and reads again; the kill is the path that reaches a parked operation, and it is the same one
        /// <see cref="RespBlockingSessionTests.ClientKillWhileBlockedTakesEffectImmediately"/> covers for the
        /// hand-written style.
        /// </para>
        /// </remarks>
        [Test]
        public void AbortReleasesABlockedBodyWithoutWaitingOutItsDelay()
        {
#if !DEBUG
            Assert.Ignore("Running bodies are only counted in Debug builds.");
#else
            const int Connections = 32;
            const int BlockSeconds = 30;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            var clients = new List<RawRespClient>(Connections);
            var ids = new List<string>(Connections);
            try
            {
                for (var i = 0; i < Connections; i++)
                {
                    var client = new RawRespClient(TestUtils.EndPoint);
                    clients.Add(client);

                    client.Send(RawRespClient.Command("CLIENT", "ID"));
                    var idReply = client.ReadLine();
                    ClassicAssert.IsTrue(idReply.StartsWith(':'), $"Unexpected CLIENT ID reply: {idReply}");
                    ids.Add(idReply[1..]);

                    client.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC", BlockSeconds.ToString()));
                }

                ClassicAssert.IsTrue(
                    SpinUntil(() => RespServerSession.AsyncBlockingCommandContext.RunningBodies >= Connections),
                    $"Only {RespServerSession.AsyncBlockingCommandContext.RunningBodies} of {Connections} "
                    + "bodies started");

                var sw = Stopwatch.StartNew();
                foreach (var id in ids)
                    ClassicAssert.AreEqual(1, (int)db.Execute("CLIENT", "KILL", "ID", id));

                var released = SpinUntil(() => RespServerSession.AsyncBlockingCommandContext.RunningBodies == 0);
                sw.Stop();

                ClassicAssert.IsTrue(released,
                    $"{RespServerSession.AsyncBlockingCommandContext.RunningBodies} bodies were still parked "
                    + $"{sw.ElapsedMilliseconds}ms after being killed, out of a {BlockSeconds}s wait: the "
                    + "abort is not releasing the wait, only cancelling the token");

                TestContext.Out.WriteLine($"{Connections} bodies unwound in {sw.ElapsedMilliseconds}ms "
                                          + $"of a {BlockSeconds}s wait");
            }
            finally
            {
                foreach (var client in clients)
                    client.Dispose();
            }

            ClassicAssert.AreEqual("PONG", db.Execute("PING").ToString());
#endif
        }

        /// <summary>
        /// A connection torn down between the read and the reply still gives the pooled buffer back, which
        /// in a body is one <c>finally</c> covering both.
        /// </summary>
        /// <remarks>
        /// The window is narrow, so this is a soak: each round parks a read that is about to resume and
        /// drops the connection underneath it. A buffer kept back here is never returned by anything, so a
        /// leak starves the pool and the reads that follow stop succeeding.
        /// </remarks>
        [Test]
        public void DisconnectDuringAsyncBlockGetDoesNotStrandTheValue()
        {
            const int Rounds = 300;

            using (var loader = new RawRespClient(TestUtils.EndPoint))
            {
                loader.Send(RawRespClient.Command("SET", "leaky", new string('v', 512)));
                ClassicAssert.AreEqual("+OK", loader.ReadLine());
            }

            for (var i = 0; i < Rounds; i++)
            {
                var client = new RawRespClient(TestUtils.EndPoint);
                client.Send(RawRespClient.Command("DEBUG", "BLOCKGETASYNC", "0", "leaky"));

                // Lands across the whole of the read-to-reply window over a few hundred rounds.
                if ((i & 3) == 0)
                    Thread.Sleep(1);

                client.Dispose();
            }

#if DEBUG
            ClassicAssert.IsTrue(SpinUntil(() => RespServerSession.AsyncBlockingCommandContext.RunningBodies == 0),
                "Bodies were still running after their connections went away");
#endif

            // The reads that follow are what a starved pool would fail.
            using var client2 = new RawRespClient(TestUtils.EndPoint);
            for (var i = 0; i < 50; i++)
            {
                client2.Send(RawRespClient.Command("DEBUG", "BLOCKGETASYNC", "0", "leaky"));
                ClassicAssert.AreEqual(512, client2.ReadBulkString().Length);
            }
        }

        /// <summary>
        /// CLIENT KILL takes effect while the target is parked, rather than leaving the session alive until
        /// its blocking operation would have finished on its own.
        /// </summary>
        [Test]
        public void ClientKillWhileAsyncBlockedTakesEffectImmediately()
        {
            using var victim = new RawRespClient(TestUtils.EndPoint);
            victim.Send(RawRespClient.Command("CLIENT", "ID"));
            var idReply = victim.ReadLine();
            ClassicAssert.IsTrue(idReply.StartsWith(':'), $"Unexpected CLIENT ID reply: {idReply}");
            var victimId = idReply[1..];

            victim.Send(RawRespClient.Command("DEBUG", "BLOCKASYNC", "30"));
            Thread.Sleep(200);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            var sw = Stopwatch.StartNew();
            var killed = db.Execute("CLIENT", "KILL", "ID", victimId).ToString();
            sw.Stop();

            ClassicAssert.AreEqual("1", killed, "CLIENT KILL did not find the parked session");
            ClassicAssert.Less(sw.ElapsedMilliseconds, 5000,
                $"CLIENT KILL against a parked session took {sw.ElapsedMilliseconds}ms");

            ClassicAssert.AreEqual("PONG", db.Execute("PING").ToString());
        }

        /// <summary>
        /// Where the session cannot park -- inside a transaction -- the command still owes a non-blocking
        /// answer, and gives one inline instead of waiting in place.
        /// </summary>
        /// <remarks>
        /// Scripts are covered by the same reasoning as the hand-written style
        /// (<see cref="RespBlockingSessionTests.BlockingFallsBackWhereParkingIsUnsafe"/>): DEBUG is
        /// <c>noscript</c>, so a script cannot reach these commands at all, and the script path could not
        /// park in any case because its session is built over a <c>ScratchBufferNetworkSender</c> rather
        /// than a park host. The assertion here is that it keeps refusing.
        /// </remarks>
        [Test]
        public void AsyncBlockingFallsBackWhereParkingIsUnsafe()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            // The reply is the one the wait would have produced, and it arrives without the wait: waiting in
            // place is what this pattern exists to remove, so a command that cannot park answers rather than
            // pinning the thread running the transaction.
            var tran = db.CreateTransaction();
            var queued = tran.ExecuteAsync("DEBUG", "BLOCKASYNC", "5");
            var sw = Stopwatch.StartNew();
            ClassicAssert.IsTrue(tran.Execute());
            sw.Stop();
            ClassicAssert.AreEqual("OK", queued.Result.ToString());
            ClassicAssert.Less(sw.ElapsedMilliseconds, 2000,
                $"DEBUG BLOCKASYNC waited {sw.ElapsedMilliseconds}ms inside EXEC instead of falling back");

            var ex = Assert.Throws<RedisServerException>(
                () => db.ScriptEvaluate("return redis.call('DEBUG', 'BLOCKASYNC', '0.2')"));
            ClassicAssert.IsTrue(ex.Message.Contains("not allowed from script", StringComparison.OrdinalIgnoreCase));

            // BLOCKGETASYNC refuses inside a transaction rather than reading a key the transaction did not
            // lock. The refusal arrives at EXEC, because MULTI only queues.
            using var client = new RawRespClient(TestUtils.EndPoint);
            client.Send(RawRespClient.Command("MULTI"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());
            client.Send(RawRespClient.Command("DEBUG", "BLOCKGETASYNC", "0", "whatever"));
            ClassicAssert.AreEqual("+QUEUED", client.ReadLine());
            client.Send(RawRespClient.Command("EXEC"));
            ClassicAssert.AreEqual("*1", client.ReadLine());
            StringAssert.StartsWith("-ERR", client.ReadLine());

            ClassicAssert.AreEqual("PONG", db.Execute("PING").ToString());
        }

        /// <summary>
        /// The two authoring styles are interchangeable from a client's point of view, which is what makes
        /// the comparison between them a comparison of the code rather than of the behaviour.
        /// </summary>
        [Test]
        public void AsyncAndHandWrittenBlockingAgreeFromTheClient()
        {
            using var client = new RawRespClient(TestUtils.EndPoint);

            client.Send(RawRespClient.Command("SET", "agreed", "same-value"));
            ClassicAssert.AreEqual("+OK", client.ReadLine());

            (string Block, string Get)[] styles = [("BLOCK", "BLOCKGET"), ("BLOCKASYNC", "BLOCKGETASYNC")];

            foreach (var (block, get) in styles)
            {
                client.Send(RawRespClient.Command("DEBUG", block, "0.2"));
                ClassicAssert.AreEqual("+OK", client.ReadLine(), $"DEBUG {block} replied differently");

                client.Send(RawRespClient.Command("DEBUG", get, "0.2", "agreed"));
                ClassicAssert.AreEqual("same-value", client.ReadBulkString(), $"DEBUG {get} replied differently");

                client.Send(RawRespClient.Command("DEBUG", get, "0", "absent"));
                ClassicAssert.IsNull(client.ReadBulkString(), $"DEBUG {get} replied differently for a miss");
            }
        }
    }
}