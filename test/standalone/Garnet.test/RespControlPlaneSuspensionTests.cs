// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
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
    /// Covers the control-plane commands that wait on work the session cannot finish on the network thread --
    /// <c>SAVE</c>, <c>BGSAVE</c>, <c>CONFIG SET index</c> and <c>ASYNC BARRIER</c> -- now that each parks the
    /// session instead of blocking.
    /// </summary>
    /// <remarks>
    /// Each of these waits used to be an <c>AsyncUtils.BlockingWait</c>, so the reply was written by the same
    /// call that started the wait. A parked command writes its reply from a resumed state machine instead,
    /// after the per-batch scope has been given back and retaken, which is where a conversion goes wrong: the
    /// reply is lost, written twice, or written into a buffer the session no longer owns. Pipelining each
    /// command between replies whose text is known pins down all three.
    /// </remarks>
    [TestFixture]
    public class RespControlPlaneSuspensionTests : TestBase
    {
        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";
        const string BgsaveStarted = "+Background saving started\r\n";

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
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Fills the store so that a checkpoint has something to write and does not finish instantly.
        /// </summary>
        static void Populate(RawRespSession session, int keys)
        {
            var value = new string('v', 512);
            for (var i = 0; i < keys; i++)
                ClassicAssert.AreEqual(Ok, session.Execute("SET", $"ckpt-{i}", value));
        }

        /// <summary>
        /// A checkpoint already running answers <c>AlreadyInProgress</c>, so a <c>SAVE</c> is only known to
        /// have waited on one if it replies OK.
        /// </summary>
        static string ExpectSaveReply(string reply)
        {
            ClassicAssert.AreEqual(Ok, reply, "SAVE did not reply OK.");
            return reply;
        }

        /// <summary>Reads one setting out of a <c>CONFIG GET</c> reply of a single pair.</summary>
        static string ConfigGet(RawRespSession session, string name)
        {
            var reply = session.Execute("CONFIG", "GET", name);
            var parts = reply.Split("\r\n");
            ClassicAssert.AreEqual(6, parts.Length, $"Unexpected CONFIG GET reply '{reply}'.");
            return parts[4];
        }

        [Test]
        public void SaveKeepsItsPlaceInAPipeline()
        {
            using var session = new RawRespSession();
            Populate(session, 2_000);

            // Everything arrives in one receive, so the parse loop reaches SAVE with the two PINGs still
            // unparsed behind it and has to serve them, in order, only once SAVE has resumed and replied.
            session.SendPipeline(
                ["PING"],
                ["SAVE"],
                ["PING"]);

            ClassicAssert.AreEqual(Pong, session.ReadReply());
            ExpectSaveReply(session.ReadReply());
            ClassicAssert.AreEqual(Pong, session.ReadReply());

            // A reply written twice, or a stale one left in the response object, surfaces as the session
            // answering the next command with the previous command's text.
            ClassicAssert.AreEqual(Pong, session.Execute("PING"));
        }

        [Test]
        public void BackgroundSaveKeepsItsPlaceInAPipeline()
        {
            using var session = new RawRespSession();
            Populate(session, 1_000);

            session.SendPipeline(
                ["PING"],
                ["BGSAVE"],
                ["PING"]);

            ClassicAssert.AreEqual(Pong, session.ReadReply());
            ClassicAssert.AreEqual(BgsaveStarted, session.ReadReply());
            ClassicAssert.AreEqual(Pong, session.ReadReply());
            ClassicAssert.AreEqual(Pong, session.Execute("PING"));
        }

        /// <summary>
        /// <c>CONFIG SET index</c> grows the index a doubling at a time and parks for each step. The settings
        /// named alongside it are applied after the resize and share its single reply, so the reply also shows
        /// that the resumed body ran the rest of the command rather than only the part before the wait.
        /// </summary>
        [Test]
        public void ConfigSetIndexAppliesTheSettingsAlongsideIt()
        {
            using var session = new RawRespSession();

            ClassicAssert.AreNotEqual("321", ConfigGet(session, "object-scan-count-limit"));

            session.SendPipeline(
                ["PING"],
                ["CONFIG", "SET", "index", "64m", "object-scan-count-limit", "321"],
                ["PING"]);

            ClassicAssert.AreEqual(Pong, session.ReadReply());
            ClassicAssert.AreEqual(Ok, session.ReadReply());
            ClassicAssert.AreEqual(Pong, session.ReadReply());

            // The setting that followed the resize in the same command took effect, which it cannot have done
            // if the body stopped at the wait.
            ClassicAssert.AreEqual("321", ConfigGet(session, "object-scan-count-limit"));
            ClassicAssert.AreEqual(Pong, session.Execute("PING"));
        }

        /// <summary>
        /// An error raised before the resize still has to be reported by the resumed body, which is the one
        /// writing the reply.
        /// </summary>
        [Test]
        public void ConfigSetIndexReportsAnErrorRaisedBeforeTheResize()
        {
            using var session = new RawRespSession();

            var reply = session.Execute("CONFIG", "SET", "index", "64m", "maxmemory", "not-a-size");
            StringAssert.StartsWith("-", reply, $"Expected an error, got '{reply}'.");
            ClassicAssert.AreEqual(Pong, session.Execute("PING"));
        }

        /// <summary>
        /// Several sessions saving at once all get an answer. Only one checkpoint runs at a time, so the rest
        /// are told one is already in progress -- but every one of them replies, which a conversion that drops
        /// a reply on the synchronous completion path would not do.
        /// </summary>
        [Test]
        public void EverySessionSavingAtOnceGetsAReply()
        {
            const int SessionCount = 16;

            using var writer = new RawRespSession();
            Populate(writer, 2_000);

            var sessions = new RawRespSession[SessionCount];
            try
            {
                for (var i = 0; i < SessionCount; i++)
                    sessions[i] = new RawRespSession();

                foreach (var s in sessions)
                    s.Send("SAVE");

                for (var i = 0; i < SessionCount; i++)
                {
                    var reply = sessions[i].ReadReply();
                    if (reply != Ok)
                        StringAssert.StartsWith("-", reply, $"Session {i} answered SAVE with '{reply}'.");
                }

                // Each session is still usable and still aligned with its own reply stream.
                foreach (var s in sessions)
                    ClassicAssert.AreEqual(Pong, s.Execute("PING"));
            }
            finally
            {
                foreach (var s in sessions)
                    s?.Dispose();
            }
        }

        /// <summary>
        /// A session waiting out a checkpoint holds no thread. The pool is cut to a single free worker, which a save
        /// that waited in place would hold until the checkpoint is done, leaving neither the sessions around it nor
        /// the checkpoint's own continuations a thread to run on.
        /// </summary>
        [Test]
        public void ASaveDoesNotStallOtherSessions()
        {
            const int PingCount = 50;
            var stallLimit = TimeSpan.FromSeconds(20);

            using var writer = new RawRespSession();
            Populate(writer, 20_000);

            using var saver = new RawRespSession();
            using var pinger = new RawRespSession();

            // With more than one free worker the claim is unfalsifiable: the other sessions are served by the
            // spare, so a blocking SAVE passes too.
            using (ThreadPoolCap.ApplyFree(1))
            {
                // Warm both sessions so neither measurement includes a first-command cost.
                ClassicAssert.AreEqual(Pong, saver.Execute("PING"));
                ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));

                saver.Send("SAVE");

                for (var i = 0; i < PingCount; i++)
                {
                    pinger.Send("PING");
                    ClassicAssert.IsTrue(pinger.WaitForReply(stallLimit),
                        $"PING {i} was not answered within {stallLimit.TotalSeconds:F0}s while another session was " +
                        "saving against a pool of one thread, which indicates the save was holding it.");
                    ClassicAssert.AreEqual(Pong, pinger.ReadReply());
                }

                ExpectSaveReply(saver.ReadReply());
            }
        }
    }

    /// <summary>
    /// Covers RESP3 async mode: <c>GET</c>s of on-disk keys that are acknowledged immediately and finished by a
    /// push, and the <c>ASYNC BARRIER</c> that waits for them.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>ASYNC BARRIER</c> is the one converted control-plane wait whose duration a client controls and which
    /// several sessions can be inside at once, so it is the one that can show the difference a park makes: the
    /// wait is per-session, so N barriering sessions used to hold N threads, and the completions they are
    /// waiting for are produced by per-session background tasks that need threads of their own. Those tasks
    /// are themselves waits on the device, and held a thread each for as long as the reads were in flight.
    /// </para>
    /// <para>
    /// An asynchronous GET is also the one command whose read outlives the command: the session keeps
    /// receiving into the same buffer while the read is on the device.
    /// </para>
    /// </remarks>
    [TestFixture]
    public class RespAsyncGetTests : TestBase
    {
        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";

        /// <summary>Enough records that the earliest ones are no longer in memory.</summary>
        const int PopulatedKeys = 8192;

        const int ValueLength = 48;

        GarnetServer server;
        DelayedReadDeviceFactoryCreator deviceFactoryCreator;

        static readonly FieldInfo SenderWaitersField = typeof(RespServerSession).GetField("senderWaiters", BindingFlags.NonPublic | BindingFlags.Instance);

        /// <summary>What is waiting for the session to let go of its response object.</summary>
        static int SenderWaiters(RespServerSession session) => (int)SenderWaitersField.GetValue(session);

        /// <summary>Whether the async GET processor is waiting for the session to let go of its response object.</summary>
        static bool ProcessorWaitsForSender(RespServerSession session) => (SenderWaiters(session) & 1) != 0;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            var options = TestUtils.GetGarnetServerOptions(
                checkpointDir: TestUtils.MethodTestDir,
                logDir: TestUtils.MethodTestDir,
                endpoint: TestUtils.EndPoint,
                enableCluster: false,
                lowMemory: true);

            deviceFactoryCreator = new DelayedReadDeviceFactoryCreator();
            options.DeviceFactoryCreator = deviceFactoryCreator;

            server = new GarnetServer(options);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            if (deviceFactoryCreator is not null)
            {
                // Shutting the server down waits for reads that are still held
                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReads();
            }

            server?.Dispose();
            server = null;
            deviceFactoryCreator?.Dispose();
            deviceFactoryCreator = null;

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        static string Key(int i) => $"barrier:{i:D6}";

        static string Value(int i) => new((char)('a' + (i % 26)), ValueLength);

        void Populate()
        {
            using var session = new RawRespSession();
            for (var i = 0; i < PopulatedKeys; i++)
                ClassicAssert.AreEqual(Ok, session.Execute("SET", Key(i), Value(i)));

            // The records the test reads back must actually be there, and must have fallen out of memory.
            ClassicAssert.AreEqual($"${ValueLength}\r\n{Value(0)}\r\n", session.Execute("GET", Key(0)));
        }

        /// <summary>
        /// Puts a session into RESP3 async mode, in which a GET of a record that is on disk is answered with an
        /// ASYNC token rather than a value and finished later through a push.
        /// </summary>
        static void EnableAsyncMode(RawRespSession session)
        {
            _ = session.Execute("HELLO", "3");
            ClassicAssert.AreEqual(Ok, session.Execute("ASYNC", "ON"));
        }

        static void AssertWentAsynchronous(string reply, string what)
            => StringAssert.StartsWith("-ASYNC ", reply, $"{what} did not go asynchronous; it answered '{reply}'.");

        static void IssueAsyncGet(RawRespSession session, int keyIndex)
        {
            EnableAsyncMode(session);
            AssertWentAsynchronous(session.Execute("GET", Key(keyIndex)), $"GET of {Key(keyIndex)}");
        }

        /// <summary>
        /// Linux only. These tests prove a negative -- that a parked session holds no thread -- by starving the
        /// pool that dispatches its work. On Linux socket completions run on worker threads, so capping them
        /// starves exactly that path; on Windows they run on the I/O-completion pool, which cannot safely be
        /// capped, so the cap would constrain nothing.
        /// </summary>
        static void RequireLinux()
        {
            if (!OperatingSystem.IsLinux())
                Assert.Ignore("Starving the worker pool only constrains session dispatch on Linux.");
        }

        /// <summary>
        /// Reads the next reply, failing the test rather than waiting out the socket timeout if none has
        /// arrived by <paramref name="deadline"/> after <paramref name="clock"/> started.
        /// </summary>
        static string ReadReplyBy(RawRespSession session, Stopwatch clock, TimeSpan deadline, string what)
        {
            var remaining = deadline - clock.Elapsed;
            if (!session.WaitForReply(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero))
                Assert.Fail($"No {what} arrived within {deadline.TotalSeconds:F0}s.");
            return session.ReadReply();
        }

        /// <summary>
        /// <see cref="SessionCount"/> sessions that each have an asynchronous GET of an on-disk record in
        /// flight, on a worker pool of <see cref="CappedWorkers"/> threads.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The pool is capped because without it nothing here is falsifiable: the runtime injects a thread for
        /// every blocked wait, in well under a second, so an implementation that holds a thread per session
        /// finishes as quickly as one that parks.
        /// </para>
        /// <para>
        /// The device delay is set before the reads are issued. Set afterwards, the reads have already completed
        /// by the time the test acts, and there is nothing left for a barrier or a drain to wait on.
        /// </para>
        /// </remarks>
        sealed class ReadsInFlight : IDisposable
        {
            internal const int SessionCount = 64;
            internal const int CappedWorkers = 8;
            internal const int ReadDelayMs = 1000;

            readonly DelayedReadDeviceFactoryCreator devices;
            ThreadPoolCap cap;

            internal ReadsInFlight(DelayedReadDeviceFactoryCreator devices) => this.devices = devices;

            internal RawRespSession[] Sessions { get; } = new RawRespSession[SessionCount];

            /// <summary>
            /// Time since just before the first read was issued, which bounds from below how long they have all
            /// been in flight: none can complete until <see cref="ReadDelayMs"/> after it was issued.
            /// </summary>
            internal Stopwatch SinceIssued { get; } = new();

            /// <summary>
            /// Connects the sessions, caps the pool and issues one read per session, returning once the server
            /// has acknowledged every one of them with an ASYNC token.
            /// </summary>
            internal void Start()
            {
                for (var i = 0; i < SessionCount; i++)
                {
                    Sessions[i] = new RawRespSession();
                    EnableAsyncMode(Sessions[i]);
                }

                cap = ThreadPoolCap.Apply(CappedWorkers);

                devices.ReadDelayMs = ReadDelayMs;

                SinceIssued.Restart();
                for (var i = 0; i < SessionCount; i++)
                    Sessions[i].Send("GET", Key(i));
                for (var i = 0; i < SessionCount; i++)
                    AssertWentAsynchronous(Sessions[i].ReadReply(), $"GET of {Key(i)}");

                ClassicAssert.Less(SinceIssued.Elapsed.TotalMilliseconds, ReadDelayMs * 0.8,
                    $"Acknowledging {SessionCount} GETs took {SinceIssued.Elapsed.TotalMilliseconds:F0}ms against a " +
                    $"{ReadDelayMs}ms device read, so the reads were not in flight when the test went on to act: " +
                    $"the sessions are holding the pool's {CappedWorkers} threads while they wait.");
            }

            public void Dispose()
            {
                cap?.Dispose();

                foreach (var s in Sessions)
                    s?.Dispose();
            }
        }

        /// <summary>
        /// The read is issued against the key in the network buffer, and the session keeps receiving into that
        /// buffer while the read is on the device. The read has to verify the record it fetched against its own
        /// copy of the key: against the buffer it would compare the bytes of a later command and answer null for
        /// a key that exists.
        /// </summary>
        [Test]
        public void AsyncGetOfAnOnDiskKeyIsUnaffectedByLaterCommandsReusingTheNetworkBuffer()
        {
            Populate();
            deviceFactoryCreator.ReadDelayMs = 200;

            using var session = new RawRespSession();
            IssueAsyncGet(session, 0);

            // Long enough to cover the whole of the GET's key, and received while the read is still on the device.
            var filler = new string('x', 128);
            ClassicAssert.AreEqual($"${filler.Length}\r\n{filler}\r\n", session.Execute("ECHO", filler));

            var push = session.ReadReply();
            StringAssert.StartsWith(">3\r\n", push);
            StringAssert.Contains($"${ValueLength}\r\n{Value(0)}\r\n", push,
                $"The asynchronous GET of an existing key did not push its value; it pushed '{push}'.");
        }

        /// <summary>
        /// The processor holds the response object while it pushes the values of the reads that finished, so a
        /// push that waits for the socket keeps the session's own sends waiting on the sender behind a client
        /// that is slow to read. Where socket completions run on the thread that receives for the session, that
        /// is the thread that would complete the wait. A push hands its bytes to the sender and carries on, as
        /// the session's own writes do.
        /// </summary>
        /// <remarks>
        /// The session is driven without a socket, against a sender that holds a send that waits for room until
        /// the test lets it go. A push that waits is the one that never reaches the wire.
        /// </remarks>
        [Test]
        public async Task AsyncGetPushesItsValueWithoutWaitingForTheSocket()
        {
            var bound = TimeSpan.FromSeconds(15);

            Populate();
            deviceFactoryCreator.ReadDelayMs = 1000;

            var sender = new ControlledSender(bufferSize: 4096) { WaitingSendsBlock = true };
            var session = (RespServerSession)server.Provider.GetSession(WireFormat.ASCII, sender);
            try
            {
                var input = Pinned(Frame("HELLO", "3"), Frame("ASYNC", "ON"), Frame("GET", Key(0)));
                var consumed = await Task.Run(() => Consume(session, input).AsTask()).WaitAsync(bound);
                ClassicAssert.AreEqual(input.Length, consumed);
                StringAssert.Contains("-ASYNC 0\r\n", sender.Sent, "The GET of an on-disk key did not go asynchronous.");

                // The sender fills while the read is on the device, so the push finds it full
                sender.Full = true;

                var push = $">3\r\n$5\r\nasync\r\n$1\r\n0\r\n${ValueLength}\r\n{Value(0)}\r\n";
                var clock = Stopwatch.StartNew();
                while (!sender.Sent.Contains(push))
                {
                    if (clock.Elapsed > bound)
                        Assert.Fail($"The value was not pushed within {bound.TotalSeconds:F0}s; the processor made " +
                            $"{sender.WaitingSends} send(s) that wait for the sender to have room.");
                    await Task.Delay(1);
                }

                ClassicAssert.AreEqual(0, sender.WaitingSends, "The push waited for the sender to have room.");
            }
            finally
            {
                sender.ReleaseWaitingSends();
                session.Dispose();
            }
        }

        /// <summary>
        /// The processor pushes the values of the reads that finished through the session's response object, which
        /// a command suspended with an operation in flight holds until it resumes. The processor waits for the
        /// resume without a thread, and pushes after it.
        /// </summary>
        [Test]
        public async Task AsyncGetWaitsWithoutAThreadForASuspendedSessionToResume()
        {
            var bound = TimeSpan.FromSeconds(15);

            Populate();
            deviceFactoryCreator.ReadDelayMs = 1000;

            var sender = new ControlledSender(bufferSize: 4096);
            var session = (RespServerSession)server.Provider.GetSession(WireFormat.ASCII, sender);
            try
            {
                var input = Pinned(Frame("HELLO", "3"), Frame("ASYNC", "ON"), Frame("GET", Key(0)));
                var consumed = await Task.Run(() => Consume(session, input).AsTask()).WaitAsync(bound);
                ClassicAssert.AreEqual(input.Length, consumed);
                StringAssert.EndsWith("-ASYNC 0\r\n", sender.Sent, "The GET of an on-disk key did not go asynchronous.");
                var before = sender.Sent;

                // Suspends holding the response object for longer than the read takes
                var park = await Task.Run(() => Consume(session, Pinned(Frame("DEBUG", "BLOCKIO", "4")))).WaitAsync(bound);
                ClassicAssert.IsFalse(park.IsCompleted, "The session did not suspend.");

                var clock = Stopwatch.StartNew();
                while (!ProcessorWaitsForSender(session))
                {
                    if (clock.Elapsed > bound)
                        Assert.Fail($"The processor did not wait for the session to resume within {bound.TotalSeconds:F0}s.");
                    await Task.Delay(1);
                }

                ClassicAssert.IsTrue(sender.Entered, "The session resumed before the processor waited for it.");
                ClassicAssert.AreEqual(before, sender.Sent);

                _ = await park.AsTask().WaitAsync(bound);
                var push = $">3\r\n$5\r\nasync\r\n$1\r\n0\r\n${ValueLength}\r\n{Value(0)}\r\n";
                clock.Restart();
                while (sender.Sent != before + Ok + push)
                {
                    if (clock.Elapsed > bound)
                        Assert.Fail($"The value was not pushed after the reply within {bound.TotalSeconds:F0}s; the session was sent '{sender.Sent[before.Length..]}'.");
                    await Task.Delay(1);
                }

                ClassicAssert.AreEqual(0, SenderWaiters(session), "Something is still waiting for the response object.");
            }
            finally
            {
                session.Dispose();
            }
        }

        /// <summary>
        /// The processor that finds the response object held says it is waiting and then tries again, so a holder
        /// that let go before it said so, and so did not wake it, does not leave it waiting.
        /// </summary>
        [Test]
        public async Task AsyncGetTakesAResponseObjectLetGoBeforeItsProcessorSaidItWasWaiting()
        {
            var bound = TimeSpan.FromSeconds(15);

            Populate();
            deviceFactoryCreator.ReadDelayMs = 1000;

            var sender = new ControlledSender(bufferSize: 4096);
            var session = (RespServerSession)server.Provider.GetSession(WireFormat.ASCII, sender);
            try
            {
                var input = Pinned(Frame("HELLO", "3"), Frame("ASYNC", "ON"), Frame("GET", Key(0)));
                var consumed = await Task.Run(() => Consume(session, input).AsTask()).WaitAsync(bound);
                ClassicAssert.AreEqual(input.Length, consumed);
                StringAssert.EndsWith("-ASYNC 0\r\n", sender.Sent, "The GET of an on-disk key did not go asynchronous.");
                var before = sender.Sent;

                // The response object is held until the processor says it is waiting, and let go without waking it
                sender.FailTryEnterUntil = () => ProcessorWaitsForSender(session);

                var push = $">3\r\n$5\r\nasync\r\n$1\r\n0\r\n${ValueLength}\r\n{Value(0)}\r\n";
                var clock = Stopwatch.StartNew();
                while (sender.Sent != before + push)
                {
                    if (clock.Elapsed > bound)
                        Assert.Fail($"The value was not pushed within {bound.TotalSeconds:F0}s; the processor " +
                            $"{(ProcessorWaitsForSender(session) ? "is waiting for the response object" : "did not say it was waiting")}.");
                    await Task.Delay(1);
                }

                ClassicAssert.IsFalse(sender.FailingTryEnter);
                ClassicAssert.AreEqual(0, SenderWaiters(session), "Something is still waiting for the response object.");
            }
            finally
            {
                sender.FailTryEnterUntil = null;
                session.Dispose();
            }
        }

        /// <summary>
        /// Tearing a session down tells its async GET processor to stop once its reads are done. A processor
        /// waiting for the session to resume when that happens has to see it too, or it keeps the session's
        /// storage sessions, and shutting the server down waits for them.
        /// </summary>
        [Test]
        public async Task AnAsyncGetWaitingForASuspendedSessionStopsWhenTheSessionIsTornDown()
        {
            var bound = TimeSpan.FromSeconds(15);

            Populate();
            deviceFactoryCreator.ReadDelayMs = 1000;

            var sender = new ControlledSender(bufferSize: 4096);
            var session = (RespServerSession)server.Provider.GetSession(WireFormat.ASCII, sender);
            var storeWrapper = server.Provider.StoreWrapper;
            try
            {
                var input = Pinned(Frame("HELLO", "3"), Frame("ASYNC", "ON"), Frame("GET", Key(0)));
                var consumed = await Task.Run(() => Consume(session, input).AsTask()).WaitAsync(bound);
                ClassicAssert.AreEqual(input.Length, consumed);
                StringAssert.EndsWith("-ASYNC 0\r\n", sender.Sent, "The GET of an on-disk key did not go asynchronous.");

                var park = await Task.Run(() => Consume(session, Pinned(Frame("DEBUG", "BLOCKIO", "60")))).WaitAsync(bound);
                ClassicAssert.IsFalse(park.IsCompleted, "The session did not suspend.");

                var clock = Stopwatch.StartNew();
                while (!ProcessorWaitsForSender(session))
                {
                    if (clock.Elapsed > bound)
                        Assert.Fail($"The processor did not wait for the session to resume within {bound.TotalSeconds:F0}s.");
                    await Task.Delay(1);
                }
            }
            finally
            {
                session.Dispose();
            }

            if (!SpinWait.SpinUntil(() => storeWrapper.StorageTeardowns == 0, bound))
            {
                // Shutting down would wait for the storage sessions forever, so it is left to run on its own
                var stuck = server;
                server = null;
                _ = Task.Factory.StartNew(stuck.Dispose, TaskCreationOptions.LongRunning);
                Assert.Fail("The async GET processor of a session torn down while suspended kept its storage sessions.");
            }
        }

        static unsafe ValueTask<int> Consume(RespServerSession session, byte[] pinnedInput)
            => session.TryConsumeMessagesAsync((byte*)Unsafe.AsPointer(ref pinnedInput[0]), pinnedInput.Length);

        static string Frame(params string[] tokens)
        {
            var frame = new StringBuilder().Append('*').Append(tokens.Length).Append("\r\n");
            foreach (var token in tokens)
                _ = frame.Append('$').Append(token.Length).Append("\r\n").Append(token).Append("\r\n");
            return frame.ToString();
        }

        static byte[] Pinned(params string[] frames)
        {
            var bytes = Encoding.ASCII.GetBytes(string.Concat(frames));
            var pinned = GC.AllocateArray<byte>(bytes.Length, pinned: true);
            bytes.CopyTo(pinned, 0);
            return pinned;
        }

        [Test]
        public void AsyncBarrierWaitsForTheGetItIsBarrieringOn()
        {
            Populate();
            deviceFactoryCreator.ReadDelayMs = 200;

            using var session = new RawRespSession();
            IssueAsyncGet(session, 0);

            // The push carrying the GET's value is written by the background processor, and the barrier's OK
            // only after it, so this order is what proves the barrier waited rather than replying straight
            // away -- and that its OK was written from a resumed body, into a response object the session had
            // given back and retaken.
            session.Send("ASYNC", "BARRIER");

            var push = session.ReadReply();
            StringAssert.StartsWith(">3\r\n", push, $"Expected the async push first, got '{push}'.");
            StringAssert.Contains(Value(0), push);

            ClassicAssert.AreEqual(Ok, session.ReadReply());
            ClassicAssert.AreEqual(Pong, session.Execute("PING"));
        }

        /// <summary>
        /// Each barriering session waits for a background task that needs a thread of its own, so holding a
        /// thread per barrier deadlocks against a pool smaller than the session count: the blocked sessions
        /// occupy every thread the completions they are waiting for would have run on.
        /// </summary>
        /// <remarks>
        /// The barriers are sent while every read is still on the device, so each has something to wait for.
        /// A blocking barrier never gets its replies, which this reports within a bounded time rather than by
        /// waiting out the socket timeout.
        /// </remarks>
        [Test]
        public void MoreSessionsBarrierThanTheThreadPoolHasThreads()
        {
            RequireLinux();
            Populate();

            using var reads = new ReadsInFlight(deviceFactoryCreator);
            reads.Start();

            foreach (var s in reads.Sessions)
                s.Send("ASYNC", "BARRIER");

            // Each barrier's OK follows the push it was waiting for, which carries that session's own value.
            var deadline = TimeSpan.FromMilliseconds(ReadsInFlight.ReadDelayMs * 5);
            for (var i = 0; i < ReadsInFlight.SessionCount; i++)
            {
                var session = reads.Sessions[i];

                var push = ReadReplyBy(session, reads.SinceIssued, deadline, $"push for session {i}");
                StringAssert.StartsWith(">3\r\n", push);
                StringAssert.Contains($"${ValueLength}\r\n{Value(i)}\r\n", push);

                ClassicAssert.AreEqual(Ok, ReadReplyBy(session, reads.SinceIssued, deadline, $"barrier reply for session {i}"));
            }
        }

        /// <summary>
        /// The task that writes an asynchronous GET's push waits for the device without holding a thread. Every
        /// session with a read in flight has one, so a task that blocked for its read would use up a pool
        /// smaller than the session count, and nothing else could run until the reads completed -- not another
        /// session's command, and not the resumes of the other tasks.
        /// </summary>
        /// <remarks>
        /// The probe is a session that never reads. It is served by the same capped pool, once the tasks have
        /// had time to start and reach their waits, so it can only be answered before the reads complete if
        /// those waits are holding no thread.
        /// </remarks>
        [Test]
        public void SessionsWaitingOnAsyncGetsLeaveTheThreadPoolFreeToServeOthers()
        {
            RequireLinux();
            Populate();

            using var idle = new RawRespSession();
            ClassicAssert.AreEqual(Pong, idle.Execute("PING"));

            using var reads = new ReadsInFlight(deviceFactoryCreator);
            reads.Start();

            Thread.Sleep(100);

            ClassicAssert.AreEqual(Pong, idle.Execute("PING"));
            ClassicAssert.Less(reads.SinceIssued.Elapsed.TotalMilliseconds, ReadsInFlight.ReadDelayMs * 0.8,
                $"An idle session was answered {reads.SinceIssued.Elapsed.TotalMilliseconds:F0}ms after " +
                $"{ReadsInFlight.SessionCount} reads were issued, no sooner than they complete on the device, so " +
                $"the sessions waiting for them are holding the pool's {ReadsInFlight.CappedWorkers} threads.");

            // The reads then complete, each push carrying its own session's value.
            var deadline = TimeSpan.FromMilliseconds(ReadsInFlight.ReadDelayMs * 5);
            for (var i = 0; i < ReadsInFlight.SessionCount; i++)
            {
                var push = ReadReplyBy(reads.Sessions[i], reads.SinceIssued, deadline, $"push for session {i}");
                StringAssert.StartsWith(">3\r\n", push);
                StringAssert.Contains($"${ValueLength}\r\n{Value(i)}\r\n", push);
            }
        }

        /// <summary>
        /// A client that goes away with asynchronous GETs still on the device has its session torn down by the
        /// socket callback that observed the disconnect. That callback must not wait out the reads: they take as
        /// long as the device does, and with fewer threads than sessions nothing else could run meanwhile --
        /// not another session's command, and not the teardown of the other sessions.
        /// </summary>
        /// <remarks>
        /// The probe is a session that never reads, served by the same capped pool once the teardowns have
        /// been dispatched.
        /// </remarks>
        [Test]
        public void SessionsThatDisconnectWithAsyncGetsInFlightLeaveTheThreadPoolFreeToServeOthers()
        {
            RequireLinux();
            Populate();

            using var idle = new RawRespSession();
            ClassicAssert.AreEqual(Pong, idle.Execute("PING"));

            using var reads = new ReadsInFlight(deviceFactoryCreator);
            reads.Start();

            foreach (var s in reads.Sessions)
                s.AbortAndDispose();

            Thread.Sleep(100);

            ClassicAssert.AreEqual(Pong, idle.Execute("PING"));
            ClassicAssert.Less(reads.SinceIssued.Elapsed.TotalMilliseconds, ReadsInFlight.ReadDelayMs * 0.8,
                $"An idle session was answered {reads.SinceIssued.Elapsed.TotalMilliseconds:F0}ms after " +
                $"{ReadsInFlight.SessionCount} sessions disconnected with reads in flight, no sooner than the reads " +
                $"complete on the device, so tearing the sessions down is holding the pool's " +
                $"{ReadsInFlight.CappedWorkers} threads.");
        }

        const int DisconnectedSessions = 4;

        /// <summary>Longer than any test waits, so a read armed with it stays on the device until released.</summary>
        const int HeldReadMs = 60_000;

        /// <summary>
        /// Holds the device's reads, issues an asynchronous GET from each of <see cref="DisconnectedSessions"/>
        /// sessions and disconnects them, returning once the server has disposed every one of them.
        /// </summary>
        StoreWrapper DisconnectWithReadsHeld()
        {
            Populate();
            deviceFactoryCreator.ReadDelayMs = HeldReadMs;

            for (var i = 0; i < DisconnectedSessions; i++)
            {
                using var session = new RawRespSession();
                IssueAsyncGet(session, i);
                session.AbortAndDispose();
            }

            // A session is counted once it is disposed with a read outstanding, and uncounted once its storage
            // sessions are released, which the held reads put off.
            var storeWrapper = server.Provider.StoreWrapper;
            if (!SpinWait.SpinUntil(() => storeWrapper.StorageTeardowns == DisconnectedSessions, TimeSpan.FromSeconds(30)))
                Assert.Fail($"{storeWrapper.StorageTeardowns} of {DisconnectedSessions} disconnected sessions were " +
                    "waiting for their reads.");
            return storeWrapper;
        }

        /// <summary>
        /// A session that disconnects with reads in flight is finished off after the connection is gone, by the
        /// task waiting for those reads, which uses its storage sessions until they complete. The stores those
        /// belong to have to stay up until then, so shutting down waits for such sessions.
        /// </summary>
        [Test]
        public void ShutdownWaitsForDisconnectedSessionsToFinishTheirReads()
        {
            var storeWrapper = DisconnectWithReadsHeld();

            // Handed over so that the teardown does not dispose it a second time should this test fail
            var shuttingDown = server;
            server = null;
            var shutdown = Task.Factory.StartNew(shuttingDown.Dispose, TaskCreationOptions.LongRunning);

            try
            {
                // Nothing signals that shutdown is waiting, so it is given time to go past where it would not.
                ClassicAssert.IsFalse(shutdown.Wait(TimeSpan.FromSeconds(1)),
                    $"Shutdown finished with {storeWrapper.StorageTeardowns} disconnected sessions still waiting " +
                    "for their reads.");
                ClassicAssert.AreEqual(DisconnectedSessions, storeWrapper.StorageTeardowns);
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReads();
            }

            ClassicAssert.IsTrue(shutdown.Wait(TimeSpan.FromSeconds(30)), "Shutdown did not finish once the reads completed.");
            ClassicAssert.AreEqual(0, storeWrapper.StorageTeardowns);
        }

        /// <summary>
        /// The task that finishes a disconnected session's reads has no caller to report to, so anything it
        /// throws is seen only when the faulted task is finalized.
        /// </summary>
        [Test]
        public void ADisconnectedSessionFinishesItsReadsWithoutFaulting()
        {
            // What earlier tests left for the finalizer is reported before the handler is attached, not to it
            GC.Collect();
            GC.WaitForPendingFinalizers();

            var unobserved = new ConcurrentQueue<Exception>();
            void Record(object sender, UnobservedTaskExceptionEventArgs e) => unobserved.Enqueue(e.Exception);
            TaskScheduler.UnobservedTaskException += Record;
            try
            {
                var storeWrapper = DisconnectWithReadsHeld();

                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReads();
                if (!SpinWait.SpinUntil(() => storeWrapper.StorageTeardowns == 0, TimeSpan.FromSeconds(30)))
                    Assert.Fail($"{storeWrapper.StorageTeardowns} disconnected sessions did not finish their reads.");

                for (var i = 0; i < 3; i++)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    Thread.Sleep(50);
                }

                CollectionAssert.IsEmpty(unobserved, "A disconnected session's task faulted: " +
                    string.Join(" | ", unobserved));
            }
            finally
            {
                TaskScheduler.UnobservedTaskException -= Record;
            }
        }
    }
}