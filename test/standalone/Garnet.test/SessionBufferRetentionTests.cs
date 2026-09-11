// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// End-to-end cover for per-session buffer retention. These buffers are sized by the largest request
    /// a session has ever served and are pinned, so without a ceiling one large or one unusually wide
    /// command permanently enlarges every session that saw it and total memory tracks session count
    /// rather than working set.
    ///
    /// Measured in-process with a forced collection: the server's own <c>gc_heap_bytes</c> is
    /// <c>GC.GetTotalMemory(false)</c>, which rises on allocation but cannot show a release.
    /// </summary>
    [TestFixture]
    public class SessionBufferRetentionTests : TestBase
    {
        const int Connections = 50;
        const int LargeValueSize = 256 * 1024;
        const int HugeValueSize = 2 * 1024 * 1024;
        const int WideCommandArgs = 20_000;

        // Comfortably past the shrink hysteresis so the release path is actually reached.
        // Two checkpoint intervals plus margin: a buffer is released at the first checkpoint that observes
        // no growth since the previous one, so reclaim takes up to two intervals.
        const int SmallRounds = Garnet.server.RespServerSession.SessionShrinkCheckInterval * 3;

        GarnetServer server;

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        void StartServer(string scratchCap = null, int? parseStateCap = null, bool enableLua = false)
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                enableLua: enableLua,
                sessionScratchBufferMaxRetainedSize: scratchCap,
                sessionParseStateMaxRetainedArgs: parseStateCap);
            server.Start();
        }

        static Socket Connect()
        {
            var s = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            s.Connect(TestUtils.EndPoint);
            return s;
        }

        static void SendAndDrain(Socket s, byte[] payload, int expectedReplies)
        {
            s.Send(payload);
            var buf = new byte[64 * 1024];
            var replies = 0;
            while (replies < expectedReplies)
            {
                var n = s.Receive(buf);
                if (n == 0) throw new Exception("connection closed");
                for (var i = 0; i < n; i++)
                    if (buf[i] == (byte)'\n') replies++;
            }
        }

        static byte[] Resp(params string[] parts)
        {
            var sb = new StringBuilder();
            sb.Append('*').Append(parts.Length).Append("\r\n");
            foreach (var p in parts)
                sb.Append('$').Append(p.Length).Append("\r\n").Append(p).Append("\r\n");
            return Encoding.ASCII.GetBytes(sb.ToString());
        }

        /// <summary>Large PFADD element: copied into the session scratch buffer, but stores only a fixed-size HLL.</summary>
        static byte[] LargeScratchCommand(int i) => Resp("PFADD", $"hll{i}", new string('z', LargeValueSize));

        // EVAL copies every ARGV into the session scratch buffer (LuaRunner.Functions.cs PrepareString),
        // which is the RESP path that actually drives the scratch ratchet.
        static byte[] HugeScratchCommand(int _) => Resp("EVAL", "return 1", "0", new string('z', HugeValueSize));

        static byte[] SmallScratchCommand(int _) => Resp("EVAL", "return 1", "0", "x");

        /// <summary>Wide command: sized purely by client arity, needing no large values at all.</summary>
        static byte[] WideCommand()
        {
            var parts = new string[WideCommandArgs + 1];
            parts[0] = "EXISTS";
            for (var j = 0; j < WideCommandArgs; j++) parts[j + 1] = "k" + j;
            return Resp(parts);
        }

        static byte[] NarrowCommand() => Resp("EXISTS", "k0");

        /// <summary>Sends a command and verifies the reply, so a silently rejected probe cannot pass.</summary>
        static void AssertReply(Socket s, byte[] payload, string expected)
        {
            s.Send(payload);
            var buf = new byte[64 * 1024];
            var n = s.Receive(buf);
            var reply = Encoding.ASCII.GetString(buf, 0, n);
            ClassicAssert.AreEqual(expected, reply, "probe command did not succeed, so it measured nothing");
        }

        static long SettledMemory()
        {
            for (var i = 0; i < 3; i++)
            {
                GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                GC.WaitForPendingFinalizers();
            }
            return GC.GetTotalMemory(forceFullCollection: true);
        }

        /// <summary>
        /// Drives the leak shape that matters: many sessions doing small work, each seeing exactly one
        /// outsized request, then returning to small work. Returns bytes still retained per session.
        /// </summary>
        long MeasureRetentionPerSession(Func<int, byte[]> big, Func<int, byte[]> small, int connections, string expectedBigReply)
        {
            var sockets = new List<Socket>();
            try
            {
                for (var i = 0; i < connections; i++) sockets.Add(Connect());

                // Warm up so per-session structures that are allocated once are already in place.
                for (var r = 0; r < 3; r++)
                    for (var i = 0; i < connections; i++)
                        SendAndDrain(sockets[i], small(i), 1);

                var baseline = SettledMemory();

                for (var i = 0; i < connections; i++)
                    AssertReply(sockets[i], big(i), expectedBigReply);

                // Sustained small work: enough batches for an unused buffer to be released.
                for (var r = 0; r < SmallRounds; r++)
                    for (var i = 0; i < connections; i++)
                        SendAndDrain(sockets[i], small(i), 1);

                var after = SettledMemory();
                return Math.Max(0, after - baseline) / connections;
            }
            finally
            {
                foreach (var s in sockets) { try { s.Dispose(); } catch { } }
            }
        }

        /// <summary>
        /// The Lua interpreter writes into the scratch buffer of the script cache's <em>inner</em>
        /// <c>RespServerSession</c>, not the network session's. That inner session never reads from a socket,
        /// so it has no batch boundary of its own and its checkpoint has to be driven from the outer session's.
        /// Without that wiring, one <c>cjson.encode</c> of a large value permanently enlarges every session
        /// that ran a script, which is the same ratchet the cap exists to stop -- just one object further in.
        /// </summary>
        [Test]
        public void LuaScratchBufferDoesNotRetainAfterOneLargeEncode()
        {
            const int Conns = 32;
            const int EncodeSize = 512 * 1024;

            // One cjson.encode of a large string grows the inner builder well past the 16 KB cap.
            var big = new string('q', EncodeSize);
            byte[] BigEncode(int _) => Resp("EVAL", "return #cjson.encode(ARGV[1])", "0", big);
            byte[] SmallEncode(int _) => Resp("EVAL", "return #cjson.encode(ARGV[1])", "0", "x");

            StartServer(scratchCap: "16k", enableLua: true);
            var bounded = MeasureRetentionPerSession(BigEncode, SmallEncode, Conns, $":{EncodeSize + 2}\r\n");

            server.Dispose();
            server = null;
            StartServer(scratchCap: "0", enableLua: true);
            var unbounded = MeasureRetentionPerSession(BigEncode, SmallEncode, Conns, $":{EncodeSize + 2}\r\n");

            TestContext.Out.WriteLine($"lua scratch retention per session: bounded={bounded / 1024} KB, unbounded={unbounded / 1024} KB");

            // Assert a magnitude, not a direction: the grown buffer rounds up to a power of two at or above
            // EncodeSize, so a working cap must give back the bulk of it. A bare Assert.Less here would be a
            // coin flip on a noisy measurement.
            ClassicAssert.Less(bounded, unbounded - (EncodeSize / 2),
                $"the Lua script processor's scratch buffer was not released (bounded={bounded}, unbounded={unbounded})");
        }

        /// <summary>
        /// The scratch buffer ratchet is real at the component level (see <see cref="SessionBufferShrinkTests"/>),
        /// but no RESP command path was found that leaves it retained: EVAL, which copies every ARGV into the
        /// scratch buffer, shows no measurable per-session retention either way. This test therefore pins the
        /// property that matters operationally - the cap must not change results or throughput characteristics
        /// for large arguments - rather than asserting a memory magnitude that is not observable here.
        /// </summary>
        [Test]
        public void ScratchBufferCapDoesNotChangeLargeArgumentResults()
        {
            const int Conns = 50;

            StartServer(scratchCap: "0", enableLua: true);
            var unbounded = MeasureRetentionPerSession(HugeScratchCommand, SmallScratchCommand, Conns, ":1\r\n");
            server.Dispose(); server = null;

            StartServer(enableLua: true);
            var bounded = MeasureRetentionPerSession(HugeScratchCommand, SmallScratchCommand, Conns, ":1\r\n");

            TestContext.Out.WriteLine($"scratch retained/session: unbounded={unbounded / 1024}KB bounded={bounded / 1024}KB");

            // Every reply was already validated by MeasureRetentionPerSession, so results are correct.
            // What remains to pin is that the cap does not make retention *worse* - a shrink policy that
            // churned would show up here as extra retained memory rather than as a wrong answer. The
            // margin is one cap's worth, which is tight enough to fail on churn and loose enough to
            // survive the variance of a forced-collection measurement.
            ClassicAssert.LessOrEqual(bounded, unbounded + (64 * 1024),
                "capping retained scratch capacity must not increase what a session holds");
        }

        /// <summary>
        /// The parse state ratchet needs no large values at all - only a high argument count, which costs
        /// the client nothing to send.
        /// </summary>
        [Test]
        public void ParseStateDoesNotRetainAfterOneWideCommand()
        {
            StartServer(parseStateCap: 0);
            var unbounded = MeasureRetentionPerSession(_ => WideCommand(), _ => NarrowCommand(), Connections, ":0\r\n");
            server.Dispose(); server = null;

            StartServer();
            var bounded = MeasureRetentionPerSession(_ => WideCommand(), _ => NarrowCommand(), Connections, ":0\r\n");

            TestContext.Out.WriteLine($"parse state retained/session: unbounded={unbounded / 1024}KB bounded={bounded / 1024}KB");

            // Assert a magnitude, not merely a direction. The root buffer holds one PinnedSpanByte
            // (16 bytes) per argument, so capping 20,000 arguments to 1,024 should return roughly
            // 300 KB per session; requiring at least half of that keeps the test insensitive to
            // measurement variance while still failing outright if the release path is neutered.
            // A bare Less() passed with 413 KB against 407 KB when the shrink was disabled.
            ClassicAssert.Less(bounded, unbounded - (WideCommandArgs * 8),
                "capping retained parse state must reduce what a session holds after one wide command");
        }

        /// <summary>
        /// The common case must be untouched: a session whose traffic never exceeds the cap should never
        /// reach the shrink path, and results must stay correct throughout.
        /// </summary>
        [Test]
        public void SmallWorkloadIsUnaffectedAndCorrect()
        {
            StartServer();
            using var s = Connect();

            for (var r = 0; r < SmallRounds * 2; r++)
            {
                SendAndDrain(s, Resp("SET", "k", "v" + r), 1);
                SendAndDrain(s, Resp("GET", "k"), 1);
            }

            s.Send(Resp("GET", "k"));
            var buf = new byte[1024];
            var n = s.Receive(buf);
            var reply = Encoding.ASCII.GetString(buf, 0, n);
            StringAssert.Contains("v" + (SmallRounds * 2 - 1), reply);
        }

        /// <summary>
        /// A session that keeps needing a large buffer must keep working after the shrink path has had
        /// many chances to fire, and must still return correct data for large values.
        /// </summary>
        [Test]
        public void RepeatedLargeValuesRemainCorrectAfterShrinking()
        {
            StartServer();
            using var s = Connect();

            var big = new string('q', LargeValueSize);
            for (var r = 0; r < 3; r++)
            {
                SendAndDrain(s, Resp("SET", "big", big), 1);

                // Long quiet stretch, so the scratch buffer is released between large values.
                for (var i = 0; i < SmallRounds; i++)
                    SendAndDrain(s, Resp("SET", "small", "v"), 1);

                s.Send(Resp("STRLEN", "big"));
                var buf = new byte[256];
                var n = s.Receive(buf);
                StringAssert.Contains(LargeValueSize.ToString(), Encoding.ASCII.GetString(buf, 0, n));
            }
        }

        /// <summary>
        /// Wide commands must keep returning correct results after the parse state has been shrunk and
        /// regrown, which is where an off-by-one in the root buffer would surface.
        /// </summary>
        [Test]
        public void WideCommandsRemainCorrectAcrossShrinkAndRegrow()
        {
            StartServer();
            using var s = Connect();

            SendAndDrain(s, Resp("SET", "k0", "v"), 1);

            for (var cycle = 0; cycle < 3; cycle++)
            {
                s.Send(WideCommand());
                var buf = new byte[4096];
                var n = s.Receive(buf);
                // k0 exists, the other 19,999 do not.
                StringAssert.StartsWith(":1\r\n", Encoding.ASCII.GetString(buf, 0, n));

                for (var i = 0; i < SmallRounds; i++)
                    SendAndDrain(s, NarrowCommand(), 1);
            }
        }

        /// <summary>
        /// A MULTI..EXEC window spans many batch boundaries, so the shrink hook fires between the
        /// queued commands and their execution. EXEC re-parses the queued commands from the network
        /// buffer, so a released parse-state root buffer must not affect the result.
        /// </summary>
        [Test]
        public void TransactionSpanningShrinkBoundaryStillExecutes()
        {
            StartServer();
            using var s = Connect();

            SendAndDrain(s, Resp("MULTI"), 1);

            // One wide command inside the transaction grows the root buffer, then enough narrow
            // batches to drive the shrink policy past its hysteresis while still queuing.
            s.Send(WideCommand());
            _ = ReadFully(s, 1);
            for (var i = 0; i < SmallRounds; i++)
                SendAndDrain(s, Resp("SET", "txnkey", "txnval"), 1);

            s.Send(Resp("EXEC"));
            var reply = ReadFully(s, SmallRounds + 2);
            StringAssert.StartsWith($"*{SmallRounds + 1}\r\n", reply);
            StringAssert.Contains(":0\r\n", reply);

            SendAndDrain(s, Resp("GET", "txnkey"), 1);
        }

        /// <summary>
        /// WATCH copies key bytes into the transaction scratch allocator precisely so they outlive the
        /// receive buffer. Capping that allocator must not break the version check, so a watched key
        /// modified by another connection must still abort the transaction after a shrink.
        /// </summary>
        [Test]
        public void WatchedKeysSurviveShrinkAndStillAbort()
        {
            StartServer();
            using var watcher = Connect();
            using var other = Connect();

            var watched = new string('w', 200 * 1024);
            SendAndDrain(watcher, Resp("SET", watched, "v0"), 1);
            SendAndDrain(watcher, Resp("WATCH", watched), 1);

            // Drive both shrink policies past hysteresis while the watch is outstanding.
            for (var i = 0; i < SmallRounds; i++)
                SendAndDrain(watcher, NarrowCommand(), 1);

            SendAndDrain(other, Resp("SET", watched, "v1"), 1);

            SendAndDrain(watcher, Resp("MULTI"), 1);
            SendAndDrain(watcher, Resp("SET", watched, "v2"), 1);
            watcher.Send(Resp("EXEC"));
            var reply = ReadFully(watcher, 1);
            ClassicAssert.AreEqual("*-1\r\n", reply, "watch must still abort the transaction after a shrink");
        }

        static string ReadFully(Socket s, int expectedLines)
        {
            var sb = new StringBuilder();
            var buf = new byte[256 * 1024];
            var lines = 0;
            while (lines < expectedLines)
            {
                var n = s.Receive(buf);
                if (n == 0) throw new Exception("connection closed");
                for (var i = 0; i < n; i++)
                    if (buf[i] == (byte)'\n') lines++;
                sb.Append(Encoding.ASCII.GetString(buf, 0, n));
            }
            return sb.ToString();
        }
    }
}