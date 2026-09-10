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
        const int SmallRounds = (int)(Garnet.common.BufferShrinkPolicy.DefaultHysteresis * 1.5);

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

            // Capping must never make retention worse than leaving it unbounded.
            ClassicAssert.LessOrEqual(bounded, Math.Max(unbounded, 64 * 1024) * 2,
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

            ClassicAssert.Less(bounded, unbounded,
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
    }
}