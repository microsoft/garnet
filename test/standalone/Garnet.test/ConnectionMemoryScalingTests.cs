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
    /// Covers how the server's pinned network buffer footprint scales with the number of connections:
    /// a connection that once received a large payload must not retain the grown buffer forever, and the
    /// per-connection buffer size must be configurable.
    /// </summary>
    [TestFixture]
    public class ConnectionMemoryScalingTests : TestBase
    {
        const int Connections = 100;
        const int InitialReceiveBufferSize = 128 * 1024;

        GarnetServer server;

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        void StartServer(string networkBufferSize = null)
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, networkBufferSize: networkBufferSize);
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

        static byte[] Ping() => Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n");

        static byte[] BuildSet(string key, int valueLength)
        {
            var value = new string('v', valueLength);
            return Encoding.ASCII.GetBytes(
                $"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n");
        }

        /// <summary>
        /// Reads the server's own accounting of buffer pool bytes. Deterministic, unlike a GC heap reading.
        /// </summary>
        static string BpStats()
        {
            using var s = Connect();
            s.Send(Encoding.ASCII.GetBytes("*2\r\n$4\r\nINFO\r\n$7\r\nBPSTATS\r\n"));
            var buf = new byte[16 * 1024];
            var n = s.Receive(buf);
            return Encoding.ASCII.GetString(buf, 0, n);
        }

        static long StatBytes(string name)
        {
            var stats = BpStats();
            var marker = name + "=";
            var idx = stats.IndexOf(marker, StringComparison.Ordinal);
            ClassicAssert.GreaterOrEqual(idx, 0, $"'{name}' not found in BPSTATS: {stats}");
            idx += marker.Length;
            var end = stats.IndexOfAny([',', '\r', '\n'], idx);
            return ParseMemoryBytes(stats[idx..end]);
        }

        static long ParseMemoryBytes(string value)
        {
            // Format.MemoryBytes emits e.g. "123KB", "1.50MB", "2.00GB".
            var (suffix, scale) = value.EndsWith("KB", StringComparison.Ordinal) ? ("KB", 1L << 10)
                : value.EndsWith("MB", StringComparison.Ordinal) ? ("MB", 1L << 20)
                : value.EndsWith("GB", StringComparison.Ordinal) ? ("GB", 1L << 30)
                : ("", 1L);
            var number = suffix.Length == 0 ? value : value[..^suffix.Length];
            return (long)(double.Parse(number, System.Globalization.CultureInfo.InvariantCulture) * scale);
        }

        /// <summary>
        /// A connection that receives one oversized payload must give the grown receive buffer back once its
        /// traffic returns to normal, so a transient burst does not permanently inflate the per-connection
        /// footprint of every connection that experienced it.
        /// </summary>
        [Test]
        public void GrownReceiveBufferIsReleasedWhenTrafficReturnsToNormal()
        {
            StartServer();

            var ping = Ping();
            var bigSet = BuildSet("ratchet", 400 * 1024);

            var sockets = new List<Socket>();
            for (var i = 0; i < Connections; i++)
            {
                var s = Connect();
                SendAndDrain(s, ping, 1);
                sockets.Add(s);
            }
            var baselineLive = StatBytes("liveBytes");

            foreach (var s in sockets)
                SendAndDrain(s, bigSet, 1);
            var burstLive = StatBytes("liveBytes");

            // Return to small requests; the shrink path applies hysteresis, so allow enough receives to trip it.
            for (var round = 0; round < 280; round++)
                foreach (var s in sockets)
                    SendAndDrain(s, ping, 1);
            var settledLive = StatBytes("liveBytes");

            TestContext.Out.WriteLine($"baseline={baselineLive / 1024} KB, burst={burstLive / 1024} KB, settled={settledLive / 1024} KB");
            TestContext.Out.WriteLine($"per-conn baseline={baselineLive / Connections / 1024} KB, settled={settledLive / Connections / 1024} KB");
            TestContext.Out.WriteLine(BpStats());

            foreach (var s in sockets) s.Dispose();

            ClassicAssert.Greater(burstLive, baselineLive, "the oversized payload should have grown receive buffers");

            // The grown buffers must come back; allow slack for the short-lived INFO connection.
            var allowed = baselineLive + (2L * InitialReceiveBufferSize);
            ClassicAssert.LessOrEqual(settledLive, allowed,
                $"grown receive buffers were not released (settled={settledLive}, baseline={baselineLive})");
        }

        /// <summary>
        /// Buffers larger than the pool's largest size class cannot be recycled, so they must be released as
        /// soon as the payload has been consumed rather than waiting out the shrink hysteresis.
        /// </summary>
        [Test]
        public void OversizedReceiveBufferIsReleasedImmediately()
        {
            StartServer();

            using var s = Connect();
            SendAndDrain(s, Ping(), 1);
            var baseline = StatBytes("liveBytes");

            // Larger than the 1 MB max receive buffer size, so the buffer is allocated outside the pool.
            SendAndDrain(s, BuildSet("oversized", 3 * 1024 * 1024), 1);
            SendAndDrain(s, Ping(), 1);
            var after = StatBytes("liveBytes");

            TestContext.Out.WriteLine($"baseline={baseline / 1024} KB, after={after / 1024} KB");
            TestContext.Out.WriteLine(BpStats());

            ClassicAssert.LessOrEqual(after, baseline + (2L * InitialReceiveBufferSize),
                "oversized receive buffer should be released as soon as the payload is consumed");
        }

        /// <summary>
        /// The per-connection buffer size drives the server's pinned memory floor, so it has to be settable.
        /// </summary>
        [Test]
        public void NetworkBufferSizeIsConfigurable()
        {
            StartServer(networkBufferSize: "16k");

            var sockets = new List<Socket>();
            for (var i = 0; i < Connections; i++)
            {
                var s = Connect();
                SendAndDrain(s, Ping(), 1);
                sockets.Add(s);
            }

            var live = StatBytes("liveBytes");
            var perConnection = live / (double)Connections;
            TestContext.Out.WriteLine($"live={live / 1024} KB over {Connections} conns => {perConnection / 1024:F1} KB/conn");
            TestContext.Out.WriteLine(BpStats());

            foreach (var s in sockets) s.Dispose();

            // Default sizing would be 128 KB per buffer; at 16 KB a connection must cost far less.
            ClassicAssert.Less(perConnection, InitialReceiveBufferSize,
                "configured network buffer size was not applied");
        }
    }
}