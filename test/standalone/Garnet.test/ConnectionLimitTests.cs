// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers what a client is told when the configured connection limit refuses it, and the
    /// rejected_connections metric that makes the refusal observable.
    ///
    /// The assertions deliberately read the bytes the server sends rather than merely observing
    /// that the connection failed. Before this behaviour existed the socket was disposed silently,
    /// so a test that only asserted "connecting fails" passed against the defect it was meant to
    /// catch.
    /// </summary>
    [TestFixture]
    public class ConnectionLimitTests : TestBase
    {
        const int ConnectionLimit = 4;

        /// <summary>The wire text Redis uses, so client error handling applies unchanged.</summary>
        const string MaxClientsError = "-ERR max number of clients reached\r\n";

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, networkConnectionLimit: ConnectionLimit);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Opens a raw socket and issues one PING, so the server creates a real RespServerSession.
        /// A bare socket is not enough: TryCreateMessageConsumer needs four bytes before a session
        /// exists, so connections that never write would not count against the limit.
        /// </summary>
        static Socket OpenAcceptedConnection()
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(TestUtils.EndPoint);
            socket.Send("PING\r\n"u8.ToArray());

            var buffer = new byte[64];
            var read = socket.Receive(buffer);
            ClassicAssert.Greater(read, 0, "an accepted connection should have answered PING");
            ClassicAssert.AreEqual("+PONG\r\n", Encoding.ASCII.GetString(buffer, 0, read));
            return socket;
        }

        /// <summary>
        /// Fills the server to its connection limit, returning the sockets so the caller can hold
        /// them open. Waits for each to be genuinely established before opening the next, since the
        /// limit counts live handlers and a racing accept would make the fill count unreliable.
        /// </summary>
        static List<Socket> FillToLimit()
        {
            var accepted = new List<Socket>();
            for (var i = 0; i < ConnectionLimit; i++)
                accepted.Add(OpenAcceptedConnection());
            return accepted;
        }

        static void DisposeAll(List<Socket> sockets)
        {
            foreach (var socket in sockets)
                socket.Dispose();
        }

        /// <summary>
        /// Reads until the peer closes, so the assertion cannot pass on a partial read that happens
        /// to arrive before the tail. Returns everything received.
        /// </summary>
        static string ReadToClose(Socket socket)
        {
            socket.ReceiveTimeout = 5000;
            var received = new List<byte>();
            var buffer = new byte[256];
            while (true)
            {
                int read;
                try
                {
                    read = socket.Receive(buffer);
                }
                catch (SocketException)
                {
                    // Peer reset rather than closing gracefully; report what arrived, if anything.
                    break;
                }

                if (read == 0) break;
                received.AddRange(new ArraySegment<byte>(buffer, 0, read));
            }

            return Encoding.ASCII.GetString(received.ToArray());
        }

        /// <summary>
        /// The load-bearing assertion: a refused client is TOLD why. Asserting only that the
        /// connection closed would pass against a silent drop, which is the behaviour this fixes.
        /// </summary>
        [Test]
        public void RejectedConnectionReceivesAnErrorRatherThanASilentClose()
        {
            var accepted = FillToLimit();
            try
            {
                using var refused = new Socket(SocketType.Stream, ProtocolType.Tcp);
                refused.Connect(TestUtils.EndPoint);

                var reply = ReadToClose(refused);

                ClassicAssert.AreEqual(MaxClientsError, reply,
                    "a connection refused by the limit must receive the RESP error before the close, " +
                    "not an unexplained reset");
            }
            finally
            {
                DisposeAll(accepted);
            }
        }

        /// <summary>
        /// The error must arrive without the client sending anything. A refused connection never
        /// gets a session, so there is no command for the server to respond to -- the write has to
        /// be unsolicited, at accept time.
        /// </summary>
        [Test]
        public void TheErrorArrivesWithoutTheClientSendingAnything()
        {
            var accepted = FillToLimit();
            try
            {
                using var refused = new Socket(SocketType.Stream, ProtocolType.Tcp);
                refused.Connect(TestUtils.EndPoint);
                refused.ReceiveTimeout = 5000;

                var buffer = new byte[128];
                var read = refused.Receive(buffer);

                ClassicAssert.AreEqual(MaxClientsError, Encoding.ASCII.GetString(buffer, 0, read));
            }
            finally
            {
                DisposeAll(accepted);
            }
        }

        /// <summary>
        /// Pins the counter, and pins that it counts rejections specifically: the accepted
        /// connections that filled the server must not be counted, nor must the normal churn of
        /// connecting and disconnecting.
        /// </summary>
        [Test]
        public void RejectedConnectionsAreCountedInInfoStats()
        {
            ClassicAssert.AreEqual(0, ReadRejectedConnections(),
                "no connection has been refused yet");

            var accepted = FillToLimit();
            try
            {
                ClassicAssert.AreEqual(0, ReadRejectedConnections(),
                    "connections that were accepted must not count as rejected");

                const int Refusals = 3;
                for (var i = 0; i < Refusals; i++)
                {
                    using var refused = new Socket(SocketType.Stream, ProtocolType.Tcp);
                    refused.Connect(TestUtils.EndPoint);
                    _ = ReadToClose(refused);
                }

                ClassicAssert.AreEqual(Refusals, ReadRejectedConnections(),
                    "every refused connection must be counted exactly once");
            }
            finally
            {
                DisposeAll(accepted);
            }
        }

        /// <summary>
        /// Once connections drain, the server accepts again and the counter stays put -- it is a
        /// cumulative total, not a gauge of the current state.
        /// </summary>
        [Test]
        public void TheCounterIsCumulativeAndTheServerRecoversWhenConnectionsDrain()
        {
            var accepted = FillToLimit();

            using (var refused = new Socket(SocketType.Stream, ProtocolType.Tcp))
            {
                refused.Connect(TestUtils.EndPoint);
                _ = ReadToClose(refused);
            }

            ClassicAssert.AreEqual(1, ReadRejectedConnections());

            DisposeAll(accepted);

            // The limit counts live handlers, so capacity returns once the server observes the
            // closes. Poll rather than sleep a fixed interval.
            var reconnected = WaitForAcceptance();
            try
            {
                ClassicAssert.AreEqual(1, ReadRejectedConnections(),
                    "a successful connection must not change the rejected count");
            }
            finally
            {
                reconnected.Dispose();
            }
        }

        /// <summary>
        /// Polls until the server has capacity again, returning the established socket.
        /// </summary>
        static Socket WaitForAcceptance()
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < TimeSpan.FromSeconds(20))
            {
                var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
                try
                {
                    socket.Connect(TestUtils.EndPoint);
                    socket.Send("PING\r\n"u8.ToArray());
                    socket.ReceiveTimeout = 1000;

                    var buffer = new byte[64];
                    var read = socket.Receive(buffer);
                    if (read > 0 && Encoding.ASCII.GetString(buffer, 0, read) == "+PONG\r\n")
                        return socket;
                }
                catch (SocketException) { }

                socket.Dispose();
                Thread.Sleep(50);
            }

            Assert.Fail("the server never regained capacity after its connections drained");
            return null;
        }

        /// <summary>
        /// Reads rejected_connections out of INFO STATS. Uses its own connection, which is one of
        /// the limited slots, so callers must leave room for it.
        /// </summary>
        static long ReadRejectedConnections()
        {
            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(TestUtils.EndPoint);
            socket.ReceiveTimeout = 5000;
            socket.Send("INFO STATS\r\n"u8.ToArray());

            var received = new List<byte>();
            var buffer = new byte[8192];

            // INFO is a single bulk string; read until its declared length has arrived.
            while (true)
            {
                var read = socket.Receive(buffer);
                ClassicAssert.Greater(read, 0, "INFO STATS returned no data -- is the probe itself being refused?");
                received.AddRange(new ArraySegment<byte>(buffer, 0, read));

                var text = Encoding.ASCII.GetString(received.ToArray());
                if (text.StartsWith('-'))
                    Assert.Fail($"INFO STATS failed: {text.Trim()}");

                var headerEnd = text.IndexOf("\r\n", StringComparison.Ordinal);
                if (headerEnd < 0) continue;

                var declared = int.Parse(text.AsSpan(1, headerEnd - 1));
                if (received.Count >= headerEnd + 2 + declared + 2)
                    return ParseRejected(text);
            }
        }

        static long ParseRejected(string info)
        {
            foreach (var line in info.Split("\r\n", StringSplitOptions.RemoveEmptyEntries))
            {
                if (!line.StartsWith("rejected_connections:", StringComparison.Ordinal)) continue;
                return long.Parse(line["rejected_connections:".Length..]);
            }

            Assert.Fail("INFO STATS did not report rejected_connections");
            return -1;
        }
    }
}
