// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers the TLS accept path's liveness: a peer that completes the TCP handshake and then
    /// sends nothing must not prevent other clients from connecting.
    ///
    /// The assertions require a <em>completed</em> RESP exchange rather than a successful
    /// <c>connect()</c>. A connect succeeds against a wedged listener because the kernel's listen
    /// backlog accepts it without the server ever admitting it, so a test asserting only on connect
    /// would pass against the defect it exists to catch.
    /// </summary>
    [TestFixture]
    public class TlsHandshakeStallTests : TestBase
    {
        /// <summary>
        /// Generous relative to a loopback handshake, which is milliseconds, but far below the time
        /// a wedged accept loop takes to admit anything -- which is unbounded, since nothing times
        /// the stalled handshake out.
        /// </summary>
        static readonly TimeSpan AdmissionDeadline = TimeSpan.FromSeconds(15);

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true);
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
        /// The minimal repro: one peer, no credentials, no traffic.
        /// </summary>
        [Test]
        public async Task ASilentPeerDoesNotBlockNewTlsConnections()
        {
            using var silent = new Socket(SocketType.Stream, ProtocolType.Tcp);
            silent.Connect(TestUtils.EndPoint);

            await AssertAdmitsATlsClientPromptly(
                "a peer that connects and never sends a ClientHello must not prevent other clients " +
                "from being admitted");
        }

        /// <summary>
        /// The same defect at the scale an attacker would use it. Kept separate from the single-peer
        /// case so a regression reports whether liveness failed outright or only degraded.
        /// </summary>
        [Test]
        public async Task ManySilentPeersDoNotBlockNewTlsConnections()
        {
            var silent = new List<Socket>();
            try
            {
                for (var i = 0; i < 16; i++)
                {
                    var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
                    socket.Connect(TestUtils.EndPoint);
                    silent.Add(socket);
                }

                await AssertAdmitsATlsClientPromptly(
                    "silent peers must not accumulate into a denial of service on the accept path");
            }
            finally
            {
                foreach (var socket in silent) socket.Dispose();
            }
        }

        /// <summary>
        /// A peer that stalls partway through the handshake, rather than before starting it, so the
        /// server is left holding an <see cref="System.Net.Security.SslStream"/> mid-negotiation.
        /// </summary>
        [Test]
        public async Task APeerThatStallsMidHandshakeDoesNotBlockNewTlsConnections()
        {
            using var partial = new Socket(SocketType.Stream, ProtocolType.Tcp);
            partial.Connect(TestUtils.EndPoint);

            // The first five bytes of a TLS record header, announcing a ClientHello whose body
            // never arrives. The server reads them, asks for the rest, and waits.
            partial.Send([0x16, 0x03, 0x01, 0x02, 0x00]);

            await AssertAdmitsATlsClientPromptly(
                "a peer that stalls partway through the handshake must not prevent other clients " +
                "from being admitted");
        }

        /// <summary>
        /// Requires a full TLS handshake and a completed PING within the deadline.
        /// </summary>
        static async Task AssertAdmitsATlsClientPromptly(string because)
        {
            using var client = TestUtils.GetGarnetClient(useTLS: true);

            var elapsed = Stopwatch.StartNew();
            var exchange = Task.Run(async () =>
            {
                await client.ConnectAsync();
                return await client.PingAsync();
            });

            var finished = await Task.WhenAny(exchange, Task.Delay(AdmissionDeadline));
            if (finished != exchange)
            {
                Assert.Fail(
                    $"no TLS client was admitted within {AdmissionDeadline.TotalSeconds:0} seconds: {because}");
            }

            // Surfaces a handshake or protocol failure as itself rather than as a timeout.
            var pong = await exchange;
            ClassicAssert.AreEqual("PONG", pong);

            Assert.That(elapsed.Elapsed, Is.LessThan(AdmissionDeadline), because);
        }
    }

    /// <summary>
    /// Covers the TLS handshake deadline: a connection holds a slot under the connection limit from the moment it
    /// is accepted, and one that does not complete its handshake by the deadline is closed, freeing that slot.
    /// </summary>
    [TestFixture]
    public class TlsHandshakeDeadlineTests : TestBase
    {
        const int Limit = 2;
        static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(1);
        static readonly TimeSpan Deadline = TimeSpan.FromSeconds(15);

        GarnetServer server;
        StringWriter log;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = new StringWriter();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true,
                networkConnectionLimit: Limit, tlsHandshakeTimeout: HandshakeTimeout, logTo: TextWriter.Synchronized(log));
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
        /// Fills every slot with a peer that never completes its handshake - one silent, one that stops partway -
        /// and requires the server to close both at the deadline and then admit a real TLS client.
        /// </summary>
        [Test]
        public async Task PeersThatNeverCompleteAHandshakeReleaseTheirSlots()
        {
            using var silent = Connect();
            using var partial = Connect();

            // A TLS record header announcing a ClientHello whose body never arrives
            partial.Send([0x16, 0x03, 0x01, 0x02, 0x00]);

            AssertClosedByServer(silent);
            AssertClosedByServer(partial);

            using var client = TestUtils.GetGarnetClient(useTLS: true);
            var exchange = Task.Run(async () =>
            {
                await client.ConnectAsync();
                return await client.PingAsync();
            });
            if (await Task.WhenAny(exchange, Task.Delay(Deadline)) != exchange)
                Assert.Fail($"no TLS client was admitted within {Deadline.TotalSeconds:0} seconds of the stalled handshakes closing");
            ClassicAssert.AreEqual("PONG", await exchange);

            Assert.That(log.ToString(), Does.Not.Contain(".(warn)").And.Not.Contain(".(errr)").And.Not.Contain(".(crit)"),
                "a handshake that does not complete by its deadline is an expected condition, not a warning or error");
        }

        /// <summary>
        /// A timeout of <see cref="int.MaxValue"/> seconds, the longest the option accepts, exceeds the longest delay a
        /// cancellation timer supports; the server must still admit TLS clients.
        /// </summary>
        [Test]
        public async Task TheLongestConfigurableHandshakeTimeoutAdmitsTlsClients()
        {
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true,
                tlsHandshakeTimeout: TimeSpan.FromSeconds(int.MaxValue), logTo: TextWriter.Synchronized(log));
            server.Start();

            using var client = TestUtils.GetGarnetClient(useTLS: true);
            var exchange = Task.Run(async () =>
            {
                await client.ConnectAsync();
                return await client.PingAsync();
            });
            if (await Task.WhenAny(exchange, Task.Delay(Deadline)) != exchange)
                Assert.Fail($"no TLS client was admitted within {Deadline.TotalSeconds:0} seconds");
            ClassicAssert.AreEqual("PONG", await exchange);
        }

        static Socket Connect()
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { ReceiveTimeout = (int)Deadline.TotalMilliseconds };
            socket.Connect(TestUtils.EndPoint);
            return socket;
        }

        static void AssertClosedByServer(Socket socket)
        {
            var buffer = new byte[1024];
            try
            {
                // Anything the server writes before closing, such as a TLS alert, is skipped
                while (socket.Receive(buffer) > 0)
                {
                }
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionReset)
            {
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.TimedOut)
            {
                Assert.Fail($"a peer that never completed its TLS handshake was still connected after {Deadline.TotalSeconds:0} seconds");
            }
        }
    }
}