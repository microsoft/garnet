// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
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
}