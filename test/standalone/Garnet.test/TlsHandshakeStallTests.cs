// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading.Tasks;
using Garnet.client;
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
    /// Starting the handshake off the accept loop moves where a handshake failure is noticed. It no
    /// longer propagates out of <c>Start</c> into the accept site's catch, so the admission slot the
    /// accept path has already taken is released by the handshake's own failure path instead.
    ///
    /// That is a property worth pinning rather than assuming: a leak here would convert a
    /// connection-limited server into one an unauthenticated peer can permanently fill.
    ///
    /// The connection limit is the instrument, in preference to <c>connected_clients</c>, which is
    /// monitor-sampled and so reports a previous sample rather than the live count.
    /// </summary>
    [TestFixture]
    public class TlsHandshakeFailureSlotTests : TestBase
    {
        const int Limit = 4;

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true,
                networkConnectionLimit: Limit);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task FailedHandshakesReleaseTheirAdmissionSlots()
        {
            // Twice the limit, so a leak of even half the slots is caught.
            for (var i = 0; i < Limit * 2; i++)
                FailOneHandshake();

            // The whole ceiling must still be available, not merely one slot.
            var admitted = new List<GarnetClient>();
            try
            {
                for (var i = 0; i < Limit; i++)
                {
                    var client = await ConnectWithinDeadline(i);
                    admitted.Add(client);
                }
            }
            finally
            {
                foreach (var client in admitted) client.Dispose();
            }
        }

        /// <summary>
        /// Drives one connection to a TLS handshake failure and waits for the server to close it,
        /// which is the observable event that orders the failure before the assertion.
        /// </summary>
        static void FailOneHandshake()
        {
            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(TestUtils.EndPoint);
            socket.ReceiveTimeout = 15_000;

            // Not a TLS record, so negotiation fails immediately rather than stalling. This is also
            // what a plaintext client reaching a TLS port sends.
            socket.Send("PING\r\n"u8.ToArray());

            try
            {
                // Drains any alert record the server writes before closing.
                var buffer = new byte[256];
                while (socket.Receive(buffer) > 0) { }
            }
            catch (SocketException)
            {
                // A reset instead of an orderly close is equally a teardown.
            }
        }

        /// <summary>
        /// Retries briefly, because the slot is released on the handshake's failure path rather than
        /// synchronously with the close observed above. A leaked slot is not recovered by waiting,
        /// so this bounds a race without weakening the assertion.
        /// </summary>
        static async Task<GarnetClient> ConnectWithinDeadline(int index)
        {
            var deadline = Stopwatch.StartNew();
            Exception last = null;

            while (deadline.Elapsed < TimeSpan.FromSeconds(15))
            {
                var client = TestUtils.GetGarnetClient(useTLS: true);
                try
                {
                    await client.ConnectAsync();
                    ClassicAssert.AreEqual("PONG", await client.PingAsync());
                    return client;
                }
                catch (Exception ex)
                {
                    last = ex;
                    client.Dispose();
                    await Task.Delay(200);
                }
            }

            Assert.Fail(
                $"connection {index} of {Limit} was refused after {Limit * 2} failed handshakes, so " +
                $"those handshakes did not release the admission slots the accept path took for " +
                $"them: {last?.Message}");
            return null;
        }
    }
}