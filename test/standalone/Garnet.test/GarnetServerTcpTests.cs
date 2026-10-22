// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture, NonParallelizable]
    public class GarnetServerTcpTests : TestBase
    {
        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        static IPEndPoint GetEndPoint(bool ipv6)
        {
            if (ipv6 && !Socket.OSSupportsIPv6)
                Assert.Ignore("IPv6 is unavailable.");

            return new IPEndPoint(ipv6 ? IPAddress.IPv6Loopback : IPAddress.Loopback, TestUtils.TestPort);
        }

        [TestCase(false)]
        [TestCase(true)]
        public void StartRejectsAnAlreadyListeningEndpoint(bool ipv6)
        {
            var endpoint = GetEndPoint(ipv6);
            using var first = new GarnetServerTcp(endpoint);
            using var second = new GarnetServerTcp(endpoint);
            first.Start();

            var exception = Assert.Throws<GarnetException>(() => second.Start());
            Assert.That(exception.InnerException, Is.TypeOf<SocketException>());
            Assert.That(((SocketException)exception.InnerException).SocketErrorCode,
                Is.EqualTo(SocketError.AddressAlreadyInUse));

            // The endpoint has to appear in the message: a bind failure that names no port leaves the reader
            // unable to tell a port conflict from any other startup fault.
            Assert.That(exception.Message, Does.Contain(endpoint.Port.ToString()));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void StartReusesEndpointAfterAnAcceptedConnectionCloses(bool ipv6)
        {
            var endpoint = GetEndPoint(ipv6);
            using (var listener = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp))
            {
                listener.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                listener.Bind(endpoint);
                listener.Listen(1);
                using var client = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                client.ReceiveTimeout = 5000;
                client.Connect(endpoint);
                using var accepted = listener.Accept();
                accepted.ReceiveTimeout = 5000;

                // The server side closes first, leaving its accepted connection in TIME_WAIT.
                accepted.Shutdown(SocketShutdown.Send);
                Assert.That(client.Receive(new byte[1]), Is.Zero);
                client.Shutdown(SocketShutdown.Send);
                Assert.That(accepted.Receive(new byte[1]), Is.Zero);
            }

            using var server = new GarnetServerTcp(endpoint);
            Assert.DoesNotThrow(() => server.Start());
        }

        [TestCase(false)]
        [TestCase(true)]
        public void StartReusesEndpointAfterListenerCloses(bool ipv6)
        {
            var endpoint = GetEndPoint(ipv6);
            using var first = new GarnetServerTcp(endpoint);
            using var second = new GarnetServerTcp(endpoint);
            first.Start();
            first.Close();

            Assert.DoesNotThrow(() => second.Start());
        }

        /// <summary>
        /// A resource pressure error backs the accept loop off. The thread that ran the loop has to go back to
        /// the socket engine for the length of the backoff rather than sleep through it: when socket completions
        /// run inline it is the thread that serves every other connection.
        /// </summary>
        [Test]
        public void BackingOffAfterResourcePressureDoesNotHoldTheCallingThread()
        {
            using var server = new GarnetServerTcp(new IPEndPoint(IPAddress.Loopback, 0));

            // Far longer than the test waits, so the restart never fires on a server that was never started
            server.acceptBackoffMs = 60_000;

            using var args = new SocketAsyncEventArgs { SocketError = SocketError.TooManyOpenSockets };
            var backoff = Task.Run(() => server.HandleAcceptError(args));

            Assert.That(backoff.Wait(TimeSpan.FromSeconds(10)),
                "The accept error handler did not return within 10s, so it held its thread for the backoff");
            Assert.That(backoff.Result, Is.False, "The accept loop carried on instead of waiting for its restart");
        }
    }

#if DEBUG
    /// <summary>
    /// After a resource pressure error the accept loop is idle until the backoff timer restarts it, so the
    /// connections made in the meantime are served once it does.
    /// </summary>
    [TestFixture]
    public class GarnetServerAcceptBackoffTests : TestBase
    {
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

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
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        [Test]
        public void TheAcceptLoopRestartsOnceTheBackoffAfterResourcePressureElapses()
        {
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Accept_Resource_Pressure);
            try
            {
                // The accept this connection completes is turned into a resource pressure error, which closes it
                using var refused = new RawRespSession { ReceiveTimeout = Bound };
                refused.ExpectEndOfStream("the connection whose accept failed");

                // Made while the loop is backing off, so served only if the timer restarts the loop
                using var served = new RawRespSession { ReceiveTimeout = Bound };
                served.Send("PING");
                Assert.That(served.WaitForReply(Bound), "The accept loop did not restart after the backoff");
                Assert.That(served.ReadReply(), Is.EqualTo("+PONG\r\n"));
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Accept_Resource_Pressure);
            }
        }
    }
#endif
}