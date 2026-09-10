// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Sockets;
using Garnet.server;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture, NonParallelizable]
    public class GarnetServerTcpTests
    {
        static IPEndPoint GetEndPoint(bool ipv6)
        {
            if (OperatingSystem.IsWindows())
                Assert.Ignore("Tests Unix listener reuse semantics.");
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

            var exception = Assert.Throws<SocketException>(() => second.Start());
            Assert.That(exception.SocketErrorCode, Is.EqualTo(SocketError.AddressAlreadyInUse));
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
    }
}