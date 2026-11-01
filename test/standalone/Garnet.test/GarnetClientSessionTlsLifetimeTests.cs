// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Authentication;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class GarnetClientSessionTlsLifetimeTests : TestBase
    {
        GarnetServer server;
        static readonly TimeSpan Deadline = TimeSpan.FromSeconds(20);

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        void StartServer()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true, defaultPassword: "secret");
            server.Start();
        }

        static SslClientAuthenticationOptions TlsOptions() => new()
        {
            TargetHost = "GarnetTest",
            ClientCertificates = [TestUtils.GetClientCertificate()],
            RemoteCertificateValidationCallback = TestUtils.ValidateServerCertificate,
        };

        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        public async Task EitherOwnerCanReleaseFirst(bool connectAsync, bool clientFirst)
        {
            StartServer();
            var settings = new NetworkBufferSettings(1 << 20, 4096);
            using var pool = settings.CreateBufferPool();
            using var client = new GarnetClientSession(TestUtils.EndPoint, settings, pool, TlsOptions(), authPassword: "secret");
            if (connectAsync)
                await client.ConnectAsync().WaitAsync(Deadline);
            else
                await Task.Run(() => client.Connect()).WaitAsync(Deadline);
            Assert.That(await client.ExecuteAsync("PING").WaitAsync(Deadline), Is.EqualTo("PONG"));
            var handler = GetHandler(client);
            var entry = (PoolEntry)handler.GetType().BaseType.BaseType
                .GetField("transportSendBufferEntry", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(handler);

            // No send is active here. Exercise both release orders independently of receive-callback timing.
            if (clientFirst)
                handler.ReleaseClientSendBuffer();
            else
                handler.Dispose();
            using (var probe = pool.Get(settings.sendBufferSize))
                Assert.That(probe, Is.Not.SameAs(entry), "The remaining owner must keep the plaintext buffer out of the pool");
            if (clientFirst)
                handler.Dispose();
            else
                handler.ReleaseClientSendBuffer();
            handler.ReleaseClientSendBuffer();
            client.Dispose();
            client.Dispose();
            await Task.Run(pool.Dispose).WaitAsync(Deadline);
            Assert.That(pool.LiveBytes, Is.Zero);
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task ReconnectAndDisposeReturnEveryLease(bool sharedPool)
        {
            StartServer();
            var settings = new NetworkBufferSettings(1 << 20, 4096);
            using var suppliedPool = sharedPool ? settings.CreateBufferPool() : null;
            using var client = new GarnetClientSession(TestUtils.EndPoint, settings, suppliedPool, TlsOptions(), authPassword: "secret");
            var pool = (LimitedFixedBufferPool)typeof(GarnetClientSession)
                .GetField("networkPool", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(client);
            await client.ConnectAsync().WaitAsync(Deadline);
            for (var i = 0; i < 3; i++)
            {
                Assert.That(await client.ExecuteAsync("PING").WaitAsync(Deadline), Is.EqualTo("PONG"));
                await client.ReconnectAsync().WaitAsync(Deadline);
            }
            Assert.That(await client.ExecuteAsync("PING").WaitAsync(Deadline), Is.EqualTo("PONG"));
            client.Dispose();
            if (sharedPool)
                await Task.Run(pool.Dispose).WaitAsync(Deadline);
            Assert.That(pool.LiveBytes, Is.Zero);
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task FailedHandshakeReleasesTheClientLease(bool connectAsync)
        {
            using var timeout = new CancellationTokenSource(Deadline);
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var settings = new NetworkBufferSettings(1 << 20, 4096);
            using var pool = settings.CreateBufferPool();
            using var client = new GarnetClientSession(listener.LocalEndpoint, settings, pool, TlsOptions());
            var accept = listener.AcceptTcpClientAsync(timeout.Token);
            var connect = connectAsync
                ? client.ConnectAsync(token: timeout.Token)
                : Task.Run(() => client.Connect(timeout.Token));
            using var peer = await accept;
            await peer.GetStream().WriteAsync(Encoding.ASCII.GetBytes("not a TLS record\r\n"), timeout.Token);
            Assert.ThrowsAsync<AuthenticationException>(async () => await connect.WaitAsync(timeout.Token));
            Assert.That(pool.LiveBytes, Is.Zero, "A failed connect must release ownership without requiring caller Dispose");
            client.Dispose();
            Assert.That(pool.LiveBytes, Is.Zero);
        }

        [Test]
        public async Task FailedAuthenticationReleasesTheClientLease()
        {
            StartServer();
            var settings = new NetworkBufferSettings(1 << 20, 4096);
            using var pool = settings.CreateBufferPool();
            using var client = new GarnetClientSession(TestUtils.EndPoint, settings, pool, TlsOptions(), authPassword: "wrong");
            var exception = Assert.ThrowsAsync<Exception>(async () => await client.ConnectAsync().WaitAsync(Deadline));
            Assert.That(exception.Message, Does.Contain("WRONGPASS"));
            await Task.Run(pool.Dispose).WaitAsync(Deadline);
            Assert.That(pool.LiveBytes, Is.Zero);
        }

        static GarnetClientSessionTcpNetworkHandler GetHandler(GarnetClientSession client)
            => (GarnetClientSessionTcpNetworkHandler)typeof(GarnetClientSession)
                .GetField("networkHandler", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(client);
    }
}