// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// TLS framing, authentication, buffer ownership and teardown coverage using a scripted peer and Garnet.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class GarnetClientTlsTests : TestBase
    {
        static readonly byte[] Echo = "$4\r\nECHO\r\n"u8.ToArray();
        static readonly byte[] Ping = "*1\r\n$4\r\nPING\r\n"u8.ToArray();
        static readonly TimeSpan Deadline = TimeSpan.FromSeconds(20);

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        public async Task FragmentedRepliesAndCoalescedRepliesPreserveCompletionOrder()
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var listener = Listen();
            using var client = TestUtils.GetGarnetClient(useTLS: true, networkWriterOptions: SmallBuffers());
            var connect = client.ConnectAsync(cts.Token);
            using var peer = await AcceptTlsAsync(listener, cts.Token);
            await connect.WaitAsync(Deadline);

            var replies = Enumerable.Range(0, 4).Select(_ => client.PingAsync(cts.Token)).ToArray();
            await ReadPingsAsync(peer, replies.Length, cts.Token);

            // Split the bulk header, body and CRLF across TLS writes. Finish with two replies in one write.
            foreach (var fragment in new[] { "$", "5\r", "\nfi", "rst", "\r", "\n+second\r\n:3\r\n$4\r\nlast\r\n" })
                await peer.WriteAsync(Encoding.ASCII.GetBytes(fragment), cts.Token);

            CollectionAssert.AreEqual(new[] { "first", "second", "3", "last" },
                await Task.WhenAll(replies).WaitAsync(Deadline));
            await AssertPingAsync(client, peer, cts.Token);
        }

        [Test]
        public async Task AReplyBufferedInsideOneTlsRecordNeedsNoFurtherSocketTraffic()
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var listener = Listen();
            using var client = TestUtils.GetGarnetClient(useTLS: true, networkWriterOptions: SmallBuffers());
            var connect = client.ConnectAsync(cts.Token);
            using var peer = await AcceptTlsAsync(listener, cts.Token);
            await connect.WaitAsync(Deadline);

            var first = client.PingAsync(cts.Token);
            var second = client.PingAsync(cts.Token);
            var third = client.PingAsync(cts.Token);
            await ReadPingsAsync(peer, 3, cts.Token);
            var expected = new string('x', 12000);
            var response = Encoding.ASCII.GetBytes($"+prefix\r\n${expected.Length}\r\n{expected}\r\n+tail\r\n");

            // One TLS write fits inside a record but exceeds the client's 256-byte plaintext buffer.
            // Consuming the prefix leaves a partial bulk reply without a full buffer to trigger growth.
            // Send nothing else until all completions have arrived.
            await peer.WriteAsync(response, cts.Token);
            ClassicAssert.AreEqual("prefix", await first.WaitAsync(Deadline));
            ClassicAssert.AreEqual(expected, await second.WaitAsync(Deadline));
            ClassicAssert.AreEqual("tail", await third.WaitAsync(Deadline));
        }

        [Test]
        public async Task AReplyEndingExactlyAtTheTransportBufferEndDoesNotStrandTheRestOfItsRecord()
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var listener = Listen();
            using var client = TestUtils.GetGarnetClient(useTLS: true, networkWriterOptions: SmallBuffers());
            var connect = client.ConnectAsync(cts.Token);
            using var peer = await AcceptTlsAsync(listener, cts.Token);
            await connect.WaitAsync(Deadline);

            var first = client.PingAsync(cts.Token);
            var second = client.PingAsync(cts.Token);
            await ReadPingsAsync(peer, 2, cts.Token);

            // The first reply fills the client's 256-byte plaintext buffer exactly and is fully consumed, so the
            // buffer is neither full nor holding a partial reply. The second reply is still inside the same TLS
            // record and no further socket traffic follows.
            var body = new string('a', 256 - 3);
            await peer.WriteAsync(Encoding.ASCII.GetBytes($"+{body}\r\n+tail\r\n"), cts.Token);
            ClassicAssert.AreEqual(body, await first.WaitAsync(Deadline));
            ClassicAssert.AreEqual("tail", await second.WaitAsync(TimeSpan.FromSeconds(5)));
        }

        [TestCase(0)]
        [TestCase(257)]
        [TestCase(1 << 20)]
        public async Task PeerCloseFaultsOutstandingReplies(int partialBodyBytes)
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var listener = Listen();
            using var client = TestUtils.GetGarnetClient(useTLS: true, networkWriterOptions: SmallBuffers());
            var connect = client.ConnectAsync(cts.Token);
            using var peer = await AcceptTlsAsync(listener, cts.Token);
            await connect.WaitAsync(Deadline);

            var replies = Enumerable.Range(0, 4).Select(_ => client.PingAsync(cts.Token)).ToArray();
            await ReadPingsAsync(peer, replies.Length, cts.Token);
            if (partialBodyBytes != 0)
            {
                await peer.WriteAsync(Encoding.ASCII.GetBytes($"${partialBodyBytes + 1}\r\n"), cts.Token);
                await peer.WriteAsync(new byte[partialBodyBytes], cts.Token);
            }
            await peer.ShutdownAsync().WaitAsync(Deadline);

            foreach (var reply in replies)
                Assert.ThrowsAsync<GarnetClientDisposedException>(async () => await reply.WaitAsync(Deadline));
        }

        [Test]
        public async Task AbruptCloseWithPartialReplyFaultsOutstandingCompletion()
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var listener = Listen();
            using var client = TestUtils.GetGarnetClient(useTLS: true, networkWriterOptions: SmallBuffers());
            var connect = client.ConnectAsync(cts.Token);
            using var socket = await listener.AcceptSocketAsync(cts.Token);
            using var peer = await AuthenticatePeerAsync(socket, cts.Token);
            await connect.WaitAsync(Deadline);

            var reply = client.PingAsync(cts.Token);
            await ReadPingsAsync(peer, 1, cts.Token);
            await peer.WriteAsync("$100\r\npartial"u8.ToArray(), cts.Token);
            socket.LingerState = new LingerOption(true, 0);
            socket.Dispose();

            Assert.ThrowsAsync<GarnetClientDisposedException>(async () => await reply.WaitAsync(Deadline));
        }

        [Test]
        public async Task GracefulCloseWhileReaderHasPartialFrameFaultsCompletion()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();
            using var cts = new CancellationTokenSource(Deadline);
            using var listener = Listen();
            using var client = TestUtils.GetGarnetClient(useTLS: true, networkWriterOptions: SmallBuffers());
            var connect = client.ConnectAsync(cts.Token);
            using var peer = await AcceptTlsAsync(listener, cts.Token);
            await connect.WaitAsync(Deadline);

            var reply = client.PingAsync(cts.Token);
            await ReadPingsAsync(peer, 1, cts.Token);
            using var pause = new InjectionPause(ExceptionInjectionType.Tls_Pause_With_Partial_Frame);
            await peer.WriteAsync("$100\r\npartial"u8.ToArray(), cts.Token);
            await pause.ArrivedAsync();
            await peer.ShutdownAsync().WaitAsync(Deadline);
            pause.Release();

            Assert.ThrowsAsync<GarnetClientDisposedException>(async () => await reply.WaitAsync(Deadline));
        }

        [Test]
        public async Task ReconnectDiscardsPartialOldReplyAndRestartsCompletionTickets()
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var listener = Listen();
            using var client = TestUtils.GetGarnetClient(useTLS: true, networkWriterOptions: SmallBuffers());
            var connect = client.ConnectAsync(cts.Token);
            using var oldPeer = await AcceptTlsAsync(listener, cts.Token);
            await connect.WaitAsync(Deadline);

            var oldReplies = Enumerable.Range(0, 4).Select(_ => client.PingAsync(cts.Token)).ToArray();
            await ReadPingsAsync(oldPeer, oldReplies.Length, cts.Token);
            await oldPeer.WriteAsync("$100\r\nold"u8.ToArray(), cts.Token);

            var reconnect = client.ReconnectAsync(cts.Token);
            using var newPeer = await AcceptTlsAsync(listener, cts.Token);
            await reconnect.WaitAsync(Deadline);
            foreach (var reply in oldReplies)
                Assert.ThrowsAsync<GarnetClientDisposedException>(async () => await reply.WaitAsync(Deadline));

            // Wrap the small completion lane repeatedly on the new connection.
            for (var i = 0; i < 40; i++)
                await AssertPingAsync(client, newPeer, cts.Token);
        }

        [Test]
        public async Task StalledHandshakeHonorsCancellationAndAllowsReconnect()
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var listener = Listen();
            using var client = TestUtils.GetGarnetClient(useTLS: true);
            using var handshakeCancellation = new CancellationTokenSource();
            var connect = client.ConnectAsync(handshakeCancellation.Token);
            using (var silentPeer = await listener.AcceptTcpClientAsync(cts.Token))
            {
                var buffer = new byte[256];
                ClassicAssert.Greater(await silentPeer.GetStream().ReadAsync(buffer, cts.Token), 0,
                    "The client must have started its TLS handshake before cancellation.");
                handshakeCancellation.Cancel();
                Assert.CatchAsync<OperationCanceledException>(async () => await connect.WaitAsync(Deadline));
            }

            var reconnect = client.ReconnectAsync(cts.Token);
            using var peer = await AcceptTlsAsync(listener, cts.Token);
            await reconnect.WaitAsync(Deadline);
            await AssertPingAsync(client, peer, cts.Token);
        }

        [Test]
        public async Task PeerDisconnectDuringHandshakeFaultsConnect()
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var listener = Listen();
            using var client = TestUtils.GetGarnetClient(useTLS: true);
            var connect = client.ConnectAsync(cts.Token);
            using (var peer = await listener.AcceptTcpClientAsync(cts.Token))
            {
                ClassicAssert.Greater(await peer.GetStream().ReadAsync(new byte[256], cts.Token), 0);
            }

            var failure = Assert.CatchAsync(async () => await connect.WaitAsync(Deadline));
            Assert.That(failure, Is.InstanceOf<AuthenticationException>().Or.InstanceOf<IOException>().Or.InstanceOf<ObjectDisposedException>());
        }

        [Test]
        public async Task CertificateValidationRejectsUntrustedCertificate()
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var listener = Listen();
            using var key = RSA.Create(2048);
            var request = new CertificateRequest("CN=GarnetTest", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            using var generated = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(1));
            using var certificate = X509CertificateLoader.LoadPkcs12(generated.Export(X509ContentType.Pkcs12), password: null);
            using var client = new GarnetClient(TestUtils.EndPoint, new SslClientAuthenticationOptions
            {
                TargetHost = "GarnetTest",
            });
            var connect = client.ConnectAsync(cts.Token);
            using var socket = await listener.AcceptSocketAsync(cts.Token);
            using var peer = new SslStream(new NetworkStream(socket, ownsSocket: false));
            var authenticate = peer.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
            {
                ServerCertificate = certificate,
            }, cts.Token);

            Assert.ThrowsAsync<AuthenticationException>(async () => await connect.WaitAsync(Deadline));
            await ObserveRejectedHandshakeAsync(authenticate);
        }

        [Test]
        public async Task CertificateValidationRejectsHostnameMismatch()
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var listener = Listen();
            var observedErrors = SslPolicyErrors.None;
            using var client = new GarnetClient(TestUtils.EndPoint, new SslClientAuthenticationOptions
            {
                TargetHost = "not-the-garnet-test-host.invalid",
                RemoteCertificateValidationCallback = (_, _, _, errors) =>
                {
                    observedErrors = errors;
                    // Ignore the test certificate's private trust chain, but not its hostname.
                    return (errors & SslPolicyErrors.RemoteCertificateNameMismatch) == 0;
                },
            });
            var connect = client.ConnectAsync(cts.Token);
            using var socket = await listener.AcceptSocketAsync(cts.Token);
            using var peer = new SslStream(new NetworkStream(socket, ownsSocket: false));
            var authenticate = peer.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
            {
                ServerCertificate = TestUtils.GetClientCertificate(),
            }, cts.Token);

            Assert.ThrowsAsync<AuthenticationException>(async () => await connect.WaitAsync(Deadline));
            Assert.That(observedErrors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch), Is.True);
            await ObserveRejectedHandshakeAsync(authenticate);
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task ServerRejectsMissingOrRejectedClientCertificate(bool provideCertificate)
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var listener = Listen();
            using var client = new GarnetClient(TestUtils.EndPoint, new SslClientAuthenticationOptions
            {
                TargetHost = "GarnetTest",
                EnabledSslProtocols = SslProtocols.Tls12,
                ClientCertificates = provideCertificate ? [TestUtils.GetClientCertificate()] : [],
                RemoteCertificateValidationCallback = TestUtils.ValidateServerCertificate,
            });
            var connect = client.ConnectAsync(cts.Token);
            using var socket = await listener.AcceptSocketAsync(cts.Token);
            using var peer = new SslStream(new NetworkStream(socket, ownsSocket: false));
            var certificateSeen = false;
            var authenticate = peer.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
            {
                ServerCertificate = TestUtils.GetClientCertificate(),
                EnabledSslProtocols = SslProtocols.Tls12,
                ClientCertificateRequired = true,
                RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                {
                    certificateSeen = certificate != null;
                    return false;
                },
            }, cts.Token);

            Assert.CatchAsync<AuthenticationException>(async () => await authenticate.WaitAsync(Deadline));
            ClassicAssert.AreEqual(provideCertificate, certificateSeen);
            peer.Dispose();
            socket.Dispose();
            // The client can finish its handshake before receiving the server's rejection alert.
            var failure = Assert.CatchAsync(async () =>
            {
                await connect.WaitAsync(Deadline);
                await client.PingAsync(cts.Token).WaitAsync(Deadline);
            });
            Assert.That(failure, Is.InstanceOf<AuthenticationException>().Or.InstanceOf<IOException>().Or.InstanceOf<GarnetClientDisposedException>());
        }

        [TestCase(8191)]
        [TestCase(8192)]
        [TestCase(8193)]
        [TestCase(131071)]
        [TestCase(131072)]
        [TestCase(131073)]
        [TestCase(1048577)]
        public async Task LargeRepliesGrowAndThenReleaseReceiveBuffer(int payloadLength)
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true);
            server.Start();
            using var client = TestUtils.GetGarnetClient(useTLS: true, networkWriterOptions: SmallBuffers());
            await client.ConnectAsync(cts.Token);
            var value = new string('x', payloadLength);

            ClassicAssert.AreEqual(value,
                await client.ExecuteForStringResultWithCancellationAsync(Echo, [value], cts.Token).WaitAsync(Deadline));
            // Each ping needs a separate receive pass so it counts toward shrink hysteresis.
            for (var i = 0; i < 300; i++)
                ClassicAssert.AreEqual("PONG", await client.PingAsync(cts.Token).WaitAsync(Deadline));

            var handler = typeof(GarnetClient).GetField("networkHandler", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(client);
            var handlerType = handler.GetType();
            while (handlerType.GetField("transportReceiveBuffer", BindingFlags.NonPublic | BindingFlags.Instance) == null)
                handlerType = handlerType.BaseType;
            var buffer = (byte[])handlerType.GetField("transportReceiveBuffer", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(handler);
            ClassicAssert.AreEqual(256, buffer.Length, "A large response must not permanently inflate the TLS plaintext buffer.");
        }

        [Test]
        public async Task PayloadsAtInlineAndChunkBoundariesSurviveRingWrap([Values] FlushResultAllocationMode mode)
        {
            using var cts = new CancellationTokenSource(Deadline);
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true);
            server.Start();
            using var client = TestUtils.GetGarnetClient(useTLS: true, networkWriterOptions: SmallBuffers(mode));
            await client.ConnectAsync(cts.Token);

            // ECHO framing adds 22 bytes for these payload sizes: encoded lengths straddle 256 and 512 bytes.
            var lengths = new[] { 233, 234, 235, 489, 490, 491 };
            for (var round = 0; round < 8; round++)
            {
                var values = lengths.Select(n => new string((char)('a' + round), n)).ToArray();
                var pending = values.Select(v => client.ExecuteForStringResultWithCancellationAsync(Echo, [v], cts.Token)).ToArray();
                CollectionAssert.AreEqual(values, await Task.WhenAll(pending).WaitAsync(Deadline));
            }
            var writer = GetWriter(client);
            await writer.WaitForIdleAsync(cts.Token);
            ClassicAssert.AreEqual(0, RentedBytes(writer));
            ClassicAssert.AreEqual("PONG", await client.PingAsync(cts.Token).WaitAsync(Deadline));
        }

        [Test]
        public async Task PausedTlsWriteRetainsBuffersUntilEncryptionCompletes(
            [Values] bool outOfLine, [Values] FlushResultAllocationMode mode)
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();
            using var cts = new CancellationTokenSource(Deadline);
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true);
            server.Start();
            using var client = TestUtils.GetGarnetClient(useTLS: true, networkWriterOptions: SmallBuffers(mode));
            await client.ConnectAsync(cts.Token);
            ClassicAssert.AreEqual("PONG", await client.PingAsync(cts.Token).WaitAsync(Deadline));
            var writer = GetWriter(client);
            using var pause = new InjectionPause(ExceptionInjectionType.GarnetClient_Tls_Pause_Before_Write);

            var value = new string('a', outOfLine ? 4096 : 100);
            var first = Task.Run(async () =>
                await client.ExecuteForStringResultWithCancellationAsync(Echo, [value], cts.Token), cts.Token);
            await pause.ArrivedAsync();

            var values = Enumerable.Range(0, 24).Select(i => $"{i}:{new string((char)('b' + i), 80)}").ToArray();
            var pending = values.Select(v => client.ExecuteForStringResultWithCancellationAsync(Echo, [v], cts.Token)).ToArray();
            Assert.That(first.IsCompleted, Is.False, "A paused TLS write must not complete its reply.");
            if (outOfLine)
                ClassicAssert.Greater(RentedBytes(writer), 0, "The out-of-line rental must remain admitted until encryption completes.");

            pause.Release();
            ClassicAssert.AreEqual(value, await first.WaitAsync(Deadline));
            CollectionAssert.AreEqual(values, await Task.WhenAll(pending).WaitAsync(Deadline));
            ClassicAssert.AreEqual("PONG", await client.PingAsync(cts.Token).WaitAsync(Deadline));
            await writer.WaitForIdleAsync(cts.Token);
            ClassicAssert.AreEqual(0, RentedBytes(writer));
        }

        [Test]
        public async Task TeardownDuringPausedTlsWriteReclaimsRental(
            [Values] bool reconnect, [Values] FlushResultAllocationMode mode)
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();
            using var cts = new CancellationTokenSource(Deadline);
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true);
            server.Start();
            using var client = TestUtils.GetGarnetClient(useTLS: true, networkWriterOptions: SmallBuffers(mode));
            await client.ConnectAsync(cts.Token);
            var oldWriter = GetWriter(client);
            using var pause = new InjectionPause(ExceptionInjectionType.GarnetClient_Tls_Pause_Before_Write);

            var pending = Task.Run(async () =>
                await client.ExecuteForStringResultWithCancellationAsync(Echo, [new string('x', 4096)], cts.Token), cts.Token);
            await pause.ArrivedAsync();
            var queued = client.PingAsync(cts.Token);
            var teardown = Task.Run(async () =>
            {
                if (reconnect)
                    await client.ReconnectAsync(cts.Token);
                else
                    client.Dispose();
            }, cts.Token);

            // The queued completion faults when teardown has reached the old connection, while its send is paused.
            Assert.ThrowsAsync<GarnetClientDisposedException>(async () => await queued.WaitAsync(Deadline));
            ClassicAssert.Greater(RentedBytes(oldWriter), 0);
            pause.Release();
            await teardown.WaitAsync(Deadline);
            Assert.ThrowsAsync<GarnetClientDisposedException>(async () => await pending.WaitAsync(Deadline));
            ClassicAssert.AreEqual(0, RentedBytes(oldWriter));
            if (reconnect)
                ClassicAssert.AreEqual("PONG", await client.PingAsync(cts.Token).WaitAsync(Deadline));
        }

        static NetworkWriter GetWriter(GarnetClient client)
            => (NetworkWriter)typeof(GarnetClient)
                .GetField("networkWriter", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(client);

        static long RentedBytes(NetworkWriter writer)
            => ((MemoryThrottle)typeof(NetworkWriter)
                .GetField("outOfLineRentedBytesThrottle", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(writer)).InUseBytes;

        static NetworkWriterOptions SmallBuffers(FlushResultAllocationMode mode = FlushResultAllocationMode.Buffered) => new(
            networkBufferSizeBytes: 256,
            requestPageSizeBytes: 256,
            requestPageCount: 2,
            maxOutstandingRequests: 8,
            maxOutstandingCompletions: 8,
            maxConcurrentNetworkSends: 2,
            maxOutOfLineRentedBytes: 8L << 20,
            flushResultAllocationMode: mode);

        static TcpListener Listen()
        {
            var listener = new TcpListener(IPAddress.Loopback, TestUtils.TestPort);
            listener.Start();
            return listener;
        }

        static async Task<SslStream> AcceptTlsAsync(TcpListener listener, CancellationToken token)
        {
            var socket = await listener.AcceptSocketAsync(token);
            try
            {
                return await AuthenticatePeerAsync(socket, token, ownsSocket: true);
            }
            catch
            {
                socket.Dispose();
                throw;
            }
        }

        static async Task<SslStream> AuthenticatePeerAsync(Socket socket, CancellationToken token, bool ownsSocket = false)
        {
            var stream = new SslStream(new NetworkStream(socket, ownsSocket));
            try
            {
                await stream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
                {
                    ServerCertificate = TestUtils.GetClientCertificate(),
                    ClientCertificateRequired = true,
                    RemoteCertificateValidationCallback = TestUtils.ValidateServerCertificate,
                }, token);
                return stream;
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        static async Task ReadPingsAsync(SslStream peer, int count, CancellationToken token)
        {
            var buffer = new byte[Ping.Length * count];
            await peer.ReadExactlyAsync(buffer, token);
            for (var i = 0; i < count; i++)
                CollectionAssert.AreEqual(Ping, buffer.AsSpan(i * Ping.Length, Ping.Length).ToArray());
        }

        static async Task AssertPingAsync(GarnetClient client, SslStream peer, CancellationToken token)
        {
            var ping = client.PingAsync(token);
            await ReadPingsAsync(peer, 1, token);
            await peer.WriteAsync("+PONG\r\n"u8.ToArray(), token);
            ClassicAssert.AreEqual("PONG", await ping.WaitAsync(Deadline));
        }

        static async Task ObserveRejectedHandshakeAsync(Task authenticate)
        {
            try
            {
                await authenticate.WaitAsync(Deadline);
            }
            catch (AuthenticationException)
            {
                // The peer can observe the client's fatal TLS alert during its own authentication.
            }
            catch (IOException)
            {
                // The client may close the transport before server authentication finishes.
            }
        }

        sealed class InjectionPause : IDisposable
        {
            readonly ExceptionInjectionType point;
            bool released;

            internal InjectionPause(ExceptionInjectionType point)
            {
                this.point = point;
                ExceptionInjectionHelper.EnableException(point);
            }

            internal Task ArrivedAsync() => ExceptionInjectionHelper.WaitOnClearAsync(point).WaitAsync(Deadline);

            internal void Release()
            {
                if (released)
                    return;
                released = true;
                ExceptionInjectionHelper.SuspendParking();
                ExceptionInjectionHelper.DisableException(point);
            }

            public void Dispose()
            {
                Release();
                ExceptionInjectionHelper.ResumeParking();
            }
        }
    }
}