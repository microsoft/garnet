// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers the TLS reader's end-of-stream handling. A peer that leaves a partial (not-yet-consumable)
    /// RESP frame buffered on the server and then performs a graceful TLS shutdown (close_notify) makes the
    /// server-side <see cref="SslStream"/> read return zero. The reader must tear the connection down rather
    /// than re-reading zero bytes forever while the buffered partial frame keeps its decrypt loop alive.
    ///
    /// The partial-frame-then-close_notify ordering is made deterministic with an injection point that parks
    /// the reader while a partial frame is buffered, so the test can deliver the graceful close before
    /// releasing it. Relies on <see cref="ExceptionInjectionHelper"/>, so it only runs in DEBUG builds.
    /// </summary>
    [TestFixture]
    public class TlsReaderEndOfStreamTests : TestBase
    {
        const ExceptionInjectionType Pause = ExceptionInjectionType.Tls_Pause_With_Partial_Frame;

        static readonly TimeSpan Deadline = TimeSpan.FromSeconds(20);

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
            ExceptionInjectionHelper.DisableException(Pause);
            server?.Dispose();
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task GracefulCloseWithPartialFrameTearsDownConnection()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            using var tcp = new TcpClient();
            await tcp.ConnectAsync((IPEndPoint)TestUtils.EndPoint);
            await using var ssl = new SslStream(tcp.GetStream(), leaveInnerStreamOpen: false, TestUtils.ValidateServerCertificate);
            await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                ClientCertificates = [TestUtils.GetClientCertificate()],
                TargetHost = "GarnetTest",
                AllowRenegotiation = false,
            });

            // Arm the rendezvous, then send a deliberately incomplete command: a bulk string that
            // declares 100 bytes but carries only four. The server buffers it, cannot consume it, and
            // parks at the injection point with the partial frame still outstanding.
            ExceptionInjectionHelper.EnableException(Pause);

            await ssl.WriteAsync(Encoding.ASCII.GetBytes("*1\r\n$100\r\nPING"));
            await ssl.FlushAsync();

            // ResetAndWaitAsync clears the flag on arrival, so this completes once the server has buffered
            // the partial frame and parked -- deterministically before the graceful close is delivered.
            await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(Deadline);

            // Graceful TLS shutdown: emits close_notify over the still-open TCP connection, so the
            // server-side read returns zero while the partial frame is still buffered.
            await ssl.ShutdownAsync();

            // Release the parked reader; with the fix it observes end-of-stream and disposes the connection.
            ExceptionInjectionHelper.EnableException(Pause);

            // The server tearing down the connection closes the socket, which surfaces here as either a
            // zero-length read (clean end-of-stream) or an IOException (reset). A timeout means the reader
            // spun on zero-byte reads instead of treating the graceful close as end-of-stream.
            var buffer = new byte[64];
            try
            {
                var read = await ssl.ReadAsync(buffer).AsTask().WaitAsync(Deadline);
                ClassicAssert.AreEqual(0, read, "expected end-of-stream after the graceful close, but data was received");
            }
            catch (IOException)
            {
                // Server closed the underlying socket as part of teardown -- also the expected outcome.
            }
            catch (TimeoutException)
            {
                Assert.Fail(
                    "the server did not tear down the connection after a graceful close with a partial frame " +
                    "buffered -- the TLS reader kept re-reading zero bytes instead of treating it as end-of-stream");
            }
        }
    }
}