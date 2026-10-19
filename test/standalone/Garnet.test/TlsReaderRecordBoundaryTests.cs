// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers the server's TLS reader when a decrypted read exactly fills the transport buffer and processing
    /// consumes all of it. The rest of the TLS record remains inside <see cref="SslStream"/>, and no further
    /// ciphertext arrives to trigger another receive, so the reader must read again before resting.
    /// </summary>
    [TestFixture]
    public class TlsReaderRecordBoundaryTests : TestBase
    {
        const int TransportBufferSize = 1 << 10;

        static readonly TimeSpan Deadline = TimeSpan.FromSeconds(5);

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            // The budget is disabled so the receive buffers start at exactly the configured size.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true,
                networkBufferSize: "1k", networkBufferMemoryBudget: "0");
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
        public async Task ACommandEndingExactlyAtTheTransportBufferEndDoesNotStrandTheRestOfItsRecord()
        {
            using var tcp = new TcpClient();
            await tcp.ConnectAsync((IPEndPoint)TestUtils.EndPoint);
            await using var ssl = new SslStream(tcp.GetStream(), leaveInnerStreamOpen: false, TestUtils.ValidateServerCertificate);
            await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                ClientCertificates = [TestUtils.GetClientCertificate()],
                TargetHost = "GarnetTest",
                AllowRenegotiation = false,
            });

            var set = SetCommandOfExactLength(TransportBufferSize);
            var ping = Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n");

            // One write produces one TLS record: the server's first read returns exactly the SET, which it
            // consumes entirely, while the PING stays decrypted inside its SslStream.
            var record = new byte[set.Length + ping.Length];
            set.CopyTo(record, 0);
            ping.CopyTo(record, set.Length);
            await ssl.WriteAsync(record);
            await ssl.FlushAsync();

            const string expected = "+OK\r\n+PONG\r\n";
            var received = new StringBuilder();
            var buffer = new byte[64];
            try
            {
                while (received.Length < expected.Length)
                {
                    var read = await ssl.ReadAsync(buffer).AsTask().WaitAsync(Deadline);
                    ClassicAssert.Greater(read, 0, "the server closed the connection before replying to both commands");
                    _ = received.Append(Encoding.ASCII.GetString(buffer, 0, read));
                }
            }
            catch (TimeoutException)
            {
                Assert.Fail($"the server replied only '{Escape(received.ToString())}': the command after one that ended " +
                    "exactly at the transport buffer end stayed inside SslStream and was never read");
            }

            ClassicAssert.AreEqual(expected, received.ToString());
        }

        static byte[] SetCommandOfExactLength(int length)
        {
            // "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$<n>\r\n<value>\r\n"
            const string prefix = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$";
            for (var valueLength = length - prefix.Length; valueLength > 0; valueLength--)
            {
                var header = prefix + valueLength + "\r\n";
                if (header.Length + valueLength + 2 == length)
                    return Encoding.ASCII.GetBytes(header + new string('v', valueLength) + "\r\n");
            }

            throw new InvalidOperationException($"no SET command encodes to exactly {length} bytes");
        }

        static string Escape(string value) => value.Replace("\r", "\\r").Replace("\n", "\\n");
    }
}