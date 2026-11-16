// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Sockets;
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
    /// Regression tests for GarnetClient convenience methods that previously double-framed their arguments.
    /// </summary>
    [TestFixture]
    public class GarnetClientArgumentFramingTests : TestBase
    {
        [TestCase(1018)]
        [TestCase(1024)]
        [TestCase(1025)]
        public void SnapshotChunkThatCannotFitBulkFramingIsRejected(int payloadSize)
        {
            using var client = new GarnetClientSession(
                new IPEndPoint(IPAddress.Loopback, TestUtils.TestPort), new NetworkBufferSettings(1024, 1024));
            var data = new byte[payloadSize];
            var error = Assert.Throws<InvalidOperationException>(() =>
                client.ExecuteClusterSnapshotData(Guid.NewGuid().ToByteArray(), 0, 0, data));
            Assert.That(error.Message, Does.Contain("bulk framing"));
        }

        [Test]
        public async Task SnapshotChunkWithExactBulkFitIsTransmitted()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            using var client = new GarnetClientSession(listener.LocalEndpoint, new NetworkBufferSettings(1024, 1024));
            var accept = listener.AcceptTcpClientAsync(timeout.Token);
            client.Connect();
            using var peer = await accept;
            using var stream = peer.GetStream();
            var payload = new string('v', 1015);
            var response = client.ExecuteClusterSnapshotData(Guid.Empty.ToByteArray(), 0, 0, Encoding.ASCII.GetBytes(payload));
            var expected = $"*6\r\n$7\r\nCLUSTER\r\n$13\r\nSNAPSHOT_DATA\r\n$16\r\n{new string('\0', 16)}\r\n$1\r\n0\r\n$1\r\n0\r\n$1015\r\n{payload}\r\n";
            var received = new byte[expected.Length];
            await stream.ReadExactlyAsync(received, timeout.Token);
            Assert.That(Encoding.ASCII.GetString(received), Is.EqualTo(expected));
            await stream.WriteAsync("+OK\r\n"u8.ToArray(), timeout.Token);
            Assert.That(await response.WaitAsync(timeout.Token), Is.EqualTo("OK"));
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        [Test]
        public void InfoSectionBytesAreUnframed()
        {
            ClassicAssert.AreEqual("MEMORY", Encoding.ASCII.GetString(InfoCommandUtils.GetInfoSectionBytes(InfoMetricsType.MEMORY)));
            ClassicAssert.AreEqual("STATS", Encoding.ASCII.GetString(InfoCommandUtils.GetInfoSectionBytes(InfoMetricsType.STATS)));
            // The default section is omitted entirely so the server returns its default INFO output.
            ClassicAssert.IsNull(InfoCommandUtils.GetInfoSectionBytes(default));
        }

        [Test]
        public void FailoverOptionBytesAreUnframed()
        {
            ClassicAssert.AreEqual("FORCE", Encoding.ASCII.GetString(FailoverUtils.GetFailoverOptionBytes(FailoverOption.FORCE)));
            ClassicAssert.AreEqual("TAKEOVER", Encoding.ASCII.GetString(FailoverUtils.GetFailoverOptionBytes(FailoverOption.TAKEOVER)));
        }

        [Test]
        public void InfoSendsSinglyFramedSection()
        {
            const string expected = "*2\r\n$4\r\nINFO\r\n$6\r\nMEMORY\r\n";
            var sent = CaptureClientRequest(db => db.Info(InfoMetricsType.MEMORY), expected.Length);
            ClassicAssert.AreEqual(expected, sent);
        }

        [Test]
        public void FailoverSendsSinglyFramedOption()
        {
            const string expectedForce = "*3\r\n$7\r\nCLUSTER\r\n$8\r\nFAILOVER\r\n$5\r\nFORCE\r\n";
            var force = CaptureClientRequest(db => db.Failover(FailoverOption.FORCE), expectedForce.Length);
            ClassicAssert.AreEqual(expectedForce, force);

            const string expectedTakeover = "*3\r\n$7\r\nCLUSTER\r\n$8\r\nFAILOVER\r\n$8\r\nTAKEOVER\r\n";
            var takeover = CaptureClientRequest(db => db.Failover(FailoverOption.TAKEOVER), expectedTakeover.Length);
            ClassicAssert.AreEqual(expectedTakeover, takeover);
        }

        [Test]
        public async Task InfoReturnsSectionContent()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            var memory = await db.Info(InfoMetricsType.MEMORY);
            ClassicAssert.IsNotNull(memory);
            ClassicAssert.IsTrue(memory.Contains("# Memory"), $"Unexpected MEMORY section: {memory}");

            var stats = await db.Info(InfoMetricsType.STATS);
            ClassicAssert.IsNotNull(stats);
            ClassicAssert.IsTrue(stats.Contains("# Stats"), $"Unexpected STATS section: {stats}");

            var def = await db.Info();
            ClassicAssert.IsNotNull(def);
            ClassicAssert.IsTrue(def.Length > 0);
        }

        /// <summary>
        /// Connects a GarnetClient to a bare loopback listener (no server-side replies), issues one
        /// request, and returns the first <paramref name="expectedLength"/> ASCII bytes the client
        /// placed on the wire.
        /// </summary>
        static string CaptureClientRequest(System.Func<GarnetClient, Task> issue, int expectedLength)
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try
            {
                var endpoint = (IPEndPoint)listener.LocalEndpoint;
                using var db = TestUtils.GetGarnetClient(endpoint);

                var acceptTask = listener.AcceptSocketAsync();
                db.Connect();
                ClassicAssert.IsTrue(acceptTask.Wait(5000), "Timed out accepting the client connection");
                using var serverSocket = acceptTask.Result;

                // Fire-and-forget: no reply is produced, so the request task never completes; observe
                // its eventual fault (on client dispose) to avoid an unobserved task exception.
                var pending = issue(db);
                _ = pending.ContinueWith(t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);

                serverSocket.ReceiveTimeout = 5000;
                var buffer = new byte[expectedLength];
                int total = 0;
                while (total < expectedLength)
                {
                    int read = serverSocket.Receive(buffer, total, expectedLength - total, SocketFlags.None);
                    if (read == 0)
                        break;
                    total += read;
                }

                return Encoding.ASCII.GetString(buffer, 0, total);
            }
            finally
            {
                listener.Stop();
            }
        }
    }
}