// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// The cluster session writes responses through its own send path, so the atomic-write fix applied to
    /// the RESP session does not cover it. CLUSTER GETKEYSINSLOT echoes client-supplied keys and the gossip
    /// reply echoes the serialized cluster config, both of which can exceed the response buffer.
    ///
    /// Run twice: once at the default send buffer and once with the adaptive budget small enough to floor
    /// it, since an adapted buffer widens the window in which an element no longer fits.
    /// </summary>
    [TestFixture("0", null, Description = "default send buffer")]
    [TestFixture("64k", "16k", Description = "send buffer floored by the budget")]
    public class ClusterLargeResponseChunkingTests : TestBase
    {
        // Comfortably larger than the 128 KB default send buffer.
        const int OversizedKey = 200 * 1024;

        readonly string networkBufferMemoryBudget;
        readonly string networkSendBufferMinSize;

        GarnetServer server;

        public ClusterLargeResponseChunkingTests(string networkBufferMemoryBudget, string networkSendBufferMinSize)
        {
            this.networkBufferMemoryBudget = networkBufferMemoryBudget;
            this.networkSendBufferMinSize = networkSendBufferMinSize;
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                enableCluster: true,
                networkBufferMemoryBudget: networkBufferMemoryBudget,
                networkSendBufferMinSize: networkSendBufferMinSize);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        static Socket Connect()
        {
            var s = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true, ReceiveTimeout = 20_000 };
            s.Connect(TestUtils.EndPoint);
            return s;
        }

        static void Send(Socket s, params string[] parts)
        {
            var sb = new StringBuilder();
            sb.Append('*').Append(parts.Length).Append("\r\n");
            foreach (var p in parts)
                sb.Append('$').Append(p.Length).Append("\r\n").Append(p).Append("\r\n");
            var bytes = Encoding.ASCII.GetBytes(sb.ToString());
            var sent = 0;
            while (sent < bytes.Length)
                sent += s.Send(bytes, sent, bytes.Length - sent, SocketFlags.None);
        }

        /// <summary>
        /// Reads until <paramref name="expected"/> appears. A dropped connection is the failure mode under
        /// test: the session is killed mid-response when an element cannot be written atomically.
        /// </summary>
        static string ReadUntil(Socket s, string expected, int budget)
        {
            var sb = new StringBuilder();
            var buf = new byte[64 * 1024];
            while (sb.Length < budget)
            {
                int n;
                try
                {
                    n = s.Receive(buf);
                }
                catch (SocketException e)
                {
                    throw new Exception($"connection dropped after {sb.Length} bytes: {e.SocketErrorCode}");
                }
                if (n == 0) throw new Exception($"connection closed after {sb.Length} bytes");
                sb.Append(Encoding.ASCII.GetString(buf, 0, n));
                if (sb.ToString().Contains(expected, StringComparison.Ordinal)) return sb.ToString();
            }
            throw new Exception($"never saw '{expected[..Math.Min(32, expected.Length)]}'");
        }

        [Test]
        public void GetKeysInSlotReturnsKeyLargerThanSendBuffer()
        {
            using var s = Connect();

            Send(s, "CLUSTER", "ADDSLOTSRANGE", "0", "16383");
            ReadUntil(s, "+OK\r\n", 1024);

            var bigKey = new string('k', OversizedKey);

            Send(s, "CLUSTER", "KEYSLOT", bigKey);
            var slotReply = ReadUntil(s, "\r\n", 64);
            ClassicAssert.IsTrue(slotReply.StartsWith(':'), $"unexpected KEYSLOT reply: {slotReply}");
            var slot = slotReply[1..slotReply.IndexOf('\r')];

            Send(s, "SET", bigKey, "v");
            ReadUntil(s, "+OK\r\n", 1024);

            Send(s, "CLUSTER", "GETKEYSINSLOT", slot, "10");
            var reply = ReadAtLeast(s, OversizedKey + 2);

            // The whole key must come back, which is what the atomic path cannot do.
            StringAssert.Contains($"${OversizedKey}\r\n", reply);
            ClassicAssert.GreaterOrEqual(reply.Length, OversizedKey, "response was truncated");
        }

        /// <summary>
        /// Accumulates at least <paramref name="count"/> bytes. A dropped connection is the failure mode
        /// under test: the session is killed mid-response.
        /// </summary>
        static string ReadAtLeast(Socket s, int count)
        {
            var sb = new StringBuilder();
            var buf = new byte[64 * 1024];
            while (sb.Length < count)
            {
                int n;
                try
                {
                    n = s.Receive(buf);
                }
                catch (SocketException e)
                {
                    throw new Exception($"connection dropped after {sb.Length} of {count} bytes: {e.SocketErrorCode}");
                }
                if (n == 0) throw new Exception($"connection closed after {sb.Length} of {count} bytes");
                sb.Append(Encoding.ASCII.GetString(buf, 0, n));
            }
            return sb.ToString();
        }
    }
}