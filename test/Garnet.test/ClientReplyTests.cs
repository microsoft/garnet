// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Tests for the <c>CLIENT REPLY ON|OFF|SKIP</c> subcommand.
    /// Uses a raw TCP socket because StackExchange.Redis cannot tolerate reply suppression.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    public class ClientReplyTests : AllureTestBase
    {
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
            server.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Small helper that wraps a raw TCP connection to the test Garnet server and exposes
        /// Send/TryRead so tests can verify byte-level reply suppression behaviour.
        /// </summary>
        sealed class RawConn : IDisposable
        {
            readonly TcpClient client;
            readonly NetworkStream stream;

            public RawConn(int readTimeoutMs = 500)
            {
                var ep = (System.Net.IPEndPoint)TestUtils.EndPoint;
                client = new TcpClient();
                client.Connect(ep.Address, ep.Port);
                stream = client.GetStream();
                stream.ReadTimeout = readTimeoutMs;
            }

            public void SendRaw(string s)
            {
                var bytes = Encoding.ASCII.GetBytes(s);
                stream.Write(bytes, 0, bytes.Length);
                stream.Flush();
            }

            /// <summary>Encode args as a RESP array of bulk strings and send.</summary>
            public void SendCommand(params string[] args)
            {
                var sb = new StringBuilder();
                sb.Append('*').Append(args.Length).Append("\r\n");
                foreach (var a in args)
                {
                    sb.Append('$').Append(Encoding.ASCII.GetByteCount(a)).Append("\r\n").Append(a).Append("\r\n");
                }
                SendRaw(sb.ToString());
            }

            /// <summary>Returns whatever bytes are available within the read timeout, or empty string on timeout.</summary>
            public string TryRead()
            {
                var buf = new byte[8192];
                try
                {
                    int n = stream.Read(buf, 0, buf.Length);
                    return n <= 0 ? string.Empty : Encoding.ASCII.GetString(buf, 0, n);
                }
                catch (IOException)
                {
                    return string.Empty;
                }
            }

            /// <summary>Read until the buffer matches the expected string (or read timeout fires).</summary>
            public string ReadExpected(string expected)
            {
                var sb = new StringBuilder();
                while (sb.Length < expected.Length)
                {
                    var chunk = TryRead();
                    if (chunk.Length == 0) break;
                    sb.Append(chunk);
                }
                return sb.ToString();
            }

            public void Dispose()
            {
                stream?.Dispose();
                client?.Dispose();
            }
        }

        [Test]
        public void ClientReplyOnReturnsOK()
        {
            using var c = new RawConn();
            c.SendCommand("CLIENT", "REPLY", "ON");
            ClassicAssert.AreEqual("+OK\r\n", c.ReadExpected("+OK\r\n"));
        }

        [Test]
        public void ClientReplyOffSuppressesResponses()
        {
            using var c = new RawConn();

            // OFF itself produces no reply.
            c.SendCommand("CLIENT", "REPLY", "OFF");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            // Subsequent commands produce no replies.
            c.SendCommand("PING");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            c.SendCommand("SET", "k", "v");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            c.SendCommand("GET", "k");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            // ON returns +OK and replies resume.
            c.SendCommand("CLIENT", "REPLY", "ON");
            ClassicAssert.AreEqual("+OK\r\n", c.ReadExpected("+OK\r\n"));

            c.SendCommand("PING");
            ClassicAssert.AreEqual("+PONG\r\n", c.ReadExpected("+PONG\r\n"));
        }

        [Test]
        public void ClientReplySkipSuppressesOnlyNextCommand()
        {
            using var c = new RawConn();

            // SKIP itself produces no reply.
            c.SendCommand("CLIENT", "REPLY", "SKIP");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            // Next command's reply is suppressed.
            c.SendCommand("PING");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            // The command after that replies normally.
            c.SendCommand("PING");
            ClassicAssert.AreEqual("+PONG\r\n", c.ReadExpected("+PONG\r\n"));
        }

        [Test]
        public void ClientReplyWrongArityReturnsError()
        {
            using var c = new RawConn();

            // Zero mode args.
            c.SendCommand("CLIENT", "REPLY");
            var reply = c.ReadExpected("-ERR");
            StringAssert.StartsWith("-ERR", reply);

            // Two mode args.
            c.SendCommand("CLIENT", "REPLY", "ON", "EXTRA");
            reply = c.ReadExpected("-ERR");
            StringAssert.StartsWith("-ERR", reply);
        }

        [Test]
        public void ClientReplyInvalidModeReturnsSyntaxError()
        {
            using var c = new RawConn();
            c.SendCommand("CLIENT", "REPLY", "GARBAGE");
            var reply = c.ReadExpected("-ERR syntax error\r\n");
            ClassicAssert.AreEqual("-ERR syntax error\r\n", reply);
        }

        [Test]
        public void ClientReplyCaseInsensitive()
        {
            using var c = new RawConn();

            // lower case off
            c.SendCommand("client", "reply", "off");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            c.SendCommand("PING");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            // Mixed-case On
            c.SendCommand("CLIENT", "REPLY", "On");
            ClassicAssert.AreEqual("+OK\r\n", c.ReadExpected("+OK\r\n"));

            // Mixed-case Off
            c.SendCommand("CLIENT", "REPLY", "Off");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            c.SendCommand("CLIENT", "REPLY", "ON");
            ClassicAssert.AreEqual("+OK\r\n", c.ReadExpected("+OK\r\n"));
        }

        [Test]
        public void ClientReplyOffPersistsAcrossManyCommands()
        {
            const int N = 50;

            using var writer = new RawConn();
            writer.SendCommand("CLIENT", "REPLY", "OFF");
            ClassicAssert.AreEqual(string.Empty, writer.TryRead());

            for (int i = 0; i < N; i++)
            {
                writer.SendCommand("SET", "k" + i, "v" + i);
            }
            // No bytes should have been emitted for any of the 50 SETs.
            ClassicAssert.AreEqual(string.Empty, writer.TryRead());

            writer.SendCommand("CLIENT", "REPLY", "ON");
            ClassicAssert.AreEqual("+OK\r\n", writer.ReadExpected("+OK\r\n"));

            // Verify on a separate connection that the SETs actually executed.
            using var reader = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = reader.GetDatabase(0);
            for (int i = 0; i < N; i++)
            {
                ClassicAssert.AreEqual("v" + i, (string)db.StringGet("k" + i));
            }
        }

        [Test]
        public void ClientReplyOffErrorAlsoSuppressed()
        {
            using var c = new RawConn();

            c.SendCommand("CLIENT", "REPLY", "OFF");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            // Invalid args to GET would normally produce an error reply.
            c.SendCommand("GET");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            // CLIENT REPLY GARBAGE while OFF — syntax error normally, must also be suppressed.
            c.SendCommand("CLIENT", "REPLY", "GARBAGE");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            // ON restores; connection is still healthy.
            c.SendCommand("CLIENT", "REPLY", "ON");
            ClassicAssert.AreEqual("+OK\r\n", c.ReadExpected("+OK\r\n"));

            c.SendCommand("PING");
            ClassicAssert.AreEqual("+PONG\r\n", c.ReadExpected("+PONG\r\n"));
        }

        [Test]
        public void ClientReplySkipDoesNotStack()
        {
            using var c = new RawConn();

            // Two SKIPs in a row: only ONE following normal command should be suppressed.
            c.SendCommand("CLIENT", "REPLY", "SKIP");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            c.SendCommand("CLIENT", "REPLY", "SKIP");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            // First PING after the two SKIPs: suppressed.
            c.SendCommand("PING");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            // Second PING: replied to normally.
            c.SendCommand("PING");
            ClassicAssert.AreEqual("+PONG\r\n", c.ReadExpected("+PONG\r\n"));
        }
    }
}
