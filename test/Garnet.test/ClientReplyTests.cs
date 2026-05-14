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

            /// <summary>
            /// Read exactly <paramref name="expected"/>.Length bytes from the socket, one byte
            /// at a time, so we never over-read and leave leftovers buffered for later assertions.
            /// Returns whatever was accumulated before a read timeout or EOF.
            /// </summary>
            public string ReadExpected(string expected)
            {
                var sb = new StringBuilder();
                var one = new byte[1];
                while (sb.Length < expected.Length)
                {
                    try
                    {
                        int n = stream.Read(one, 0, 1);
                        if (n <= 0) break;
                        sb.Append((char)one[0]);
                    }
                    catch (IOException)
                    {
                        break;
                    }
                }
                return sb.ToString();
            }

            /// <summary>
            /// Read a single RESP simple-string (<c>+...</c>) or error (<c>-...</c>) reply,
            /// up to and including the trailing CRLF. One byte at a time so subsequent reads
            /// don't pick up any stray bytes from this reply.
            /// </summary>
            public string ReadSimpleReply()
            {
                var sb = new StringBuilder();
                var one = new byte[1];
                int prev = -1;
                while (true)
                {
                    try
                    {
                        int n = stream.Read(one, 0, 1);
                        if (n <= 0) break;
                    }
                    catch (IOException)
                    {
                        break;
                    }
                    sb.Append((char)one[0]);
                    if (prev == '\r' && one[0] == '\n') break;
                    prev = one[0];
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
            var reply = c.ReadSimpleReply();
            StringAssert.StartsWith("-ERR", reply);
            StringAssert.EndsWith("\r\n", reply);

            // Two mode args.
            c.SendCommand("CLIENT", "REPLY", "ON", "EXTRA");
            reply = c.ReadSimpleReply();
            StringAssert.StartsWith("-ERR", reply);
            StringAssert.EndsWith("\r\n", reply);
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

        /// <summary>
        /// Unknown commands must burn a pending SKIP (the SKIP target was that command,
        /// even though the server can't dispatch it). Matches Redis semantics.
        /// </summary>
        [Test]
        public void ClientReplySkipBurnedByUnknownCommand()
        {
            using var c = new RawConn();

            c.SendCommand("CLIENT", "REPLY", "SKIP");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            // Unknown command — its error reply is suppressed AND it burns the SKIP.
            c.SendCommand("NOSUCHCMD");
            ClassicAssert.AreEqual(string.Empty, c.TryRead());

            // Next command replies normally (SKIP was burned, not still pending).
            c.SendCommand("PING");
            ClassicAssert.AreEqual("+PONG\r\n", c.ReadExpected("+PONG\r\n"));
        }

        /// <summary>
        /// Pipeline four commands in a single TCP write where SKIP is interleaved.
        /// Verifies the reply stream ordering is preserved: the prior PING's reply must
        /// not be dropped when the next command runs under suppression.
        /// </summary>
        [Test]
        public void ClientReplyPipelinedSkipBetweenReplies()
        {
            using var c = new RawConn();

            var sb = new StringBuilder();
            sb.Append("*1\r\n$4\r\nPING\r\n");                                            // -> +PONG
            sb.Append("*3\r\n$6\r\nCLIENT\r\n$5\r\nREPLY\r\n$4\r\nSKIP\r\n");             // -> (none)
            sb.Append("*1\r\n$4\r\nPING\r\n");                                            // -> suppressed
            sb.Append("*1\r\n$4\r\nPING\r\n");                                            // -> +PONG
            c.SendRaw(sb.ToString());

            const string expected = "+PONG\r\n+PONG\r\n";
            ClassicAssert.AreEqual(expected, c.ReadExpected(expected));
        }

        /// <summary>
        /// Pipeline OFF/ON transitions inside a single batch. Prior PING reply must survive
        /// the buffer-rewind that the suppressed PING triggers, and the trailing CLIENT REPLY
        /// ON must produce its +OK.
        /// </summary>
        [Test]
        public void ClientReplyPipelinedOffOnOrdering()
        {
            using var c = new RawConn();

            var sb = new StringBuilder();
            sb.Append("*1\r\n$4\r\nPING\r\n");                                            // -> +PONG
            sb.Append("*3\r\n$6\r\nCLIENT\r\n$5\r\nREPLY\r\n$3\r\nOFF\r\n");              // -> (none)
            sb.Append("*1\r\n$4\r\nPING\r\n");                                            // -> suppressed
            sb.Append("*3\r\n$6\r\nCLIENT\r\n$5\r\nREPLY\r\n$2\r\nON\r\n");               // -> +OK
            c.SendRaw(sb.ToString());

            const string expected = "+PONG\r\n+OK\r\n";
            ClassicAssert.AreEqual(expected, c.ReadExpected(expected));
        }
    }
}
