// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp
{
    /// <summary>
    /// Tests for the per-session MRU command cache in the RESP parser.
    /// </summary>
    [TestFixture]
    public class RespCommandCacheTests : TestBase
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// A command cached with an argument count that does not fit in the cache's byte-wide
        /// counter must not be replayed with a truncated count: the parser would stop short of the
        /// end of the frame and interpret the remaining argument data as new commands.
        /// </summary>
        [Test]
        [TestCase(255)]
        [TestCase(256)]
        [TestCase(259)]
        [TestCase(600)]
        public void CachedCommandDoesNotTruncateArgumentCount(int argCount)
        {
            const string InjectedCommand = "*1\r\n$4\r\nPING\r\n";

            using var socket = new Socket(TestUtils.EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(TestUtils.EndPoint);

            // Prime the MRU cache with a benign frame that shares the exact 16-byte header prefix.
            socket.Send(BuildDel(argCount, injectAt: -1, InjectedCommand));
            ClassicAssert.AreEqual(":0\r\n", ReadAll(socket));

            // Fire the same header with an argument holding a complete RESP command. The injected
            // command sits right after the truncated count's boundary, where a desynchronized
            // parser would resume.
            socket.Send(BuildDel(argCount, injectAt: argCount & 0xFF, InjectedCommand));
            ClassicAssert.AreEqual(":0\r\n", ReadAll(socket));
        }

        private static byte[] BuildDel(int argCount, int injectAt, string injectedCommand)
        {
            var sb = new StringBuilder();
            sb.Append($"*{argCount + 1}\r\n$3\r\nDEL\r\n");
            for (var i = 0; i < argCount; i++)
            {
                var arg = i == injectAt ? injectedCommand : $"k{i}";
                sb.Append($"${arg.Length}\r\n{arg}\r\n");
            }

            return Encoding.ASCII.GetBytes(sb.ToString());
        }

        /// <summary>
        /// Drains everything the server sends, including any replies beyond the expected one.
        /// </summary>
        private static string ReadAll(Socket socket)
        {
            var sb = new StringBuilder();
            var buffer = new byte[8192];
            var deadline = DateTime.UtcNow.AddSeconds(5);

            while (DateTime.UtcNow < deadline)
            {
                if (socket.Available == 0)
                {
                    Thread.Sleep(10);
                    continue;
                }

                var read = socket.Receive(buffer);
                sb.Append(Encoding.ASCII.GetString(buffer, 0, read));

                // Keep draining briefly so extra replies from a desynchronized parser are observed.
                deadline = DateTime.UtcNow.AddMilliseconds(500);
            }

            return sb.ToString();
        }
    }
}
