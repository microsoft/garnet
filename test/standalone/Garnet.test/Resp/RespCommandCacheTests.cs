// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp
{
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

        [Test]
        [TestCase(255)]
        [TestCase(256)]
        [TestCase(259)]
        [TestCase(600)]
        public async Task CachedCommandDoesNotTruncateArgumentCount(int argCount)
        {
            using var socket = Connect();

            ClassicAssert.AreEqual(":0\r\n", await SendAsync(socket, Resp("DEL", CreateArguments(argCount))));
            ClassicAssert.AreEqual(":0\r\n", await SendAsync(socket, Resp("DEL", CreateArguments(argCount, argCount & 0xFF))));
        }

        private static string[] CreateArguments(int count, int pingIndex = -1)
        {
            var arguments = new string[count];
            for (var i = 0; i < arguments.Length; i++)
                arguments[i] = i == pingIndex ? Resp("PING") : $"arg{i}";

            return arguments;
        }

        private static string Resp(string command, params string[] arguments)
        {
            var builder = new StringBuilder().Append('*').Append(arguments.Length + 1).Append("\r\n");
            AppendBulkString(builder, command);
            foreach (var argument in arguments)
                AppendBulkString(builder, argument);

            return builder.ToString();
        }

        private static void AppendBulkString(StringBuilder builder, string value) => builder.Append('$').Append(value.Length).Append("\r\n").Append(value).Append("\r\n");

        private static Socket Connect()
        {
            var socket = new Socket(TestUtils.EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(TestUtils.EndPoint);
            return socket;
        }

        private static async Task<string> SendAsync(Socket socket, string command)
        {
            var request = Encoding.ASCII.GetBytes(command);
            var bytesSent = 0;
            while (bytesSent < request.Length)
                bytesSent += await socket.SendAsync(request.AsMemory(bytesSent), SocketFlags.None);

            var response = new StringBuilder();
            var buffer = new byte[8192];
            var timeout = TimeSpan.FromSeconds(5);
            while (true)
            {
                using var cancellationSource = new CancellationTokenSource(timeout);
                try
                {
                    var bytesReceived = await socket.ReceiveAsync(buffer, SocketFlags.None, cancellationSource.Token);
                    if (bytesReceived == 0) break;
                    response.Append(Encoding.ASCII.GetString(buffer, 0, bytesReceived));
                    timeout = TimeSpan.FromMilliseconds(500);
                }
                catch (OperationCanceledException) when (response.Length > 0)
                {
                    break;
                }
            }

            return response.ToString();
        }
    }
}