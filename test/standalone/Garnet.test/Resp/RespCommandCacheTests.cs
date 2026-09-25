// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Net.Sockets;
using System.Text;
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
            using var stream = Connect();

            ClassicAssert.AreEqual(":0\r\n", await SendAsync(stream, Resp("DEL", CreateArguments(argCount))));
            ClassicAssert.AreEqual(":0\r\n", await SendAsync(stream, Resp("DEL", CreateArguments(argCount, argCount & 0xFF))));
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

        private static NetworkStream Connect()
        {
            var socket = new Socket(TestUtils.EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(TestUtils.EndPoint);
            return new NetworkStream(socket, ownsSocket: true);
        }

        private static async Task<string> SendAsync(NetworkStream stream, string command)
        {
            const string marker = "$17\r\nresponse-complete\r\n";
            await stream.WriteAsync(Encoding.ASCII.GetBytes(command + Resp("ECHO", "response-complete")));

            var response = new StringBuilder();
            var buffer = new byte[8192];
            while (!response.ToString().EndsWith(marker))
            {
                var bytesReceived = await stream.ReadAsync(buffer);
                if (bytesReceived == 0) throw new EndOfStreamException();
                response.Append(Encoding.ASCII.GetString(buffer, 0, bytesReceived));
            }

            return response.Remove(response.Length - marker.Length, marker.Length).ToString();
        }
    }
}