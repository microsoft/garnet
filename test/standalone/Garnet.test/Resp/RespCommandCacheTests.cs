// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
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
        public void CachedCommandDoesNotTruncateArgumentCount(int argCount)
        {
            var injectedCommand = Resp("PING");
            using var socket = Connect();

            ClassicAssert.AreEqual(":0\r\n", Send(socket, Resp("DEL", CreateArguments(argCount, -1, injectedCommand))));
            ClassicAssert.AreEqual(":0\r\n", Send(socket, Resp("DEL", CreateArguments(argCount, argCount & 0xFF, injectedCommand))));
        }

        [Test]
        public void HsetPayloadCannotInjectAclCommand()
        {
            RestartWithAcl();

            var injectedCommand = Resp("ACL", "SETUSER", "backdoor", "on", ">Pwn3d!", "~*", "+@all");
            using var application = Connect();
            ClassicAssert.AreEqual("+OK\r\n", Send(application, Resp("AUTH", "default", "AdminPass123")));
            ClassicAssert.IsTrue(Send(application, Resp("HSET", CreateArguments(259, -1, injectedCommand, "app"))).StartsWith(':'));

            var response = Send(application, Resp("HSET", CreateArguments(259, 3, injectedCommand, "app")));
            ClassicAssert.IsFalse(response.Contains("+OK\r\n"), "Injected ACL SETUSER command was executed.");

            using var attacker = Connect();
            ClassicAssert.IsFalse(Send(attacker, Resp("AUTH", "backdoor", "Pwn3d!")).Contains("+OK\r\n"), "Backdoor ACL account was created.");
        }

        private void RestartWithAcl()
        {
            server.Dispose();
            var aclFile = Path.Combine(TestUtils.MethodTestDir, "repro.acl");
            File.WriteAllText(aclFile, "user default on >AdminPass123 ~* +@all\r\nuser lowpriv on >LowPass123 ~* -@all +get +set +hset +ping +echo +hget");
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: aclFile);
            server.Start();
        }

        private static string[] CreateArguments(int count, int injectAt, string injectedCommand, string firstArgument = null)
        {
            var arguments = new string[count];
            for (var i = 0; i < arguments.Length; i++)
                arguments[i] = i == injectAt ? injectedCommand : i == 0 && firstArgument != null ? firstArgument : $"arg{i}";

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

        private static string Send(Socket socket, string command)
        {
            socket.Send(Encoding.ASCII.GetBytes(command));
            var response = new StringBuilder();
            var buffer = new byte[8192];
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < deadline)
            {
                if (socket.Available == 0)
                {
                    Thread.Sleep(10);
                    continue;
                }

                response.Append(Encoding.ASCII.GetString(buffer, 0, socket.Receive(buffer)));
                deadline = DateTime.UtcNow.AddMilliseconds(500);
            }

            return response.ToString();
        }
    }
}