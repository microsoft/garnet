// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using System.Text;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture, NonParallelizable]
    public class RespServerControlTests : TestBase
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

        static Socket Connect()
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { ReceiveTimeout = 10000 };
            socket.Connect(TestUtils.EndPoint);
            return socket;
        }

        static void Send(Socket socket, params string[] arguments)
        {
            var request = new StringBuilder($"*{arguments.Length}\r\n");
            foreach (var argument in arguments)
                request.Append($"${Encoding.UTF8.GetByteCount(argument)}\r\n{argument}\r\n");
            var bytes = Encoding.UTF8.GetBytes(request.ToString());
            var sent = 0;
            while (sent < bytes.Length)
                sent += socket.Send(bytes.AsSpan(sent));
        }

        static string ReadReply(Socket socket)
        {
            var reply = new StringBuilder();
            var next = new byte[1];
            do
            {
                Assert.That(socket.Receive(next), Is.EqualTo(1), "Connection closed before a reply arrived.");
                reply.Append((char)next[0]);
            } while (next[0] != '\n');

            var header = reply.ToString();
            if (header[0] == '$' && int.Parse(header[1..^2]) is var length && length >= 0)
            {
                for (var i = 0; i < length + 2; i++)
                {
                    Assert.That(socket.Receive(next), Is.EqualTo(1));
                    reply.Append((char)next[0]);
                }
            }
            else if (header[0] == '*')
            {
                for (var i = 0; i < int.Parse(header[1..^2]); i++)
                    reply.Append(ReadReply(socket));
            }
            return reply.ToString();
        }

        [TestCase("ALL")]
        [TestCase("WRITE")]
        public void PausePreservesPipelineAndUnpauseResumesIt(string mode)
        {
            using var client = Connect();
            using var control = Connect();
            Send(client, "CLIENT", "PAUSE", "10000", mode);
            Send(client, "SET", "key", "value");
            Send(client, "GET", "key");
            Assert.That(ReadReply(client), Is.EqualTo("+OK\r\n"));
            Assert.That(client.Poll(100000, SelectMode.SelectRead), Is.False);
            if (mode == "WRITE")
            {
                Send(control, "GET", "key");
                Assert.That(ReadReply(control), Is.EqualTo("$-1\r\n"));
            }
            Send(control, "CLIENT", "UNPAUSE");
            Assert.That(ReadReply(control), Is.EqualTo("+OK\r\n"));
            Assert.That(ReadReply(client), Is.EqualTo("+OK\r\n"));
            Assert.That(ReadReply(client), Is.EqualTo("$5\r\nvalue\r\n"));
        }

        [Test]
        public void DefaultPauseDelaysReadsAndExpiresWithoutNewInput()
        {
            using var client = Connect();
            using var control = Connect();
            Send(control, "CLIENT", "PAUSE", "500");
            Assert.That(ReadReply(control), Is.EqualTo("+OK\r\n"));
            Send(client, "PING");
            Assert.That(client.Poll(100000, SelectMode.SelectRead), Is.False);
            Assert.That(ReadReply(client), Is.EqualTo("+PONG\r\n"));
        }

        [Test]
        public void RepeatedPauseDoesNotShortenExistingTimeout()
        {
            using var client = Connect();
            using var control = Connect();
            Send(control, "CLIENT", "PAUSE", "10000", "WRITE");
            Assert.That(ReadReply(control), Is.EqualTo("+OK\r\n"));
            Send(control, "CLIENT", "PAUSE", "1", "WRITE");
            Assert.That(ReadReply(control), Is.EqualTo("+OK\r\n"));
            Send(client, "SET", "key", "value");
            Assert.That(client.Poll(100000, SelectMode.SelectRead), Is.False);
            Send(control, "CLIENT", "UNPAUSE");
            Assert.That(ReadReply(control), Is.EqualTo("+OK\r\n"));
            Assert.That(ReadReply(client), Is.EqualTo("+OK\r\n"));
        }

        [Test]
        public void PauseInsideTransactionTakesEffectAfterExec()
        {
            using var client = Connect();
            using var control = Connect();
            Send(client, "MULTI");
            Send(client, "CLIENT", "PAUSE", "10000", "WRITE");
            Send(client, "SET", "key", "value");
            Send(client, "EXEC");
            Assert.That(ReadReply(client), Is.EqualTo("+OK\r\n"));
            Assert.That(ReadReply(client), Is.EqualTo("+QUEUED\r\n"));
            Assert.That(ReadReply(client), Is.EqualTo("+QUEUED\r\n"));
            Assert.That(ReadReply(client), Is.EqualTo("*2\r\n+OK\r\n+OK\r\n"));
            Send(client, "SET", "key", "new-value");
            Assert.That(client.Poll(100000, SelectMode.SelectRead), Is.False);
            Send(control, "GET", "key");
            Assert.That(ReadReply(control), Is.EqualTo("$5\r\nvalue\r\n"));
            Send(control, "CLIENT", "UNPAUSE");
            Assert.That(ReadReply(control), Is.EqualTo("+OK\r\n"));
            Assert.That(ReadReply(client), Is.EqualTo("+OK\r\n"));
        }

        [TestCase("CLIENT", "PAUSE", "-1")]
        [TestCase("CLIENT", "PAUSE", "9223372036854775808")]
        [TestCase("CLIENT", "PAUSE", "100", "INVALID")]
        [TestCase("CLIENT", "UNPAUSE", "extra")]
        [TestCase("SHUTDOWN", "INVALID")]
        [TestCase("SHUTDOWN", "SAVE", "NOSAVE")]
        [TestCase("SHUTDOWN", "ABORT")]
        public void InvalidControlCommandsLeaveServerUsable(params string[] command)
        {
            using var client = Connect();
            Send(client, command);
            Assert.That(ReadReply(client), Does.StartWith("-ERR"));
            Send(client, "PING");
            Assert.That(ReadReply(client), Is.EqualTo("+PONG\r\n"));
        }

        [Test]
        public void ShutdownIsRejectedInsideMulti()
        {
            using var client = Connect();
            Send(client, "MULTI");
            Assert.That(ReadReply(client), Is.EqualTo("+OK\r\n"));
            Send(client, "SHUTDOWN", "NOSAVE", "NOW");
            Assert.That(ReadReply(client), Does.StartWith("-ERR"));
            Send(client, "DISCARD");
            Assert.That(ReadReply(client), Is.EqualTo("+OK\r\n"));
            Send(client, "PING");
            Assert.That(ReadReply(client), Is.EqualTo("+PONG\r\n"));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ShutdownPreservesRecoveryFilesAndStopsPipeline(bool aof)
        {
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: aof);
            server.Start();
            using (var client = Connect())
            {
                Send(client, "SET", "key", "value");
                Assert.That(ReadReply(client), Is.EqualTo("+OK\r\n"));
                Send(client, "SHUTDOWN", aof ? "NOSAVE" : "SAVE", "NOW");
                Send(client, "SET", "key", "unexpected");
                Assert.That(client.Receive(new byte[1]), Is.Zero);
            }
            Assert.That(server.ShutdownCompletion.Wait(TimeSpan.FromSeconds(10)), Is.True);
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: aof, tryRecover: true);
            server.Start();
            using var recovered = Connect();
            Send(recovered, "GET", "key");
            Assert.That(ReadReply(recovered), Is.EqualTo("$5\r\nvalue\r\n"));
        }
    }
}