// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class InlineCommandlineTests
    {
        GarnetServer server;
        Socket client;
        byte[] buffer;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: false);
            server.Start();

            client = new Socket(TestUtils.EndPoint.AddressFamily,
                                SocketType.Stream,
                                ProtocolType.Tcp);
            buffer = new byte[4096];
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            client.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        private async Task<string> Send(string message, string suffix = "\r\n")
        {
            _ = await client.SendAsync(Encoding.UTF8.GetBytes(message + suffix));
            var received = await client.ReceiveAsync(buffer, SocketFlags.None);
            return Encoding.UTF8.GetString(buffer, 0, received);
        }

        [Test]
        public async Task InlineCommandParseTest()
        {
            var clientName = "name";
            var key = "key";
            var value = "1";

            await client.ConnectAsync(TestUtils.EndPoint);

            // Test inline command without arguments
            var response = await Send("HELLO");
            ClassicAssert.AreEqual('*', response[0]);
            // Test lowercase
            response = await Send("hello 3");
            ClassicAssert.AreEqual('%', response[0]);
            // Test extranous whitespace
            response = await Send("HELLO  2\t ");
            ClassicAssert.AreEqual('*', response[0]);
            // References accept this too
            response = await Send("HElLO 3 SETNAME a SETNAME b");
            ClassicAssert.AreEqual('%', response[0]);
            // Test setting client name inline
            response = await Send($"HELLO 2 SETNAME {clientName}");
            ClassicAssert.AreEqual('*', response[0]);

            // Should fail due to missing argument. We test such failures to ensure
            // readhead is not messed up and commands can be placed afterwards.
            response = await Send("CLIENT");
            ClassicAssert.AreEqual('-', response[0]);

            // Test client name was actually set
            response = await Send("CLIENT GETNAME");
            ClassicAssert.AreEqual($"${clientName.Length}\r\n{clientName}\r\n", response);

            // Test inline ping
            response = await Send("PING");
            ClassicAssert.AreEqual("+PONG\r\n", response);

            // Test accepting both CRLF and LF as terminating characters.
            response = await Send("PING\nPING");
            ClassicAssert.AreEqual("+PONG\r\n+PONG\r\n", response);
            // CR is a valid separator
            response = await Send("PING\rPING");
            ClassicAssert.AreEqual("$4\r\nPING\r\n", response);
            // As is TAB
            response = await Send("PING\tPING");
            ClassicAssert.AreEqual("$4\r\nPING\r\n", response);

            // Test command failure
            response = await Send("PIN");
            ClassicAssert.AreEqual('-', response[0]);

            // Test ordinary commands
            response = await Send($"SET {key} {value}");
            ClassicAssert.AreEqual("+OK\r\n", response);
            response = await Send($"GET {key}");
            ClassicAssert.AreEqual($"${value.Length}\r\n{value}\r\n", response);
            response = await Send($"EXISTS {key}");
            ClassicAssert.AreEqual(":1\r\n", response);
            response = await Send($"DEL {key}");
            ClassicAssert.AreEqual(":1\r\n", response);

            // Test command failure in normal RESP doesn't interfere
            response = await Send("*1\r\n$3\r\nPIN");
            ClassicAssert.AreEqual('-', response[0]);

            // Test quit
            response = await Send("QUIT");
            ClassicAssert.AreEqual("+OK\r\n", response);
        }

        [Test]
        public async Task InlineCommandEscapeTest()
        {
            var key = "key";

            await client.ConnectAsync(TestUtils.EndPoint);

            var response = await Send("PING \\t");
            ClassicAssert.AreEqual("$2\r\n\\t\r\n", response);
            // With ' quoting most escapes aren't recognized
            response = await Send("PING '\\t'");
            ClassicAssert.AreEqual("$2\r\n\\t\r\n", response);
            // Except this one form of escaping
            response = await Send("PING '\'\\t\''");
            ClassicAssert.AreEqual("$4\r\n'\\t'\r\n", response);

            // Test escape
            response = await Send("PING \"\\t\"");
            ClassicAssert.AreEqual("$1\r\n\t\r\n", response);

            // This should lead to quoting failure
            response = await Send(@"PING ""\\\""");
            ClassicAssert.AreEqual('-', response[0]);
            // This should work
            response = await Send(@"PING ""\\\\""");
            ClassicAssert.AreEqual("$2\r\n\\\\\r\n", response);

            // Incomplete hex escape 1
            response = await Send("PING \"\\x\"");
            ClassicAssert.AreEqual("$1\r\nx\r\n", response);
            // Incomplete hex escape 2
            response = await Send("PING \"\\x0\"");
            ClassicAssert.AreEqual("$2\r\nx0\r\n", response);
            // Invalid hex escape
            response = await Send("PING \"\\xGG\"");
            ClassicAssert.AreEqual("$3\r\nxGG\r\n", response);
            // Complete hex escape
            response = await Send("PING \"\\x0A\"");
            ClassicAssert.AreEqual("$1\r\n\n\r\n", response);

            // Test escapes in command position
            response = await Send(@"""\x50\x49\x4E\x47""");
            ClassicAssert.AreEqual("+PONG\r\n", response);
            response = await Send(@"""P\i\x6Eg""");
            ClassicAssert.AreEqual("+PONG\r\n", response);

            // Test value being passed
            response = await Send($"SET {key} \"a\\x0Ab\"");
            ClassicAssert.AreEqual("+OK\r\n", response);
            response = await Send($"GET {key}");
            ClassicAssert.AreEqual("$3\r\na\nb\r\n", response);
        }

        [Test]
        public async Task InlineCommandQuoteTest()
        {
            await client.ConnectAsync(TestUtils.EndPoint);

            // Test quoted argument
            var response = await Send("ping \"hello world\"");
            ClassicAssert.AreEqual("$11\r\nhello world\r\n", response);

            // Test quoting failure
            // We need to test failures too to be sure readHead is reset right,
            // and that there are no leftovers that would interfere with future commands.
            response = await Send("PING 'unfinished quote");
            ClassicAssert.AreEqual('-', response[0]);

            // Test empty and short strings
            response = await Send("ECHO ''");
            ClassicAssert.AreEqual("$0\r\n\r\n", response);

            response = await Send("ECHO 'a'");
            ClassicAssert.AreEqual("$1\r\na\r\n", response);

            // We can even accept commands formed like this
            response = await Send("\"PING\"\tword ");
            ClassicAssert.AreEqual("$4\r\nword\r\n", response);
            response = await Send("PINg \"hello 'world'!\"");
            ClassicAssert.AreEqual("$14\r\nhello 'world'!\r\n", response);
            response = await Send("P'ING' ab");
            ClassicAssert.AreEqual("$2\r\nab\r\n", response);

            // Extension
            response = await Send("PING '\"'\"''");
            ClassicAssert.AreEqual("$4\r\n\"'\"'\r\n", response);
        }
    }
}