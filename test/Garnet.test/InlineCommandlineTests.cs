// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class InlineCommandlineTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: false);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public async Task InlineCommandParseTest()
        {
            var clientName = "name";
            var key = "key";
            var value = "1";

            using var c = TestUtils.GetGarnetClientSession(rawResult: true, rawSend: true);
            c.Connect();

            // Test inline command without arguments
            var response = await c.ExecuteAsync("HELLO");
            ClassicAssert.AreEqual('*', response[0]);
            // Test lowercase
            response = await c.ExecuteAsync("hello 3");
            ClassicAssert.AreEqual('%', response[0]);
            // Test extranous whitespace
            response = await c.ExecuteAsync("HELLO  2\t ");
            ClassicAssert.AreEqual('*', response[0]);
            // References accept this too
            response = await c.ExecuteAsync("HElLO 3 SETNAME a SETNAME b");
            ClassicAssert.AreEqual('%', response[0]);
            // Test setting client name inline
            response = await c.ExecuteAsync($"HELLO 2 SETNAME {clientName}");
            ClassicAssert.AreEqual('*', response[0]);

            // Should fail due to missing argument. We test such failures to ensure
            // readhead is not messed up and commands can be placed afterwards.
            response = await c.ExecuteAsync("CLIENT");
            ClassicAssert.AreEqual('-', response[0]);
            c.RawResult = false;
            // Test client name was actually set
            response = await c.ExecuteAsync("CLIENT GETNAME");
            ClassicAssert.AreEqual(clientName, response);
            c.RawResult = true;

            // Test inline ping
            response = await c.ExecuteAsync("PING");
            ClassicAssert.AreEqual("+PONG\r\n", response);

            // Test accepting both CRLF and LF as terminating characters.
            response = await c.ExecuteAsync("PING\nPING");
            ClassicAssert.AreEqual("+PONG\r\n+PONG\r\n", response);
            // CR is a valid separator
            response = await c.ExecuteAsync("PING\rPING");
            ClassicAssert.AreEqual("$4\r\nPING\r\n", response);
            // As is TAB
            response = await c.ExecuteAsync("PING\tPING");
            ClassicAssert.AreEqual("$4\r\nPING\r\n", response);

            // Test command failure
            response = await c.ExecuteAsync("PIN");
            ClassicAssert.AreEqual('-', response[0]);

            // Test ordinary commands
            response = await c.ExecuteAsync($"SET {key} {value}");
            ClassicAssert.AreEqual("+OK\r\n", response);
            response = await c.ExecuteAsync($"GET {key}");
            ClassicAssert.AreEqual($"${value.Length}\r\n{value}\r\n", response);
            response = await c.ExecuteAsync($"EXISTS {key}");
            ClassicAssert.AreEqual(":1\r\n", response);
            response = await c.ExecuteAsync($"DEL {key}");
            ClassicAssert.AreEqual(":1\r\n", response);

            // Test command failure in normal RESP doesn't interfere
            response = await c.ExecuteAsync("*1\r\n$3\r\nPIN");
            ClassicAssert.AreEqual('-', response[0]);

            // Test quit
            response = await c.ExecuteAsync("QUIT");
            ClassicAssert.AreEqual("+OK\r\n", response);
        }

        [Test]
        public async Task InlineCommandEscapeTest()
        {
            var key = "key";

            using var c = TestUtils.GetGarnetClientSession(rawResult: true, rawSend: true);
            c.Connect();

            var response = await c.ExecuteAsync("PING \\t");
            ClassicAssert.AreEqual("$2\r\n\\t\r\n", response);
            // With ' quoting most escapes aren't recognized
            response = await c.ExecuteAsync("PING '\\t'");
            ClassicAssert.AreEqual("$2\r\n\\t\r\n", response);
            // Except this one form of escaping
            response = await c.ExecuteAsync("PING '\'\\t\''");
            ClassicAssert.AreEqual("$4\r\n'\\t'\r\n", response);

            // Test escape
            response = await c.ExecuteAsync("PING \"\\t\"");
            ClassicAssert.AreEqual("$1\r\n\t\r\n", response);

            // This should lead to quoting failure
            response = await c.ExecuteAsync(@"PING ""\\\""");
            ClassicAssert.AreEqual('-', response[0]);
            // This should work
            response = await c.ExecuteAsync(@"PING ""\\\\""");
            ClassicAssert.AreEqual("$2\r\n\\\\\r\n", response);

            // Incomplete hex escape 1
            response = await c.ExecuteAsync("PING \"\\x\"");
            ClassicAssert.AreEqual("$1\r\nx\r\n", response);
            // Incomplete hex escape 2
            response = await c.ExecuteAsync("PING \"\\x0\"");
            ClassicAssert.AreEqual("$2\r\nx0\r\n", response);
            // Invalid hex escape
            response = await c.ExecuteAsync("PING \"\\xGG\"");
            ClassicAssert.AreEqual("$3\r\nxGG\r\n", response);
            // Complete hex escape
            response = await c.ExecuteAsync("PING \"\\x0A\"");
            ClassicAssert.AreEqual("$1\r\n\n\r\n", response);

            // Test escapes in command position
            response = await c.ExecuteAsync(@"""\x50\x49\x4E\x47""");
            ClassicAssert.AreEqual("+PONG\r\n", response);
            response = await c.ExecuteAsync(@"""P\i\x6Eg""");
            ClassicAssert.AreEqual("+PONG\r\n", response);

            // Test value being passed
            response = await c.ExecuteAsync($"SET {key} \"a\\x0Ab\"");
            ClassicAssert.AreEqual("+OK\r\n", response);
            response = await c.ExecuteAsync($"GET {key}");
            ClassicAssert.AreEqual("$3\r\na\nb\r\n", response);
        }

        [Test]
        public async Task InlineCommandQuoteTest()
        {
            using var c = TestUtils.GetGarnetClientSession(rawResult: true, rawSend: true);
            c.Connect();

            // Test quoted argument
            var response = await c.ExecuteAsync("ping \"hello world\"");
            ClassicAssert.AreEqual("$11\r\nhello world\r\n", response);

            // Test quoting failure
            // We need to test failures too to be sure readHead is reset right,
            // and that there are no leftovers that would interfere with future commands.
            response = await c.ExecuteAsync("PING 'unfinished quote");
            ClassicAssert.AreEqual('-', response[0]);

            // Test empty and short strings
            response = await c.ExecuteAsync("ECHO ''");
            ClassicAssert.AreEqual("$0\r\n\r\n", response);

            response = await c.ExecuteAsync("ECHO 'a'");
            ClassicAssert.AreEqual("$1\r\na\r\n", response);

            // We can even accept commands formed like this
            response = await c.ExecuteAsync("\"PING\"\tword ");
            ClassicAssert.AreEqual("$4\r\nword\r\n", response);
            response = await c.ExecuteAsync("PINg \"hello 'world'!\"");
            ClassicAssert.AreEqual("$14\r\nhello 'world'!\r\n", response);
            response = await c.ExecuteAsync("P'ING' ab");
            ClassicAssert.AreEqual("$2\r\nab\r\n", response);

            // Extension
            response = await c.ExecuteAsync("PING '\"'\"''");
            ClassicAssert.AreEqual("$4\r\n\"'\"'\r\n", response);
        }
    }
}