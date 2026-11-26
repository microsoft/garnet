// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Garnet.common.Parsing;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Tests reading, from the lengths a RESP message declares, how many bytes a receive buffer needs to hold it.
    /// </summary>
    [TestFixture]
    public unsafe class RespReceiveSizeTests : TestBase
    {
        static long RequiredBytes(string resp)
        {
            var bytes = Encoding.ASCII.GetBytes(resp);
            fixed (byte* ptr = bytes)
                return RespReceiveSize.GetRequiredBytes(ptr, bytes.Length);
        }

        [Test]
        public void ABulkStringThatRunsPastTheBufferNeedsItsDeclaredLength()
        {
            const string head = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$100\r\n";
            ClassicAssert.AreEqual(head.Length + 100 + 2, RequiredBytes(head + "partial"));
        }

        [Test]
        public void ElementsBeforeTheIncompleteOneAreCounted()
        {
            // A complete command, then nested aggregates, a null bulk string and simple types ahead of the incomplete bulk string
            const string head = "*1\r\n$4\r\nPING\r\n*2\r\n*4\r\n$-1\r\n:5\r\n+OK\r\n%1\r\n$10\r\n";
            ClassicAssert.AreEqual(head.Length + 10 + 2, RequiredBytes(head + "abc"));
        }

        [Test]
        public void OnlyLengthsAnArgumentCanHaveAreReported()
        {
            var head = $"*2\r\n$3\r\nSET\r\n${RespLimits.MaxArgumentLengthBytes}\r\n";
            ClassicAssert.AreEqual(head.Length + RespLimits.MaxArgumentLengthBytes + 2L, RequiredBytes(head + "abc"));

            // The parser never accepts a longer argument, so no receive buffer needs to hold it
            ClassicAssert.AreEqual(0, RequiredBytes($"*2\r\n$3\r\nSET\r\n${RespLimits.MaxArgumentLengthBytes + 1L}\r\nabc"));
        }

        [TestCase("*1\r\n$4\r\nPING\r\n", Description = "Complete message")]
        [TestCase("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$10", Description = "Length header not yet terminated")]
        [TestCase("*3\r", Description = "Aggregate header not yet terminated")]
        [TestCase("SET k v", Description = "Inline command")]
        [TestCase("*1\r\n$1234567890123456789\r\nabc", Description = "Length too long to be valid")]
        [TestCase("*1\r\n$x\r\nabc", Description = "Malformed length")]
        public void NothingIsReportedWhenTheBytesDoNotDeclareAnIncompleteLength(string resp)
            => ClassicAssert.AreEqual(0, RequiredBytes(resp));
    }
}