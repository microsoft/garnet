// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using Garnet.common.Parsing;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp
{
    /// <summary>
    /// Tests for RespReadUtils parsing functions.
    /// </summary>
    unsafe class RespReadUtilsTests
    {
        /// <summary>
        /// Tests that ReadLengthHeader successfully parses valid numbers.
        /// </summary>
        /// <param name="text">Header length encoded as an ASCII string.</param>
        /// <param name="expected">Expected parsed header length as int.</param>
        [TestCase("0", 0)]
        [TestCase("-1", -1)]
        [TestCase("2147483647", 2147483647)]
        public static unsafe void ReadLengthHeaderTest(string text, int expected)
        {
            var bytes = Encoding.ASCII.GetBytes($"${text}\r\n");
            fixed (byte* ptr = bytes)
            {
                var start = ptr;
                var end = ptr + bytes.Length;
                var success = RespReadUtils.ReadSignedLengthHeader(out var length, ref start, end);

                ClassicAssert.IsTrue(success);
                ClassicAssert.AreEqual(expected, length);
                ClassicAssert.IsTrue(start == end);
            }
        }

        /// <summary>
        /// Tests that ReadLengthHeader throws exceptions for invalid inputs.
        /// </summary>
        /// <param name="text">Invalid ASCII-encoded string length header (including '$').</param>
        [TestCase("$\r\n\r\n")]        // Empty input length
        [TestCase("$-1\r\n")]          // NULL should be disallowed
        [TestCase("123\r\n")]          // Missing $
        [TestCase("$-2147483648\r\n")] // Valid Int32 value but negative (not allowed)
        [TestCase("$-2\r\n")]          // -1 should be legal, but -2 should not be
        [TestCase("$2147483648\r\n")]  // Should cause an overflow 
        [TestCase("$123ab\r\n")]       // Not a number
        [TestCase("$123ab")]           // Missing "\r\n"
        public static unsafe void ReadLengthHeaderExceptionsTest(string text)
        {
            var bytes = Encoding.ASCII.GetBytes(text);
            _ = Assert.Throws<RespParsingException>(() =>
            {
                fixed (byte* ptr = bytes)
                {
                    var start = ptr;
                    _ = RespReadUtils.ReadUnsignedLengthHeader(out var length, ref start, ptr + bytes.Length);
                }
            });
        }

        /// <summary>
        /// Tests that ReadArrayLength successfully parses valid numbers.
        /// </summary>
        /// <param name="text">Header length encoded as an ASCII string.</param>
        /// <param name="expected">Expected parsed header length as int.</param>
        [TestCase("0", 0)]
        [TestCase("2147483647", 2147483647)]
        public static unsafe void ReadArrayLengthTest(string text, int expected)
        {
            var bytes = Encoding.ASCII.GetBytes($"*{text}\r\n");
            fixed (byte* ptr = bytes)
            {
                var start = ptr;
                var end = ptr + bytes.Length;
                var success = RespReadUtils.ReadUnsignedArrayLength(out var length, ref start, end);

                ClassicAssert.IsTrue(success);
                ClassicAssert.AreEqual(expected, length);
                ClassicAssert.IsTrue(start == end);
            }
        }

        /// <summary>
        /// Tests that ReadArrayLength throws exceptions for invalid inputs.
        /// </summary>
        /// <param name="text">Invalid ASCII-encoded array length header (including '*').</param>
        [TestCase("*\r\n\r\n")]        // Empty input length
        [TestCase("123\r\n")]          // Missing *
        [TestCase("*-2147483648\r\n")] // Valid Int32 value but negative (not allowed)
        [TestCase("*-2\r\n")]          // -1 should be legal, but -2 should not be
        [TestCase("*2147483648\r\n")]  // Should cause an overflow 
        [TestCase("*123ab\r\n")]       // Not a number
        [TestCase("*123ab")]           // Missing "\r\n"
        public static unsafe void ReadArrayLengthExceptionsTest(string text)
        {
            var bytes = Encoding.ASCII.GetBytes(text);
            _ = Assert.Throws<RespParsingException>(() =>
            {
                fixed (byte* ptr = bytes)
                {
                    var start = ptr;
                    _ = RespReadUtils.ReadUnsignedArrayLength(out var length, ref start, ptr + bytes.Length);
                }
            });
        }

        /// <summary>
        /// Tests that ReadIntWithLengthHeader successfully parses valid integers.
        /// </summary>
        /// <param name="text">Int encoded as an ASCII string.</param>
        /// <param name="expected">Expected parsed value.</param>
        [TestCase("0", 0)]
        [TestCase("-2147483648", -2147483648)]
        [TestCase("2147483647", 2147483647)]
        public static unsafe void ReadIntWithLengthHeaderTest(string text, int expected)
        {
            var bytes = Encoding.ASCII.GetBytes($"${text.Length}\r\n{text}\r\n");
            fixed (byte* ptr = bytes)
            {
                var start = ptr;
                var end = ptr + bytes.Length;
                var success = RespReadUtils.ReadIntWithLengthHeader(out var length, ref start, end);

                ClassicAssert.IsTrue(success);
                ClassicAssert.AreEqual(expected, length);
                ClassicAssert.IsTrue(start == end);
            }
        }

        /// <summary>
        /// Tests that ReadIntWithLengthHeader throws exceptions for invalid inputs.
        /// </summary>
        /// <param name="text">Invalid ASCII-encoded input number.</param>
        [TestCase("2147483648")]  // Should cause overflow
        [TestCase("-2147483649")] // Should cause overflow
        [TestCase("123abc")]      // Not a number
        [TestCase("abc121cba")]   // Not a number
        public static unsafe void ReadIntWithLengthHeaderExceptionsTest(string text)
        {
            var bytes = Encoding.ASCII.GetBytes($"${text.Length}\r\n{text}\r\n");

            _ = Assert.Throws<RespParsingException>(() =>
            {
                fixed (byte* ptr = bytes)
                {
                    var start = ptr;
                    _ = RespReadUtils.ReadIntWithLengthHeader(out var length, ref start, ptr + bytes.Length);
                }
            });
        }

        /// <summary>
        /// Tests that ReadLongWithLengthHeader successfully parses valid longs.
        /// </summary>
        /// <param name="text">Long int encoded as an ASCII string.</param>
        /// <param name="expected">Expected parsed value.</param>
        [TestCase("0", 0L)]
        [TestCase("-9223372036854775808", -9223372036854775808L)]
        [TestCase("9223372036854775807", 9223372036854775807L)]
        public static unsafe void ReadLongWithLengthHeaderTest(string text, long expected)
        {
            var bytes = Encoding.ASCII.GetBytes($"${text.Length}\r\n{text}\r\n");
            fixed (byte* ptr = bytes)
            {
                var start = ptr;
                var end = ptr + bytes.Length;
                var success = RespReadUtils.ReadLongWithLengthHeader(out var length, ref start, end);

                ClassicAssert.IsTrue(success);
                ClassicAssert.AreEqual(expected, length);
                ClassicAssert.IsTrue(start == end);
            }
        }

        /// <summary>
        /// Tests that ReadLongWithLengthHeader throws exceptions for invalid inputs.
        /// </summary>
        /// <param name="text">Invalid ASCII-encoded input number.</param>
        [TestCase("9223372036854775808")]  // Should cause overflow
        [TestCase("-9223372036854775809")] // Should cause overflow
        [TestCase("10000000000000000000")] // Should cause overflow
        [TestCase("123abc")]               // Not a number
        [TestCase("abc121cba")]            // Not a number
        public static unsafe void ReadLongWithLengthHeaderExceptionsTest(string text)
        {
            var bytes = Encoding.ASCII.GetBytes($"${text.Length}\r\n{text}\r\n");

            _ = Assert.Throws<RespParsingException>(() =>
            {
                fixed (byte* ptr = bytes)
                {
                    var start = ptr;
                    _ = RespReadUtils.ReadLongWithLengthHeader(out var length, ref start, ptr + bytes.Length);
                }
            });
        }

        /// <summary>
        /// Tests that ReadULongWithLengthHeader successfully parses valid ulong integers.
        /// </summary>
        /// <param name="text">Unsigned long int encoded as an ASCII string.</param>
        /// <param name="expected">Expected parsed value.</param>
        [TestCase("0", 0UL)]
        [TestCase("18446744073709551615", 18446744073709551615UL)]
        public static unsafe void ReadULongWithLengthHeaderTest(string text, ulong expected)
        {
            var bytes = Encoding.ASCII.GetBytes($"${text.Length}\r\n{text}\r\n");
            fixed (byte* ptr = bytes)
            {
                var start = ptr;
                var end = ptr + bytes.Length;
                var success = RespReadUtils.ReadULongWithLengthHeader(out var length, ref start, end);

                ClassicAssert.IsTrue(success);
                ClassicAssert.AreEqual(expected, length);
                ClassicAssert.IsTrue(start == end);
            }
        }

        /// <summary>
        /// Tests that ReadULongWithLengthHeader throws exceptions for invalid inputs.
        /// </summary>
        /// <param name="text">Invalid ASCII-encoded input number.</param>
        [TestCase("18446744073709551616")]  // Should cause overflow
        [TestCase("-1")]                    // Negative numbers are not allowed
        [TestCase("123abc")]                // Not a number
        [TestCase("abc121cba")]             // Not a number
        public static unsafe void ReadULongWithLengthHeaderExceptionsTest(string text)
        {
            var bytes = Encoding.ASCII.GetBytes($"${text.Length}\r\n{text}\r\n");

            _ = Assert.Throws<RespParsingException>(() =>
            {
                fixed (byte* ptr = bytes)
                {
                    var start = ptr;
                    _ = RespReadUtils.ReadULongWithLengthHeader(out var length, ref start, ptr + bytes.Length);
                }
            });
        }

        /// <summary>
        /// Tests that ReadPtrWithLengthHeader successfully parses simple strings.
        /// </summary>
        /// <param name="text">Input ASCII string.</param>
        [TestCase("test")]
        [TestCase("")]
        public static unsafe void ReadPtrWithLengthHeaderTest(string text)
        {
            var bytes = Encoding.ASCII.GetBytes($"${text.Length}\r\n{text}\r\n");
            fixed (byte* ptr = bytes)
            {
                byte* result = null;
                var length = -1;
                var start = ptr;
                var end = ptr + bytes.Length;
                var success = RespReadUtils.ReadPtrWithLengthHeader(ref result, ref length, ref start, end);

                ClassicAssert.IsTrue(success);
                ClassicAssert.IsTrue(result != null);
                ClassicAssert.IsTrue(start == end);
                ClassicAssert.IsTrue(length == text.Length);
            }
        }

        /// <summary>
        /// Tests that ReadBoolWithLengthHeader successfully parses valid inputs.
        /// </summary>
        /// <param name="text">Int encoded as an ASCII string.</param>
        /// <param name="expected">Expected parsed value.</param>
        [TestCase("1", true)]
        [TestCase("0", false)]
        public static unsafe void ReadBoolWithLengthHeaderTest(string text, bool expected)
        {
            var bytes = Encoding.ASCII.GetBytes($"${text.Length}\r\n{text}\r\n");
            fixed (byte* ptr = bytes)
            {
                var start = ptr;
                var end = ptr + bytes.Length;
                var success = RespReadUtils.ReadBoolWithLengthHeader(out var result, ref start, end);

                ClassicAssert.IsTrue(success);
                ClassicAssert.AreEqual(expected, result);
                ClassicAssert.IsTrue(start == end);
            }
        }

        /// <summary>
        /// Tests that Readnil successfully parses valid inputs.
        /// </summary>
        [TestCase("", false, null)] // Too short
        [TestCase("S$-1\r\n", false, "S")] // Long enough but not nil leading
        [TestCase("$-1\n1738\r\n", false, "1")] // Long enough but not nil
        [TestCase("$-1\r\n", true, null)] // exact nil
        [TestCase("$-1\r\nxyzextra", true, null)] // leading nil but with extra bytes after
        public static unsafe void ReadBoolWithLengthHeaderTest(string testSequence, bool expected, string firstMismatch)
        {
            ReadOnlySpan<byte> testSeq = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes(testSequence));

            fixed (byte* ptr = testSeq)
            {
                byte* start = ptr;
                byte* end = ptr + testSeq.Length;
                var isNil = RespReadUtils.ReadNil(ref start, end, out byte? unexpectedToken);

                ClassicAssert.AreEqual(expected, isNil);
                ClassicAssert.AreEqual((byte?)firstMismatch?[0], unexpectedToken);
            }
        }
    }
}