// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.Text;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Tests for <see cref="Garnet.common.NumUtils"/> formatting and the RespWriteUtils integer writers built on it.
    /// </summary>
    [TestFixture]
    public unsafe class NumUtilsTests : TestBase
    {
        const int GuardBytes = 32;
        const byte GuardByte = 0xAA;

        [TestCase(int.MinValue, 10)]
        [TestCase(int.MinValue + 1, 10)]
        [TestCase(-1, 1)]
        [TestCase(0, 1)]
        [TestCase(9, 1)]
        [TestCase(10, 2)]
        [TestCase(int.MaxValue, 10)]
        public void CountDigitsInt32(int value, int expected)
            => ClassicAssert.AreEqual(expected, Garnet.common.NumUtils.CountDigits(value));

        [TestCase(int.MinValue)]
        [TestCase(int.MinValue + 1)]
        [TestCase(-1)]
        [TestCase(0)]
        [TestCase(int.MaxValue)]
        public void TryWriteInt32WritesExactlyTheInteger(int value)
        {
            var expected = $":{value.ToString(CultureInfo.InvariantCulture)}\r\n";
            AssertWritesExactly(expected, (ref byte* curr, byte* end) => RespWriteUtils.TryWriteInt32(value, ref curr, end));
        }

        [TestCase(int.MinValue)]
        [TestCase(int.MinValue + 1)]
        [TestCase(-1)]
        [TestCase(0)]
        [TestCase(int.MaxValue)]
        public void TryWriteInt32AsBulkStringWritesExactlyTheInteger(int value)
        {
            var text = value.ToString(CultureInfo.InvariantCulture);
            var expected = $"${text.Length}\r\n{text}\r\n";
            ClassicAssert.AreEqual(expected.Length, RespWriteUtils.GetIntegerAsBulkStringLength(value));
            AssertWritesExactly(expected, (ref byte* curr, byte* end) => RespWriteUtils.TryWriteInt32AsBulkString(value, ref curr, end));
        }

        delegate bool Writer(ref byte* curr, byte* end);

        /// <summary>
        /// A double is written in exactly the number of characters that CountCharsInDouble counts: just below a power of ten,
        /// where a logarithm counts one digit too many, and at magnitudes where the digits are exact.
        /// </summary>
        [TestCase(0.0, "0")]
        [TestCase(2.5, "2.5")]
        [TestCase(-0.5, "-0.5")]
        [TestCase(123.456, "123.456")]
        [TestCase(0.00001, "0.00001")]
        [TestCase(9.999999999999998, "9.999999999999998")]
        [TestCase(99.99999999999999, "99.99999999999999")]
        [TestCase(-99.99999999999999, "-99.99999999999999")]
        [TestCase(9999999999999998.0, "9999999999999998")]
        [TestCase(1e22, "10000000000000000000000")]
        [TestCase(1e23, "99999999999999991611392")]
        public void WriteDoubleWritesExactlyTheCountedCharacters(double value, string expected)
        {
            var length = Garnet.common.NumUtils.CountCharsInDouble(value, out var integerDigits, out var signSize, out var fractionalDigits);
            ClassicAssert.AreEqual(expected.Length, length);
            ClassicAssert.AreEqual(length, signSize + integerDigits + (fractionalDigits > 0 ? 1 + fractionalDigits : 0));

            AssertWritesExactly(expected, (ref byte* curr, byte* end) =>
            {
                Garnet.common.NumUtils.WriteDouble(value, (int)(end - curr), ref curr);
                return true;
            });
        }

        /// <summary>
        /// The written digits of a double parse back to the same value, up to the largest double.
        /// </summary>
        [TestCase(double.MaxValue)]
        [TestCase(-double.MaxValue)]
        [TestCase(1e308)]
        [TestCase(1e100)]
        [TestCase(1e23)]
        [TestCase(999999999999999.9)]
        [TestCase(99.99999999999999)]
        [TestCase(0.1)]
        public void WriteDoubleRoundTrips(double value)
        {
            var length = Garnet.common.NumUtils.CountCharsInDouble(value, out _, out _, out _);
            ClassicAssert.LessOrEqual(length, Garnet.common.NumUtils.MaximumFormatDoubleLength);

            var buffer = new byte[length + GuardBytes];
            buffer.AsSpan(length).Fill(GuardByte);
            ClassicAssert.AreEqual(length, Garnet.common.NumUtils.WriteDouble(value, buffer.AsSpan(0, length)));
            ClassicAssert.AreEqual(-1, buffer.AsSpan(length).IndexOfAnyExcept(GuardByte), "Wrote past the end of the reserved space");

            ClassicAssert.IsTrue(Garnet.common.NumUtils.TryReadDouble(buffer.AsSpan(0, length), out var parsed), Encoding.ASCII.GetString(buffer, 0, length));
            ClassicAssert.AreEqual(value, parsed);
        }

        /// <summary>
        /// Writes into a buffer sized exactly for <paramref name="expected"/>, followed by guard bytes that must stay untouched.
        /// </summary>
        static void AssertWritesExactly(string expected, Writer write)
        {
            var buffer = new byte[expected.Length + GuardBytes];
            buffer.AsSpan(expected.Length).Fill(GuardByte);

            fixed (byte* ptr = buffer)
            {
                var curr = ptr;
                ClassicAssert.IsTrue(write(ref curr, ptr + expected.Length));
                ClassicAssert.AreEqual(expected.Length, (int)(curr - ptr), "Bytes written");
            }

            ClassicAssert.AreEqual(expected, Encoding.ASCII.GetString(buffer, 0, expected.Length));
            ClassicAssert.AreEqual(-1, buffer.AsSpan(expected.Length).IndexOfAnyExcept(GuardByte), "Wrote past the end of the reserved space");
        }
    }
}