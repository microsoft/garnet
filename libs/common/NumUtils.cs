// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Garnet.common
{
    /// <summary>
    /// Utilities for numeric parsing and formatting
    /// </summary>
    public static unsafe class NumUtils
    {
        public const int MaximumFormatInt64Length = 20;   // 19 + sign (i.e. -9223372036854775808)
        public const int MaximumFormatDoubleLength = 310;   // (i.e. -1.7976931348623157E+308)

        /// <summary>
        /// Fixed-point formats, indexed by the number of fractional digits that <see cref="CountCharsInDouble"/> counts
        /// </summary>
        static readonly string[] FixedPointFormats = ["F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10", "F11", "F12", "F13", "F14", "F15"];

        /// <summary>
        /// Writes 64-bit signed integer as ASCII.
        /// </summary>
        /// <param name="value">The value to write</param>
        /// <param name="destination">Span Byte </param>
        /// <returns>The length of written text in bytes.</returns>
        public static int WriteInt64(long value, Span<byte> destination)
        {
            var valueLen = CountDigits(value);
            var signLen = (byte)(value < 0 ? 1 : 0);
            var totalLen = signLen + valueLen;
            if (totalLen > destination.Length)
                return 0;
            fixed (byte* ptr = destination)
            {
                byte* curr = ptr;
                WriteInt64(value, valueLen, ref curr);
            }

            return totalLen;
        }

        /// <summary>
        /// Writes 64-bit signed integer as ASCII.
        /// </summary>
        /// <param name="value">The value to write</param>
        /// <param name="length">Number of digits in <paramref name="value"/>; does *not* include space for the negative sign if <paramref name="value"/> is negative.
        ///     The space pointed to by <paramref name="result"/> must include sufficient space including sign if negative.</param>
        /// <param name="result">Byte pointer, will be updated to point after the written number</param>
        public static void WriteInt64(long value, int length, ref byte* result)
        {
            var isNegative = value < 0;
            if (value == long.MinValue)
            {
                *(long*)(result) = 3618417120593983789L;
                *(long*)(result + 8) = 3978706198986109744L;
                *(int*)(result + 8 + 8) = 942684213;
                result += 20;
                return;
            }

            if (value == 0)
            {
                *result++ = (byte)'0';
                return;
            }

            if (isNegative)
            {
                *result++ = 0x2d;
                value = -value;
            }

            result += length;
            do
            {
                *--result = (byte)((byte)'0' + (value % 10));
                value /= 10;
            } while (value > 0);
            result += length;
        }

        /// <summary>
        /// Writes <see langword="double"/> as ASCII.
        /// </summary>
        /// <param name="value">The value to write</param>
        /// <param name="destination">Buffer to write the ASCII formatted value to</param>
        /// <returns>The length of written text in bytes.</returns>
        public static int WriteDouble(double value, Span<byte> destination)
        {
            var totalLen = CountCharsInDouble(value, out var integerDigits, out var signSize, out var fractionalDigits);
            if (totalLen > destination.Length)
                return 0;
            fixed (byte* ptr = destination)
            {
                byte* curr = ptr;
                WriteDouble(value, integerDigits, fractionalDigits, ref curr);
            }

            return totalLen;
        }

        /// <summary>
        /// Writes <see langword="double"/> as ASCII.
        /// </summary>
        /// <param name="value">The value to write</param>
        /// <param name="length">Size of the destination at <paramref name="result"/>; includes space for the negative sign if present</param>
        /// <param name="result">Byte pointer, will be updated to point after the written number</param>
        public static void WriteDouble(double value, int length, ref byte* result)
        {
            Debug.Assert(!double.IsNaN(value) && !double.IsInfinity(value), "Cannot convert NaN or Infinity to bytes.");
            var totalLen = CountCharsInDouble(value, out var integerDigits, out var signSize, out var fractionalDigits);
            if (totalLen <= length)
                WriteDouble(value, integerDigits, fractionalDigits, ref result);
        }

        /// <summary>
        /// Writes <see langword="double"/> as ASCII.
        /// </summary>
        /// <param name="value">The value to write</param>
        /// <param name="integerDigits">Number of digits in the integer part of the double value, as counted by <see cref="CountCharsInDouble"/></param>
        /// <param name="fractionalDigits">Number of digits in the fractional part of the double value, as counted by <see cref="CountCharsInDouble"/></param>
        /// <param name="result">Byte pointer, will be updated to point after the written number</param>
        public static void WriteDouble(double value, int integerDigits, int fractionalDigits, ref byte* result)
        {
            Debug.Assert(!double.IsNaN(value) && !double.IsInfinity(value), "Cannot convert NaN or Infinity to bytes.");

            if (value == 0)
            {
                *result++ = (byte)'0';
                return;
            }

            // Fixed-point formatting writes exact digits at any magnitude, in the length CountCharsInDouble counted
            var length = (value < 0 ? 1 : 0) + integerDigits + (fractionalDigits > 0 ? 1 + fractionalDigits : 0);
            var success = value.TryFormat(new Span<byte>(result, length), out var written, FixedPointFormats[fractionalDigits], CultureInfo.InvariantCulture);
            Debug.Assert(success && written == length, "The value is written in the number of characters that CountCharsInDouble counted");
            result += written;
        }

        /// <summary>
        /// Parses 64-bit signed integer from ASCII.
        /// </summary>
        /// <param name="source">Source buffer</param>
        /// <returns>The double value parsed from the <paramref name="source"/> buffer.</returns>
        public static long ReadInt64(ReadOnlySpan<byte> source)
        {
            fixed (byte* ptr = source)
                return ReadInt64(source.Length, ptr);
        }

        /// <summary>
        /// Parses 64-bit signed integer from ASCII.
        /// </summary>
        /// <param name="length">Length of number</param>
        /// <param name="source">Source bytes</param>
        /// <returns>The double value parsed from the buffer which <paramref name="source"/> points to.</returns>
        public static long ReadInt64(int length, byte* source)
        {
            var isNegative = (*source == '-');
            var beg = isNegative ? source + 1 : source;
            var end = source + length;
            long result = 0;
            while (beg < end)
                result = result * 10 + (*beg++ - '0');
            return isNegative ? -result : result;
        }

        /// <summary>
        /// Parses 64-bit signed integer from ASCII.
        /// </summary>
        /// <param name="source">Pointer to source buffer</param>
        /// <param name="result">Long value extracted from sequence</param>
        /// <returns>True if buffer <paramref name="source"/> consisted only of a canonical signed 64-bit integer
        /// (no '+' sign, leading zeros or -0), otherwise false</returns>
        public static bool TryReadInt64(ReadOnlySpan<byte> source, out long result)
        {
            fixed (byte* ptr = source)
                return TryReadInt64(source.Length, ptr, out result);
        }

        /// <summary>
        /// Parses 64-bit signed integer from ASCII.
        /// </summary>
        /// <param name="length">The expected length of the integer in the buffer which <paramref name="source"/> points to</param>
        /// <param name="source">Pointer to the source buffer</param>
        /// <param name="result">64-bit signed integer parsed from the buffer which <paramref name="source"/> points to</param>
        /// <returns>True if the buffer which <paramref name="source"/> points to consisted only of a canonical signed 64-bit integer
        /// (no '+' sign, leading zeros or -0), otherwise false</returns>
        public static bool TryReadInt64(int length, byte* source, out long result)
        {
            result = 0;
            if (length == 0)
                return false;

            var isNegative = *source == '-';
            var beg = isNegative ? source + 1 : source;
            var len = isNegative ? length - 1 : length;

            // Accept only the canonical form: no '+' sign, no leading zeros, and no -0
            if (len == 0 || *beg == '+' || (*beg == '0' && (len > 1 || isNegative)))
                return false;

            // Parse the sign as well so long.MinValue does not overflow as a positive magnitude.
            return TryParse(new ReadOnlySpan<byte>(source, length), out result);
        }

        /// <summary>
        /// Parses <see langword="double"/> from ASCII.
        /// </summary>
        /// <param name="source">Source buffer</param>
        /// <param name="result"><see langword="double"/> parsed from the <paramref name="source"/> buffer</param>
        /// <returns>True if the <paramref name="source"/> buffer consisted only of a valid <see langword="double"/>, otherwise false</returns>
        public static bool TryReadDouble(ReadOnlySpan<byte> source, out double result)
        {
            fixed (byte* ptr = source)
                return TryReadDouble(source.Length, ptr, out result);
        }

        /// <summary>
        /// Writes <see langword="double"/> as ASCII.
        /// </summary>
        /// <param name="length">The expected length of the number in the buffer <paramref name="source"/> points to</param>
        /// <param name="source">Pointer to the source buffer</param>
        /// <param name="result">Double value extracted from sequence</param>
        /// <returns>True if the buffer which <paramref name="source"/> points to consisted only of a valid <see langword="double"/>, otherwise false</returns>
        public static bool TryReadDouble(int length, byte* source, out double result)
        {
            var isNegative = *source == '-';
            var beg = isNegative ? source + 1 : source;
            var len = isNegative ? length - 1 : length;
            result = 0;

            // Do not allow leading zeros
            if (len > 1 && *beg == '0' && *(beg + 1) != '.')
                return false;

            // Parse number and check consumed bytes to avoid alphanumeric strings
            if (!TryParse(new ReadOnlySpan<byte>(beg, len), out result))
                return false;

            // Negate if parsed value has a leading negative sign
            result = isNegative ? -result : result;
            return true;
        }

        /// <summary>
        /// Convert integer into sequence of ASCII bytes
        /// </summary>
        /// <param name="value">The value to write</param>
        /// <param name="length">Number of digits in the integer value</param>
        /// <param name="result">Byte pointer, will updated to point after the written number</param>
        public static unsafe void WriteInt32(int value, int length, ref byte* result)
        {
            var isNegative = value < 0;
            if (value == 0)
            {
                *result++ = (byte)'0';
                return;
            }

            long v = value;
            if (isNegative)
            {
                *result++ = (byte)'-';

                // Negate in 64 bits; the magnitude of int.MinValue does not fit in an int
                v = -v;
            }

            result += length;
            do
            {
                *--result = (byte)((byte)'0' + (v % 10));
                v /= 10;
            } while (v > 0);
            result += length;
        }

        /// <summary>
        /// Counts the number of digits in a given integer.
        /// </summary>
        /// <param name="value">Integer value</param>
        /// <remarks>Doesn't count the sign as a digit.</remarks>
        /// <returns>The number of digits in <paramref name="value"/>.</returns>
        public static int CountDigits(int value)
        {
            // The magnitude of int.MinValue does not fit in an int
            if (value == int.MinValue) return 10;
            value = value < 0 ? ((~value) + 1) : value;

            if (value < 10) return 1;
            if (value < 100) return 2;
            if (value < 1000) return 3;
            if (value < 100000000L)
            {
                if (value < 1000000)
                {
                    if (value < 10000) return 4;
                    return 5 + (value >= 100000 ? 1 : 0);
                }
                return 7 + (value >= 10000000L ? 1 : 0);
            }
            return 9 + (value >= 1000000000L ? 1 : 0);
        }


        /// <inheritdoc cref="CountDigits(int)"/>
        public static int CountDigits(long value)
        {
            if (value == long.MinValue) return 19;
            value = value < 0 ? -value : value;

            if (value < 10000000000L)//1 - 10000000000L
            {
                if (value < 100000) //1 - 100000
                {
                    if (value < 100) //1 - 100
                    {
                        if (value < 10) return 1; else return 2;
                    }
                    else//100 - 100000
                    {
                        if (value < 10000)//100 - 10000
                        {
                            if (value < 1000) return 3; else return 4;
                        }
                        else//10000 - 100000
                        {
                            return 5;
                        }
                    }
                }
                else // 100 000 - 10 000 000 000L
                {
                    if (value < 10000000) // 100 000 - 10 000 000
                    {
                        if (value < 1000000) return 6; else return 7;
                    }
                    else // 10 000 000 - 10 000 000 000L
                    {
                        if (value < 1000000000)
                        {
                            if (value < 100000000) return 8; else return 9;
                        }
                        else // 1 000 000 000 - 10 000 000 000L
                        {
                            return 10;
                        }
                    }
                }
            }
            else // 10 000 000 000L - 1 000 000 000 000 000 000L
            {
                if (value < 100000000000000L) //10 000 000 000L - 100 000 000 000 000L
                {
                    if (value < 1000000000000L) // 10 000 000 000L - 1 000 000 000 000L
                    {
                        if (value < 100000000000L) return 11; else return 12;
                    }
                    else // 1 000 000 000 000L - 100 000 000 000 000L
                    {
                        if (value < 10000000000000L) // 1 000 000 000 000L - 10 000 000 000 000L
                        {
                            return 13;
                        }
                        else
                        {
                            return 14;
                        }
                    }
                }
                else//100 000 000 000 000L - 1 000 000 000 000 000 000L
                {
                    if (value < 10000000000000000L)//100 000 000 000 000L - 10 000 000 000 000 000L                    
                    {
                        if (value < 1000000000000000L) return 15; else return 16;
                    }
                    else
                    {
                        if (value < 1000000000000000000L)
                        {
                            if (value < 100000000000000000L) return 17; else return 18;
                        }
                        else
                        {
                            return 19;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Counts the number of digits in a given integer.
        /// </summary>
        /// <param name="value">Integer value</param>
        /// <param name="isNegative">Whether the <paramref name="value"/> is negative or not</param>
        /// <remarks>Doesn't count the sign as a digit.</remarks>
        /// <returns>The number of digits in <paramref name="value"/>.</returns>
        public static int CountDigits(long value, out bool isNegative)
        {
            if (value == long.MinValue)
            {
                isNegative = true;
                return 19;
            }

            isNegative = false;
            if (value < 0)
            {
                isNegative = true;
                value = -value;
            }

            if (value < 10) return 1;
            if (value < 100) return 2;
            if (value < 1000) return 3;
            if (value < 1_000_000_00L) // 9 digit
            {
                if (value < 1000000)// 7 dgiit
                {
                    if (value < 10000) return 4;
                    return 5 + (value >= 100000 ? 1 : 0); // 5 or 6 digit
                }
                return 7 + (value >= 1_000_000_0L ? 1 : 0);
            }
            if (value < 1000000000L) return 9;
            if (value < 10000000000L) return 10;
            if (value < 100000000000L) return 11;
            if (value < 1000000000000L) return 12;
            if (value < 10000000000000L) return 13;
            if (value < 100000000000000L) return 14;
            if (value < 1000000000000000L) return 15;
            if (value < 10000000000000000L) return 16;
            if (value < 100000000000000000L) return 17;
            if (value < 1000000000000000000L) return 18;
            return 19;
        }

        /// <summary>
        /// Returns the number of characters WriteDouble writes for a double: sign, integer digits, and a decimal point
        /// with up to 15 fractional digits
        /// </summary>
        /// <param name="value">Double value</param>
        /// <param name="integerDigits">Number of digits in the integer part of the double value</param>
        /// <param name="signSize">1 if the double value is negative, otherwise 0</param>
        /// <param name="fractionalDigits">Number of digits in the fractional part of the double value</param>
        /// <returns>Number of characters needed to write the double value</returns>
        [SkipLocalsInit]
        public static int CountCharsInDouble(double value, out int integerDigits, out byte signSize, out int fractionalDigits)
        {
            if (value == 0)
            {
                integerDigits = 1;
                signSize = 0;
                fractionalDigits = 0;
                return 1;
            }

            Debug.Assert(!double.IsNaN(value) && !double.IsInfinity(value));

            signSize = (byte)(value < 0 ? 1 : 0); // Add sign if the number is negative
            value = Math.Abs(value);

            fractionalDigits = 0; // Max of 15 significant digits
            while (fractionalDigits <= 14 && Math.Abs(value - Math.Round(value, fractionalDigits)) > 2 * Double.Epsilon) // 2 * Double.Epsilon is used to handle floating point errors
            {
                fractionalDigits++;
            }

            // Count integer digits as formatted: Log10 is inexact just below a power of ten, and rounding can carry into the integer part
            Span<byte> formatted = stackalloc byte[MaximumFormatDoubleLength];
            var success = value.TryFormat(formatted, out var length, FixedPointFormats[fractionalDigits], CultureInfo.InvariantCulture);
            Debug.Assert(success, "A double without its sign is at most MaximumFormatDoubleLength characters");

            var dotSize = fractionalDigits != 0 ? 1 : 0; // Add decimal point if there are significant digits
            integerDigits = length - dotSize - fractionalDigits;

            return signSize + length;
        }

        /// <inheritdoc cref="Utf8Parser.TryParse(ReadOnlySpan{byte}, out int, out int, char)"/>
        public static bool TryParse(ReadOnlySpan<byte> source, out int value)
        {
            return Utf8Parser.TryParse(source, out value, out var bytesConsumed, default) &&
                bytesConsumed == source.Length;
        }
        /// <inheritdoc cref="Utf8Parser.TryParse(ReadOnlySpan{byte}, out long, out int, char)"/>
        public static bool TryParse(ReadOnlySpan<byte> source, out long value)
        {
            return Utf8Parser.TryParse(source, out value, out var bytesConsumed, default) &&
                bytesConsumed == source.Length;
        }
        /// <inheritdoc cref="Utf8Parser.TryParse(ReadOnlySpan{byte}, out float, out int, char)"/>
        public static bool TryParse(ReadOnlySpan<byte> source, out float value)
        {
            return Utf8Parser.TryParse(source, out value, out var bytesConsumed, default) &&
                bytesConsumed == source.Length;
        }
        /// <inheritdoc cref="Utf8Parser.TryParse(ReadOnlySpan{byte}, out double, out int, char)"/>
        public static bool TryParse(ReadOnlySpan<byte> source, out double value)
        {
            return Utf8Parser.TryParse(source, out value, out var bytesConsumed, default) &&
                bytesConsumed == source.Length;
        }

        /// <inheritdoc cref="Utf8Parser.TryParse(ReadOnlySpan{byte}, out double, out int, char)" path="//*[not(self::summary)]"/>
        /// <summary>
        /// Parses a Double at the start of a Utf8 string, including RESP's infinity format
        /// </summary>
        /// <remarks>
        /// Formats supported:
        ///     G/g  (default)
        ///     F/f             12.45       Fixed point
        ///     E/e             1.245000e1  Exponential
        ///     [+-]inf         plus/minus infinity
        /// NaN is rejected.
        /// </remarks>
        public static bool TryParseWithInfinity(ReadOnlySpan<byte> source, out double value)
        {
            if (Utf8Parser.TryParse(source, out value, out var bytesConsumed, default) &&
                bytesConsumed == source.Length)
            {
                // Utf8Parser accepts NaN, which is not a valid number
                if (!double.IsNaN(value))
                    return true;

                value = default;
                return false;
            }

            return RespReadUtils.TryReadInfinity(source, out value);
        }

        /// <summary>
        /// Get the leftmost bit set offset and clear the associated bit
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static int GetNextOffset(this ref ulong value)
        {
            var offset = BitOperations.TrailingZeroCount(value);
            value &= ~((1UL) << offset);
            return offset;
        }
    }
}