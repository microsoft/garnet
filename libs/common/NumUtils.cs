// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Diagnostics;

namespace Garnet.common
{
    /// <summary>
    /// Utilities for numeric parsing
    /// </summary>
    public static unsafe class NumUtils
    {
        public const int MaximumFormatInt64Length = 20;   // 19 + sign (i.e. -9223372036854775808)

        /// <summary>
        /// Convert long number into sequence of ASCII bytes
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <param name="dest">Span Byte </param>
        /// <returns>Length of number in result</returns>
        public static int LongToSpanByte(long value, Span<byte> dest)
        {
            int valueLen = NumDigitsInLong(value);
            byte sign = (byte)(value < 0 ? 1 : 0);
            int totalLen = sign + valueLen;
            if (totalLen > dest.Length)
                return 0;
            fixed (byte* ptr = dest)
            {
                byte* curr = ptr;
                LongToBytes(value, valueLen, ref curr);
            }

            return totalLen;
        }

        /// <summary>
        /// Convert long into sequence of ASCII bytes
        /// </summary>
        /// <param name="value"></param>
        /// <param name="length"></param>
        /// <param name="result"></param>
        public static unsafe void LongToBytes(long value, int length, ref byte* result)
        {
            byte sign = (byte)(value < 0 ? 1 : 0);
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

            if (sign == 0x1)
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
        /// Convert sequence of ASCII bytes into long number
        /// </summary>
        /// <param name="source">Source bytes</param>
        /// <returns>Result</returns>
        public static long BytesToLong(ReadOnlySpan<byte> source)
        {
            fixed (byte* ptr = source)
                return BytesToLong(source.Length, ptr);
        }

        /// <summary>
        /// Convert sequence of ASCII bytes into long number
        /// </summary>
        /// <param name="length">Length of number</param>
        /// <param name="source">Source bytes</param>
        /// <returns>Result</returns>
        public static long BytesToLong(int length, byte* source)
        {
            bool fNeg = (*source == '-');
            var beg = fNeg ? source + 1 : source;
            var end = source + length;
            long result = 0;
            while (beg < end)
                result = result * 10 + (*beg++ - '0');
            return fNeg ? -(result) : result;
        }

        /// <summary>
        /// Convert sequence of ASCII bytes into long number
        /// </summary>
        /// <param name="length">Length of number</param>
        /// <param name="source">Source bytes</param>
        /// <param name="result">Long value extracted from sequence</param>
        /// <returns>True if sequence contains only numeric digits, otherwise false</returns>
        public static bool TryBytesToLong(int length, byte* source, out long result)
        {
            var fNeg = *source == '-';
            var beg = fNeg ? source + 1 : source;
            var len = fNeg ? length - 1 : length;
            result = 0;

            // Do not allow leading zeros
            if (len > 1 && *beg == '0')
                return false;

            // Parse number and check consumed bytes to avoid alphanumeric strings
            if (!TryParse(new ReadOnlySpan<byte>(beg, len), out result))
                return false;

            // Negate if parsed value has a leading negative sign
            result = fNeg ? -result : result;
            return true;
        }

        /// <summary>
        /// Convert sequence of ASCII bytes into ulong number
        /// </summary>
        /// <param name="length">Length of number</param>
        /// <param name="source">Source bytes</param>
        /// <returns>Result</returns>
        public static ulong BytesToULong(int length, byte* source)
        {
            Debug.Assert(*source != '-');
            var beg = source;
            var end = source + length;
            ulong result = 0;
            while (beg < end)
                result = result * 10 + (ulong)(*beg++ - '0');
            return result;
        }

        /// <summary>
        /// Convert sequence of ASCII bytes into int number
        /// </summary>
        /// <param name="length">Length of number</param>
        /// <param name="source">Source bytes</param>
        /// <returns></returns>
        public static int BytesToInt(int length, byte* source)
        {
            bool fNeg = (*source == '-');
            var beg = fNeg ? source + 1 : source;
            var end = source + length;
            int result = 0;
            while (beg < end)
                result = result * 10 + (*beg++ - '0');
            return fNeg ? -(result) : result;
        }

        /// <summary>
        /// Convert integer into sequence of ASCII bytes
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <param name="length">Number of digits in value</param>
        /// <param name="result">Byte pointer, will updated to point after the written number</param>
        public static unsafe void IntToBytes(int value, int length, ref byte* result)
        {
            bool isNegative = value < 0;
            if (value == 0)
            {
                *result++ = (byte)'0';
                return;
            }

            long v = value;
            if (isNegative)
            {
                *result++ = (byte)'-';
                v = -value;
            }

            result += length;
            do
            {
                *--result = (byte)((byte)'0' + (v % 10));
                v /= 10;
            } while (v > 0);
            result += length;
        }

        static ReadOnlySpan<byte> digits => "00010203040506070809101112131415161718192021222324252627282930313233343536373839404142434445464748495051525354555657585960616263646566676869707172737475767778798081828384858687888990919293949596979899"u8;

        /// <summary>
        /// Return number of digits in given number
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        public static int NumDigits(int v)
        {
            v = v < 0 ? ((~v) + 1) : v;

            if (v < 10) return 1;
            if (v < 100) return 2;
            if (v < 1000) return 3;
            if (v < 100000000L)
            {
                if (v < 1000000)
                {
                    if (v < 10000) return 4;
                    return 5 + (v >= 100000 ? 1 : 0);
                }
                return 7 + (v >= 10000000L ? 1 : 0);
            }
            return 9 + (v >= 1000000000L ? 1 : 0);
        }

        /// <summary>
        /// Num digits in long divide n conquer.
        /// </summary>
        /// <param name="v"></param>
        /// <returns>returns digit count not including sign.</returns>
        public static int NumDigitsInLong(long v)
        {
            if (v == long.MinValue) return 19;
            v = v < 0 ? -v : v;
            int c = 0;

            if (v < 10000000000L)//1 - 10000000000L
            {
                if (v < 100000) //1 - 100000
                {
                    if (v < 100) //1 - 100
                    {
                        if (v < 10) return c + 1; else return c + 2;
                    }
                    else//100 - 100000
                    {
                        if (v < 10000)//100 - 10000
                        {
                            if (v < 1000) return c + 3; else return c + 4;
                        }
                        else//10000 - 100000
                        {
                            return c + 5;
                        }
                    }
                }
                else // 100 000 - 10 000 000 000L
                {
                    if (v < 10000000) // 100 000 - 10 000 000
                    {
                        if (v < 1000000) return c + 6; else return c + 7;
                    }
                    else // 10 000 000 - 10 000 000 000L
                    {
                        if (v < 1000000000)
                        {
                            if (v < 100000000) return c + 8; else return c + 9;
                        }
                        else // 1 000 000 000 - 10 000 000 000L
                        {
                            return c + 10;
                        }
                    }
                }
            }
            else // 10 000 000 000L - 1 000 000 000 000 000 000L
            {
                if (v < 100000000000000L) //10 000 000 000L - 100 000 000 000 000L
                {
                    if (v < 1000000000000L) // 10 000 000 000L - 1 000 000 000 000L
                    {
                        if (v < 100000000000L) return c + 11; else return c + 12;
                    }
                    else // 1 000 000 000 000L - 100 000 000 000 000L
                    {
                        if (v < 10000000000000L) // 1 000 000 000 000L - 10 000 000 000 000L
                        {
                            return c + 13;
                        }
                        else
                        {
                            return c + 14;
                        }
                    }
                }
                else//100 000 000 000 000L - 1 000 000 000 000 000 000L
                {
                    if (v < 10000000000000000L)//100 000 000 000 000L - 10 000 000 000 000 000L                    
                    {
                        if (v < 1000000000000000L) return c + 15; else return c + 16;
                    }
                    else
                    {
                        if (v < 1000000000000000000L)
                        {
                            if (v < 100000000000000000L) return c + 17; else return c + 18;
                        }
                        else
                        {
                            return c + 19;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Return number of digits in given number
        /// </summary>
        /// <param name="v"></param>
        /// <param name="fNeg"></param>
        /// <returns></returns>
        public static int NumDigitsInLong(long v, ref bool fNeg)
        {
            if (v == long.MinValue)
            {
                fNeg = true;
                return 19;
            }

            fNeg = false;
            if (v < 0)
            {
                fNeg = true;
                v = -(v);
            }

            if (v < 10) return 1;
            if (v < 100) return 2;
            if (v < 1000) return 3;
            if (v < 1_000_000_00L) // 9 digit
            {
                if (v < 1000000)// 7 dgiit
                {
                    if (v < 10000) return 4;
                    return 5 + (v >= 100000 ? 1 : 0); // 5 or 6 digit
                }
                return 7 + (v >= 1_000_000_0L ? 1 : 0);
            }
            if (v < 1000000000L) return 9;
            if (v < 10000000000L) return 10;
            if (v < 100000000000L) return 11;
            if (v < 1000000000000L) return 12;
            if (v < 10000000000000L) return 13;
            if (v < 100000000000000L) return 14;
            if (v < 1000000000000000L) return 15;
            if (v < 10000000000000000L) return 16;
            if (v < 100000000000000000L) return 17;
            if (v < 1000000000000000000L) return 18;
            return 19;
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
    }
}