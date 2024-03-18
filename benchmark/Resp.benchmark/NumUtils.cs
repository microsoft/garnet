// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Resp.benchmark
{
    internal static class NumUtils
    {
        /// <summary>
        /// Convert integer into sequence of ASCII bytes
        /// </summary>
        /// <param name="value"></param>
        /// <param name="result"></param>
        public static unsafe void IntToBytes(int value, ref byte* result)
        {
            var length = NumDigits(value);
            result += length;
            do
            {
                *--result = (byte)((byte)'0' + (value % 10));
                value /= 10;
            } while (value > 0);
            result += length;
        }

        /// <summary>
        /// Convert integer into sequence of ASCII bytes
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <param name="length">Number of digits in value</param>
        /// <param name="result">Byte pointer, will updated to point after the written number</param>
        public static unsafe void IntToBytes(int value, int length, ref byte* result)
        {
            byte sign = (byte)(value < 0 ? 1 : 0);
            if (value == 0)
            {
                *result++ = (byte)'0';
                return;
            }

            long v = value;
            if (sign == 0x1)
            {
                *result++ = 0x2d;
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
        /// Return number of digits in given number
        /// </summary>
        /// <param name="v"></param>
        /// <returns>Return number of decimal digits in number (count does not include)</returns>
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

        public static unsafe long BytesToLong(byte* source)
        {
            bool fNeg = (*source == '-');
            var beg = fNeg ? source + 1 : source;
            long result = 0;
            while (*beg != '\r')
                result = result * 10 + (*beg++ - '0');
            return fNeg ? -(result) : result;
        }
    }
}