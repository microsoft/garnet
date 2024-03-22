// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Garnet.common
{
    /// <summary>
    /// Utilities for numeric parsing
    /// </summary>
    public static unsafe class NumUtils
    {
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

        internal static unsafe int IndexOfByte(byte* src, byte value, int index, int count)
        {
            byte* pByte = src + index;

            // Align up the pointer to sizeof(int).
            while (((int)pByte & 3) != 0)
            {
                if (count == 0)
                    return -1;
                else if (*pByte == value)
                    return (int)(pByte - src);

                count--;
                pByte++;
            }

            // Fill comparer with value byte for comparisons
            //
            // comparer = 0/0/value/value
            uint comparer = (((uint)value << 8) + (uint)value);
            // comparer = value/value/value/value
            comparer = (comparer << 16) + comparer;

            // Run through buffer until we hit a 4-byte section which contains
            // the byte we're looking for or until we exhaust the buffer.
            while (count > 3)
            {
                // Test the buffer for presence of value. comparer contains the byte
                // replicated 4 times.
                uint t1 = *(uint*)pByte;
                t1 = t1 ^ comparer;
                uint t2 = 0x7efefeff + t1;
                t1 = t1 ^ 0xffffffff;
                t1 = t1 ^ t2;
                t1 = t1 & 0x81010100;

                // if t1 is zero then these 4-bytes don't contain a match
                if (t1 != 0)
                {
                    // We've found a match for value, figure out which position it's in.
                    int foundIndex = (int)(pByte - src);
                    if (pByte[0] == value)
                        return foundIndex;
                    else if (pByte[1] == value)
                        return foundIndex + 1;
                    else if (pByte[2] == value)
                        return foundIndex + 2;
                    else if (pByte[3] == value)
                        return foundIndex + 3;
                }

                count -= 4;
                pByte += 4;

            }

            // Catch any bytes that might be left at the tail of the buffer
            while (count > 0)
            {
                if (*pByte == value)
                    return (int)(pByte - src);

                count--;
                pByte++;
            }

            // If we don't have a match return -1;
            return -1;
        }

        static readonly ushort[] CRC_TABLE = new ushort[256]
        {
            0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50A5, 0x60C6, 0x70E7,
            0x8108, 0x9129, 0xA14A, 0xB16B, 0xC18C, 0xD1AD, 0xE1CE, 0xF1EF,
            0x1231, 0x0210, 0x3273, 0x2252, 0x52B5, 0x4294, 0x72F7, 0x62D6,
            0x9339, 0x8318, 0xB37B, 0xA35A, 0xD3BD, 0xC39C, 0xF3FF, 0xE3DE,
            0x2462, 0x3443, 0x0420, 0x1401, 0x64E6, 0x74C7, 0x44A4, 0x5485,
            0xA56A, 0xB54B, 0x8528, 0x9509, 0xE5EE, 0xF5CF, 0xC5AC, 0xD58D,
            0x3653, 0x2672, 0x1611, 0x0630, 0x76D7, 0x66F6, 0x5695, 0x46B4,
            0xB75B, 0xA77A, 0x9719, 0x8738, 0xF7DF, 0xE7FE, 0xD79D, 0xC7BC,
            0x48C4, 0x58E5, 0x6886, 0x78A7, 0x0840, 0x1861, 0x2802, 0x3823,
            0xC9CC, 0xD9ED, 0xE98E, 0xF9AF, 0x8948, 0x9969, 0xA90A, 0xB92B,
            0x5AF5, 0x4AD4, 0x7AB7, 0x6A96, 0x1A71, 0x0A50, 0x3A33, 0x2A12,
            0xDBFD, 0xCBDC, 0xFBBF, 0xEB9E, 0x9B79, 0x8B58, 0xBB3B, 0xAB1A,
            0x6CA6, 0x7C87, 0x4CE4, 0x5CC5, 0x2C22, 0x3C03, 0x0C60, 0x1C41,
            0xEDAE, 0xFD8F, 0xCDEC, 0xDDCD, 0xAD2A, 0xBD0B, 0x8D68, 0x9D49,
            0x7E97, 0x6EB6, 0x5ED5, 0x4EF4, 0x3E13, 0x2E32, 0x1E51, 0x0E70,
            0xFF9F, 0xEFBE, 0xDFDD, 0xCFFC, 0xBF1B, 0xAF3A, 0x9F59, 0x8F78,
            0x9188, 0x81A9, 0xB1CA, 0xA1EB, 0xD10C, 0xC12D, 0xF14E, 0xE16F,
            0x1080, 0x00A1, 0x30C2, 0x20E3, 0x5004, 0x4025, 0x7046, 0x6067,
            0x83B9, 0x9398, 0xA3FB, 0xB3DA, 0xC33D, 0xD31C, 0xE37F, 0xF35E,
            0x02B1, 0x1290, 0x22F3, 0x32D2, 0x4235, 0x5214, 0x6277, 0x7256,
            0xB5EA, 0xA5CB, 0x95A8, 0x8589, 0xF56E, 0xE54F, 0xD52C, 0xC50D,
            0x34E2, 0x24C3, 0x14A0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
            0xA7DB, 0xB7FA, 0x8799, 0x97B8, 0xE75F, 0xF77E, 0xC71D, 0xD73C,
            0x26D3, 0x36F2, 0x0691, 0x16B0, 0x6657, 0x7676, 0x4615, 0x5634,
            0xD94C, 0xC96D, 0xF90E, 0xE92F, 0x99C8, 0x89E9, 0xB98A, 0xA9AB,
            0x5844, 0x4865, 0x7806, 0x6827, 0x18C0, 0x08E1, 0x3882, 0x28A3,
            0xCB7D, 0xDB5C, 0xEB3F, 0xFB1E, 0x8BF9, 0x9BD8, 0xABBB, 0xBB9A,
            0x4A75, 0x5A54, 0x6A37, 0x7A16, 0x0AF1, 0x1AD0, 0x2AB3, 0x3A92,
            0xFD2E, 0xED0F, 0xDD6C, 0xCD4D, 0xBDAA, 0xAD8B, 0x9DE8, 0x8DC9,
            0x7C26, 0x6C07, 0x5C64, 0x4C45, 0x3CA2, 0x2C83, 0x1CE0, 0x0CC1,
            0xEF1F, 0xFF3E, 0xCF5D, 0xDF7C, 0xAF9B, 0xBFBA, 0x8FD9, 0x9FF8,
            0x6E17, 0x7E36, 0x4E55, 0x5E74, 0x2E93, 0x3EB2, 0x0ED1, 0x1EF0
        };

        // No need to free or track until end of process
        static readonly ushort* crc_table_ptr = (ushort*)GCHandle.Alloc(CRC_TABLE, GCHandleType.Pinned).AddrOfPinnedObject();

        /// <summary>
        /// Compute CRC of given data
        /// </summary>
        /// <param name="data"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public static unsafe ushort CRC16(byte* data, int len)
        {
            ushort result = 0;
            byte* end = data + len;
            while (data < end)
                result = (ushort)(*(crc_table_ptr + (((result >> 8) ^ *data++) & 0xff)) ^ (result << 8));
            return result;
        }

        /// <summary>
        /// Compute CRC16 of given data (via array access)
        /// </summary>
        /// <param name="data"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public static unsafe ushort CRC16v2(byte* data, int len)
        {
            ushort result = 0;
            byte* end = data + len;
            while (data < end)
                result = (ushort)(CRC_TABLE[((result >> 8) ^ *data++) & 0xff] ^ (result << 8));
            return result;
        }

        /// <summary>
        /// Compute hash slot of given data
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static unsafe ushort HashSlot(byte[] key)
        {
            fixed (byte* keyPtr = key)
                return HashSlot(keyPtr, key.Length);
        }

        /// <summary>
        /// Compute hash slot of given data
        /// </summary>
        /// <param name="keyPtr"></param>
        /// <param name="ksize"></param>
        /// <returns></returns>
        public static unsafe ushort HashSlot(byte* keyPtr, int ksize)
        {
            var startTag = keyPtr;
            var end = keyPtr + ksize;
            while (startTag < end && *startTag++ != '{') ;
            if (startTag < end - 1)
            {
                var endTag = startTag;
                while (endTag < end && *endTag++ != '}') ;
                if (endTag <= end && endTag > startTag + 1)
                {
                    keyPtr = startTag;
                    ksize = (int)(endTag - startTag - 1);
                    Debug.Assert(ksize > 0);
                }
            }
            return (ushort)(CRC16(keyPtr, ksize) & 16383);
        }

        /// <summary>
        /// Try to parse from pointer to integer
        /// </summary>
        /// <param name="source"></param>
        /// <param name="len"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public static unsafe bool TryBytesToInt(byte* source, int len, out int result)
        {
            bool fNeg = (*source == '-');
            var beg = fNeg ? source + 1 : source;
            result = 0;
            for (int i = 0; i < len; ++i)
            {
                if (!(source[i] >= 48 && source[i] <= 57))
                {
                    return false;
                }
                result = result * 10 + (*beg++ - '0');
            }
            result = fNeg ? -(result) : result;
            return true;
        }

    }
}