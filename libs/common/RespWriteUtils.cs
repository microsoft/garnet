// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;

namespace Garnet.common
{
    /// <summary>
    /// Utilities for writing RESP protocol
    /// </summary>
    public static unsafe class RespWriteUtils
    {
        /// <summary>
        /// Write array length
        /// </summary>
        public static bool WriteArrayLength(int len, ref byte* curr, byte* end)
        {
            int numDigits = NumUtils.NumDigits(len);
            int totalLen = 1 + numDigits + 2;
            if (totalLen > (int)(end - curr))
                return false;
            *curr++ = (byte)'*';
            NumUtils.IntToBytes(len, numDigits, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write array item
        /// </summary>
        public static bool WriteArrayItem(long integer, ref byte* curr, byte* end)
        {
            int integerLen = NumUtils.NumDigitsInLong(integer);
            byte sign = (byte)(integer < 0 ? 1 : 0);
            int integerLenLen = NumUtils.NumDigits(sign + integerLen);

            //$[integerLen]\r\n[integer]\r\n
            int totalLen = 1 + integerLenLen + 2 + sign + integerLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.IntToBytes(sign + integerLen, integerLenLen, ref curr);
            WriteNewline(ref curr);
            NumUtils.LongToBytes(integer, integerLen, ref curr);
            WriteNewline(ref curr);

            return true;
        }

        /// <summary>
        /// Write null
        /// </summary>
        public static bool WriteNull(ref byte* curr, byte* end)
        {
            if (5 > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            *curr++ = (byte)'-';
            *curr++ = (byte)'1';
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write null array
        /// </summary>
        public static bool WriteNullArray(ref byte* curr, byte* end)
        {
            if (5 > (int)(end - curr))
                return false;

            *curr++ = (byte)'*';
            *curr++ = (byte)'-';
            *curr++ = (byte)'1';
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write simple string
        /// </summary>
        public static bool WriteSimpleString(byte[] item, ref byte* curr, byte* end)
        {
            // Simple strings are of the form "+OK\r\n"
            int totalLen = 1 + item.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'+';
            new ReadOnlySpan<byte>(item).CopyTo(new Span<byte>(curr, item.Length));
            curr += item.Length;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write simple string
        /// </summary>
        public static bool WriteSimpleString(ReadOnlySpan<char> item, int encodedLen, ref byte* curr, byte* end)
        {
            // Simple strings are of the form "+OK\r\n"
            int totalLen = 1 + encodedLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'+';
            Encoding.ASCII.GetBytes(item, new Span<byte>(curr, encodedLen));
            curr += encodedLen;
            WriteNewline(ref curr);
            return true;
        }


        /// <summary>
        /// Write a long as a simple string
        /// </summary>
        public static bool WriteLongAsSimpleString(long value, ref byte* curr, byte* end)
        {
            // Simple strings are of the form "+cc\r\n"
            int longLength = NumUtils.NumDigitsInLong(value);
            int totalLen = 1 + longLength + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'+';
            NumUtils.LongToBytes(value, longLength, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write error
        /// </summary>
        public static bool WriteError(byte[] item, ref byte* curr, byte* end)
        {
            // Simple strings are of the form "+OK\r\n"
            int totalLen = 1 + item.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'-';
            new ReadOnlySpan<byte>(item).CopyTo(new Span<byte>(curr, item.Length));
            curr += item.Length;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write byte array directly
        /// </summary>
        public static bool WriteDirect(ReadOnlySpan<byte> item, ref byte* curr, byte* end)
        {
            int totalLen = item.Length;
            if (totalLen > (int)(end - curr))
                return false;

            item.CopyTo(new Span<byte>(curr, item.Length));
            curr += item.Length;
            return true;
        }

        /// <summary>
        /// Write struct directly
        /// </summary>
        public static bool WriteDirect<T>(ref T item, ref byte* curr, byte* end) where T : unmanaged
        {
            int totalLen = sizeof(T);
            if (totalLen > (int)(end - curr))
                return false;
            *(T*)curr = item;
            curr += totalLen;
            return true;
        }

        /// <summary>
        /// Write bulk string
        /// </summary>
        public static bool WriteBulkString(ReadOnlySpan<byte> item, ref byte* curr, byte* end)
        {
            var itemDigits = NumUtils.NumDigits(item.Length);
            int totalLen = 1 + itemDigits + 2 + item.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.IntToBytes(item.Length, itemDigits, ref curr);
            WriteNewline(ref curr);
            item.CopyTo(new Span<byte>(curr, item.Length));
            curr += item.Length;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write bulk string
        /// </summary>
        public static bool WriteBulkString(ReadOnlySpan<char> item, ref byte* curr, byte* end)
        {
            var itemDigits = NumUtils.NumDigits(item.Length);
            int totalLen = 1 + itemDigits + 2 + item.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.IntToBytes(item.Length, itemDigits, ref curr);
            WriteNewline(ref curr);
            Encoding.UTF8.GetBytes(item, new Span<byte>(curr, item.Length));
            curr += item.Length;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write bulk string
        /// </summary>
        public static bool WriteBulkString(Span<byte> item, ref byte* curr, byte* end)
        {
            var itemDigits = NumUtils.NumDigits(item.Length);
            int totalLen = 1 + itemDigits + 2 + item.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.IntToBytes(item.Length, itemDigits, ref curr);
            WriteNewline(ref curr);
            item.CopyTo(new Span<byte>(curr, item.Length));
            curr += item.Length;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Get length of bulk string
        /// </summary>
        public static int GetBulkStringLength(int length)
            => 1 + NumUtils.NumDigits(length) + 2 + length + 2;

        /// <summary>
        /// Write integer
        /// </summary>
        public static bool WriteInteger(int integer, ref byte* curr, byte* end)
        {
            int integerLen = NumUtils.NumDigitsInLong(integer);
            byte sign = (byte)(integer < 0 ? 1 : 0);

            //:integer\r\n
            int totalLen = 1 + sign + integerLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)':';
            NumUtils.IntToBytes(integer, integerLen, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write integer
        /// </summary>
        public static bool WriteInteger(long integer, ref byte* curr, byte* end)
        {
            int integerLen = NumUtils.NumDigitsInLong(integer);
            byte sign = (byte)(integer < 0 ? 1 : 0);

            //:integer\r\n
            int totalLen = 1 + sign + integerLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)':';
            NumUtils.LongToBytes(integer, integerLen, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write integer
        /// </summary>
        public static bool WriteInteger(long integer, ref byte* curr, byte* end, out int integerLen, out int totalLen)
        {
            integerLen = NumUtils.NumDigitsInLong(integer);
            byte sign = (byte)(integer < 0 ? 1 : 0);

            //:integer\r\n
            totalLen = 1 + sign + integerLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)':';
            NumUtils.LongToBytes(integer, integerLen, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write integer from bytes
        /// </summary>
        public static bool WriteIntegerFromBytes(byte* src, int srclen, ref byte* curr, byte* end)
        {
            int totalLen = 1 + srclen + 2;
            if ((int)(end - curr) < totalLen)
                return false;

            *curr++ = (byte)':';
            int digit = 0;
            while (digit < srclen)
            {
                *curr++ = *src++;
                digit++;
            }
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write integer as bulk string
        /// </summary>
        public static bool WriteIntegerAsBulkString(int integer, ref byte* curr, byte* end)
        {
            int integerLen = NumUtils.NumDigitsInLong(integer);
            byte sign = (byte)(integer < 0 ? 1 : 0);

            int integerLenSize = NumUtils.NumDigits(integerLen + sign);

            //$size\r\ninteger\r\n
            int totalLen = 1 + integerLenSize + 2 + sign + integerLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.IntToBytes(integerLen + sign, integerLenSize, ref curr);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';
            NumUtils.IntToBytes(integer, integerLen, ref curr);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';
            return true;
        }

        /// <summary>
        /// Write integer as bulk string
        /// </summary>
        public static bool WriteIntegerAsBulkString(long integer, ref byte* curr, byte* end, out int totalLen)
        {
            int integerLen = NumUtils.NumDigitsInLong(integer);
            byte sign = (byte)(integer < 0 ? 1 : 0);

            int integerLenSize = NumUtils.NumDigits(integerLen + sign);

            //$size\r\ninteger\r\n
            totalLen = 1 + integerLenSize + 2 + sign + integerLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.IntToBytes(integerLen + sign, integerLenSize, ref curr);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';
            NumUtils.LongToBytes(integer, integerLen, ref curr);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';
            return true;
        }

        /// <summary>
        /// Get length of integer as bulk string
        /// </summary>
        public static int GetIntegerAsBulkStringLength(int integer)
        {
            int integerLen = NumUtils.NumDigitsInLong(integer);
            byte sign = (byte)(integer < 0 ? 1 : 0);

            int integerLenSize = NumUtils.NumDigits(integerLen + sign);

            //$size\r\ninteger\r\n
            return 1 + integerLenSize + 2 + sign + integerLen + 2;
        }

        /// <summary>
        /// Write response from ReadOnlySpan of byte
        /// </summary>
        public static bool WriteResponse(ReadOnlySpan<byte> response, ref byte* curr, byte* end)
        {
            if ((int)(end - curr) < response.Length)
                return false;
            response.CopyTo(new Span<byte>(curr, response.Length));
            curr += response.Length;
            return true;
        }


        /// <summary>
        /// Create header for *Scan output
        /// *scan commands have an array of two elements
        /// a cursor and an array of items or fields
        /// </summary>
        /// <param name="cursor"></param>
        /// <param name="curr"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        public static bool WriteScanOutputHeader(long cursor, ref byte* curr, byte* end)
        {
            if (!WriteArrayLength(2, ref curr, end))
                return false;

            // Cursor value
            if (!WriteIntegerAsBulkString((int)cursor, ref curr, end))
                return false;

            return true;
        }

        /// <summary>
        /// Write empty array
        /// </summary>
        public static bool WriteEmptyArray(ref byte* curr, byte* end)
        {
            if (4 > (int)(end - curr))
                return false;

            *curr++ = (byte)'*';
            *curr++ = (byte)'0';
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write newline
        /// </summary>
        public static void WriteNewline(ref byte* curr)
        {
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';
        }

        /// <summary>
        /// Write an array with len number of null elements
        /// </summary>
        public static bool WriteArrayWithNullElements(int len, ref byte* curr, byte* end)
        {
            int numDigits = NumUtils.NumDigits(len);
            int totalLen = 1 + numDigits + 2;
            totalLen += len * 5; // 5 is the length of $-1\r\n

            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'*';
            NumUtils.IntToBytes(len, numDigits, ref curr);
            WriteNewline(ref curr);
            for (int i = 0; i < len; i++)
            {
                WriteNull(ref curr, end);
            }
            return true;
        }
    }
}