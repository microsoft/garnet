﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
            WriteBytes<uint>(ref curr, "-1\r\n"u8);
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
            WriteBytes<uint>(ref curr, "-1\r\n"u8);
            return true;
        }

        /// <summary>
        /// Write simple string
        /// </summary>
        /// <param name="simpleString">An ASCII encoded simple string. The string mustn't contain a CR (\r) or LF (\n) bytes.</param>
        public static bool WriteSimpleString(ReadOnlySpan<byte> simpleString, ref byte* curr, byte* end)
        {
            // Simple strings are of the form "+OK\r\n"
            int totalLen = 1 + simpleString.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'+';
            simpleString.CopyTo(new Span<byte>(curr, simpleString.Length));
            curr += simpleString.Length;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write simple string
        /// </summary>
        /// <param name="simpleString">An ASCII simple string. The string mustn't contain a CR (\r) or LF (\n) characters.</param>
        public static bool WriteSimpleString(ReadOnlySpan<char> simpleString, ref byte* curr, byte* end)
        {
            // Simple strings are of the form "+OK\r\n"
            int totalLen = 1 + simpleString.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'+';
            int bytesWritten = Encoding.ASCII.GetBytes(simpleString, new Span<byte>(curr, simpleString.Length));
            curr += bytesWritten;
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
        /// Write simple error
        /// </summary>
        /// <param name="errorString">An ASCII encoded error string. The string mustn't contain a CR (\r) or LF (\n) bytes.</param>
        public static bool WriteError(ReadOnlySpan<byte> errorString, ref byte* curr, byte* end)
        {
            int totalLen = 1 + errorString.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'-';
            errorString.CopyTo(new Span<byte>(curr, errorString.Length));
            curr += errorString.Length;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write simple error
        /// </summary>
        /// <param name="errorString">An ASCII error string. The string mustn't contain a CR (\r) or LF (\n) characters.</param>
        public static bool WriteError(ReadOnlySpan<char> errorString, ref byte* curr, byte* end)
        {
            int totalLen = 1 + errorString.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'-';
            int bytesWritten = Encoding.ASCII.GetBytes(errorString, new Span<byte>(curr, errorString.Length));
            curr += bytesWritten;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Writes the contents of <paramref name="span"/> as byte array to <paramref name="curr"/>
        /// </summary>
        /// <returns><see langword="true"/> if the the <paramref name="span"/> could be written to <paramref name="curr"/>; <see langword="false"/> otherwise.</returns>
        public static bool WriteDirect(ReadOnlySpan<byte> span, ref byte* curr, byte* end)
        {
            if (span.Length > (int)(end - curr))
                return false;

            span.CopyTo(new Span<byte>(curr, span.Length));
            curr += span.Length;
            return true;
        }

        /// <summary>
        /// Encodes the <paramref name="span"/> as ASCII to <paramref name="curr"/>
        /// </summary>
        /// <returns><see langword="true"/> if the the <paramref name="span"/> could be written to <paramref name="curr"/>; <see langword="false"/> otherwise.</returns>
        public static bool WriteAsciiDirect(ReadOnlySpan<char> span, ref byte* curr, byte* end)
        {
            if (span.Length > (int)(end - curr))
                return false;

            int bytesWritten = Encoding.ASCII.GetBytes(span, new Span<byte>(curr, span.Length));
            curr += bytesWritten;
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
        /// Encodes the <paramref name="chars"/> as ASCII bulk string to <paramref name="curr"/>
        /// </summary>
        public static bool WriteAsciiBulkString(ReadOnlySpan<char> chars, ref byte* curr, byte* end)
        {
            var itemDigits = NumUtils.NumDigits(chars.Length);
            int totalLen = 1 + itemDigits + 2 + chars.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.IntToBytes(chars.Length, itemDigits, ref curr);
            WriteNewline(ref curr);
            int bytesWritten = Encoding.ASCII.GetBytes(chars, new Span<byte>(curr, chars.Length));
            curr += bytesWritten;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Encodes the <paramref name="chars"/> as UTF8 bulk string to <paramref name="curr"/>
        /// </summary>
        public static bool WriteUtf8BulkString(ReadOnlySpan<char> chars, ref byte* curr, byte* end)
        {
            // Calculate the amount of bytes it takes to encoded the UTF16 string as UTF8
            int encodedByteCount = Encoding.UTF8.GetByteCount(chars);

            var itemDigits = NumUtils.NumDigits(encodedByteCount);
            int totalLen = 1 + itemDigits + 2 + encodedByteCount + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.IntToBytes(encodedByteCount, itemDigits, ref curr);
            WriteNewline(ref curr);
            int bytesWritten = Encoding.UTF8.GetBytes(chars, new Span<byte>(curr, encodedByteCount));
            curr += bytesWritten;
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
        public static bool WriteIntegerFromBytes(ReadOnlySpan<byte> integerBytes, ref byte* curr, byte* end)
        {
            int totalLen = 1 + integerBytes.Length + 2;
            if ((int)(end - curr) < totalLen)
                return false;

            *curr++ = (byte)':';
            integerBytes.CopyTo(new Span<byte>(curr, integerBytes.Length));
            curr += integerBytes.Length;
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
            WriteNewline(ref curr);
            NumUtils.IntToBytes(integer, integerLen, ref curr);
            WriteNewline(ref curr);
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
            WriteNewline(ref curr);
            NumUtils.LongToBytes(integer, integerLen, ref curr);
            WriteNewline(ref curr);
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

            WriteBytes<uint>(ref curr, "*0\r\n"u8);
            return true;
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

        /// <summary>
        /// Write newline (\r\n) to <paramref name="curr"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void WriteNewline(ref byte* curr) => WriteBytes<ushort>(ref curr, "\r\n"u8);

        /// <summary>
        /// Write <paramref name="bytes"/> to <paramref name="curr"/> as type <typeparamref name="T"/> sized value.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteBytes<T>(ref byte* curr, ReadOnlySpan<byte> bytes)
            where T : unmanaged
        {
            Debug.Assert(bytes.Length == sizeof(T));
            Unsafe.WriteUnaligned(curr, MemoryMarshal.Read<T>(bytes));
            curr += sizeof(T);
        }
    }
}