﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Text;
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
        /// Writes a map length
        /// </summary>
        public static bool TryWriteMapLength(int len, ref byte* curr, byte* end)
        {
            var numDigits = NumUtils.CountDigits(len);
            var totalLen = 1 + numDigits + 2;
            if (totalLen > (int)(end - curr))
                return false;
            *curr++ = (byte)'%';
            NumUtils.WriteInt32(len, numDigits, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Writes a push type length
        /// </summary>
        public static bool TryWritePushLength(int len, ref byte* curr, byte* end)
        {
            var numDigits = NumUtils.CountDigits(len);
            var totalLen = 1 + numDigits + 2;
            if (totalLen > (int)(end - curr))
                return false;
            *curr++ = (byte)'>';
            NumUtils.WriteInt32(len, numDigits, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Writes a bulk string length header, padded to specified total length (including protocol bytes)
        /// </summary>
        public static bool TryWritePaddedBulkStringLength(int len, int paddedLen, ref byte* curr, byte* end)
        {
            var numDigits = NumUtils.CountDigits(len);
            var totalLen = 1 + numDigits + 2;
            Debug.Assert(totalLen <= paddedLen);
            if (paddedLen > (int)(end - curr))
                return false;
            *curr++ = (byte)'$';
            for (int i = 0; i < paddedLen - totalLen; i++)
                *curr++ = (byte)'0';
            NumUtils.WriteInt32(len, numDigits, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Writes an array length
        /// </summary>
        public static bool TryWriteArrayLength(int len, ref byte* curr, byte* end)
        {
            var numDigits = NumUtils.CountDigits(len);
            var totalLen = 1 + numDigits + 2;
            if (totalLen > (int)(end - curr))
                return false;
            *curr++ = (byte)'*';
            NumUtils.WriteInt32(len, numDigits, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Writes an set length
        /// </summary>
        public static bool TryWriteSetLength(int len, ref byte* curr, byte* end)
        {
            var numDigits = NumUtils.CountDigits(len);
            var totalLen = 1 + numDigits + 2;
            if (totalLen > (int)(end - curr))
                return false;
            *curr++ = (byte)'~';
            NumUtils.WriteInt32(len, numDigits, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        public static bool TryWriteArrayLength(int len, ref byte* curr, byte* end, out int numDigits, out int totalLen)
        {
            numDigits = NumUtils.CountDigits(len);
            totalLen = 1 + numDigits + 2;
            if (totalLen > (int)(end - curr))
                return false;
            *curr++ = (byte)'*';
            NumUtils.WriteInt32(len, numDigits, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Writes an array item
        /// </summary>
        public static bool TryWriteArrayItem(long integer, ref byte* curr, byte* end)
        {
            var integerLen = NumUtils.CountDigits(integer);
            var sign = (byte)(integer < 0 ? 1 : 0);
            var integerLenLen = NumUtils.CountDigits(sign + integerLen);

            // $[integerLen]\r\n[integer]\r\n
            var totalLen = 1 + integerLenLen + 2 + sign + integerLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.WriteInt32(sign + integerLen, integerLenLen, ref curr);
            WriteNewline(ref curr);
            NumUtils.WriteInt64(integer, integerLen, ref curr);
            WriteNewline(ref curr);

            return true;
        }

        /// <summary>
        /// Writes a RESP2 null ($-1\r\n)
        /// </summary>
        public static bool TryWriteNull(ref byte* curr, byte* end)
        {
            if (5 > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            WriteBytes<uint>(ref curr, "-1\r\n"u8);
            return true;
        }

        /// <summary>
        /// Writes a RESP3 null (_\r\n)
        /// </summary>
        public static bool TryWriteResp3Null(ref byte* curr, byte* end)
        {
            if (3 > (int)(end - curr))
                return false;

            *curr++ = (byte)'_';
            WriteBytes<ushort>(ref curr, "\r\n"u8);
            return true;
        }

        /// <summary>
        /// Writes a null array
        /// </summary>
        public static bool TryWriteNullArray(ref byte* curr, byte* end)
        {
            if (5 > (int)(end - curr))
                return false;

            *curr++ = (byte)'*';
            WriteBytes<uint>(ref curr, "-1\r\n"u8);
            return true;
        }

        /// <summary>
        /// Writes a simple string
        /// </summary>
        /// <param name="simpleString">An ASCII encoded simple string. The string mustn't contain a CR (\r) or LF (\n) bytes.</param>
        public static bool TryWriteSimpleString(ReadOnlySpan<byte> simpleString, ref byte* curr, byte* end)
        {
            // Simple strings are of the form "+OK\r\n"
            var totalLen = 1 + simpleString.Length + 2;
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
        public static bool TryWriteSimpleString(ReadOnlySpan<char> simpleString, ref byte* curr, byte* end)
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
        /// Write a signed 64-bit integer as a simple string
        /// </summary>
        public static bool TryWriteInt64AsSimpleString(long value, ref byte* curr, byte* end)
        {
            // Simple strings are of the form "+cc\r\n"
            var longLength = NumUtils.CountDigits(value);
            var totalLen = 1 + longLength + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'+';
            NumUtils.WriteInt64(value, longLength, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write simple error
        /// </summary>
        /// <param name="errorString">An ASCII encoded error string. The string mustn't contain a CR (\r) or LF (\n) bytes.</param>
        public static bool TryWriteError(ReadOnlySpan<byte> errorString, ref byte* curr, byte* end)
        {
            var totalLen = 1 + errorString.Length + 2;
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
        /// <param name="errorString">An ASCII encoded error string. The string mustn't contain a CR (\r) or LF (\n) bytes.</param>
        public static bool TryWriteError(ReadOnlySpan<byte> errorString, ref MemoryResult<byte> output)
        {
            var totalLen = 1 + errorString.Length + 2;
            output.MemoryOwner = MemoryPool<byte>.Shared.Rent(totalLen);
            output.Length = totalLen;

            fixed (byte* ptr = output.MemoryOwner.Memory.Span)
            {
                var curr = ptr;
                *curr++ = (byte)'-';
                errorString.CopyTo(new Span<byte>(curr, errorString.Length));
                curr += errorString.Length;
                WriteNewline(ref curr);
            }
            return true;
        }

        /// <summary>
        /// Write simple error
        /// </summary>
        /// <param name="errorString">An ASCII error string. The string mustn't contain a CR (\r) or LF (\n) characters.</param>
        public static bool TryWriteError(ReadOnlySpan<char> errorString, ref byte* curr, byte* end)
        {
            var totalLen = 1 + errorString.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'-';
            var bytesWritten = Encoding.ASCII.GetBytes(errorString, new Span<byte>(curr, errorString.Length));
            curr += bytesWritten;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Writes the contents of <paramref name="span"/> as byte array to <paramref name="curr"/>
        /// </summary>
        /// <returns><see langword="true"/> if the <paramref name="span"/> could be written to <paramref name="curr"/>; <see langword="false"/> otherwise.</returns>
        public static bool TryWriteDirect(ReadOnlySpan<byte> span, ref byte* curr, byte* end)
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
        /// <returns><see langword="true"/> if the <paramref name="span"/> could be written to <paramref name="curr"/>; <see langword="false"/> otherwise.</returns>
        public static bool TryWriteAsciiDirect(ReadOnlySpan<char> span, ref byte* curr, byte* end)
        {
            if (span.Length > (int)(end - curr))
                return false;

            var bytesWritten = Encoding.ASCII.GetBytes(span, new Span<byte>(curr, span.Length));
            curr += bytesWritten;
            return true;
        }

        /// <summary>
        /// Write struct directly
        /// </summary>
        public static bool TryWriteDirect<T>(ref T item, ref byte* curr, byte* end) where T : unmanaged
        {
            int totalLen = sizeof(T);
            if (totalLen > (int)(end - curr))
                return false;
            *(T*)curr = item;
            curr += totalLen;
            return true;
        }

        /// <summary>
        /// Write length header of bulk string
        /// </summary>
        public static bool TryWriteBulkStringLength(ReadOnlySpan<byte> item, ref byte* curr, byte* end)
        {
            var itemDigits = NumUtils.CountDigits(item.Length);
            var totalLen = 1 + itemDigits + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.WriteInt32(item.Length, itemDigits, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write bulk string
        /// </summary>
        public static bool TryWriteBulkString(ReadOnlySpan<byte> item, ref byte* curr, byte* end)
        {
            var itemDigits = NumUtils.CountDigits(item.Length);
            int totalLen = 1 + itemDigits + 2 + item.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.WriteInt32(item.Length, itemDigits, ref curr);
            WriteNewline(ref curr);
            item.CopyTo(new Span<byte>(curr, item.Length));
            curr += item.Length;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Encodes the <paramref name="chars"/> as ASCII bulk string to <paramref name="curr"/>
        /// </summary>
        public static bool TryWriteAsciiBulkString(ReadOnlySpan<char> chars, ref byte* curr, byte* end)
        {
            var itemDigits = NumUtils.CountDigits(chars.Length);
            var totalLen = 1 + itemDigits + 2 + chars.Length + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.WriteInt32(chars.Length, itemDigits, ref curr);
            WriteNewline(ref curr);
            var bytesWritten = Encoding.ASCII.GetBytes(chars, new Span<byte>(curr, chars.Length));
            curr += bytesWritten;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Encodes the <paramref name="chars"/> as UTF8 bulk string to <paramref name="curr"/>
        /// </summary>
        public static bool TryWriteUtf8BulkString(ReadOnlySpan<char> chars, ref byte* curr, byte* end)
        {
            // Calculate the amount of bytes it takes to encoded the UTF16 string as UTF8
            var encodedByteCount = Encoding.UTF8.GetByteCount(chars);

            var itemDigits = NumUtils.CountDigits(encodedByteCount);
            var totalLen = 1 + itemDigits + 2 + encodedByteCount + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.WriteInt32(encodedByteCount, itemDigits, ref curr);
            WriteNewline(ref curr);
            var bytesWritten = Encoding.UTF8.GetBytes(chars, new Span<byte>(curr, encodedByteCount));
            curr += bytesWritten;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write new line
        /// </summary>
        public static bool TryWriteNewLine(ref byte* curr, byte* end)
        {
            var totalLen = 2;
            if (totalLen > (int)(end - curr))
                return false;

            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Get length of bulk string
        /// </summary>
        public static int GetBulkStringLength(int length)
            => 1 + NumUtils.CountDigits(length) + 2 + length + 2;

        /// <summary>
        /// Write integer
        /// </summary>
        public static bool TryWriteInt32(int value, ref byte* curr, byte* end)
        {
            var integerLen = NumUtils.CountDigits((long)value);
            var sign = (byte)(value < 0 ? 1 : 0);

            //:integer\r\n
            var totalLen = 1 + sign + integerLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)':';
            NumUtils.WriteInt32(value, integerLen, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write integer
        /// </summary>
        public static bool TryWriteInt64(long value, ref byte* curr, byte* end)
        {
            var integerLen = NumUtils.CountDigits(value);
            var sign = (byte)(value < 0 ? 1 : 0);

            //:integer\r\n
            var totalLen = 1 + sign + integerLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)':';
            NumUtils.WriteInt64(value, integerLen, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write integer
        /// </summary>
        public static bool TryWriteInt64(long value, ref byte* curr, byte* end, out int integerLen, out int totalLen)
        {
            integerLen = NumUtils.CountDigits(value);
            byte sign = (byte)(value < 0 ? 1 : 0);

            //:integer\r\n
            totalLen = 1 + sign + integerLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)':';
            NumUtils.WriteInt64(value, integerLen, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write integer from bytes
        /// </summary>
        public static bool TryWriteIntegerFromBytes(ReadOnlySpan<byte> integerBytes, ref byte* curr, byte* end)
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
        /// Write a signed 32-bit integer as bulk string
        /// </summary>
        public static bool TryWriteInt32AsBulkString(int value, ref byte* curr, byte* end)
        {
            var integerLen = NumUtils.CountDigits((long)value);
            var sign = (byte)(value < 0 ? 1 : 0);

            var integerLenSize = NumUtils.CountDigits(integerLen + sign);

            //$size\r\ninteger\r\n
            var totalLen = 1 + integerLenSize + 2 + sign + integerLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.WriteInt32(integerLen + sign, integerLenSize, ref curr);
            WriteNewline(ref curr);
            NumUtils.WriteInt32(value, integerLen, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Write a signed 64-bit integer as bulk string
        /// </summary>
        public static bool TryWriteInt64AsBulkString(long integer, ref byte* curr, byte* end, out int totalLen)
        {
            var integerLen = NumUtils.CountDigits(integer);
            var sign = (byte)(integer < 0 ? 1 : 0);

            var integerLenSize = NumUtils.CountDigits(integerLen + sign);

            //$size\r\ninteger\r\n
            totalLen = 1 + integerLenSize + 2 + sign + integerLen + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.WriteInt32(integerLen + sign, integerLenSize, ref curr);
            WriteNewline(ref curr);
            NumUtils.WriteInt64(integer, integerLen, ref curr);
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Get length of integer as bulk string
        /// </summary>
        public static int GetIntegerAsBulkStringLength(int integer)
        {
            var integerLen = NumUtils.CountDigits((long)integer);
            var sign = (byte)(integer < 0 ? 1 : 0);

            var integerLenSize = NumUtils.CountDigits(integerLen + sign);

            //$size\r\ninteger\r\n
            return 1 + integerLenSize + 2 + sign + integerLen + 2;
        }

        /// <summary>
        /// Try to write a double-precision floating-point <paramref name="value"/> as bulk string.
        /// </summary>
        /// <returns><see langword="true"/> if the <paramref name="value"/> could be written to <paramref name="curr"/>; <see langword="false"/> otherwise.</returns>
        [SkipLocalsInit]
        public static bool TryWriteDoubleBulkString(double value, ref byte* curr, byte* end)
        {
            if (double.IsNaN(value))
            {
                return TryWriteNaN(value, ref curr, end);
            }
            else if (double.IsInfinity(value))
            {
                return TryWriteInfinity(value, ref curr, end);
            }

            Span<byte> buffer = stackalloc byte[32];
            if (!Utf8Formatter.TryFormat(value, buffer, out var bytesWritten, format: default))
                return false;

            var itemDigits = NumUtils.CountDigits(bytesWritten);
            int totalLen = 1 + itemDigits + 2 + bytesWritten + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'$';
            NumUtils.WriteInt32(bytesWritten, itemDigits, ref curr);
            WriteNewline(ref curr);
            buffer.Slice(0, bytesWritten).CopyTo(new Span<byte>(curr, bytesWritten));
            curr += bytesWritten;
            WriteNewline(ref curr);
            return true;
        }

        /// <summary>
        /// Try to write a double-precision floating-point <paramref name="value"/> as bulk string.
        /// </summary>
        /// <returns><see langword="true"/> if the <paramref name="value"/> could be written to <paramref name="curr"/>; <see langword="false"/> otherwise.</returns>
        [SkipLocalsInit]
        public static bool TryWriteDoubleNumeric(double value, ref byte* curr, byte* end)
        {
            if (double.IsNaN(value))
            {
                return TryWriteNaN_Numeric(value, ref curr, end);
            }
            else if (double.IsInfinity(value))
            {
                return TryWriteInfinity_Numeric(value, ref curr, end);
            }

            Span<byte> buffer = stackalloc byte[32];
            if (!Utf8Formatter.TryFormat(value, buffer, out var bytesWritten, format: default))
                return false;

            var itemDigits = NumUtils.CountDigits(bytesWritten);
            int totalLen = 1 + bytesWritten + 2;
            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)',';
            buffer.Slice(0, bytesWritten).CopyTo(new Span<byte>(curr, bytesWritten));
            curr += bytesWritten;
            WriteNewline(ref curr);
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static bool TryWriteInfinity(double value, ref byte* curr, byte* end)
        {
            var buffer = new Span<byte>(curr, (int)(end - curr));
            if (double.IsPositiveInfinity(value))
            {
                if (!"$4\r\n+inf\r\n"u8.TryCopyTo(buffer))
                    return false;
            }
            else
            {
                if (!"$4\r\n-inf\r\n"u8.TryCopyTo(buffer))
                    return false;
            }

            curr += 10;
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static bool TryWriteInfinity_Numeric(double value, ref byte* curr, byte* end)
        {
            var buffer = new Span<byte>(curr, (int)(end - curr));
            if (double.IsPositiveInfinity(value))
            {
                if (!",+inf\r\n"u8.TryCopyTo(buffer))
                    return false;
            }
            else
            {
                if (!",-inf\r\n"u8.TryCopyTo(buffer))
                    return false;
            }

            curr += 7;
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static bool TryWriteNaN(double value, ref byte* curr, byte* end)
        {
            var buffer = new Span<byte>(curr, (int)(end - curr));
            if (!"$3\r\nnan\r\n"u8.TryCopyTo(buffer))
                return false;
            curr += 9;
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static bool TryWriteNaN_Numeric(double value, ref byte* curr, byte* end)
        {
            var buffer = new Span<byte>(curr, (int)(end - curr));
            if (!",nan\r\n"u8.TryCopyTo(buffer))
                return false;
            curr += 6;
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
        public static bool TryWriteScanOutputHeader(long cursor, ref byte* curr, byte* end)
        {
            if (!TryWriteArrayLength(2, ref curr, end))
                return false;

            // Cursor value
            if (!TryWriteInt32AsBulkString((int)cursor, ref curr, end))
                return false;

            return true;
        }

        /// <summary>
        /// Write empty array
        /// </summary>
        public static bool TryWriteEmptyArray(ref byte* curr, byte* end)
        {
            if (4 > (int)(end - curr))
                return false;

            WriteBytes<uint>(ref curr, "*0\r\n"u8);
            return true;
        }

        /// <summary>
        /// Write empty set
        /// </summary>
        public static bool TryWriteEmptySet(ref byte* curr, byte* end)
        {
            if (4 > (int)(end - curr))
                return false;

            WriteBytes<uint>(ref curr, "~0\r\n"u8);
            return true;
        }

        /// <summary>
        /// Write RESP3 true
        /// </summary>
        public static bool TryWriteTrue(ref byte* curr, byte* end)
        {
            if (4 > (int)(end - curr))
                return false;

            WriteBytes<uint>(ref curr, "#t\r\n"u8);
            return true;
        }

        /// <summary>
        /// Write RESP3 false
        /// </summary>
        public static bool TryWriteFalse(ref byte* curr, byte* end)
        {
            if (4 > (int)(end - curr))
                return false;

            WriteBytes<uint>(ref curr, "#f\r\n"u8);
            return true;
        }

        /// <summary>
        /// Write an array with len number of null elements
        /// </summary>
        public static bool TryWriteArrayWithNullElements(int len, ref byte* curr, byte* end)
        {
            var numDigits = NumUtils.CountDigits(len);
            var totalLen = 1 + numDigits + 2;
            totalLen += len * 5; // 5 is the length of $-1\r\n

            if (totalLen > (int)(end - curr))
                return false;

            *curr++ = (byte)'*';
            NumUtils.WriteInt32(len, numDigits, ref curr);
            WriteNewline(ref curr);
            for (var i = 0; i < len; i++)
            {
                if (!TryWriteNull(ref curr, end))
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Writes an array consisting of an ETag followed by a Bulk string value into the buffer.
        /// NOTE: Caller should make sure there is enough space in the buffer for sending the etag, and value array. Otherwise, this will quietly fail.
        /// </summary>
        /// <param name="etag">etag value to write in the array</param>
        /// <param name="value">value to write in the array</param>
        /// <param name="curr">start of destination buffer</param>
        /// <param name="end">end of destincation buffer</param>
        /// <param name="writeDirect">Whether to write the value directly to buffer or transform it to a resp bulk string</param>
        public static void WriteEtagValArray(long etag, ref ReadOnlySpan<byte> value, ref byte* curr, byte* end, bool writeDirect)
        {
            // Writes a Resp encoded Array of Integer for ETAG as first element, and bulk string for value as second element
            RespWriteUtils.TryWriteArrayLength(2, ref curr, end);
            RespWriteUtils.TryWriteInt64(etag, ref curr, end);

            if (writeDirect)
                RespWriteUtils.TryWriteDirect(value, ref curr, end);
            else
                RespWriteUtils.TryWriteBulkString(value, ref curr, end);
        }

        /// <summary>
        /// Writes newline (\r\n) to <paramref name="curr"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void WriteNewline(ref byte* curr) => WriteBytes<ushort>(ref curr, "\r\n"u8);

        /// <summary>
        /// Writes <paramref name="bytes"/> to <paramref name="curr"/> as type <typeparamref name="T"/> sized value.
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