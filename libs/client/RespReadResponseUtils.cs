// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Garnet.common.Parsing;

namespace Garnet.client
{
    public static unsafe class RespReadResponseUtils
    {
        /// <inheritdoc cref="RespReadUtils.TryReadSimpleString(out string, ref byte*, byte*)"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadSimpleString(out string result, ref byte* ptr, byte* end)
            => RespReadUtils.TryReadSimpleString(out result, ref ptr, end);

        /// <summary>
        /// Read simple string
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadSimpleString(MemoryPool<byte> pool, out MemoryResult<byte> result, ref byte* ptr, byte* end)
        {
            result = default;
            if (ptr + 2 >= end)
                return false;

            // Simple strings need to start with a '+'
            if (*ptr != '+')
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            ptr++;

            return TryReadString(pool, out result, ref ptr, end);
        }

        /// <inheritdoc cref="RespReadUtils.TryReadIntegerAsString(out string, ref byte*, byte*)"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadIntegerAsString(out string result, ref byte* ptr, byte* end)
            => RespReadUtils.TryReadIntegerAsString(out result, ref ptr, end);

        /// <summary>
        /// Read integer as string
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadIntegerAsString(MemoryPool<byte> pool, out MemoryResult<byte> result, ref byte* ptr, byte* end)
        {
            result = default;
            if (ptr + 2 >= end)
                return false;

            // Integer strings need to start with a ':'
            if (*ptr != ':')
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            ptr++;

            return TryReadString(pool, out result, ref ptr, end);
        }

        /// <summary>
        /// Try to read a RESP formatted bulk string
        /// NOTE: This is used with client implementation to parse responses that may include a null value (i.e. $-1\r\n)
        /// </summary>
        /// <param name="result"></param>
        /// <param name="ptr"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadStringWithLengthHeader(out string result, ref byte* ptr, byte* end)
        {
            result = null;

            byte* keyPtr = null;
            var length = 0;
            if (!TryReadPtrWithSignedLengthHeader(ref keyPtr, ref length, ref ptr, end))
                return false;

            if (length < 0)
                return true;

            result = Encoding.UTF8.GetString(new ReadOnlySpan<byte>(keyPtr, length));
            return true;
        }

        /// <summary>
        /// Read string with length header
        /// </summary>
        /// <param name="pool">Memory pool to rent space for storing the result.</param>
        /// <param name="result">If parsing was successful, contains the extracted byte sequence.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP string was successfully read.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadStringWithLengthHeader(MemoryPool<byte> pool, out MemoryResult<byte> result, ref byte* ptr, byte* end)
        {
            result = default;

            byte* keyPtr = null;
            var length = 0;
            if (!TryReadPtrWithSignedLengthHeader(ref keyPtr, ref length, ref ptr, end))
                return false;

            if (length < 0)
                return true;

            result = MemoryResult<byte>.Create(pool, length);
            new ReadOnlySpan<byte>(keyPtr, length).CopyTo(result.Span);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool TryReadPtrWithSignedLengthHeader(ref byte* keyPtr, ref int length, ref byte* ptr, byte* end)
        {
            // Parse RESP string header
            if (!RespReadUtils.TryReadSignedLengthHeader(out length, ref ptr, end))
            {
                return false;
            }

            // A reply that can never be accepted is a protocol error, not a reason to wait for more bytes
            if (length > RespReadUtils.MaxArgumentLengthBytes)
            {
                RespParsingException.ThrowInvalidStringLength(length);
            }

            // Allow for null
            if (length < 0)
            {
                // NULL value ('$-1\r\n')
                keyPtr = null;
                return true;
            }

            keyPtr = ptr;

            // Parse content: ensure that input contains key + '\r\n'
            ptr += length + 2;
            if (ptr > end)
            {
                return false;
            }

            if (*(ushort*)(ptr - 2) != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*(ptr - 2));
            }

            return true;
        }

        /// <inheritdoc cref="RespReadUtils.TryReadErrorAsString(out string, ref byte*, byte*)"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadErrorAsString(out string error, ref byte* ptr, byte* end)
            => RespReadUtils.TryReadErrorAsString(out error, ref ptr, end);

        /// <summary>
        /// Read string array with length header. Nested arrays are flattened into a comma-separated string.
        /// </summary>
        /// <remarks>
        /// Nothing is allocated until <see cref="RespReadUtils.TrySkipArrayReply"/> has confirmed that the whole
        /// reply has been received, so a reply still arriving costs no allocation however many receives it
        /// spans, and arrays are sized only from counts the received bytes back. <paramref name="ptr"/> is
        /// advanced only when the whole reply has been read.
        /// </remarks>
        /// <exception cref="RespParsingException">Thrown if the reply is malformed, declares an element count
        /// that can never be received, or nests deeper than <see cref="RespReadUtils.MaxNestingDepth"/>.</exception>
        public static bool TryReadStringArrayWithLengthHeader(out string[] result, ref byte* ptr, byte* end)
        {
            result = null;

            var cur = ptr;
            if (!RespReadUtils.TrySkipArrayReply(ref cur, end, allowNestedArrays: true))
            {
                return false;
            }

            var replyEnd = cur;
            cur = ptr;
            if (!TryReadStringArray(out result, ref cur, replyEnd))
            {
                Debug.Fail("A validated reply must read in full");
                return false;
            }

            Debug.Assert(cur == replyEnd, "A validated reply must read in full");
            ptr = cur;
            return true;
        }

        static bool TryReadStringArray(out string[] result, ref byte* ptr, byte* end)
        {
            result = null;

            if (!RespReadUtils.TryReadSignedArrayLength(out var length, ref ptr, end))
            {
                return false;
            }

            if (length < 0)
            {
                // NULL value ('*-1\r\n')
                return true;
            }

            var elements = new string[length];
            Stack<(string[] Elements, int Index)> parents = null;
            var i = 0;
            while (true)
            {
                if (i == elements.Length)
                {
                    if (parents == null || parents.Count == 0)
                    {
                        result = elements;
                        return true;
                    }

                    var flattened = string.Join(", ", elements);
                    (elements, i) = parents.Pop();
                    elements[i++] = flattened;
                    continue;
                }

                if (ptr >= end)
                {
                    return false;
                }

                if (*ptr == '$')
                {
                    if (!TryReadStringWithLengthHeader(out elements[i], ref ptr, end))
                        return false;
                }
                else if (*ptr == '+')
                {
                    if (!TryReadSimpleString(out elements[i], ref ptr, end))
                        return false;
                }
                else if (*ptr == '*')
                {
                    if (!RespReadUtils.TryReadSignedArrayLength(out var nestedLength, ref ptr, end))
                        return false;
                    if (nestedLength < 0)
                    {
                        elements[i++] = null;
                        continue;
                    }

                    parents ??= new Stack<(string[] Elements, int Index)>();
                    parents.Push((elements, i));
                    elements = new string[nestedLength];
                    i = 0;
                    continue;
                }
                else
                {
                    if (!TryReadIntegerAsString(out elements[i], ref ptr, end))
                        return false;
                }
                i++;
            }
        }

        /// <summary>
        /// Read string array with length header
        /// </summary>
        /// <remarks>
        /// Nothing is rented or allocated until <see cref="RespReadUtils.TrySkipArrayReply"/> has confirmed that
        /// the whole reply has been received, so a reply still arriving costs no allocation however many receives
        /// it spans, and the array is sized only from a count the received bytes back. <paramref name="ptr"/> is
        /// advanced only when the whole reply has been read.
        /// </remarks>
        /// <exception cref="RespParsingException">Thrown if the reply is malformed, contains a nested array, or
        /// declares an element count that can never be received.</exception>
        public static bool TryReadStringArrayWithLengthHeader(MemoryPool<byte> pool, out MemoryResult<byte>[] result, ref byte* ptr, byte* end)
        {
            result = null;

            var cur = ptr;
            if (!RespReadUtils.TrySkipArrayReply(ref cur, end, allowNestedArrays: false))
            {
                return false;
            }

            var replyEnd = cur;
            cur = ptr;
            if (!RespReadUtils.TryReadSignedArrayLength(out var length, ref cur, replyEnd))
            {
                return false;
            }

            if (length < 0)
            {
                // NULL value ('*-1\r\n')
                ptr = cur;
                return true;
            }

            var elements = new MemoryResult<byte>[length];
            var parsed = 0;
            try
            {
                for (; parsed < length; parsed++)
                {
                    if (cur >= replyEnd)
                    {
                        return false;
                    }

                    if (*cur == '$')
                    {
                        if (!TryReadStringWithLengthHeader(pool, out elements[parsed], ref cur, replyEnd))
                            return false;
                    }
                    else if (*cur == '+')
                    {
                        if (!TryReadSimpleString(pool, out elements[parsed], ref cur, replyEnd))
                            return false;
                    }
                    else
                    {
                        if (!TryReadIntegerAsString(pool, out elements[parsed], ref cur, replyEnd))
                            return false;
                    }
                }
            }
            finally
            {
                // Return the buffers rented so far if the reply could not be read in full
                if (parsed < length)
                {
                    for (var i = 0; i < parsed; i++)
                        elements[i].Dispose();
                }
            }

            Debug.Assert(cur == replyEnd, "A validated reply must read in full");
            result = elements;
            ptr = cur;
            return true;
        }

        /// <inheritdoc cref="RespReadUtils.TryReadInt32WithLengthHeader(out int, ref byte*, byte*)"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadIntWithLengthHeader(out int number, ref byte* ptr, byte* end)
            => RespReadUtils.TryReadInt32WithLengthHeader(out number, ref ptr, end);

        /// <summary>
        /// Read ASCII string without header until string terminator ('\r\n').
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadString(MemoryPool<byte> pool, out MemoryResult<byte> result, ref byte* ptr, byte* end)
        {
            result = default;
            if (ptr + 1 >= end)
                return false;

            var start = ptr;
            while (ptr < end - 1)
            {
                if (*(ushort*)ptr == MemoryMarshal.Read<ushort>("\r\n"u8))
                {
                    result = MemoryResult<byte>.Create(pool, (int)(ptr - start));
                    new ReadOnlySpan<byte>(start, result.Length).CopyTo(result.Span);
                    ptr += 2;
                    return true;
                }
                ptr++;
            }

            return false;
        }
    }
}