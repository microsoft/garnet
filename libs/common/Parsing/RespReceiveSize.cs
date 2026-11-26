// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.common.Parsing
{
    /// <summary>
    /// Reads how large the RESP message at the head of a receive buffer is, from the lengths its elements declare.
    /// </summary>
    public static unsafe class RespReceiveSize
    {
        /// <summary>
        /// Lower bound on the bytes, counted from <paramref name="buffer"/>, needed to receive the RESP data at its head: the end of the first
        /// length-prefixed element whose payload runs past the buffer. Elements are walked in order without tracking aggregate structure, since
        /// only the first incomplete one matters.
        /// </summary>
        /// <param name="buffer">Unparsed bytes</param>
        /// <param name="length">Number of bytes at <paramref name="buffer"/></param>
        /// <returns>The bound, or zero if every element is complete, an element header is incomplete, an element declares a length longer than
        /// <see cref="RespLimits.MaxArgumentLengthBytes"/>, which no message can be parsed with, or the bytes are not RESP (such as an inline
        /// command).</returns>
        public static long GetRequiredBytes(byte* buffer, int length)
        {
            var ptr = buffer;
            var end = buffer + length;
            while (ptr < end)
            {
                switch (*ptr)
                {
                    // Bulk string, verbatim string, bulk error: a length line, then the payload and CRLF
                    case (byte)'$':
                    case (byte)'=':
                    case (byte)'!':
                        if (!TryReadLengthLine(ptr + 1, end, out var payloadLength, out var payload) || payloadLength > RespLimits.MaxArgumentLengthBytes)
                            return 0;
                        if (payloadLength < 0)
                        {
                            // Null bulk string
                            ptr = payload;
                            break;
                        }
                        var elementEnd = payload - buffer + payloadLength + 2;
                        if (elementEnd > length)
                            return elementEnd;
                        ptr = buffer + elementEnd;
                        break;

                    // Aggregate headers and simple types: a single CRLF-terminated line
                    case (byte)'*':
                    case (byte)'%':
                    case (byte)'~':
                    case (byte)'>':
                    case (byte)'|':
                    case (byte)'+':
                    case (byte)'-':
                    case (byte)':':
                    case (byte)'_':
                    case (byte)'#':
                    case (byte)',':
                    case (byte)'(':
                        var lineLength = new ReadOnlySpan<byte>(ptr, (int)(end - ptr)).IndexOf((byte)'\n');
                        if (lineLength < 0)
                            return 0;
                        ptr += lineLength + 1;
                        break;

                    default:
                        return 0;
                }
            }
            return 0;
        }

        /// <summary>
        /// Read a CRLF-terminated decimal length. Fails on a header that is incomplete, malformed, or too long for any length the receive buffer
        /// can hold, all of which the parser deals with.
        /// </summary>
        static bool TryReadLengthLine(byte* ptr, byte* end, out long value, out byte* next)
        {
            const int MaxDigits = 18;

            value = 0;
            next = null;
            var negative = ptr < end && *ptr == '-';
            if (negative)
                ptr++;

            var digits = 0;
            while (ptr < end && *ptr >= '0' && *ptr <= '9')
            {
                if (++digits > MaxDigits)
                    return false;
                value = value * 10 + (*ptr - '0');
                ptr++;
            }

            if (digits == 0 || end - ptr < 2 || ptr[0] != '\r' || ptr[1] != '\n')
                return false;

            if (negative)
                value = -value;
            next = ptr + 2;
            return true;
        }
    }
}