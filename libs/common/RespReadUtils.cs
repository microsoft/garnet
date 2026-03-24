// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common.Parsing;
using Tsavorite.core;

namespace Garnet.common
{
    /// <summary>
    /// Utilities for reading RESP protocol messages.
    /// </summary>
    public static unsafe class RespReadUtils
    {
        /// <summary>
        /// Tries to read the leading sign of the given ASCII-encoded number.
        /// </summary>
        /// <param name="ptr">String to try reading sign from.</param>
        /// <param name="negative">Whether the sign is '-'.</param>
        /// <returns>True if either '+' or '-' was found, false otherwise.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryReadSign(byte* ptr, out bool negative)
        {
            negative = (*ptr == '-');
            return negative || (*ptr == '+');
        }

        /// <summary>
        /// Tries to read an unsigned 64-bit integer from a given ASCII-encoded input stream.
        /// The input may include leading zeros.
        /// </summary>
        /// <param name="ptr">Pointer to the beginning of the ASCII encoded input string.</param>
        /// <param name="end">The end of the string to parse.</param>
        /// <param name="value">If parsing was successful, contains the parsed ulong value.</param>
        /// <param name="bytesRead">If parsing was successful, contains the number of bytes that were parsed.</param>
        /// <returns>
        /// True if a ulong was successfully parsed, false if the input string did not start with
        /// a valid integer or the end of the string was reached before finishing parsing.
        /// </returns>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryReadUInt64(ref byte* ptr, byte* end, out ulong value, out ulong bytesRead)
        {
            bytesRead = 0;
            value = 0;
            var readHead = ptr;

            // Fast path for the first 19 digits.
            // NOTE: UINT64 overflows can only happen on digit 20 or later (if integer contains leading zeros).
            var fastPathEnd = ptr + 19;
            while (readHead < fastPathEnd)
            {
                if (readHead > end)
                {
                    return false;
                }

                var nextDigit = (uint)(*readHead - '0');
                if (nextDigit > 9 || readHead == end)
                {
                    goto Done;
                }

                value = (10 * value) + nextDigit;

                readHead++;
            }

            // Parse remaining digits, while checking for overflows.
            while (true)
            {
                if (readHead > end)
                {
                    return false;
                }

                var nextDigit = (uint)(*readHead - '0');
                if (nextDigit > 9 || readHead == end)
                {
                    goto Done;
                }

                if ((value == 1844674407370955161UL && ((int)nextDigit > 5)) || (value > 1844674407370955161UL))
                {
                    RespParsingException.ThrowIntegerOverflow(ptr, (int)(readHead - ptr));
                }

                value = (10 * value) + nextDigit;

                readHead++;
            }

        Done:
            bytesRead = (ulong)(readHead - ptr);
            ptr = readHead;

            return true;
        }

        /// <summary>
        /// Tries to read a signed 64-bit integer from a given ASCII-encoded input stream.
        /// This method will throw if an overflow occurred.
        /// </summary>
        /// <param name="ptr">Pointer to the beginning of the ASCII encoded input string.</param>
        /// <param name="end">The end of the string to parse.</param>
        /// <param name="value">If parsing was successful, contains the parsed long value.</param>
        /// <param name="bytesRead">If parsing was successful, contains the number of bytes that were parsed.</param>
        /// <param name="allowLeadingZeros">True if leading zeros allowed</param>
        /// <returns>
        /// True if a long was successfully parsed, false if the input string did not start with
        /// a valid integer or the end of the string was reached before finishing parsing.
        /// </returns>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadInt64(ref byte* ptr, byte* end, out long value, out ulong bytesRead, bool allowLeadingZeros = true)
        {
            if (TryReadInt64Safe(ref ptr, end, out value, out bytesRead, out var signRead,
                    out var overflow, allowLeadingZeros))
                return true;

            if (overflow)
            {
                var digitsRead = signRead ? bytesRead - 1 : bytesRead;
                RespParsingException.ThrowIntegerOverflow(ptr - digitsRead, (int)digitsRead);
                return false;
            }

            return false;
        }

        /// <summary>
        /// Tries to read a signed 64-bit integer from a given ASCII-encoded input stream.
        /// </summary>
        /// <param name="ptr">Pointer to the beginning of the ASCII encoded input string.</param>
        /// <param name="end">The end of the string to parse.</param>
        /// <param name="value">If parsing was successful, contains the parsed long value.</param>
        /// <param name="bytesRead">If parsing was successful, contains the number of bytes that were parsed.</param>
        /// <param name="signRead">True if +/- sign was read during parsing</param>
        /// <param name="overflow">True if overflow occured during parsing</param>
        /// <param name="allowLeadingZeros">True if leading zeros allowed</param>
        /// <returns>
        /// True if a long was successfully parsed, false if the input string did not start with
        /// a valid integer or the end of the string was reached before finishing parsing.
        /// </returns>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadInt64Safe(ref byte* ptr, byte* end, out long value, out ulong bytesRead, out bool signRead, out bool overflow, bool allowLeadingZeros = true)
        {
            bytesRead = 0;
            value = 0;
            overflow = false;

            // Parse optional leading sign
            signRead = TryReadSign(ptr, out var negative);
            if (signRead)
            {
                ptr++;
                bytesRead = 1;
            }

            if (!allowLeadingZeros)
            {
                // Do not allow leading zeros
                if (end - ptr > 1 && *ptr == '0')
                    return false;
            }

            // Parse digits as ulong
            if (!TryReadUInt64(ref ptr, end, out var number, out var digitsRead))
            {
                return false;
            }

            // Check for overflows and convert digits to long, if possible
            if (negative)
            {
                if (number > ((ulong)long.MaxValue) + 1)
                {
                    overflow = true;
                    return false;
                }

                value = -1 - (long)(number - 1);
            }
            else
            {
                if (number > long.MaxValue)
                {
                    overflow = true;
                    return false;
                }
                value = (long)number;
            }

            bytesRead += digitsRead;

            return true;
        }

        /// <summary>
        /// Tries to read a signed 32-bit integer from a given ASCII-encoded input stream.
        /// This method will throw if an overflow occurred.
        /// </summary>
        /// <param name="ptr">Pointer to the beginning of the ASCII encoded input string.</param>
        /// <param name="end">The end of the string to parse.</param>
        /// <param name="value">If parsing was successful, contains the parsed int value.</param>
        /// <param name="bytesRead">If parsing was successful, contains the number of bytes that were parsed.</param>
        /// <param name="allowLeadingZeros">True if leading zeros allowed</param>
        /// <returns>
        /// True if an int was successfully parsed, false if the input string did not start with
        /// a valid integer or the end of the string was reached before finishing parsing.
        /// </returns>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadInt32(ref byte* ptr, byte* end, out int value, out ulong bytesRead, bool allowLeadingZeros = true)
        {
            if (TryReadInt32Safe(ref ptr, end, out value, out bytesRead, out var signRead,
                    out var overflow, allowLeadingZeros))
                return true;

            if (overflow)
            {
                var digitsRead = signRead ? bytesRead - 1 : bytesRead;
                RespParsingException.ThrowIntegerOverflow(ptr - digitsRead, (int)digitsRead);
                return false;
            }

            return false;
        }

        /// <summary>
        /// Tries to read a signed 32-bit integer from a given ASCII-encoded input stream.
        /// </summary>
        /// <param name="ptr">Pointer to the beginning of the ASCII encoded input string.</param>
        /// <param name="end">The end of the string to parse.</param>
        /// <param name="value">If parsing was successful, contains the parsed int value.</param>
        /// <param name="bytesRead">If parsing was successful, contains the number of bytes that were parsed.</param>
        /// <param name="signRead">True if +/- sign was read during parsing</param>
        /// <param name="overflow">True if overflow occured during parsing</param>
        /// <param name="allowLeadingZeros">True if leading zeros allowed</param>
        /// <returns>
        /// True if an int was successfully parsed, false if the input string did not start with
        /// a valid integer or the end of the string was reached before finishing parsing.
        /// </returns>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadInt32Safe(ref byte* ptr, byte* end, out int value, out ulong bytesRead, out bool signRead, out bool overflow, bool allowLeadingZeros = true)
        {
            bytesRead = 0;
            value = 0;
            overflow = false;

            // Parse optional leading sign
            signRead = TryReadSign(ptr, out var negative);
            if (signRead)
            {
                ptr++;
                bytesRead = 1;
            }

            // Parse digits as unsigned 64-bit integer
            if (!TryReadUInt64(ref ptr, end, out var number, out var digitsRead))
            {
                return false;
            }

            // Check for overflows and convert digits to int, if possible
            if (negative)
            {
                if (number > ((ulong)int.MaxValue) + 1)
                {
                    overflow = true;
                    return false;
                }

                value = (int)(0 - (long)number);
            }
            else
            {
                if (number > int.MaxValue)
                {
                    overflow = true;
                    return false;
                }
                value = (int)number;
            }

            bytesRead += digitsRead;

            return true;
        }

        /// <summary>
        /// Tries to read a RESP length header from the given ASCII-encoded RESP string
        /// and, if successful, moves the given ptr to the end of the length header.
        /// <para />
        /// NOTE:
        ///     It will throw an <see cref="RespParsingException"/> if length header is negative.
        ///     It is primarily used for parsing header length from packets received from server side.
        /// </summary>
        /// <param name="length">If parsing was successful, contains the extracted length from the header.</param>
        /// <param name="ptr">The starting position in the RESP string. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP string.</param>
        /// <param name="expectedSigil">Expected type of RESP header, defaults to string ('$').</param>
        /// <returns>True if a length header was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadUnsignedLengthHeader(out int length, ref byte* ptr, byte* end, char expectedSigil = '$')
        {
            length = -1;
            if (ptr + 3 > end)
                return false;

            if (!TryReadSignedLengthHeader(out length, ref ptr, end, expectedSigil))
                return false;

            if (length < 0)
            {
                RespParsingException.ThrowInvalidStringLength(length);
            }

            return true;
        }

        /// <summary>
        /// Tries to read a RESP a signed length header from the given ASCII-encoded RESP string
        /// and, if successful, moves the given ptr to the end of the length header.
        /// <para />
        /// NOTE:
        ///     It will not throw an exception if length header is negative.
        ///     It is primarily used by client side code.
        ///     Should not be called by any server code since server side does not accept null values
        /// </summary>
        /// <param name="length">If parsing was successful, contains the extracted length from the header.</param>
        /// <param name="ptr">The starting position in the RESP string. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP string.</param>
        /// <param name="expectedSigil">Expected type of RESP header, defaults to string ('$').</param>
        /// <returns>True if a length header was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadSignedLengthHeader(out int length, ref byte* ptr, byte* end, char expectedSigil = '$')
        {
            length = -1;
            if (ptr + 3 > end)
                return false;

            var readHead = ptr + 1;
            var negative = *readHead == '-';

            // Special case '_\r\n' (RESP3 NULL value)
            if (*ptr == '_')
            {
                // The initial condition ensures at least two more bytes so no need for extra check.
                if (*(ushort*)readHead == MemoryMarshal.Read<ushort>("\r\n"u8))
                {
                    length = -1;
                    ptr = readHead + 2;
                    return true;
                }
            }

            // String length headers must start with a '$', array headers with '*'
            if (*ptr != expectedSigil)
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            // Special case: '$-1' (NULL value)
            if (negative)
            {
                if (readHead + 4 > end)
                {
                    return false;
                }

                if (*(uint*)readHead == MemoryMarshal.Read<uint>("-1\r\n"u8))
                {
                    ptr = readHead + 4;
                    return true;
                }
                readHead++;
            }

            // Parse length
            if (!TryReadUInt64(ref readHead, end, out var value, out var digitsRead))
            {
                return false;
            }

            if (digitsRead == 0)
            {
                RespParsingException.ThrowUnexpectedToken(*readHead);
            }

            // Validate length
            if (value > int.MaxValue && (!negative || value > int.MaxValue + (ulong)1)) // int.MinValue = -(int.MaxValue + 1)
            {
                RespParsingException.ThrowIntegerOverflow(readHead - digitsRead, (int)digitsRead);
            }

            // Convert to signed value
            length = negative ? -(int)value : (int)value;

            // Ensure terminator has been received
            ptr = readHead + 2;
            if (ptr > end)
            {
                return false;
            }

            if (*(ushort*)readHead != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            return true;
        }

        /// <summary>
        /// Read signed 64 bit integer
        /// </summary>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        public static bool TryReadInt64(out long number, ref byte* ptr, byte* end)
        {
            var success = TryReadInt64(out number, ref ptr, end, out var unexpectedToken);

            if (!success && unexpectedToken.HasValue)
            {
                RespParsingException.ThrowUnexpectedToken(unexpectedToken.Value);
            }

            return success;
        }

        /// <summary>
        /// Try read signed 64 bit integer
        /// </summary>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TryReadInt64(out long number, ref byte* ptr, byte* end, out byte? unexpectedToken)
        {
            number = 0;
            unexpectedToken = null;

            if (ptr + 3 >= end)
                return false;

            // Integer header must start with ':'
            if (*ptr != ':')
            {
                unexpectedToken = *ptr;
                return false;
            }

            ptr++;

            // Parse length
            if (!TryReadInt64(ref ptr, end, out number, out _))
            {
                return false;
            }

            // Ensure terminator has been received
            ptr += 2;
            if (ptr > end)
            {
                return false;
            }

            if (*(ushort*)(ptr - 2) != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                unexpectedToken = *ptr;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Tries to read a RESP array length header from the given ASCII-encoded RESP string
        /// and, if successful, moves the given ptr to the end of the length header.
        /// <para />
        /// NOTE: We use ReadUnsignedLengthHeader because server does not accept $-1\r\n headers
        /// </summary>
        /// <param name="length">If parsing was successful, contains the extracted length from the header.</param>
        /// <param name="ptr">The starting position in the RESP string. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP string.</param>
        /// <returns>True if a length header was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TryReadUnsignedArrayLength(out int length, ref byte* ptr, byte* end)
            => TryReadUnsignedLengthHeader(out length, ref ptr, end, expectedSigil: '*');

        /// <summary>
        /// Tries to read a RESP array length header from the given ASCII-encoded RESP string
        /// and, if successful, moves the given ptr to the end of the length header.
        /// <para />
        /// NOTE: It will not throw an exception if length header is negative.
        /// </summary>
        /// <param name="length">If parsing was successful, contains the extracted length from the header.</param>
        /// <param name="ptr">The starting position in the RESP string. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP string.</param>
        /// <returns>True if a length header was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TryReadSignedArrayLength(out int length, ref byte* ptr, byte* end)
            => TryReadSignedLengthHeader(out length, ref ptr, end, expectedSigil: '*');

        /// <summary>
        /// Tries to read a RESP3 map length header from the given ASCII-encoded RESP string
        /// and, if successful, moves the given ptr to the end of the length header.
        /// <para />
        /// NOTE: It will not throw an exception if length header is negative.
        /// </summary>
        /// <param name="length">If parsing was successful, contains the extracted length from the header.</param>
        /// <param name="ptr">The starting position in the RESP string. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP string.</param>
        /// <returns>True if a length header was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TryReadSignedMapLength(out int length, ref byte* ptr, byte* end)
            => TryReadSignedLengthHeader(out length, ref ptr, end, expectedSigil: '%');

        /// <summary>
        /// Tries to read a RESP3 set length header from the given ASCII-encoded RESP string
        /// and, if successful, moves the given ptr to the end of the length header.
        /// <para />
        /// NOTE: It will not throw an exception if length header is negative.
        /// </summary>
        /// <param name="length">If parsing was successful, contains the extracted length from the header.</param>
        /// <param name="ptr">The starting position in the RESP string. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP string.</param>
        /// <returns>True if a length header was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TryReadSignedSetLength(out int length, ref byte* ptr, byte* end)
            => TryReadSignedLengthHeader(out length, ref ptr, end, expectedSigil: '~');

        /// <summary>
        /// Tries to read a RESP3 verbatim string length header from the given ASCII-encoded RESP string
        /// and, if successful, moves the given ptr to the end of the length header.
        /// <para />
        /// NOTE: It will not throw an exception if length header is negative.
        /// </summary>
        /// <param name="length">If parsing was successful, contains the extracted length from the header.</param>
        /// <param name="ptr">The starting position in the RESP string. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP string.</param>
        /// <returns>True if a length header was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TryReadVerbatimStringLength(out int length, ref byte* ptr, byte* end)
            => TryReadSignedLengthHeader(out length, ref ptr, end, expectedSigil: '=');

        /// <summary>
        /// Reads a signed 32-bit integer with length header
        /// </summary>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        /// <exception cref="RespParsingException">Thrown if not a number is read.</exception>
        public static bool TryReadInt32WithLengthHeader(out int number, ref byte* ptr, byte* end)
        {
            number = 0;

            // Parse RESP string header
            if (!TryReadUnsignedLengthHeader(out var numberLength, ref ptr, end))
                return false;

            if (ptr + numberLength + 2 > end)
                return false;

            // Parse associated integer value
            var numberStart = ptr;
            if (!TryReadInt32(ref ptr, end, out number, out var bytesRead))
            {
                return false;
            }

            if ((int)bytesRead != numberLength)
            {
                RespParsingException.ThrowNotANumber(numberStart, numberLength);
            }

            // Ensure terminator has been received
            if (*(ushort*)ptr != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            ptr += 2;

            return true;
        }

        /// <summary>
        /// Read a signed 64-bit integer with length header
        /// </summary>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        /// <exception cref="RespParsingException">Thrown if not a number is read.</exception>
        public static bool TryReadInt64WithLengthHeader(out long number, ref byte* ptr, byte* end)
        {
            number = 0;

            // Parse RESP string header
            if (!TryReadUnsignedLengthHeader(out var numberLength, ref ptr, end))
                return false;

            if (ptr + numberLength + 2 > end)
                return false;

            // Parse associated integer value
            var numberStart = ptr;
            if (!TryReadInt64(ref ptr, end, out number, out var bytesRead))
            {
                return false;
            }

            if ((int)bytesRead != numberLength)
            {
                RespParsingException.ThrowNotANumber(numberStart, numberLength);
            }

            // Ensure terminator has been received
            if (*(ushort*)ptr != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            ptr += 2;

            return true;
        }

        /// <summary>
        /// Read long with length header
        /// </summary>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        /// <exception cref="RespParsingException">Thrown if not a number is read.</exception>
        public static bool TryReadUInt64WithLengthHeader(out ulong number, ref byte* ptr, byte* end)
        {
            number = 0;

            // Parse RESP string header
            if (!TryReadUnsignedLengthHeader(out var numberLength, ref ptr, end))
                return false;

            if (ptr + numberLength + 2 > end)
                return false;

            // Parse associated integer value
            var numberStart = ptr;
            if (!TryReadUInt64(ref ptr, end, out number, out var bytesRead))
            {
                return false;
            }

            if ((int)bytesRead != numberLength)
            {
                RespParsingException.ThrowNotANumber(numberStart, numberLength);
            }

            // Ensure terminator has been received
            if (*(ushort*)ptr != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            ptr += 2;

            return true;
        }

        /// <summary>
        /// Skip byte array with length header
        /// </summary>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TrySkipByteArrayWithLengthHeader(ref byte* ptr, byte* end)
        {
            // Parse RESP string header
            if (!TryReadUnsignedLengthHeader(out var length, ref ptr, end))
                return false;

            // Advance read pointer to the end of the array (including terminator)
            var keyPtr = ptr;

            ptr += length + 2;

            if (ptr > end)
                return false;

            // Ensure terminator has been received
            if (*(ushort*)(ptr - 2) != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*(ptr - 2));
            }
            return true;
        }

        /// <summary>
        /// Read byte array with length header
        /// </summary>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TryReadByteArrayWithLengthHeader(out byte[] result, ref byte* ptr, byte* end)
        {
            result = null;
            if (!TrySliceWithLengthHeader(out var resultSpan, ref ptr, end))
                return false;

            result = resultSpan.ToArray();
            return true;
        }

        /// <summary>
        /// Try slice a byte array with length header.
        /// </summary>
        /// <remarks>
        /// SAFETY: Because this hands out a span over the underlying buffer to the caller,
        /// it must be aware that any changes in the memory where <paramref name="ptr"/> pointed to
        /// will be reflected in the <paramref name="result"/> span. i.e.
        /// <code>
        /// byte[] buffer = "$2\r\nAB\r\n"u8.ToArray();
        /// fixed (byte* ptr = buffer)
        /// {
        ///     TrySliceWithLengthHeader(out var result, ref ptr, ptr + buffer.Length);
        ///     Debug.Assert(result.SequenceEquals("AB"u8)); // True
        ///
        ///     *(ptr - 4) = (byte)'C';
        ///     *(ptr - 3) = (byte)'D';
        ///     Debug.Assert(result.SequenceEquals("CD"u8)); // True
        /// }
        /// </code>
        /// </remarks>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TrySliceWithLengthHeader(out ReadOnlySpan<byte> result, scoped ref byte* ptr, byte* end)
        {
            result = default;

            // Parse RESP string header
            if (!TryReadUnsignedLengthHeader(out var length, ref ptr, end))
                return false;

            // Advance read pointer to the end of the array (including terminator)
            var keyPtr = ptr;

            ptr += length + 2;

            if (ptr > end)
                return false;

            // Ensure terminator has been received
            if (*(ushort*)(ptr - 2) != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*(ptr - 2));
            }

            result = new ReadOnlySpan<byte>(keyPtr, length);
            return true;
        }

        /// <summary>
        /// Read boolean value with length header
        /// </summary>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TryReadBoolWithLengthHeader(out bool result, ref byte* ptr, byte* end)
        {
            result = false;

            if (ptr + 7 > end)
                return false;


            // Fast path: RESP string header should have length 1
            if (*(uint*)ptr == MemoryMarshal.Read<uint>("$1\r\n"u8))
            {
                ptr += 4;
            }
            else
            {
                // Parse malformed RESP string header
                if (!TryReadUnsignedLengthHeader(out var length, ref ptr, end))
                    return false;

                if (length != 1)
                {
                    RespParsingException.ThrowInvalidLength(length);
                }
            }

            // Parse contents (needs to be 1 character)
            result = (*ptr++ == '1');

            // Ensure terminator has been received
            if (*(ushort*)ptr != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            ptr += 2;

            return true;
        }

        /// <summary>
        /// Tries to read a RESP-formatted string including its length header from the given ASCII-encoded
        /// RESP message and, if successful, moves the given ptr to the end of the string value.
        /// <para />
        /// NOTE: We use ReadUnsignedLengthHeader because server does not accept $-1\r\n headers
        /// </summary>
        /// <param name="result">If parsing was successful, contains the extracted string value.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP string was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TryReadStringWithLengthHeader(out string result, ref byte* ptr, byte* end)
        {
            TryReadSpanWithLengthHeader(out var resultSpan, ref ptr, end);
            result = Encoding.UTF8.GetString(resultSpan);
            return true;
        }

        /// <summary>
        /// Tries to read a RESP-formatted string as span including its length header from the given ASCII-encoded
        /// RESP message and, if successful, moves the given ptr to the end of the string value.
        /// <para />
        /// NOTE: We use ReadUnsignedLengthHeader because server does not accept $-1\r\n headers
        /// </summary>
        /// <param name="result">If parsing was successful, contains the extracted string value.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP string was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TryReadSpanWithLengthHeader(out ReadOnlySpan<byte> result, ref byte* ptr, byte* end)
        {
            result = null;

            if (ptr + 3 > end)
                return false;

            // Parse RESP string header
            if (!TryReadUnsignedLengthHeader(out var length, ref ptr, end))
                return false;

            // Extract string content + '\r\n' terminator
            var keyPtr = ptr;

            ptr += length + 2;

            if (ptr > end)
                return false;

            // Ensure terminator has been received
            if (*(ushort*)(ptr - 2) != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*(ptr - 2));
            }

            result = new ReadOnlySpan<byte>(keyPtr, length);

            return true;
        }

        /// <summary>
        /// Try to read a RESP formatted bulk string
        /// <para />
        /// NOTE: This is used with client implementation to parse responses that may include a null value (i.e. $-1\r\n)
        /// </summary>
        /// <param name="result">If parsing was successful, contains the extracted string value.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP string was successfully read.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadStringResponseWithLengthHeader(out string result, ref byte* ptr, byte* end)
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
        /// Try to read a RESP string and return pointer to the start of the string
        /// <para />
        /// NOTE: This is used with client implementation to parse responses that may include a null value (i.e. $-1\r\n)
        /// </summary>
        /// <param name="stringPtr">If parsing was successful, contains the pointer to start of the parsed string value.</param>
        /// <param name="length">If parsing was successful, contains the length of the string value.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP string was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadPtrWithSignedLengthHeader(ref byte* stringPtr, ref int length, ref byte* ptr, byte* end)
        {
            // Parse RESP string header
            if (!TryReadSignedLengthHeader(out length, ref ptr, end))
            {
                return false;
            }

            // Allow for null
            if (length < 0)
            {
                // NULL value ('$-1\r\n')
                stringPtr = null;
                return true;
            }

            stringPtr = ptr;

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

        /// <summary>
        /// Read simple string
        /// </summary>
        /// <param name="result">If parsing was successful, contains the extracted string value.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP simple string was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        public static bool TryReadSimpleString(out string result, ref byte* ptr, byte* end)
        {
            result = null;

            if (ptr + 2 >= end)
                return false;

            // Simple strings need to start with a '+'
            if (*ptr != '+')
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            ptr++;

            return TryReadString(out result, ref ptr, end);
        }

        /// <summary>
        /// Read error as string
        /// </summary>
        /// <param name="result">If parsing was successful, contains the extracted string value.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP error string was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        public static bool TryReadErrorAsString(out string result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 2 >= end)
                return false;

            // Error strings need to start with a '-'
            if (*ptr != '-')
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            ptr++;

            return TryReadString(out result, ref ptr, end);
        }

        /// <summary>
        /// Read error as span
        /// </summary>
        /// <param name="result">If parsing was successful, contains the span pointing to parsed error string value.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP error was successfully read.</returns>
        public static bool TryReadErrorAsSpan(out ReadOnlySpan<byte> result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 2 >= end)
                return false;

            // Error strings need to start with a '-'
            if (*ptr != '-')
            {
                return false;
            }

            ptr++;

            return TryReadAsSpan(out result, ref ptr, end);
        }

        /// <summary>
        /// Read integer as string
        /// </summary>
        /// <param name="result">If parsing was successful, contains the parsed integer as a string.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP integer was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        public static bool TryReadIntegerAsString(out string result, ref byte* ptr, byte* end)
        {
            var success = TryReadIntegerAsSpan(out var resultSpan, ref ptr, end);
            result = Encoding.UTF8.GetString(resultSpan);
            return success;
        }

        /// <summary>
        /// Read integer as string
        /// </summary>
        /// <param name="result">If parsing was successful, contains a span pointing to the integer in the buffer.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP integer was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        public static bool TryReadIntegerAsSpan(out ReadOnlySpan<byte> result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 2 >= end)
                return false;

            // Integer strings need to start with a ':'
            if (*ptr != ':')
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            ptr++;

            return TryReadAsSpan(out result, ref ptr, end);
        }

        /// <summary>
        /// Read string array with length header
        /// <para />
        /// NOTE: We use ReadUnsignedLengthHeader because server does not accept *-1\r\n headers.
        /// </summary>
        /// <param name="result">If parsing was successful, contains a array of parsed string values.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP string array was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if the array length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        public static bool TryReadStringArrayWithLengthHeader(out string[] result, ref byte* ptr, byte* end)
        {
            result = null;

            // Parse RESP array header
            if (!TryReadUnsignedArrayLength(out var length, ref ptr, end))
            {
                return false;
            }

            // Parse individual strings in the array
            result = new string[length];
            for (var i = 0; i < length; i++)
            {
                if (*ptr == '$')
                {
                    if (!TryReadStringWithLengthHeader(out result[i], ref ptr, end))
                        return false;
                }
                else
                {
                    if (!TryReadIntegerAsString(out result[i], ref ptr, end))
                        return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Read double with length header
        /// </summary>
        /// <param name="result">If parsing was successful, contains the parsed <see langword="double"/> value.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP double was successfully read.</returns>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        public static bool TryReadDoubleWithLengthHeader(out double result, out bool parsed, ref byte* ptr, byte* end)
        {
            if (!TrySliceWithLengthHeader(out var resultBytes, ref ptr, end))
            {
                result = 0;
                parsed = false;
                return false;
            }

            parsed = Utf8Parser.TryParse(resultBytes, out result, out var bytesConsumed, default) &&
                bytesConsumed == resultBytes.Length;
            return true;
        }

        /// <summary>
        /// Read pointer to byte array, with length header.
        /// </summary>
        /// <param name="result">Pointer to the beginning of the read byte array (including empty).</param>
        /// <param name="len">Length of byte array.</param>
        /// <param name="ptr">Current read head of the input RESP stream.</param>
        /// <param name="end">Current end of the input RESP stream.</param>
        /// <returns>True if input was complete, otherwise false.</returns>
        /// <exception cref="RespParsingException">Thrown if the length header is negative.</exception>
        /// <exception cref="RespParsingException">Thrown if unexpected token is read.</exception>
        /// <exception cref="RespParsingException">Thrown if integer overflow occurs.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadPtrWithLengthHeader(ref byte* result, ref int len, ref byte* ptr, byte* end)
        {
            // Parse RESP string header
            if (!TryReadUnsignedLengthHeader(out len, ref ptr, end))
            {
                return false;
            }

            result = ptr;

            // Parse content: ensure that input contains key + '\r\n'
            ptr += len + 2;
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

        /// <summary>
        /// Read ASCII string without header until string terminator ('\r\n').
        /// </summary>
        /// <param name="result">If parsing was successful, contains the parsed string value.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        public static bool TryReadString(out string result, ref byte* ptr, byte* end)
        {
            result = null;

            if (ptr + 1 >= end)
                return false;

            var start = ptr;

            while (ptr < end - 1)
            {
                if (*(ushort*)ptr == MemoryMarshal.Read<ushort>("\r\n"u8))
                {
                    result = Encoding.UTF8.GetString(new ReadOnlySpan<byte>(start, (int)(ptr - start)));
                    ptr += 2;
                    return true;
                }
                ptr++;
            }

            return false;
        }

        /// <summary>
        /// Read ASCII string as span without header until string terminator ('\r\n').
        /// </summary>
        public static bool TryReadAsSpan(out ReadOnlySpan<byte> result, ref byte* ptr, byte* end)
        {
            result = null;

            if (ptr + 1 >= end)
                return false;

            var start = ptr;

            while (ptr < end - 1)
            {
                if (*(ushort*)ptr == MemoryMarshal.Read<ushort>("\r\n"u8))
                {
                    result = new ReadOnlySpan<byte>(start, (int)(ptr - start));
                    ptr += 2;
                    return true;
                }
                ptr++;
            }

            return false;
        }

        /// <summary>
        /// Read serialized data for migration and replication. For details of the layout see <see cref="DiskLogRecord.Serialize"/>.
        /// </summary>  
        public static bool GetSerializedRecordSpan(out PinnedSpanByte recordSpan, ref byte* ptr, byte* end)
        {
            // 1. Safe read recordSize.
            if (ptr + sizeof(int) > end)
            {
                recordSpan = default;
                return false;
            }
            var recordLength = *(int*)ptr;
            ptr += sizeof(int);

            // 2. The record starts immediately after the length prefix.
            recordSpan = PinnedSpanByte.FromPinnedPointer(ptr, recordLength);
            ptr += recordLength;
            return true;
        }

        /// <summary>
        /// Parses "[+/-]inf" string and returns double.PositiveInfinity/double.NegativeInfinity respectively.
        /// If string is not an infinity, parsing fails.
        /// </summary>
        /// <param name="value">input data</param>
        /// <param name="number">If parsing was successful,contains positive or negative infinity</param>
        /// <returns>True is infinity was read, false otherwise</returns>
        public static bool TryReadInfinity(ReadOnlySpan<byte> value, out double number)
        {
            if (value.Length == 3)
            {
                if (value.EqualsUpperCaseSpanIgnoringCase(RespStrings.INFINITY))
                {
                    number = double.PositiveInfinity;
                    return true;
                }
            }
            else if (value.Length == 4)
            {
                if (value.EqualsUpperCaseSpanIgnoringCase(RespStrings.POS_INFINITY, true))
                {
                    number = double.PositiveInfinity;
                    return true;
                }
                else if (value.EqualsUpperCaseSpanIgnoringCase(RespStrings.NEG_INFINITY, true))
                {
                    number = double.NegativeInfinity;
                    return true;
                }
            }

            number = default;
            return false;
        }

        /// <summary>
        /// Parses "[+/-]inf" string and returns float.PositiveInfinity/float.NegativeInfinity respectively.
        /// If string is not an infinity, parsing fails.
        /// </summary>
        /// <param name="value">input data</param>
        /// <param name="number">If parsing was successful,contains positive or negative infinity</param>
        /// <returns>True is infinity was read, false otherwise</returns>
        public static bool TryReadInfinity(ReadOnlySpan<byte> value, out float number)
        {
            if (value.Length == 3)
            {
                if (value.EqualsUpperCaseSpanIgnoringCase(RespStrings.INFINITY))
                {
                    number = float.PositiveInfinity;
                    return true;
                }
            }
            else if (value.Length == 4)
            {
                if (value.EqualsUpperCaseSpanIgnoringCase(RespStrings.POS_INFINITY, true))
                {
                    number = float.PositiveInfinity;
                    return true;
                }
                else if (value.EqualsUpperCaseSpanIgnoringCase(RespStrings.NEG_INFINITY, true))
                {
                    number = float.NegativeInfinity;
                    return true;
                }
            }

            number = default;
            return false;
        }
    }
}