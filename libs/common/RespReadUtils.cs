// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common.Parsing;

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryReadUlong(ref byte* ptr, byte* end, out ulong value, out ulong bytesRead)
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
                if (nextDigit > 9)
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
                if (nextDigit > 9)
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
        /// </summary>
        /// <param name="ptr">Pointer to the beginning of the ASCII encoded input string.</param>
        /// <param name="end">The end of the string to parse.</param>
        /// <param name="value">If parsing was successful, contains the parsed long value.</param>
        /// <param name="bytesRead">If parsing was successful, contains the number of bytes that were parsed.</param>
        /// <returns>
        /// True if a long was successfully parsed, false if the input string did not start with
        /// a valid integer or the end of the string was reached before finishing parsing.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryReadLong(ref byte* ptr, byte* end, out long value, out ulong bytesRead)
        {
            bytesRead = 0;
            value = 0;

            // Parse optional leading sign
            if (TryReadSign(ptr, out var negative))
            {
                ptr++;
                bytesRead = 1;
            }

            // Parse digits as ulong
            if (!TryReadUlong(ref ptr, end, out var number, out var digitsRead))
            {
                return false;
            }

            // Check for overflows and convert digits to long, if possible
            if (negative)
            {
                if (number > ((ulong)long.MaxValue) + 1)
                {
                    RespParsingException.ThrowIntegerOverflow(ptr - digitsRead, (int)digitsRead);
                }

                value = -1 - (long)(number - 1);
            }
            else
            {
                if (number > long.MaxValue)
                {
                    RespParsingException.ThrowIntegerOverflow(ptr - digitsRead, (int)digitsRead);
                }
                value = (long)number;
            }

            bytesRead += digitsRead;

            return true;
        }

        /// <summary>
        /// Tries to read a signed 32-bit integer from a given ASCII-encoded input stream.
        /// </summary>
        /// <param name="ptr">Pointer to the beginning of the ASCII encoded input string.</param>
        /// <param name="end">The end of the string to parse.</param>
        /// <param name="value">If parsing was successful, contains the parsed int value.</param>
        /// <param name="bytesRead">If parsing was successful, contains the number of bytes that were parsed.</param>
        /// <returns>
        /// True if an int was successfully parsed, false if the input string did not start with
        /// a valid integer or the end of the string was reached before finishing parsing.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryReadInt(ref byte* ptr, byte* end, out int value, out ulong bytesRead)
        {
            bytesRead = 0;
            value = 0;

            // Parse optional leading sign
            if (TryReadSign(ptr, out var negative))
            {
                ptr++;
                bytesRead = 1;
            }

            // Parse digits as ulong
            if (!TryReadUlong(ref ptr, end, out var number, out var digitsRead))
            {
                return false;
            }

            // Check for overflows and convert digits to int, if possible
            if (negative)
            {
                if (number > ((ulong)int.MaxValue) + 1)
                {
                    RespParsingException.ThrowIntegerOverflow(ptr - digitsRead, (int)digitsRead);
                }

                value = (int)(0 - (long)number);
            }
            else
            {
                if (number > int.MaxValue)
                {
                    RespParsingException.ThrowIntegerOverflow(ptr - digitsRead, (int)digitsRead);
                }
                value = (int)number;
            }

            bytesRead += digitsRead;

            return true;
        }

        /// <summary>
        /// Tries to read a RESP length header from the given ASCII-encoded RESP string
        /// and, if successful, moves the given ptr to the end of the length header.
        /// NOTE:
        ///     It will throw an exception if length header is negative. 
        ///     It is primarily used for parsing header length from packets received from server side.
        /// </summary>
        /// <param name="length">If parsing was successful, contains the extracted length from the header.</param>
        /// <param name="ptr">The starting position in the RESP string. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP string.</param>
        /// <param name="isArray">Whether to parse an array length header ('*...\r\n') or a string length header ('$...\r\n').</param>
        /// <returns>True if a length header was successfully read.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ReadUnsignedLengthHeader(out int length, ref byte* ptr, byte* end, bool isArray = false)
        {
            length = -1;
            if (ptr + 3 > end)
                return false;

            var readHead = ptr + 1;
            var negative = *readHead == '-';

            if (negative)
            {
                RespParsingException.ThrowInvalidStringLength(length);
            }

            if (!ReadSignedLengthHeader(out length, ref ptr, end, isArray))
                return false;

            return true;
        }

        /// <summary>
        /// Tries to read a RESP a signed length header from the given ASCII-encoded RESP string
        /// and, if successful, moves the given ptr to the end of the length header.
        /// NOTE:
        ///     It will not throw an exception if length header is negative.
        ///     It is primarily used by client side code.
        ///     Should not be called by any server code since server side does not accept null values
        /// </summary>
        /// <param name="length">If parsing was successful, contains the extracted length from the header.</param>
        /// <param name="ptr">The starting position in the RESP string. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP string.</param>
        /// <param name="isArray">Whether to parse an array length header ('*...\r\n') or a string length header ('$...\r\n').</param>
        /// <returns>True if a length header was successfully read.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ReadSignedLengthHeader(out int length, ref byte* ptr, byte* end, bool isArray = false)
        {
            length = -1;
            if (ptr + 3 > end)
                return false;

            var readHead = ptr + 1;
            var negative = *readHead == '-';

            // String length headers must start with a '$', array headers with '*'
            if (*ptr != (isArray ? '*' : '$'))
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
            if (!TryReadUlong(ref readHead, end, out var value, out var digitsRead))
            {
                return false;
            }

            if (digitsRead == 0)
            {
                RespParsingException.ThrowUnexpectedToken(*readHead);
            }

            // Validate length
            if (value > int.MaxValue)
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
        /// Read signed 64 bit number
        /// </summary>
        public static bool Read64Int(out long number, ref byte* ptr, byte* end)
        {
            number = 0;
            if (ptr + 3 >= end)
                return false;

            // Integer header must start with ':'
            if (*ptr++ != ':')
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            // Parse length
            if (!TryReadLong(ref ptr, end, out number, out var bytesRead))
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
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            return true;
        }

        /// <summary>
        /// Tries to read a RESP array length header from the given ASCII-encoded RESP string
        /// and, if successful, moves the given ptr to the end of the length header.
        /// NOTE: We use ReadUnsignedLengthHeader because server does not accept $-1\r\n headers
        /// </summary>
        /// <param name="length">If parsing was successful, contains the extracted length from the header.</param>
        /// <param name="ptr">The starting position in the RESP string. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP string.</param>
        /// <returns>True if a length header was successfully read.</returns>
        public static bool ReadUnsignedArrayLength(out int length, ref byte* ptr, byte* end)
            => ReadUnsignedLengthHeader(out length, ref ptr, end, isArray: true);

        /// <summary>
        /// Tries to read a RESP array length header from the given ASCII-encoded RESP string
        /// and, if successful, moves the given ptr to the end of the length header.
        /// NOTE: It will not throw an exception if length header is negative.
        /// </summary>
        /// <param name="length">If parsing was successful, contains the extracted length from the header.</param>
        /// <param name="ptr">The starting position in the RESP string. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP string.</param>
        /// <returns>True if a length header was successfully read.</returns>
        public static bool ReadSignedArrayLength(out int length, ref byte* ptr, byte* end)
            => ReadSignedLengthHeader(out length, ref ptr, end, isArray: true);

        /// <summary>
        /// Read int with length header
        /// </summary>
        public static bool ReadIntWithLengthHeader(out int number, ref byte* ptr, byte* end)
        {
            number = 0;

            // Parse RESP string header
            if (!ReadUnsignedLengthHeader(out var numberLength, ref ptr, end))
                return false;

            if (ptr + numberLength + 2 > end)
                return false;

            // Parse associated integer value
            var numberStart = ptr;
            if (!TryReadInt(ref ptr, end, out number, out var bytesRead))
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
        public static bool ReadLongWithLengthHeader(out long number, ref byte* ptr, byte* end)
        {
            number = 0;

            // Parse RESP string header
            if (!ReadUnsignedLengthHeader(out var numberLength, ref ptr, end))
                return false;

            if (ptr + numberLength + 2 > end)
                return false;

            // Parse associated integer value
            var numberStart = ptr;
            if (!TryReadLong(ref ptr, end, out number, out var bytesRead))
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
        public static bool ReadULongWithLengthHeader(out ulong number, ref byte* ptr, byte* end)
        {
            number = 0;

            // Parse RESP string header
            if (!ReadUnsignedLengthHeader(out var numberLength, ref ptr, end))
                return false;

            if (ptr + numberLength + 2 > end)
                return false;

            // Parse associated integer value
            var numberStart = ptr;
            if (!TryReadUlong(ref ptr, end, out number, out var bytesRead))
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
        public static bool SkipByteArrayWithLengthHeader(ref byte* ptr, byte* end)
        {
            // Parse RESP string header
            if (!ReadUnsignedLengthHeader(out var length, ref ptr, end))
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
        public static bool ReadByteArrayWithLengthHeader(out byte[] result, ref byte* ptr, byte* end)
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
        public static bool TrySliceWithLengthHeader(out ReadOnlySpan<byte> result, scoped ref byte* ptr, byte* end)
        {
            result = default;

            // Parse RESP string header
            if (!ReadUnsignedLengthHeader(out var length, ref ptr, end))
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
        public static bool ReadBoolWithLengthHeader(out bool result, ref byte* ptr, byte* end)
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
                if (!ReadUnsignedLengthHeader(out var length, ref ptr, end))
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
        /// NOTE: We use ReadUnsignedLengthHeader because server does not accept $-1\r\n headers
        /// </summary>
        /// <param name="result">If parsing was successful, contains the extracted string value.</param>
        /// <param name="ptr">The starting position in the RESP message. Will be advanced if parsing is successful.</param>
        /// <param name="end">The current end of the RESP message.</param>
        /// <returns>True if a RESP string was successfully read.</returns>
        public static bool ReadStringWithLengthHeader(out string result, ref byte* ptr, byte* end)
        {
            result = null;

            if (ptr + 3 > end)
                return false;

            // Parse RESP string header
            if (!ReadUnsignedLengthHeader(out var length, ref ptr, end))
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

            result = Encoding.UTF8.GetString(new ReadOnlySpan<byte>(keyPtr, length));

            return true;
        }

        /// <summary>
        /// Read simple string
        /// </summary>
        public static bool ReadSimpleString(out string result, ref byte* ptr, byte* end)
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

            return ReadString(out result, ref ptr, end);
        }

        /// <summary>
        /// Read error as string
        /// </summary>
        public static bool ReadErrorAsString(out string result, ref byte* ptr, byte* end)
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

            return ReadString(out result, ref ptr, end);
        }

        /// <summary>
        /// Read integer as string
        /// </summary>
        public static bool ReadIntegerAsString(out string result, ref byte* ptr, byte* end)
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

            return ReadString(out result, ref ptr, end);
        }

        /// <summary>
        /// Read string array with length header
        /// NOTE: We use ReadUnsignedLengthHeader because server does not accept *-1\r\n headers.
        /// </summary>
        public static bool ReadStringArrayWithLengthHeader(out string[] result, ref byte* ptr, byte* end)
        {
            result = null;

            // Parse RESP array header
            if (!ReadUnsignedArrayLength(out var length, ref ptr, end))
            {
                return false;
            }

            // Parse individual strings in the array
            result = new string[length];
            for (var i = 0; i < length; i++)
            {
                if (*ptr == '$')
                {
                    if (!ReadStringWithLengthHeader(out result[i], ref ptr, end))
                        return false;
                }
                else
                {
                    if (!ReadIntegerAsString(out result[i], ref ptr, end))
                        return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Read double with length header
        /// </summary>
        public static bool ReadDoubleWithLengthHeader(out double result, out bool parsed, ref byte* ptr, byte* end)
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
        /// <exception cref="RespParsingException">Thrown if array length was invalid.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ReadPtrWithLengthHeader(ref byte* result, ref int len, ref byte* ptr, byte* end)
        {
            // Parse RESP string header
            if (!ReadUnsignedLengthHeader(out len, ref ptr, end))
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
        public static bool ReadString(out string result, ref byte* ptr, byte* end)
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
        /// Read serialized data for migration
        /// </summary>        
        public static bool ReadSerializedSpanByte(ref byte* keyPtr, ref byte keyMetaDataSize, ref byte* valPtr, ref byte valMetaDataSize, ref byte* ptr, byte* end)
        {
            //1. safe read ksize
            if (ptr + sizeof(int) > end)
                return false;
            var ksize = *(int*)ptr;
            ptr += sizeof(int);

            //2. safe read key bytes
            if (ptr + ksize + 1 > end)
                return false;
            keyPtr = ptr - sizeof(int);
            ptr += ksize;
            keyMetaDataSize = *ptr++;

            //3. safe read vsize
            if (ptr + 4 > end)
                return false;
            var vsize = *(int*)ptr;
            ptr += sizeof(int);

            //4. safe read value bytes
            if (ptr + vsize + 1 > end)
                return false;
            valPtr = ptr - sizeof(int);
            ptr += vsize;
            valMetaDataSize = *ptr++;

            return true;
        }

        /// <summary>
        /// Read serialized data for migration
        /// </summary>  
        public static bool ReadSerializedData(out byte[] key, out byte[] value, out long expiration, ref byte* ptr, byte* end)
        {
            expiration = -1;
            key = null;
            value = null;

            //1. safe read ksize
            if (ptr + 4 > end)
                return false;
            var keyLen = *(int*)ptr;
            ptr += 4;

            //2. safe read keyPtr
            if (ptr + keyLen > end)
                return false;
            var keyPtr = ptr;
            ptr += keyLen;

            //3. safe read vsize
            if (ptr + 4 > end)
                return false;
            var valLen = *(int*)ptr;
            ptr += 4;

            //4. safe read valPtr
            if (ptr + valLen > end)
                return false;
            var valPtr = ptr;
            ptr += valLen;

            //5. safe read expiration info
            if (ptr + 8 > end)
                return false;
            expiration = *(long*)ptr;
            ptr += 8;

            key = new byte[keyLen];
            value = new byte[valLen];
            fixed (byte* kPtr = key)
                Buffer.MemoryCopy(keyPtr, kPtr, keyLen, keyLen);
            fixed (byte* vPtr = value)
                Buffer.MemoryCopy(valPtr, vPtr, valLen, valLen);

            return true;
        }
    }
}