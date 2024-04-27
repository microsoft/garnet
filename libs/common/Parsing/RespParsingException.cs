// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics.CodeAnalysis;

namespace Garnet.common.Parsing
{
    /// <summary>
    /// Exception wrapper for RESP parsing errors.
    /// </summary>
    public class RespParsingException : GarnetException
    {
        /// <summary>
        /// Construct a new RESP parsing exception with the given message.
        /// </summary>
        /// <param name="message">Message that described the exception that has occurred.</param>
        RespParsingException(String message) : base(message)
        {
            // Nothing...
        }

        /// <summary>
        /// Throw an "Unexcepted Token" exception.
        /// </summary>
        /// <param name="token">The character that was unexpected.</param>
        [DoesNotReturn]
        public static void ThrowUnexpectedToken(byte token)
        {
            var c = (char)token;
            var escaped = char.IsControl(c) ? $"\\x{token:x2}" : c.ToString();
            Throw($"Unexpected character '{escaped}'.");
        }

        /// <summary>
        /// Throw an invalid string length exception.
        /// </summary>
        /// <param name="len">The invalid string length.</param>
        [DoesNotReturn]
        public static void ThrowInvalidStringLength(long len)
        {
            Throw($"Invalid string length '{len}'.");
        }

        /// <summary>
        /// Throw an invalid length exception.
        /// </summary>
        /// <param name="len">The invalid length.</param>
        [DoesNotReturn]
        public static void ThrowInvalidLength(long len)
        {
            Throw($"Invalid length '{len}'.");
        }

        /// <summary>
        /// Throw NaN (not a number) exception.
        /// </summary>
        /// <param name="buffer">Pointer to an ASCII-encoded byte buffer containing the string that could not be converted.</param>
        /// <param name="length">Length of the buffer.</param>
        [DoesNotReturn]
        public static unsafe void ThrowNotANumber(byte* buffer, int length)
        {
            var ascii = new System.Text.ASCIIEncoding();
            Throw($"Unable to parse number: {ascii.GetString(buffer, length)}");
        }

        /// <summary>
        /// Throw a exception indicating that an integer overflow has occurred.
        /// </summary>
        /// <param name="buffer">Pointer to an ASCII-encoded byte buffer containing the string that caused the overflow.</param>
        /// <param name="length">Length of the buffer.</param>
        [DoesNotReturn]
        public static unsafe void ThrowIntegerOverflow(byte* buffer, int length)
        {
            var ascii = new System.Text.ASCIIEncoding();
            Throw($"Unable to parse integer. The given number is larger than allowed: {ascii.GetString(buffer, length)}");
        }

        /// <summary>
        /// Throw helper that throws a RespParsingException.
        /// </summary>
        /// <param name="message">Exception message.</param>
        [DoesNotReturn]
        public static void Throw(string message) =>
            throw new RespParsingException(message);
    }
}