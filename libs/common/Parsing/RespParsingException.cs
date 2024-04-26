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
        /// <param name="c">The character that was unexpected.</param>
        [DoesNotReturn]
        public static void ThrowUnexpectedToken(byte c)
        {
            Throw($"Unexpected byte ({c}) in RESP command package.");
        }

        /// <summary>
        /// Throw an invalid string length exception.
        /// </summary>
        /// <param name="len">The invalid string length.</param>
        [DoesNotReturn]
        public static void ThrowInvalidStringLength(long len)
        {
            Throw($"Invalid string length '{len}' in RESP command package.");
        }

        /// <summary>
        /// Throw an invalid length exception.
        /// </summary>
        /// <param name="len">The invalid length.</param>
        [DoesNotReturn]
        public static void ThrowInvalidLength(long len)
        {
            Throw($"Invalid length '{len}' in RESP command package.");
        }

        /// <summary>
        /// Throw NaN (not a number) exception.
        /// </summary>
        /// <param name="buffer">The input buffer that could not be converted into a number/</param>
        [DoesNotReturn]
        public static void ThrowNotANumber(ReadOnlySpan<byte> buffer)
        {
            var ascii = new System.Text.ASCIIEncoding();
            Throw($"Unable to parse number: {ascii.GetString(buffer)}");
        }

        /// <summary>
        /// Throw a exception indicating that an integer overflow has occurred.
        /// </summary>
        [DoesNotReturn]
        public static void ThrowIntegerOverflow()
        {
            var ascii = new System.Text.ASCIIEncoding();
            Throw($"Unable to parse integer. The given number is larger than allowed.");
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