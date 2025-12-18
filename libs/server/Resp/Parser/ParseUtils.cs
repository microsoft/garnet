// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Text;
using System.Runtime.CompilerServices;
using System.Text;
using Garnet.common;
using Garnet.common.Parsing;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Utilities for parsing RESP protocol messages.
    /// </summary>
    public static unsafe class ParseUtils
    {
        /// <summary>
        /// Read a signed 32-bit integer from a given ArgSlice.
        /// </summary>
        /// <returns>
        /// Parsed integer
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadInt(PinnedSpanByte slice)
        {
            int number = default;
            var ptr = slice.ptr;

            if (slice.length == 0 ||
                !RespReadUtils.TryReadInt32(ref ptr, slice.ptr + slice.length, out number, out var bytesRead) ||
                (int)bytesRead != slice.length)
            {
                RespParsingException.ThrowNotANumber(slice.ptr, slice.length);
            }

            return number;
        }

        /// <summary>
        /// Try to read a signed 32-bit integer from a given ArgSlice.
        /// </summary>
        /// <returns>
        /// True if integer read successfully
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadInt(PinnedSpanByte slice, out int number)
        {
            number = default;
            var ptr = slice.ptr;
            return slice.length != 0 &&
                   RespReadUtils.TryReadInt32Safe(ref ptr, slice.ptr + slice.length, out number, out var bytesRead, out _,
                       out _, allowLeadingZeros: false) &&
                   (int)bytesRead == slice.length;
        }

        /// <summary>
        /// Read a signed 64-bit long from a given ArgSlice.
        /// </summary>
        /// <returns>
        /// Parsed long
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ReadLong(PinnedSpanByte slice)
        {
            long number = default;
            var ptr = slice.ptr;

            if (slice.length == 0 ||
                !RespReadUtils.TryReadInt64(ref ptr, slice.ptr + slice.length, out number, out var bytesRead) ||
                (int)bytesRead != slice.length)
            {
                RespParsingException.ThrowNotANumber(slice.ptr, slice.length);
            }

            return number;
        }

        /// <summary>
        /// Try to read a signed 64-bit long from a given ArgSlice.
        /// </summary>
        /// <returns>
        /// True if long parsed successfully
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadLong(PinnedSpanByte slice, out long number)
        {
            number = default;
            var ptr = slice.ptr;
            return slice.length != 0 &&
                   RespReadUtils.TryReadInt64Safe(ref ptr, slice.ptr + slice.length, out number, out var bytesRead,
                       out _, out _, allowLeadingZeros: false) &&
                   (int)bytesRead == slice.length;
        }

        /// <summary>
        /// Read a signed 64-bit double from a given ArgSlice.
        /// </summary>
        /// <param name="slice">Source</param>
        /// <param name="canBeInfinite">Allow reading an infinity</param>
        /// <returns>
        /// Parsed double
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static double ReadDouble(PinnedSpanByte slice, bool canBeInfinite)
        {
            if (!TryReadDouble(slice, out var number, canBeInfinite))
            {
                RespParsingException.ThrowNotANumber(slice.ptr, slice.length);
            }
            return number;
        }

        /// <summary>
        /// Try to read a signed 64-bit double from a given ArgSlice.
        /// </summary>
        /// <param name="slice">Source</param>
        /// <param name="number">Result</param>
        /// <param name="canBeInfinite">Allow reading an infinity</param>
        /// <returns>
        /// True if double parsed successfully
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadDouble(PinnedSpanByte slice, out double number, bool canBeInfinite)
        {
            var sbNumber = slice.ReadOnlySpan;
            if (Utf8Parser.TryParse(sbNumber, out number, out var bytesConsumed) &&
                            bytesConsumed == sbNumber.Length)
                return true;

            return canBeInfinite && RespReadUtils.TryReadInfinity(sbNumber, out number);
        }

        /// <summary>
        /// Read a signed 32-bit float from a given ArgSlice.
        /// </summary>
        /// <param name="slice">Source</param>
        /// <param name="canBeInfinite">Allow reading an infinity</param>
        /// <returns>
        /// Parsed double
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float ReadFloat(ref PinnedSpanByte slice, bool canBeInfinite)
        {
            if (!TryReadFloat(ref slice, out var number, canBeInfinite))
            {
                RespParsingException.ThrowNotANumber(slice.ptr, slice.length);
            }
            return number;
        }

        /// <summary>
        /// Try to read a signed 32-bit float from a given ArgSlice.
        /// </summary>
        /// <param name="slice">Source</param>
        /// <param name="number">Result</param>
        /// <param name="canBeInfinite">Allow reading an infinity</param>
        /// <returns>
        /// True if float parsed successfully
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadFloat(ref PinnedSpanByte slice, out float number, bool canBeInfinite)
        {
            var sbNumber = slice.ReadOnlySpan;
            if (Utf8Parser.TryParse(sbNumber, out number, out var bytesConsumed) &&
                            bytesConsumed == sbNumber.Length)
                return true;

            return canBeInfinite && RespReadUtils.TryReadInfinity(sbNumber, out number);
        }

        /// <summary>
        /// Read an ASCII string from a given ArgSlice.
        /// </summary>
        /// <returns>
        /// Parsed string
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ReadString(PinnedSpanByte slice)
        {
            return Encoding.ASCII.GetString(slice.ReadOnlySpan);
        }

        /// <summary>
        /// Read a boolean from a given ArgSlice.
        /// </summary>
        /// <returns>
        /// Parsed integer
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ReadBool(PinnedSpanByte slice)
        {
            if (!TryReadBool(slice, out var value))
            {
                RespParsingException.ThrowNotANumber(slice.ptr, slice.length);
            }
            return value;
        }

        /// <summary>
        /// Try to read a signed 32-bit integer from a given ArgSlice.
        /// </summary>
        /// <returns>
        /// True if integer read successfully
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadBool(PinnedSpanByte slice, out bool value)
        {
            value = false;

            if (slice.Length != 1) return false;

            if (*slice.ptr == '1')
            {
                value = true;
                return true;
            }

            return *slice.ptr == '0';
        }
    }
}