// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Text;
using Garnet.common;
using Garnet.common.Parsing;

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
        public static int ReadInt(ref ArgSlice slice)
        {
            if (!TryReadInt(ref slice, out var number))
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
        public static bool TryReadInt(ref ArgSlice slice, out int number)
        {
            var ptr = slice.ptr;
            return RespReadUtils.TryReadInt(ref ptr, slice.ptr + slice.length, out number, out var bytesRead)
                   && (int)bytesRead == slice.length;
        }

        /// <summary>
        /// Read a signed 64-bit long from a given ArgSlice.
        /// </summary>
        /// <returns>
        /// Parsed long
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ReadLong(ref ArgSlice slice)
        {
            if (!TryReadLong(ref slice, out var number))
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
        public static bool TryReadLong(ref ArgSlice slice, out long number)
        {
            var ptr = slice.ptr;
            return RespReadUtils.TryReadLong(ref ptr, slice.ptr + slice.length, out number, out var bytesRead)
                   && (int)bytesRead == slice.length;
        }

        /// <summary>
        /// Read an ASCII string from a given ArgSlice.
        /// </summary>
        /// <returns>
        /// Parsed string
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ReadString(ref ArgSlice slice)
        {
            return Encoding.ASCII.GetString(slice.ReadOnlySpan);
        }
    }
}