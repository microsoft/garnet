﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
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
            var ptr = slice.ptr;
            if (!RespReadUtils.TryReadInt(ref ptr, slice.ptr + slice.length, out var number, out var bytesRead)
                || ((int)bytesRead != slice.length))
            {
                RespParsingException.ThrowNotANumber(slice.ptr, slice.length);
            }
            return number;
        }
    }
}