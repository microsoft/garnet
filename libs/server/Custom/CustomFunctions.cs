// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Base class for custom function
    /// </summary>
    public abstract class CustomFunctions
    {
        /// <summary>
        /// Shared memory pool used by functions
        /// </summary>
        protected static MemoryPool<byte> MemoryPool => MemoryPool<byte>.Shared;

        /// <summary>
        /// Create output as simple string, from given string
        /// </summary>
        protected static unsafe void WriteSimpleString(ref (IMemoryOwner<byte>, int) output, ReadOnlySpan<char> simpleString)
        {
            // Get space for simple string
            var len = 1 + simpleString.Length + 2;
            if (output.Item1 != null)
            {
                if (output.Item1.Memory.Length < len)
                {
                    output.Item1.Dispose();
                    output.Item1 = MemoryPool.Rent(len);
                }
            }
            else
                output.Item1 = MemoryPool.Rent(len);

            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                // NOTE: Expected to always have enough space to write into pre-allocated buffer
                var success = RespWriteUtils.WriteSimpleString(simpleString, ref curr, ptr + len);
                Debug.Assert(success, "Insufficient space in pre-allocated buffer");
            }
            output.Item2 = len;
        }

        /// <summary>
        /// Create output as simple string, from given string
        /// </summary>
        protected static unsafe void WriteSimpleString(ref MemoryResult<byte> output, ReadOnlySpan<char> simpleString)
        {
            var _output = (output.MemoryOwner, output.Length);
            WriteSimpleString(ref _output, simpleString);
            output.MemoryOwner = _output.MemoryOwner;
            output.Length = _output.Length;
        }

        /// <summary>
        /// Create output as an array of bulk strings, from given array of ArgSlice values
        /// </summary>
        protected static unsafe void WriteBulkStringArray(ref MemoryResult<byte> output, params ArgSlice[] values)
        {
            var totalLen = 1 + NumUtils.NumDigits(values.Length) + 2;
            for (var i = 0; i < values.Length; i++)
                totalLen += RespWriteUtils.GetBulkStringLength(values[i].Length);

            output.MemoryOwner?.Dispose();
            output.MemoryOwner = MemoryPool.Rent(totalLen);
            output.Length = totalLen;

            fixed (byte* ptr = output.MemoryOwner.Memory.Span)
            {
                var curr = ptr;
                // NOTE: Expected to always have enough space to write into pre-allocated buffer
                var success = RespWriteUtils.WriteArrayLength(values.Length, ref curr, ptr + totalLen);
                Debug.Assert(success, "Insufficient space in pre-allocated buffer");
                for (var i = 0; i < values.Length; i++)
                {
                    // NOTE: Expected to always have enough space to write into pre-allocated buffer
                    success = RespWriteUtils.WriteBulkString(values[i].Span, ref curr, ptr + totalLen);
                    Debug.Assert(success, "Insufficient space in pre-allocated buffer");
                }
            }
        }

        /// <summary>
        /// Create output as an array of bulk strings, from given array of ArgSlice values
        /// </summary>
        protected static unsafe void WriteBulkStringArray(ref MemoryResult<byte> output, List<ArgSlice> values)
        {
            var totalLen = 1 + NumUtils.NumDigits(values.Count) + 2;
            for (var i = 0; i < values.Count; i++)
                totalLen += RespWriteUtils.GetBulkStringLength(values[i].Length);

            output.MemoryOwner?.Dispose();
            output.MemoryOwner = MemoryPool.Rent(totalLen);
            output.Length = totalLen;

            fixed (byte* ptr = output.MemoryOwner.Memory.Span)
            {
                var curr = ptr;
                // NOTE: Expected to always have enough space to write into pre-allocated buffer
                var success = RespWriteUtils.WriteArrayLength(values.Count, ref curr, ptr + totalLen);
                Debug.Assert(success, "Insufficient response buffer space");
                for (var i = 0; i < values.Count; i++)
                {
                    // NOTE: Expected to always have enough space to write into pre-allocated buffer
                    success = RespWriteUtils.WriteBulkString(values[i].Span, ref curr, ptr + totalLen);
                    Debug.Assert(success, "Insufficient space in pre-allocated buffer");
                }
            }
        }

        /// <summary>
        /// Create output as bulk string, from given Span
        /// </summary>
        protected static unsafe void WriteBulkString(ref (IMemoryOwner<byte>, int) output, Span<byte> bulkString)
        {
            // Get space for bulk string
            var len = RespWriteUtils.GetBulkStringLength(bulkString.Length);
            output.Item1?.Dispose();
            output.Item1 = MemoryPool.Rent(len);
            output.Item2 = len;
            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                // NOTE: Expected to always have enough space to write into pre-allocated buffer
                var success = RespWriteUtils.WriteBulkString(bulkString, ref curr, ptr + len);
                Debug.Assert(success, "Insufficient space in pre-allocated buffer");
            }
        }

        /// <summary>
        /// Create null output as bulk string
        /// </summary>
        protected static unsafe void WriteNullBulkString(ref (IMemoryOwner<byte>, int) output)
        {
            // Get space for null bulk string "$-1\r\n"
            var len = 5;
            output.Item1?.Dispose();
            output.Item1 = MemoryPool.Rent(len);
            output.Item2 = len;
            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                // NOTE: Expected to always have enough space to write into pre-allocated buffer
                var success = RespWriteUtils.WriteNull(ref curr, ptr + len);
                Debug.Assert(success, "Insufficient space in pre-allocated buffer");
            }
        }

        /// <summary>
        /// Create output as error message, from given string
        /// </summary>
        protected static unsafe void WriteError(ref (IMemoryOwner<byte>, int) output, ReadOnlySpan<char> errorMessage)
        {
            // Get space for error
            var len = 1 + errorMessage.Length + 2;
            output.Item1?.Dispose();
            output.Item1 = MemoryPool.Rent(len);
            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                // NOTE: Expected to always have enough space to write into pre-allocated buffer
                var success = RespWriteUtils.WriteError(errorMessage, ref curr, ptr + len);
                Debug.Assert(success, "Insufficient space in pre-allocated buffer");
            }
            output.Item2 = len;
        }

        /// <summary>
        /// Get argument from input, at specified offset (starting from 0)
        /// </summary>
        /// <param name="parseState">Current parse state</param>
        /// <param name="parseStateStartIdx"></param>
        /// <param name="offset">Current offset into parse state</param>
        /// <returns>Argument as a span</returns>
        protected static unsafe ArgSlice GetNextArg(ref SessionParseState parseState, int parseStateStartIdx, ref int offset)
        {
            var arg = parseStateStartIdx + offset < parseState.Count
                ? parseState.GetArgSliceByRef(parseStateStartIdx + offset)
                : default;
            offset++;
            return arg;
        }
    }
}