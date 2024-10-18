// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using Garnet.common;
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    public static class CustomCommandUtils
    {
        /// <summary>
        /// Shared memory pool used by functions
        /// </summary>
        private static MemoryPool<byte> MemoryPool => MemoryPool<byte>.Shared;

        /// <summary>
        /// Get first arg from input
        /// </summary>
        /// <param name="input">Object store input</param>
        /// <returns></returns>
        public static ReadOnlySpan<byte> GetFirstArg(ref ObjectInput input)
        {
            var offset = 0;
            return GetNextArg(ref input, ref offset);
        }

        /// <summary>
        /// Get first arg from input
        /// </summary>
        /// <param name="input">Main store input</param>
        /// <returns></returns>
        public static ReadOnlySpan<byte> GetFirstArg(ref RawStringInput input)
        {
            var offset = 0;
            return GetNextArg(ref input, ref offset);
        }

        /// <summary>
        /// Get argument from input, at specified offset (starting from 0)
        /// </summary>
        /// <param name="input">Object store input</param>
        /// <param name="offset">Current offset into input</param>
        /// <returns>Argument as a span</returns>
        public static ReadOnlySpan<byte> GetNextArg(ref ObjectInput input, scoped ref int offset)
        {
            var arg = input.parseStateFirstArgIdx + offset < input.parseState.Count
                ? input.parseState.GetArgSliceByRef(input.parseStateFirstArgIdx + offset).ReadOnlySpan
                : default;
            offset++;
            return arg;
        }

        /// <summary>
        /// Get argument from input, at specified offset (starting from 0)
        /// </summary>
        /// <param name="input">Main store input</param>
        /// <param name="offset">Current offset into input</param>
        /// <returns>Argument as a span</returns>
        public static ReadOnlySpan<byte> GetNextArg(ref RawStringInput input, scoped ref int offset)
        {
            var arg = input.parseStateFirstArgIdx + offset < input.parseState.Count
                ? input.parseState.GetArgSliceByRef(input.parseStateFirstArgIdx + offset).ReadOnlySpan
                : default;
            offset++;
            return arg;
        }

        /// <summary>
        /// Create output as bulk string, from given Span
        /// </summary>
        public static unsafe void WriteBulkString(ref (IMemoryOwner<byte>, int) output, Span<byte> bulkString)
        {
            // Get space for bulk string
            var len = RespWriteUtils.GetBulkStringLength(bulkString.Length);
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
        /// Create output as error message, from given string
        /// </summary>
        public static unsafe void WriteError(ref (IMemoryOwner<byte>, int) output, string errorMessage)
        {
            var bytes = System.Text.Encoding.ASCII.GetBytes(errorMessage);
            // Get space for error
            var len = 1 + bytes.Length + 2;
            output.Item1 = MemoryPool.Rent(len);
            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                // NOTE: Expected to always have enough space to write into pre-allocated buffer
                var success = RespWriteUtils.WriteError(bytes, ref curr, ptr + len);
                Debug.Assert(success, "Insufficient space in pre-allocated buffer");
            }
            output.Item2 = len;
        }

        /// <summary>
        /// Create null output as bulk string
        /// </summary>
        public static unsafe void WriteNullBulkString(ref (IMemoryOwner<byte>, int) output)
        {
            // Get space for null bulk string "$-1\r\n"
            var len = 5;
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
        /// Create output as simple string, from given string
        /// </summary>
        public static unsafe void WriteSimpleString(ref (IMemoryOwner<byte>, int) output, string simpleString)
        {
            var bytes = System.Text.Encoding.ASCII.GetBytes(simpleString);
            // Get space for simple string
            var len = 1 + bytes.Length + 2;
            output.Item1 = MemoryPool.Rent(len);
            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                // NOTE: Expected to always have enough space to write into pre-allocated buffer
                var success = RespWriteUtils.WriteSimpleString(bytes, ref curr, ptr + len);
                Debug.Assert(success, "Insufficient space in pre-allocated buffer");
            }
            output.Item2 = len;
        }
    }
}