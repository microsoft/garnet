// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
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
        protected static unsafe void WriteSimpleString(ref (IMemoryOwner<byte>, int) output, string simpleString)
        {
            var encodedLen = System.Text.Encoding.ASCII.GetByteCount(simpleString);

            // Get space for simple string
            int len = 1 + encodedLen + 2;
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
                RespWriteUtils.WriteSimpleString(simpleString, encodedLen, ref curr, ptr + len);
            }
            output.Item2 = len;
        }

        /// <summary>
        /// Create output as simple string, from given string
        /// </summary>
        protected static unsafe void WriteSimpleString(ref MemoryResult<byte> output, string simpleString)
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
            int totalLen = 1 + NumUtils.NumDigits(values.Length) + 2;
            for (int i = 0; i < values.Length; i++)
                totalLen += RespWriteUtils.GetBulkStringLength(values[i].Length);

            output.MemoryOwner?.Dispose();
            output.MemoryOwner = MemoryPool.Rent(totalLen);
            output.Length = totalLen;

            fixed (byte* ptr = output.MemoryOwner.Memory.Span)
            {
                var curr = ptr;
                RespWriteUtils.WriteArrayLength(values.Length, ref curr, ptr + totalLen);
                for (int i = 0; i < values.Length; i++)
                    RespWriteUtils.WriteBulkString(values[i].Span, ref curr, ptr + totalLen);
            }
        }

        /// <summary>
        /// Create output as bulk string, from given Span
        /// </summary>
        protected static unsafe void WriteBulkString(ref (IMemoryOwner<byte>, int) output, Span<byte> bulkString)
        {
            // Get space for bulk string
            int len = RespWriteUtils.GetBulkStringLength(bulkString.Length);
            output.Item1?.Dispose();
            output.Item1 = MemoryPool.Rent(len);
            output.Item2 = len;
            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                RespWriteUtils.WriteBulkString(bulkString, ref curr, ptr + len);
            }
        }

        /// <summary>
        /// Create output as bulk string, from given Span
        /// </summary>
        protected static unsafe void WriteBulkString(ref MemoryResult<byte> output, Span<byte> bulkString)
        {
            var _output = (output.MemoryOwner, output.Length);
            WriteBulkString(ref _output, bulkString);
            output.MemoryOwner = _output.MemoryOwner;
            output.Length = _output.Length;
        }

        /// <summary>
        /// Create null output as bulk string
        /// </summary>
        protected static unsafe void WriteNullBulkString(ref (IMemoryOwner<byte>, int) output)
        {
            // Get space for null bulk string "$-1\r\n"
            int len = 5;
            output.Item1?.Dispose();
            output.Item1 = MemoryPool.Rent(len);
            output.Item2 = len;
            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                RespWriteUtils.WriteNull(ref curr, ptr + len);
            }
        }

        /// <summary>
        /// Create null output as bulk string
        /// </summary>
        protected static unsafe void WriteNullBulkString(ref MemoryResult<byte> output)
        {
            var _output = (output.MemoryOwner, output.Length);
            WriteNullBulkString(ref _output);
            output.MemoryOwner = _output.MemoryOwner;
            output.Length = _output.Length;
        }

        /// <summary>
        /// Create output as error message, from given string
        /// </summary>
        protected static unsafe void WriteError(ref (IMemoryOwner<byte>, int) output, string errorMessage)
        {
            var bytes = System.Text.Encoding.ASCII.GetBytes(errorMessage);
            // Get space for error
            int len = 1 + bytes.Length + 2;
            output.Item1?.Dispose();
            output.Item1 = MemoryPool.Rent(len);
            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                RespWriteUtils.WriteError(bytes, ref curr, ptr + len);
            }
            output.Item2 = len;
        }

        /// <summary>
        /// Create output as error message, from given string
        /// </summary>
        protected static unsafe void WriteError(ref MemoryResult<byte> output, string errorMessage)
        {
            var _output = (output.MemoryOwner, output.Length);
            WriteError(ref _output, errorMessage);
            output.MemoryOwner = _output.MemoryOwner;
            output.Length = _output.Length;
        }
    }
}