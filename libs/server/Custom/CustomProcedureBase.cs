// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Base class for custom procedures
    /// </summary>
    public abstract class CustomProcedureBase
    {
        /// <summary>
        /// Shared memory pool used by procedures
        /// </summary>
        protected static MemoryPool<byte> MemoryPool => MemoryPool<byte>.Shared;

        internal RespServerSession respServerSession;

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
                var success = RespWriteUtils.TryWriteSimpleString(simpleString, ref curr, ptr + len);
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
        protected static unsafe void WriteBulkStringArray(ref MemoryResult<byte> output, params PinnedSpanByte[] values)
        {
            var totalLen = 1 + NumUtils.CountDigits(values.Length) + 2;
            for (var i = 0; i < values.Length; i++)
                totalLen += RespWriteUtils.GetBulkStringLength(values[i].Length);

            output.MemoryOwner?.Dispose();
            output.MemoryOwner = MemoryPool.Rent(totalLen);
            output.Length = totalLen;

            fixed (byte* ptr = output.MemoryOwner.Memory.Span)
            {
                var curr = ptr;
                // NOTE: Expected to always have enough space to write into pre-allocated buffer
                var success = RespWriteUtils.TryWriteArrayLength(values.Length, ref curr, ptr + totalLen);
                Debug.Assert(success, "Insufficient space in pre-allocated buffer");
                for (var i = 0; i < values.Length; i++)
                {
                    // NOTE: Expected to always have enough space to write into pre-allocated buffer
                    success = RespWriteUtils.TryWriteBulkString(values[i].Span, ref curr, ptr + totalLen);
                    Debug.Assert(success, "Insufficient space in pre-allocated buffer");
                }
            }
        }

        /// <summary>
        /// Create output as an array of bulk strings, from given array of ArgSlice values
        /// </summary>
        protected static unsafe void WriteBulkStringArray(ref MemoryResult<byte> output, List<PinnedSpanByte> values)
        {
            var totalLen = 1 + NumUtils.CountDigits(values.Count) + 2;
            for (var i = 0; i < values.Count; i++)
                totalLen += RespWriteUtils.GetBulkStringLength(values[i].Length);

            output.MemoryOwner?.Dispose();
            output.MemoryOwner = MemoryPool.Rent(totalLen);
            output.Length = totalLen;

            fixed (byte* ptr = output.MemoryOwner.Memory.Span)
            {
                var curr = ptr;
                // NOTE: Expected to always have enough space to write into pre-allocated buffer
                var success = RespWriteUtils.TryWriteArrayLength(values.Count, ref curr, ptr + totalLen);
                Debug.Assert(success, "Insufficient response buffer space");
                for (var i = 0; i < values.Count; i++)
                {
                    // NOTE: Expected to always have enough space to write into pre-allocated buffer
                    success = RespWriteUtils.TryWriteBulkString(values[i].Span, ref curr, ptr + totalLen);
                    Debug.Assert(success, "Insufficient space in pre-allocated buffer");
                }
            }
        }

        /// <summary>
        /// Create output as bulk string, from given Span
        /// </summary>
        protected static unsafe void WriteBulkString(ref MemoryResult<byte> output, Span<byte> simpleString)
        {
            var _output = (output.MemoryOwner, output.Length);
            WriteBulkString(ref _output, simpleString);
            output.MemoryOwner = _output.MemoryOwner;
            output.Length = _output.Length;
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
                var success = RespWriteUtils.TryWriteBulkString(bulkString, ref curr, ptr + len);
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
                var success = RespWriteUtils.TryWriteNull(ref curr, ptr + len);
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
                var success = RespWriteUtils.TryWriteError(errorMessage, ref curr, ptr + len);
                Debug.Assert(success, "Insufficient space in pre-allocated buffer");
            }
            output.Item2 = len;
        }

        /// <summary>
        /// Create output as error message, from given string
        /// </summary>
        protected static unsafe void WriteError(ref MemoryResult<byte> output, ReadOnlySpan<char> errorMessage)
        {
            var _output = (output.MemoryOwner, output.Length);
            WriteError(ref _output, errorMessage);
            output.MemoryOwner = _output.MemoryOwner;
            output.Length = _output.Length;
        }

        /// <summary>
        /// Get argument from parse state, at specified index (starting from 0)
        /// </summary>
        /// <param name="parseState">Current parse state</param>
        /// <param name="idx">Current argument index in parse state</param>
        /// <returns>Argument as a span</returns>
        protected static unsafe PinnedSpanByte GetNextArg(ref SessionParseState parseState, ref int idx)
        {
            var arg = idx < parseState.Count
                ? parseState.GetArgSliceByRef(idx)
                : default;
            idx++;
            return arg;
        }

        /// <summary>
        /// Get argument from input, at specified index (starting from 0)
        /// </summary>
        /// <param name="procInput">Procedure input</param>
        /// <param name="idx">Current argument index in parse state</param>
        /// <returns>Argument as a span</returns>
        protected static unsafe PinnedSpanByte GetNextArg(ref CustomProcedureInput procInput, ref int idx)
        {
            return GetNextArg(ref procInput.parseState, ref idx);
        }

        /// <summary>Parse custom raw string command</summary>
        /// <param name="cmd">Command name</param>
        /// <param name="rawStringCommand">Parsed raw string command</param>
        /// <returns>True if command found, false otherwise</returns>
        protected bool ParseCustomRawStringCommand(string cmd, out CustomRawStringCommand rawStringCommand) =>
            respServerSession.ParseCustomRawStringCommand(cmd, out rawStringCommand);

        /// <summary>Parse custom object command</summary>
        /// <param name="cmd">Command name</param>
        /// <param name="objectCommand">Parsed object command</param>
        /// <returns>True if command found, false othrewise</returns>
        protected bool ParseCustomObjectCommand(string cmd, out CustomObjectCommand objectCommand) =>
            respServerSession.ParseCustomObjectCommand(cmd, out objectCommand);

        /// <summary>Execute a specific custom raw string command</summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="garnetApi"></param>
        /// <param name="rawStringCommand">Custom raw string command to execute</param>
        /// <param name="key">Key param</param>
        /// <param name="input">Args to the command</param>
        /// <param name="output">Output from the command</param>
        /// <returns>True if successful</returns>
        protected bool ExecuteCustomRawStringCommand<TGarnetApi>(TGarnetApi garnetApi, CustomRawStringCommand rawStringCommand, PinnedSpanByte key, PinnedSpanByte[] input, out PinnedSpanByte output)
            where TGarnetApi : IGarnetApi
        {
            return respServerSession.InvokeCustomRawStringCommand(ref garnetApi, rawStringCommand, key, input, out output);
        }

        /// <summary>Execute a specific custom object command</summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="garnetApi"></param>
        /// <param name="objectCommand">Custom object command to execute</param>
        /// <param name="key">Key parameter</param>
        /// <param name="input">Args to the command</param>
        /// <param name="output">Output from the command</param>
        /// <returns>True if successful</returns>
        protected bool ExecuteCustomObjectCommand<TGarnetApi>(TGarnetApi garnetApi, CustomObjectCommand objectCommand, PinnedSpanByte key, PinnedSpanByte[] input, out PinnedSpanByte output)
            where TGarnetApi : IGarnetApi
        {
            return respServerSession.InvokeCustomObjectCommand(ref garnetApi, objectCommand, key, input, out output);
        }
    }
}