// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal static class ObjectUtils
    {
        /// <summary>
        /// Reads and parses scan parameters from RESP format
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="cursorInput"></param>
        /// <param name="pattern"></param>
        /// <param name="patternLength"></param>
        /// <param name="countInInput"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        public static unsafe bool ReadScanInput(ref ObjectInput input, ref SpanByteAndMemory output,
            out int cursorInput, out byte* pattern, out int patternLength, out int countInInput, out bool isNoValue, out ReadOnlySpan<byte> error)
        {
            // Cursor
            cursorInput = input.arg1;

            // Largest number of items to print 
            var limitCountInOutput = input.arg2;

            patternLength = 0;
            pattern = default;

            // Default of items in output
            countInInput = 10;

            error = default;
            isNoValue = false;

            var currTokenIdx = 0;

            while (currTokenIdx < input.parseState.Count)
            {
                var sbParam = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                if (sbParam.SequenceEqual(CmdStrings.MATCH) || sbParam.SequenceEqual(CmdStrings.match))
                {
                    // Read pattern for keys filter
                    var sbPattern = input.parseState.GetArgSliceByRef(currTokenIdx++).SpanByte;
                    pattern = sbPattern.ToPointer();
                    patternLength = sbPattern.Length;
                }
                else if (sbParam.SequenceEqual(CmdStrings.COUNT) || sbParam.SequenceEqual(CmdStrings.count))
                {
                    if (!input.parseState.TryGetInt(currTokenIdx++, out countInInput))
                    {
                        error = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                        return false;
                    }

                    // Limiting number of items to send to the output
                    if (countInInput > limitCountInOutput)
                        countInInput = limitCountInOutput;
                }
                else if (sbParam.SequenceEqual(CmdStrings.NOVALUES) || sbParam.SequenceEqual(CmdStrings.novalues))
                {
                    isNoValue = true;
                }
            }

            return true;
        }


        /// <summary>
        /// Writes output for scan command using RESP format
        /// </summary>
        /// <param name="items"></param>
        /// <param name="cursor"></param>
        /// <param name="output"></param>
        public static unsafe void WriteScanOutput(List<byte[]> items, long cursor, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;

            try
            {
                while (!RespWriteUtils.TryWriteScanOutputHeader(cursor, ref curr, end))
                    RespMemoryWriter.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (items.Count == 0)
                {
                    // Empty array
                    while (!RespWriteUtils.TryWriteEmptyArray(ref curr, end))
                        RespMemoryWriter.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    // Write size of the array
                    while (!RespWriteUtils.TryWriteArrayLength(items.Count, ref curr, end))
                        RespMemoryWriter.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    foreach (var item in items)
                    {
                        if (item != null)
                        {
                            while (!RespWriteUtils.TryWriteBulkString(item, ref curr, end))
                                RespMemoryWriter.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        else
                        {
                            if (respProtocolVersion >= 3)
                            {
                                while (!RespWriteUtils.TryWriteResp3Null(ref curr, end))
                                    RespMemoryWriter.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }
                            else
                            {
                                while (!RespWriteUtils.TryWriteNull(ref curr, end))
                                    RespMemoryWriter.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }
                        }
                    }
                }
                _output.result1 = items.Count;
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
                    RespMemoryWriter.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        /// <summary>
        /// Writes output for scan command using RESP format
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <param name="output"></param>
        public static unsafe void WriteScanError(ReadOnlySpan<byte> errorMessage, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;

            try
            {
                while (!RespWriteUtils.TryWriteError(errorMessage, ref curr, end))
                    RespMemoryWriter.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
                    RespMemoryWriter.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }
    }
}