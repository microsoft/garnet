// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal static class ObjectUtils
    {
        public static unsafe void ReallocateOutput(ref SpanByteAndMemory output, ref bool isMemory, ref byte* ptr, ref MemoryHandle ptrHandle, ref byte* curr, ref byte* end)
        {
            int length = Math.Max(output.Length * 2, 1024);
            var newMem = MemoryPool<byte>.Shared.Rent(length);
            var newPtrHandle = newMem.Memory.Pin();
            var newPtr = (byte*)newPtrHandle.Pointer;
            int bytesWritten = (int)(curr - ptr);
            Buffer.MemoryCopy(ptr, newPtr, length, bytesWritten);
            if (isMemory)
            {
                ptrHandle.Dispose();
                output.Memory.Dispose();
            }
            else
            {
                isMemory = true;
                output.ConvertToHeap();
            }
            ptrHandle = newPtrHandle;
            ptr = newPtr;
            output.Memory = newMem;
            output.Length = length;
            curr = ptr + bytesWritten;
            end = ptr + output.Length;
        }

        /// <summary>
        /// Reads and parses scan parameters from RESP format
        /// </summary>
        /// <param name="input"></param>
        /// <param name="length"></param>
        /// <param name="output"></param>
        /// <param name="cursorInput"></param>
        /// <param name="pattern"></param>
        /// <param name="patternLength"></param>
        /// <param name="countInInput"></param>
        /// <param name="bytesDone"></param>
        /// <returns></returns>
        public static unsafe bool ReadScanInput(byte* input, int length, ref SpanByteAndMemory output, out int cursorInput, out byte* pattern, out int patternLength, out int countInInput, out int bytesDone)
        {
            var _input = (ObjectInputHeader*)input;

            // HeaderSize + Integer for limitCountInOutput
            byte* input_startptr = input + ObjectInputHeader.Size + sizeof(int);
            byte* input_currptr = input_startptr;

            int leftTokens = _input->count;

            // Largest number of items to print 
            int limitCountInOutput = *(int*)(input + ObjectInputHeader.Size);

            int parameterLength = 0;
            byte* parameterWord = null;

            // Cursor
            cursorInput = _input->done;

            patternLength = 0;
            pattern = default;

            // Default of items in output
            countInInput = 10;

            ObjectOutputHeader _output = default;

            // This value is used to indicate partial command execution
            _output.countDone = Int32.MinValue;
            bytesDone = 0;

            while (leftTokens > 0)
            {
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref parameterWord, ref parameterLength, ref input_currptr, input + length))
                    return false;

                var parameterSB = new ReadOnlySpan<byte>(parameterWord, parameterLength);

                if (parameterSB.SequenceEqual(CmdStrings.MATCH) || parameterSB.SequenceEqual(CmdStrings.match))
                {
                    // Read pattern for keys filter
                    if (!RespReadUtils.ReadPtrWithLengthHeader(ref pattern, ref patternLength, ref input_currptr, input + length))
                        return false;
                    leftTokens--;
                }
                else if (parameterSB.SequenceEqual(CmdStrings.COUNT) || parameterSB.SequenceEqual(CmdStrings.count))
                {
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var countParameterValue, ref input_currptr, input + length))
                        return false;

                    int.TryParse(Encoding.ASCII.GetString(countParameterValue), out countInInput);

                    // Limiting number of items to send to the output
                    if (countInInput > limitCountInOutput)
                        countInInput = limitCountInOutput;

                    leftTokens--;
                }
                leftTokens--;
            }

            bytesDone = (int)(input_currptr - input_startptr);
            return true;
        }


        /// <summary>
        /// Writes output for scan command using RESP format
        /// </summary>
        /// <param name="items"></param>
        /// <param name="cursor"></param>
        /// <param name="output"></param>
        /// <param name="bytesDone"></param>
        public static unsafe void WriteScanOutput(List<byte[]> items, long cursor, ref SpanByteAndMemory output, int bytesDone)
        {
            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;

            try
            {
                while (!RespWriteUtils.WriteScanOutputHeader(cursor, ref curr, end))
                    ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (items.Count == 0)
                {
                    // Empty array
                    while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                        ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    // Write size of the array
                    while (!RespWriteUtils.WriteArrayLength(items.Count, ref curr, end))
                        ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    foreach (var item in items)
                    {
                        if (item != null)
                        {
                            while (!RespWriteUtils.WriteBulkString(item, ref curr, end))
                                ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        else
                            while (!RespWriteUtils.WriteNull(ref curr, end))
                                ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = bytesDone;
                _output.countDone = items.Count;
                _output.opsDone = items.Count;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }
    }
}