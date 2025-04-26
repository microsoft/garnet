// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal static class ObjectUtils
    {
        /// <summary>
        /// Scan an object
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="respProtocolVersion"></param>
        /// <returns></returns>
        public static unsafe void Scan(GarnetObjectBase obj, ref ObjectInput input,
                                       ref GarnetObjectStoreOutput output, byte respProtocolVersion)
        {
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            if (ReadScanInput(ref input, ref output.SpanByteAndMemory, out var cursorInput, out var pattern,
                              out var patternLength, out var limitCount, out var isNoValue, out var error))
            {
                if (obj is not HashObject)
                    isNoValue = false;

                obj.Scan(cursorInput, out var items, out var cursorOutput, limitCount, pattern,
                         patternLength, isNoValue);

                writer.WriteArrayLength(2);
                writer.WriteInt64AsBulkString(cursorOutput);

                if (items.Count == 0)
                {
                    // Empty array
                    writer.WriteEmptyArray();
                }
                else
                {
                    // Write size of the array
                    writer.WriteArrayLength(items.Count);

                    foreach (var item in items)
                    {
                        if (item != null)
                        {
                            writer.WriteBulkString(item);
                        }
                        else
                        {
                            writer.WriteNull();
                        }
                    }
                }

                output.Header.result1 = items.Count;
            }
            else
            {
                writer.WriteError(error);
            }
        }

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
        private static unsafe bool ReadScanInput(ref ObjectInput input, ref SpanByteAndMemory output,
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
    }
}