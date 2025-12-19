// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Base class for Garnet heap objects
    /// </summary>
    public abstract class GarnetObjectBase : HeapObjectBase, IGarnetObject
    {
        /// <inheritdoc />
        public abstract byte Type { get; }

        protected GarnetObjectBase(long heapMemorySize)
        {
            HeapMemorySize = heapMemorySize;
        }

        protected GarnetObjectBase(BinaryReader reader, long heapMemorySize)
            : this(heapMemorySize)
        {
            // Add anything here that should match DoSerialize()
        }

        /// <inheritdoc />
        public abstract bool Operate(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion, bool execOp, long updatedEtag, out long sizeChange);

        /// <summary>
        /// Serialize to given writer
        /// NOTE: Make sure to first call base.DoSerialize(writer) in all derived classes.
        /// </summary>
        public override void DoSerialize(BinaryWriter writer)
        {
            // Add anything here that needs to be in front of the derived object data
        }

        /// <inheritdoc />
        public override void WriteType(BinaryWriter writer, bool isNull) => writer.Write(isNull ? (byte)GarnetObjectType.Null : Type);

        /// <summary>
        /// Scan the items of the collection
        /// </summary>
        /// <param name="start">Shift the scan to this index</param>
        /// <param name="items">The matching items in the collection</param>
        /// <param name="cursor">The cursor in the current page</param>
        /// <param name="count">The number of items being taken in one iteration</param>
        /// <param name="pattern">A patter used to match the members of the collection</param>
        /// <param name="patternLength">The number of characters in the pattern</param>
        /// <returns></returns>
        public abstract unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0, bool isNoValue = false);

        /// <summary>
        /// Implement Scan command
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="respProtocolVersion"></param>
        protected unsafe void Scan(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            if (ReadScanInput(ref input, ref output.SpanByteAndMemory, out var cursorInput, out var pattern,
                              out var patternLength, out var limitCount, out var isNoValue, out var error))
            {
                Scan(cursorInput, out var items, out var cursorOutput, limitCount, pattern,
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
            out long cursorInput, out byte* pattern, out int patternLength, out int countInInput, out bool isNoValue, out ReadOnlySpan<byte> error)
        {
            // Largest number of items to print
            var limitCountInOutput = input.arg2;

            patternLength = 0;
            pattern = default;

            // Default of items in output
            countInInput = 10;

            error = default;
            isNoValue = false;

            // Cursor
            if (!input.parseState.TryGetLong(0, out cursorInput) || cursorInput < 0)
            {
                error = CmdStrings.RESP_ERR_GENERIC_INVALIDCURSOR;
                return false;
            }

            var currTokenIdx = 1;

            while (currTokenIdx < input.parseState.Count)
            {
                var sbParam = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                if (sbParam.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MATCH))
                {
                    // Read pattern for keys filter
                    var sbPattern = input.parseState.GetArgSliceByRef(currTokenIdx++);
                    pattern = sbPattern.ToPointer();
                    patternLength = sbPattern.Length;
                }
                else if (sbParam.EqualsUpperCaseSpanIgnoringCase(CmdStrings.COUNT))
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
                else if (sbParam.EqualsUpperCaseSpanIgnoringCase(CmdStrings.NOVALUES))
                {
                    isNoValue = true;
                }
            }

            return true;
        }
    }
}