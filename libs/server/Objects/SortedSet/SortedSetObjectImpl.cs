// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Sorted Set - RESP specific operations
    /// </summary>
    public unsafe partial class SortedSetObject : GarnetObjectBase
    {
        /// <summary>
        /// Small struct to store options for ZRange command
        /// </summary>
        private struct ZRangeOptions
        {
            public bool ByScore { get; set; }
            public bool ByLex { get; set; }
            public bool Reverse { get; set; }
            public (int, int) Limit { get; set; }
            public bool ValidLimit { get; set; }
            public bool WithScores { get; set; }
        };

        bool GetOptions(ref ObjectInput input, ref int currTokenIdx, out SortedSetAddOption options, ref byte* curr, byte* end, ref SpanByteAndMemory output, ref bool isMemory, ref byte* ptr, ref MemoryHandle ptrHandle)
        {
            options = SortedSetAddOption.None;

            while (currTokenIdx < input.parseState.Count)
            {
                if (!input.parseState.TryGetSortedSetAddOption(currTokenIdx, out var currOption))
                    break;

                options |= currOption;
                currTokenIdx++;
            }

            // Validate ZADD options combination
            ReadOnlySpan<byte> optionsError = default;

            // XX & NX are mutually exclusive
            if (options.HasFlag(SortedSetAddOption.XX) && options.HasFlag(SortedSetAddOption.NX))
                optionsError = CmdStrings.RESP_ERR_XX_NX_NOT_COMPATIBLE;

            // NX, GT & LT are mutually exclusive
            if ((options.HasFlag(SortedSetAddOption.GT) && options.HasFlag(SortedSetAddOption.LT)) ||
               ((options.HasFlag(SortedSetAddOption.GT) || options.HasFlag(SortedSetAddOption.LT)) &&
                options.HasFlag(SortedSetAddOption.NX)))
                optionsError = CmdStrings.RESP_ERR_GT_LT_NX_NOT_COMPATIBLE;

            // INCR supports only one score-element pair
            if (options.HasFlag(SortedSetAddOption.INCR) && (input.parseState.Count - currTokenIdx > 2))
                optionsError = CmdStrings.RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR;

            if (!optionsError.IsEmpty)
            {
                while (!RespWriteUtils.WriteError(optionsError, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                return false;
            }

            // From here on we expect only score-element pairs
            // Remaining token count should be positive and even
            if (currTokenIdx == input.parseState.Count || (input.parseState.Count - currTokenIdx) % 2 != 0)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                return false;
            }

            return true;
        }

        private void SortedSetAdd(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader outputHeader = default;
            var addedOrChanged = 0;
            double incrResult = 0;

            try
            {
                var options = SortedSetAddOption.None;
                var currTokenIdx = 0;
                var parsedOptions = false;

                while (currTokenIdx < input.parseState.Count)
                {
                    // Try to parse a Score field
                    if (!input.parseState.TryGetDouble(currTokenIdx, out var score))
                    {
                        // Try to get and validate options before the Score field, if any
                        if (!parsedOptions)
                        {
                            parsedOptions = true;
                            if (!GetOptions(ref input, ref currTokenIdx, out options, ref curr, end, ref output, ref isMemory, ref ptr, ref ptrHandle))
                                return;
                            continue; // retry after parsing options
                        }
                        else
                        {
                            // Invalid Score encountered
                            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            return;
                        }
                    }

                    parsedOptions = true;
                    currTokenIdx++;

                    // Member
                    var memberSpan = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;
                    var member = memberSpan.ToArray();

                    // Add new member
                    if (!sortedSetDict.TryGetValue(member, out var scoreStored))
                    {
                        // Don't add new member if XX flag is set
                        if (options.HasFlag(SortedSetAddOption.XX)) continue;

                        sortedSetDict.Add(member, score);
                        if (sortedSet.Add((score, member)))
                            addedOrChanged++;

                        this.UpdateSize(memberSpan);
                    }
                    // Update existing member
                    else
                    {
                        // Update new score if INCR flag is set
                        if (options.HasFlag(SortedSetAddOption.INCR))
                        {
                            score += scoreStored;
                            incrResult = score;
                        }

                        // No need for update
                        if (score == scoreStored)
                            continue;

                        // Don't update existing member if NX flag is set
                        // or if GT/LT flag is set and existing score is higher/lower than new score, respectively
                        if (options.HasFlag(SortedSetAddOption.NX) ||
                            (options.HasFlag(SortedSetAddOption.GT) && scoreStored > score) ||
                            (options.HasFlag(SortedSetAddOption.LT) && scoreStored < score)) continue;

                        sortedSetDict[member] = score;
                        var success = sortedSet.Remove((scoreStored, member));
                        Debug.Assert(success);
                        success = sortedSet.Add((score, member));
                        Debug.Assert(success);

                        // If CH flag is set, add changed member to final count
                        if (options.HasFlag(SortedSetAddOption.CH))
                            addedOrChanged++;
                    }
                }

                if (options.HasFlag(SortedSetAddOption.INCR))
                {
                    while (!RespWriteUtils.TryWriteDoubleBulkString(incrResult, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    while (!RespWriteUtils.WriteInteger(addedOrChanged, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRemove(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var value = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;
                var valueArray = value.ToArray();

                if (!sortedSetDict.TryGetValue(valueArray, out var key))
                    continue;

                _output->result1++;
                sortedSetDict.Remove(valueArray);
                sortedSet.Remove((key, valueArray));

                this.UpdateSize(value, false);
            }
        }

        private void SortedSetLength(byte* output)
        {
            // Check both objects
            Debug.Assert(sortedSetDict.Count == sortedSet.Count, "SortedSet object is not in sync.");
            ((ObjectOutputHeader*)output)->result1 = sortedSetDict.Count;
        }

        private void SortedSetScore(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // ZSCORE key member
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            var member = input.parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();

            ObjectOutputHeader outputHeader = default;
            try
            {
                if (!sortedSetDict.TryGetValue(member, out var score))
                {
                    while (!RespWriteUtils.WriteNull(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                outputHeader.result1 = 1;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetScores(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // ZMSCORE key member
            var count = input.parseState.Count;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader outputHeader = default;

            try
            {
                while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                for (var i = 0; i < count; i++)
                {
                    var member = input.parseState.GetArgSliceByRef(i).SpanByte.ToByteArray();

                    if (!sortedSetDict.TryGetValue(member, out var score))
                    {
                        while (!RespWriteUtils.WriteNull(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
                outputHeader.result1 = count;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetCount(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            // Read min & max
            var minParamSpan = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            var maxParamSpan = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

            ObjectOutputHeader outputHeader = default;

            try
            {
                // Check if parameters are valid
                if (!TryParseParameter(minParamSpan, out var minValue, out var minExclusive) ||
                    !TryParseParameter(maxParamSpan, out var maxValue, out var maxExclusive))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                // get the elements within the score range and write the result
                var count = 0;
                if (sortedSet.Count > 0)
                {
                    foreach (var item in sortedSet.GetViewBetween((minValue, null), sortedSet.Max))
                    {
                        if (item.Item1 > maxValue || (maxExclusive && item.Item1 == maxValue)) break;
                        if (minExclusive && item.Item1 == minValue) continue;
                        count++;
                    }
                }

                while (!RespWriteUtils.WriteInteger(count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetIncrement(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // ZINCRBY key increment member
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader outputHeader = default;

            try
            {
                // Try to read increment value
                if (!input.parseState.TryGetDouble(0, out var incrValue))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                // Read member
                var member = input.parseState.GetArgSliceByRef(1).SpanByte.ToByteArray();

                if (sortedSetDict.TryGetValue(member, out var score))
                {
                    sortedSetDict[member] += incrValue;
                    sortedSet.Remove((score, member));
                    sortedSet.Add((sortedSetDict[member], member));
                }
                else
                {
                    sortedSetDict.Add(member, incrValue);
                    sortedSet.Add((incrValue, member));

                    this.UpdateSize(member);
                }

                // Write the new score
                while (!RespWriteUtils.TryWriteDoubleBulkString(sortedSetDict[member], ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRange(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            //ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
            //ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
            //ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
            var count = input.parseState.Count;
            var respProtocolVersion = input.arg1;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            var currIdx = 0;

            ObjectOutputHeader _output = default;
            try
            {
                // Read min & max
                var minSpan = input.parseState.GetArgSliceByRef(currIdx++).ReadOnlySpan;
                var maxSpan = input.parseState.GetArgSliceByRef(currIdx++).ReadOnlySpan;

                // read the rest of the arguments
                ZRangeOptions options = new();
                switch (input.header.SortedSetOp)
                {
                    case SortedSetOperation.ZRANGEBYSCORE:
                        options.ByScore = true;
                        break;
                    case SortedSetOperation.ZREVRANGE:
                        options.Reverse = true;
                        break;
                    case SortedSetOperation.ZREVRANGEBYSCORE:
                        options.ByScore = true;
                        options.Reverse = true;
                        break;
                }

                if (count > 2)
                {
                    while (currIdx < count)
                    {
                        var tokenSpan = input.parseState.GetArgSliceByRef(currIdx++).ReadOnlySpan;

                        if (tokenSpan.EqualsUpperCaseSpanIgnoringCase("BYSCORE"u8))
                        {
                            options.ByScore = true;
                        }
                        else if (tokenSpan.EqualsUpperCaseSpanIgnoringCase("BYLEX"u8))
                        {
                            options.ByLex = true;
                        }
                        else if (tokenSpan.EqualsUpperCaseSpanIgnoringCase("REV"u8))
                        {
                            options.Reverse = true;
                        }
                        else if (tokenSpan.EqualsUpperCaseSpanIgnoringCase("LIMIT"u8))
                        {
                            // Verify that there are at least 2 more tokens to read
                            if (input.parseState.Count - currIdx < 2)
                            {
                                while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                return;
                            }

                            // Read the next two tokens
                            if (!input.parseState.TryGetInt(currIdx++, out var offset) ||
                                !input.parseState.TryGetInt(currIdx++, out var countLimit))
                            {
                                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                return;
                            }

                            options.Limit = (offset, countLimit);
                            options.ValidLimit = true;
                        }
                        else if (tokenSpan.EqualsUpperCaseSpanIgnoringCase("WITHSCORES"u8))
                        {
                            options.WithScores = true;
                        }
                    }
                }

                if (count >= 2 && ((!options.ByScore && !options.ByLex) || options.ByScore))
                {
                    if (!TryParseParameter(minSpan, out var minValue, out var minExclusive) ||
                        !TryParseParameter(maxSpan, out var maxValue, out var maxExclusive))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        return;
                    }

                    if (options.ByScore)
                    {
                        var scoredElements = GetElementsInRangeByScore(minValue, maxValue, minExclusive, maxExclusive, options.WithScores, options.Reverse, options.ValidLimit, false, options.Limit);

                        WriteSortedSetResult(options.WithScores, scoredElements.Count, respProtocolVersion, scoredElements, ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {  // byIndex
                        int minIndex = (int)minValue, maxIndex = (int)maxValue;
                        if (options.ValidLimit)
                        {
                            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_LIMIT_NOT_SUPPORTED, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            return;
                        }
                        else if (minValue > sortedSetDict.Count - 1)
                        {
                            // return empty list
                            while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            return;
                        }
                        else
                        {
                            //shift from the end of the set
                            if (minIndex < 0)
                            {
                                minIndex = sortedSetDict.Count + minIndex;
                            }
                            if (maxIndex < 0)
                            {
                                maxIndex = sortedSetDict.Count + maxIndex;
                            }
                            else if (maxIndex >= sortedSetDict.Count)
                            {
                                maxIndex = sortedSetDict.Count - 1;
                            }

                            // No elements to return if both indexes fall outside the range or min is higher than max
                            if ((minIndex < 0 && maxIndex < 0) || (minIndex > maxIndex))
                            {
                                while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                return;
                            }
                            else
                            {
                                // Clamp minIndex to 0, if it is beyond the number of elements
                                minIndex = Math.Max(0, minIndex);

                                // calculate number of elements
                                var n = maxIndex - minIndex + 1;
                                var iterator = options.Reverse ? sortedSet.Reverse() : sortedSet;
                                iterator = iterator.Skip(minIndex).Take(n);

                                WriteSortedSetResult(options.WithScores, n, respProtocolVersion, iterator, ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }
                        }
                    }
                }

                // by Lex
                if (count >= 2 && options.ByLex)
                {
                    var elementsInLex = GetElementsInRangeByLex(minSpan, maxSpan, options.Reverse, options.ValidLimit, false, out var errorCode, options.Limit);

                    if (errorCode == int.MaxValue)
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        WriteSortedSetResult(options.WithScores, elementsInLex.Count, respProtocolVersion, elementsInLex, ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        void WriteSortedSetResult(bool withScores, int count, int respProtocolVersion, IEnumerable<(double, byte[])> iterator, ref SpanByteAndMemory output, ref bool isMemory, ref byte* ptr, ref MemoryHandle ptrHandle, ref byte* curr, ref byte* end)
        {
            if (withScores && respProtocolVersion >= 3)
            {
                // write the size of the array reply
                while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                foreach (var (score, element) in iterator)
                {
                    while (!RespWriteUtils.WriteArrayLength(2, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    while (!RespWriteUtils.WriteBulkString(element, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    while (!RespWriteUtils.TryWriteDoubleNumeric(score, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
            }
            else
            {
                // write the size of the array reply
                while (!RespWriteUtils.WriteArrayLength(withScores ? count * 2 : count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                foreach (var (score, element) in iterator)
                {
                    while (!RespWriteUtils.WriteBulkString(element, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    if (withScores)
                    {
                        while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
            }
        }

        private void SortedSetRemoveRangeByRank(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // ZREMRANGEBYRANK key start stop
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader outputHeader = default;

            try
            {
                if (!input.parseState.TryGetInt(0, out var start) ||
                    !input.parseState.TryGetInt(1, out var stop))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                if (start > sortedSetDict.Count - 1)
                    return;

                // Shift from the end of the set
                start = start < 0 ? sortedSetDict.Count + start : start;
                stop = stop < 0
                    ? sortedSetDict.Count + stop
                    : stop >= sortedSetDict.Count ? sortedSetDict.Count - 1 : stop;

                // Calculate number of elements
                var elementCount = stop - start + 1;

                // Using to list to avoid modified enumerator exception
                foreach (var item in sortedSet.Skip(start).Take(elementCount).ToList())
                {
                    if (sortedSetDict.Remove(item.Item2, out var key))
                    {
                        sortedSet.Remove((key, item.Item2));

                        this.UpdateSize(item.Item2, false);
                    }
                }

                // Write the number of elements
                while (!RespWriteUtils.WriteInteger(elementCount, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRemoveRangeByScore(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // ZREMRANGEBYSCORE key min max
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader outputHeader = default;

            try
            {
                // Read min and max
                var minParamBytes = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                var maxParamBytes = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                if (!TryParseParameter(minParamBytes, out var minValue, out var minExclusive) ||
                    !TryParseParameter(maxParamBytes, out var maxValue, out var maxExclusive))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                var elementCount = GetElementsInRangeByScore(minValue, maxValue, minExclusive, maxExclusive, false,
                    false, false, true).Count;

                // Write the number of elements
                while (!RespWriteUtils.WriteInteger(elementCount, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRandomMember(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var count = input.arg1 >> 2;
            var withScores = (input.arg1 & 1) == 1;
            var includedCount = ((input.arg1 >> 1) & 1) == 1;
            var seed = input.arg2;

            if (count > 0 && count > sortedSet.Count)
                count = sortedSet.Count;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                // The count parameter can have a negative value, but the array length can't
                var arrayLength = Math.Abs(withScores ? count * 2 : count);
                if (arrayLength > 1 || (arrayLength == 1 && includedCount))
                {
                    while (!RespWriteUtils.WriteArrayLength(arrayLength, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }

                var indexes = RandomUtils.PickKRandomIndexes(sortedSetDict.Count, Math.Abs(count), seed, count > 0);

                foreach (var item in indexes)
                {
                    var (element, score) = sortedSetDict.ElementAt(item);

                    while (!RespWriteUtils.WriteBulkString(element, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    if (withScores)
                    {
                        while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }

                // Write count done into output footer
                _output.result1 = count;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRemoveOrCountRangeByLex(ref ObjectInput input, byte* output, SortedSetOperation op)
        {
            // ZREMRANGEBYLEX key min max
            // ZLEXCOUNT key min max
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            // Using minValue for partial execution detection
            _output->result1 = int.MinValue;

            var minParamBytes = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            var maxParamBytes = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

            var rem = GetElementsInRangeByLex(minParamBytes, maxParamBytes, false, false, op != SortedSetOperation.ZLEXCOUNT, out int errorCode);

            _output->result1 = errorCode;
            if (errorCode == 0)
                _output->result1 = rem.Count;
        }

        /// <summary>
        /// Gets the rank of a member of the sorted set
        /// in ascending or descending order
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="ascending"></param>
        private void SortedSetRank(ref ObjectInput input, ref SpanByteAndMemory output, bool ascending = true)
        {
            //ZRANK key member
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            var withScore = input.arg1 == 1;

            ObjectOutputHeader outputHeader = default;
            try
            {
                var member = input.parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();

                if (!sortedSetDict.TryGetValue(member, out var score))
                {
                    while (!RespWriteUtils.WriteNull(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    var rank = 0;
                    foreach (var item in sortedSet)
                    {
                        if (item.Item2.SequenceEqual(member))
                            break;
                        rank++;
                    }

                    if (!ascending)
                        rank = sortedSet.Count - rank - 1;

                    if (withScore)
                    {
                        while (!RespWriteUtils.WriteArrayLength(2, ref curr, end)) // Rank and score
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        while (!RespWriteUtils.WriteInteger(rank, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteInteger(rank, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();

                output.Length = (int)(curr - ptr);
            }
        }

        /// <summary>
        /// Removes and returns up to COUNT members with the low or high score
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="op"></param>
        private void SortedSetPopMinOrMaxCount(ref ObjectInput input, ref SpanByteAndMemory output, SortedSetOperation op)
        {
            var count = input.arg1;
            var countDone = 0;

            if (sortedSet.Count < count)
                count = sortedSet.Count;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader outputHeader = default;

            try
            {
                while (!RespWriteUtils.WriteArrayLength(count * 2, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                while (count > 0)
                {
                    var max = op == SortedSetOperation.ZPOPMAX ? sortedSet.Max : sortedSet.Min;
                    sortedSet.Remove(max);
                    sortedSetDict.Remove(max.Element);

                    this.UpdateSize(max.Element, false);

                    while (!RespWriteUtils.WriteBulkString(max.Element, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    while (!RespWriteUtils.TryWriteDoubleBulkString(max.Score, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    countDone++;
                    count--;
                }

                outputHeader.result1 = countDone;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        #region CommonMethods

        /// <summary>
        /// Gets the elements that belong to the Range using lexicographical order
        /// </summary>
        /// <param name="minParamByteArray"></param>
        /// <param name="maxParamByteArray"></param>
        /// <param name="doReverse">Perfom reverse order</param>
        /// <param name="validLimit">Use a limit offset count</param>
        /// <param name="rem">Remove elements</param>
        /// <param name="errorCode">errorCode</param>
        /// <param name="limit">offset and count values</param>
        /// <returns></returns>
        private List<(double, byte[])> GetElementsInRangeByLex(
            ReadOnlySpan<byte> minParamByteArray,
            ReadOnlySpan<byte> maxParamByteArray,
            bool doReverse,
            bool validLimit,
            bool rem,
            out int errorCode,
            (int, int) limit = default)
        {
            var elementsInLex = new List<(double, byte[])>();

            // parse boundaries
            if (!TryParseLexParameter(minParamByteArray, out var minValueChars, out bool minValueExclusive) ||
                !TryParseLexParameter(maxParamByteArray, out var maxValueChars, out bool maxValueExclusive))
            {
                errorCode = int.MaxValue;
                return elementsInLex;
            }

            try
            {
                if (doReverse)
                {
                    var tmpMinValueChars = minValueChars;
                    minValueChars = maxValueChars;
                    maxValueChars = tmpMinValueChars;
                }

                var iterator = sortedSet.GetViewBetween((sortedSet.Min.Item1, minValueChars.ToArray()), sortedSet.Max);

                // using ToList method so we avoid the Invalid operation ex. when removing
                foreach (var item in iterator.ToList())
                {
                    var inRange = new ReadOnlySpan<byte>(item.Item2).SequenceCompareTo(minValueChars);
                    if (inRange < 0 || (inRange == 0 && minValueExclusive))
                        continue;

                    var outRange = maxValueChars.IsEmpty ? -1 : new ReadOnlySpan<byte>(item.Item2).SequenceCompareTo(maxValueChars);
                    if (outRange > 0 || (outRange == 0 && maxValueExclusive))
                        break;

                    if (rem)
                    {
                        if (sortedSetDict.TryGetValue(item.Item2, out var _key))
                        {
                            sortedSetDict.Remove(item.Item2);
                            sortedSet.Remove((_key, item.Item2));

                            this.UpdateSize(item.Item2, false);
                        }
                    }
                    elementsInLex.Add(item);
                }

                if (doReverse) elementsInLex.Reverse();

                if (validLimit)
                {
                    elementsInLex = elementsInLex
                                        .Skip(limit.Item1 > 0 ? limit.Item1 : 0)
                                        .Take(limit.Item2 > 0 ? limit.Item2 : elementsInLex.Count)
                                        .ToList();
                }
            }
            catch (ArgumentException)
            {
                // this exception is thrown when the SortedSet is empty
                Debug.Assert(sortedSet.Count == 0);
            }

            errorCode = 0;
            return elementsInLex;
        }

        /// <summary>
        /// Gets a range of elements using by score filters, when
        /// rem flag is true, removes the elements in the range
        /// </summary>
        /// <param name="minValue"></param>
        /// <param name="maxValue"></param>
        /// <param name="minExclusive"></param>
        /// <param name="maxExclusive"></param>
        /// <param name="withScore"></param>
        /// <param name="doReverse"></param>
        /// <param name="validLimit"></param>
        /// <param name="rem"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        private List<(double, byte[])> GetElementsInRangeByScore(double minValue, double maxValue, bool minExclusive, bool maxExclusive, bool withScore, bool doReverse, bool validLimit, bool rem, (int, int) limit = default)
        {
            if (doReverse)
            {
                (minValue, maxValue) = (maxValue, minValue);
            }

            List<(double, byte[])> scoredElements = new();
            if (sortedSet.Max.Item1 < minValue)
            {
                return scoredElements;
            }

            foreach (var item in sortedSet.GetViewBetween((minValue, null), sortedSet.Max))
            {
                if (item.Item1 > maxValue || (maxExclusive && item.Item1 == maxValue)) break;
                if (minExclusive && item.Item1 == minValue) continue;
                scoredElements.Add(item);
            }
            if (doReverse) scoredElements.Reverse();
            if (validLimit)
            {
                scoredElements = scoredElements
                                 .Skip(limit.Item1 > 0 ? limit.Item1 : 0)
                                 .Take(limit.Item2 > 0 ? limit.Item2 : scoredElements.Count)
                                 .ToList();
            }

            if (rem)
            {
                foreach (var item in scoredElements.ToList())
                {
                    if (sortedSetDict.TryGetValue(item.Item2, out var _key))
                    {
                        sortedSetDict.Remove(item.Item2);
                        sortedSet.Remove((_key, item.Item2));

                        this.UpdateSize(item.Item2, false);
                    }
                }
            }

            return scoredElements;
        }

        #endregion

        #region HelperMethods

        /// <summary>
        /// Helper method to parse parameters min and max
        /// in commands including +inf -inf
        /// </summary>
        private static bool TryParseParameter(ReadOnlySpan<byte> val, out double valueDouble, out bool exclusive)
        {
            exclusive = false;

            // adjust for exclusion
            if (val[0] == '(')
            {
                val = val.Slice(1);
                exclusive = true;
            }

            if (NumUtils.TryParse(val, out valueDouble))
            {
                return true;
            }

            var strVal = Encoding.ASCII.GetString(val);
            if (string.Equals("+inf", strVal, StringComparison.OrdinalIgnoreCase))
            {
                valueDouble = double.PositiveInfinity;
                exclusive = false;
                return true;
            }
            else if (string.Equals("-inf", strVal, StringComparison.OrdinalIgnoreCase))
            {
                valueDouble = double.NegativeInfinity;
                exclusive = false;
                return true;
            }
            return false;
        }

        /// <summary>
        /// Helper method to parse parameter when using Lexicographical ranges
        /// </summary>
        private static bool TryParseLexParameter(ReadOnlySpan<byte> val, out ReadOnlySpan<byte> limitChars, out bool limitExclusive)
        {
            limitChars = default;
            limitExclusive = false;

            switch (val[0])
            {
                case (byte)'+':
                case (byte)'-':
                    return true;
                case (byte)'[':
                    limitChars = val.Slice(1);
                    limitExclusive = false;
                    return true;
                case (byte)'(':
                    limitChars = val.Slice(1);
                    limitExclusive = true;
                    return true;
                default:
                    return false;
            }
        }

        #endregion
    }
}