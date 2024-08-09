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

        private void SortedSetAdd(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;

            var count = input.arg1;
            *_output = default;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            for (var c = 0; c < count; c++)
            {
                if (!RespReadUtils.ReadDoubleWithLengthHeader(out var score, out var parsed, ref input_currptr, input_endptr))
                    return;
                if (!RespReadUtils.TrySliceWithLengthHeader(out var member, ref input_currptr, input_endptr))
                    return;

                if (parsed)
                {
                    var memberArray = member.ToArray();
                    if (!sortedSetDict.TryGetValue(memberArray, out var scoreStored))
                    {
                        sortedSetDict.Add(memberArray, score);
                        if (sortedSet.Add((score, memberArray)))
                        {
                            _output->result1++;
                        }

                        this.UpdateSize(member);
                    }
                    else if (scoreStored != score)
                    {
                        sortedSetDict[memberArray] = score;
                        var success = sortedSet.Remove((scoreStored, memberArray));
                        Debug.Assert(success);
                        success = sortedSet.Add((score, memberArray));
                        Debug.Assert(success);
                    }
                }
            }
        }

        private void SortedSetRemove(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;

            var count = input.arg1;
            *_output = default;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            for (var c = 0; c < count; c++)
            {
                if (!RespReadUtils.TrySliceWithLengthHeader(out var value, ref input_currptr, input_endptr))
                    return;

                var valueArray = value.ToArray();
                if (sortedSetDict.TryGetValue(valueArray, out var key))
                {
                    _output->result1++;
                    sortedSetDict.Remove(valueArray);
                    sortedSet.Remove((key, valueArray));

                    this.UpdateSize(value, false);
                }
            }
        }

        private void SortedSetLength(byte* output)
        {
            // Check both objects
            Debug.Assert(sortedSetDict.Count == sortedSet.Count, "SortedSet object is not in sync.");
            ((ObjectOutputHeader*)output)->result1 = sortedSetDict.Count;
        }

        private void SortedSetPop(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            PopMinOrMaxCount(ref input, ref output, SortedSetOperation.ZPOPMAX);
        }

        private void SortedSetScore(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // ZSCORE key member
            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            byte* scorePtr = default;
            var scoreLength = 0;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref scorePtr, ref scoreLength, ref input_currptr, input_endptr))
                return;

            var scoreKey = new Span<byte>(scorePtr, scoreLength).ToArray();

            ObjectOutputHeader outputHeader = default;
            try
            {
                if (!sortedSetDict.TryGetValue(scoreKey, out var score))
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
            var count = input.arg1;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

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

                for (int c = 0; c < count; c++)
                {
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var scoreKey, ref input_currptr, input_endptr))
                        return;
                    if (!sortedSetDict.TryGetValue(scoreKey, out var score))
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

        private void SortedSetCount(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            // read min
            if (!RespReadUtils.TrySliceWithLengthHeader(out var minParamSpan, ref input_currptr, input_endptr))
                return;

            // read max
            if (!RespReadUtils.TrySliceWithLengthHeader(out var maxParamSpan, ref input_currptr, input_endptr))
                return;

            //check if parameters are valid
            if (!TryParseParameter(minParamSpan, out var minValue, out var minExclusive) ||
                !TryParseParameter(maxParamSpan, out var maxValue, out var maxExclusive))
            {
                _output->result1 = int.MaxValue;
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
            _output->result1 = count;
        }

        private void SortedSetIncrement(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // ZINCRBY key increment member
            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;

            // To validate partial execution
            _output.result1 = int.MinValue;
            try
            {
                // read increment
                if (!RespReadUtils.TrySliceWithLengthHeader(out var incrementBytes, ref input_currptr, input_endptr))
                    return;

                // read member
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var memberByteArray, ref input_currptr, input_endptr))
                    return;

                //check if increment value is valid
                int countDone;
                if (!NumUtils.TryParse(incrementBytes, out double incrValue))
                {
                    countDone = int.MaxValue;
                }
                else
                {
                    if (sortedSetDict.TryGetValue(memberByteArray, out var score))
                    {
                        sortedSetDict[memberByteArray] += incrValue;
                        sortedSet.Remove((score, memberByteArray));
                        sortedSet.Add((sortedSetDict[memberByteArray], memberByteArray));
                    }
                    else
                    {
                        sortedSetDict.Add(memberByteArray, incrValue);
                        sortedSet.Add((incrValue, memberByteArray));

                        this.UpdateSize(memberByteArray);
                    }

                    // write the new score
                    while (!RespWriteUtils.TryWriteDoubleBulkString(sortedSetDict[memberByteArray], ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    countDone = 1;
                }
                _output.result1 = countDone;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRank(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            GetRank(ref input, ref output);
        }

        private void SortedSetRange(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            //ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
            //ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
            //ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
            var count = input.arg1;
            var respProtocolVersion = input.arg2;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                // read min
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var minParamByteArray, ref input_currptr, input_endptr))
                    return;

                // read max
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var maxParamByteArray, ref input_currptr, input_endptr))
                    return;

                var countDone = 2;

                // read the rest of the arguments
                ZRangeOptions options = new();
                if (input.header.SortedSetOp == SortedSetOperation.ZRANGEBYSCORE) options.ByScore = true;
                if (input.header.SortedSetOp == SortedSetOperation.ZREVRANGE) options.Reverse = true;
                if (input.header.SortedSetOp == SortedSetOperation.ZREVRANGEBYSCORE)
                {
                    options.Reverse = true;
                    options.ByScore = true;
                }

                if (count > 2)
                {
                    int i = 0;
                    while (i < count - 2)
                    {
                        if (!RespReadUtils.TrySliceWithLengthHeader(out var token, ref input_currptr, input_startptr + length))
                            return;

                        if (token.EqualsUpperCaseSpanIgnoringCase("BYSCORE"u8))
                        {
                            options.ByScore = true;
                        }
                        else if (token.EqualsUpperCaseSpanIgnoringCase("BYLEX"u8))
                        {
                            options.ByLex = true;
                        }
                        else if (token.EqualsUpperCaseSpanIgnoringCase("REV"u8))
                        {
                            options.Reverse = true;
                        }
                        else if (token.EqualsUpperCaseSpanIgnoringCase("LIMIT"u8))
                        {
                            // read the next two tokens
                            if (!RespReadUtils.TrySliceWithLengthHeader(out var offset, ref input_currptr, input_startptr + length) ||
                                !RespReadUtils.TrySliceWithLengthHeader(out var countLimit, ref input_currptr, input_startptr + length))
                            {
                                return;
                            }

                            if (TryParseParameter(offset, out var offsetLimit, out _) &&
                                TryParseParameter(countLimit, out var countLimitNumber, out _))
                            {
                                options.Limit = ((int)offsetLimit, (int)countLimitNumber);
                                options.ValidLimit = true;
                                i += 2;
                            }
                        }
                        else if (token.EqualsUpperCaseSpanIgnoringCase("WITHSCORES"u8))
                        {
                            options.WithScores = true;
                        }
                        i++;
                    }
                }

                if (count >= 2 && ((!options.ByScore && !options.ByLex) || options.ByScore))
                {
                    if (!TryParseParameter(minParamByteArray, out var minValue, out var minExclusive) |
                        !TryParseParameter(maxParamByteArray, out var maxValue, out var maxExclusive))
                    {
                        while (!RespWriteUtils.WriteError("ERR max or min value is not a float value."u8, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        countDone = input.arg1;
                        count = 0;
                    }

                    if (options.ByScore)
                    {
                        var scoredElements = GetElementsInRangeByScore(minValue, maxValue, minExclusive, maxExclusive, options.WithScores, options.Reverse, options.ValidLimit, false, options.Limit);

                        WriteSortedSetResult(options.WithScores, scoredElements.Count, respProtocolVersion, scoredElements, ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        countDone = input.arg1;
                    }
                    else
                    {  // byIndex
                        int minIndex = (int)minValue, maxIndex = (int)maxValue;
                        if (options.ValidLimit)
                        {
                            while (!RespWriteUtils.WriteError("ERR syntax error, LIMIT is only supported in BYSCORE or BYLEX."u8, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            countDone = input.arg1;
                            count = 0;
                        }
                        else if (minValue > sortedSetDict.Count - 1)
                        {
                            // return empty list
                            while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            countDone = input.arg1;
                            count = 0;
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
                                countDone = input.arg1;
                                count = 0;
                            }
                            else
                            {
                                // Clamp minIndex to 0, if it is beyond the number of elements
                                minIndex = Math.Max(0, minIndex);

                                // calculate number of elements
                                int n = maxIndex - minIndex + 1;
                                var iterator = options.Reverse ? sortedSet.Reverse() : sortedSet;
                                iterator = iterator.Skip(minIndex).Take(n);

                                WriteSortedSetResult(options.WithScores, n, respProtocolVersion, iterator, ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                countDone = input.arg1;
                            }
                        }
                    }
                }

                // by Lex
                if (count >= 2 && options.ByLex)
                {
                    var elementsInLex = GetElementsInRangeByLex(minParamByteArray, maxParamByteArray, options.Reverse, options.ValidLimit, false, out int errorCode, options.Limit);

                    if (errorCode == int.MaxValue)
                    {
                        while (!RespWriteUtils.WriteError("ERR max or min value not valid string range."u8, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        countDone = input.arg1;
                        count = 0;
                    }
                    else
                    {
                        WriteSortedSetResult(options.WithScores, elementsInLex.Count, respProtocolVersion, elementsInLex, ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        countDone = input.arg1;
                    }
                }
                _output.result1 = countDone;
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

        private void SortedSetRangeByScore(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            SortedSetRange(ref input, ref output);
        }

        private void SortedSetReverseRange(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            SortedSetRange(ref input, ref output);
        }

        private void SortedSetReverseRank(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            GetRank(ref input, ref output, ascending: false);
        }

        private void SortedSetRemoveRangeByLex(ref ObjectInput input, byte* output)
        {
            GetRangeOrCountByLex(ref input, output, SortedSetOperation.ZREMRANGEBYLEX);
        }

        private void SortedSetRemoveRangeByRank(ref ObjectInput input, byte* output)
        {
            // ZREMRANGEBYRANK key start stop
            var _output = (ObjectOutputHeader*)output;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            // Using minValue for partial execution detection
            _output->result1 = int.MinValue;

            if (!RespReadUtils.TrySliceWithLengthHeader(out var startBytes, ref input_currptr, input_endptr) ||
                !RespReadUtils.TrySliceWithLengthHeader(out var stopBytes, ref input_currptr, input_endptr))
            {
                return;
            }

            _output->result1 = int.MaxValue;

            if (!NumUtils.TryParse(startBytes, out int start) ||
                !NumUtils.TryParse(stopBytes, out int stop))
            {
                return;
            }

            _output->result1 = 0;

            if (start > sortedSetDict.Count - 1)
            {
                return;
            }

            // Shift from the end of the set
            if (start < 0)
            {
                start = sortedSetDict.Count + start;
            }
            if (stop < 0)
            {
                stop = sortedSetDict.Count + stop;
            }
            else if (stop >= sortedSetDict.Count)
            {
                stop = sortedSetDict.Count - 1;
            }

            // Calculate number of elements
            _output->result1 = stop - start + 1;

            // Using to list to avoid modified enumerator exception
            foreach (var item in sortedSet.Skip(start).Take(stop - start + 1).ToList())
            {
                if (sortedSetDict.Remove(item.Item2, out var key))
                {
                    sortedSet.Remove((key, item.Item2));

                    this.UpdateSize(item.Item2, false);
                }
            }
        }

        private void SortedSetRemoveRangeByScore(ref ObjectInput input, byte* output)
        {
            // ZREMRANGEBYSCORE key min max
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            // command could be partially executed
            _output->result1 = int.MinValue;

            // read min and max
            if (!RespReadUtils.TrySliceWithLengthHeader(out var minParamBytes, ref input_currptr, input_endptr) ||
                !RespReadUtils.TrySliceWithLengthHeader(out var maxParamBytes, ref input_currptr, input_endptr))
            {
                return;
            }

            if (!TryParseParameter(minParamBytes, out var minValue, out var minExclusive) ||
                !TryParseParameter(maxParamBytes, out var maxValue, out var maxExclusive))
            {
                _output->result1 = int.MaxValue;
            }
            else
            {
                var rem = GetElementsInRangeByScore(minValue, maxValue, minExclusive, maxExclusive, false, false, false, true);
                _output->result1 = rem.Count;
            }
        }

        private void SortedSetCountByLex(ref ObjectInput input, byte* output)
        {
            GetRangeOrCountByLex(ref input, output, SortedSetOperation.ZLEXCOUNT);
        }

        private void SortedSetPopMin(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            PopMinOrMaxCount(ref input, ref output, SortedSetOperation.ZPOPMIN);
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

        #region CommonMethods

        private void GetRangeOrCountByLex(ref ObjectInput input, byte* output, SortedSetOperation op)
        {
            //ZREMRANGEBYLEX key min max
            //ZLEXCOUNT key min max
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            // Using minValue for partial execution detection
            _output->result1 = int.MinValue;

            // read min and max
            if (!RespReadUtils.TrySliceWithLengthHeader(out var minParamBytes, ref input_currptr, input_endptr) ||
                !RespReadUtils.TrySliceWithLengthHeader(out var maxParamBytes, ref input_currptr, input_endptr))
            {
                return;
            }

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
        private void GetRank(ref ObjectInput input, ref SpanByteAndMemory output, bool ascending = true)
        {
            //ZRANK key member
            var input_startptr = input.payload.ptr;
            var input_currptr = input_startptr;
            var length = input.payload.length;
            var input_endptr = input_startptr + length;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            var withScore = input.arg2 == 1;

            ObjectOutputHeader _output = default;
            try
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var member, ref input_currptr, input_endptr))
                    return;

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
                        while (!RespWriteUtils.WriteArrayLength(2, ref curr, end)) // rank and score
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

                _output.result1 = input.arg1;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();

                output.Length = (int)(curr - ptr);
            }
        }

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


        /// <summary>
        /// Removes and returns up to COUNT members with the low or high score
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="op"></param>
        private void PopMinOrMaxCount(ref ObjectInput input, ref SpanByteAndMemory output, SortedSetOperation op)
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