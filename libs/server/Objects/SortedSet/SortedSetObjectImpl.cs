﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
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

        private void SortedSetAdd(byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;

            int count = _input->count;
            *_output = default;

            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            byte* end = input + length;
            for (int c = 0; c < count; c++)
            {
                if (!RespReadUtils.ReadDoubleWithLengthHeader(out var score, out var parsed, ref ptr, end))
                    return;
                if (!RespReadUtils.TrySliceWithLengthHeader(out var member, ref ptr, end))
                    return;

                if (c < _input->done)
                    continue;

                _output->countDone++;

                if (parsed)
                {
                    var memberArray = member.ToArray();
                    if (!sortedSetDict.TryGetValue(memberArray, out var _scoreStored))
                    {
                        _output->opsDone++;
                        sortedSetDict.Add(memberArray, score);
                        sortedSet.Add((score, memberArray));

                        this.UpdateSize(member);
                    }
                    else if (_scoreStored != score)
                    {
                        sortedSetDict[memberArray] = score;
                        var success = sortedSet.Remove((_scoreStored, memberArray));
                        Debug.Assert(success);
                        success = sortedSet.Add((score, memberArray));
                        Debug.Assert(success);
                    }
                }
                _output->bytesDone = (int)(ptr - startptr);
            }
        }

        private void SortedSetRemove(byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;

            int count = _input->count;
            *_output = default;

            byte* startptr = input + sizeof(ObjectInputHeader);
            byte* ptr = startptr;
            byte* end = input + length;

            for (int c = 0; c < count; c++)
            {
                if (!RespReadUtils.TrySliceWithLengthHeader(out var value, ref ptr, end))
                    return;

                if (c < _input->done)
                    continue;

                _output->countDone++;

                var valueArray = value.ToArray();
                if (sortedSetDict.TryGetValue(valueArray, out var _key))
                {
                    _output->opsDone++;
                    sortedSetDict.Remove(valueArray);
                    sortedSet.Remove((_key, valueArray));

                    this.UpdateSize(value, false);
                }

                _output->bytesDone = (int)(ptr - startptr);
            }
        }

        private void SortedSetLength(byte* output)
        {
            // Check both objects
            Debug.Assert(sortedSetDict.Count == sortedSet.Count, "SortedSet object is not in sync.");
            ((ObjectOutputHeader*)output)->opsDone = sortedSetDict.Count;
        }

        private void SortedSetPop(byte* input, ref SpanByteAndMemory output)
        {
            PopMinOrMaxCount(input, ref output, SortedSetOperation.ZPOPMAX);
        }

        private void SortedSetScore(byte* input, ref SpanByteAndMemory output)
        {
            //ZSCORE key member
            var _input = (ObjectInputHeader*)input;
            var scoreKey = new Span<byte>(input + sizeof(ObjectInputHeader), _input->count).ToArray();

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                if (!sortedSetDict.TryGetValue(scoreKey, out var score))
                {
                    while (!RespWriteUtils.WriteNull(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    while (!RespWriteUtils.WriteAsciiBulkString(score.ToString(CultureInfo.InvariantCulture), ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                _output.bytesDone = 0;
                _output.countDone = 1;
                _output.opsDone = 1;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetScores(byte* input, int length, ref SpanByteAndMemory output)
        {
            //ZMSCORE key member
            var _input = (ObjectInputHeader*)input;
            ObjectOutputHeader _output = default;

            int count = _input->count;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;

            byte* ptr = output.SpanByte.ToPointer();
            var curr = ptr;
            var end = curr + output.Length;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;
            byte* input_endptr = input + length;

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
                        while (!RespWriteUtils.WriteAsciiBulkString(score.ToString(CultureInfo.InvariantCulture), ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = count;
                _output.opsDone = count;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetCount(byte* input, int length, byte* output)
        {
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;
            var end = input + length;
            var count = 0;

            _output->opsDone = 0;
            _output->countDone = Int32.MinValue;

            // read min
            if (!RespReadUtils.TrySliceWithLengthHeader(out var minParamSpan, ref input_currptr, end))
                return;

            // read max
            if (!RespReadUtils.TrySliceWithLengthHeader(out var maxParamSpan, ref input_currptr, end))
                return;

            //check if parameters are valid
            if (!TryParseParameter(minParamSpan, out var minValue, out var minExclusive) ||
                !TryParseParameter(maxParamSpan, out var maxValue, out var maxExclusive))
            {
                count = int.MaxValue;
            }
            else
            {
                // get the elements within the score range and write the result
                if (sortedSet.Count > 0)
                {
                    foreach (var item in sortedSet.GetViewBetween((minValue, null), sortedSet.Max))
                    {
                        if (item.Item1 > maxValue || (maxExclusive && item.Item1 == maxValue)) break;
                        if (minExclusive && item.Item1 == minValue) continue;
                        _output->opsDone++;
                    }
                }
            }
            _output->countDone = count;
            _output->bytesDone = (int)(input_currptr - input_startptr);
        }

        private void SortedSetIncrement(byte* input, int length, ref SpanByteAndMemory output)
        {
            //ZINCRBY key increment member
            var _input = (ObjectInputHeader*)input;
            int countDone = _input->count;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;

            //to validate partial execution
            _output.countDone = int.MinValue;
            try
            {
                // read increment
                if (!RespReadUtils.TrySliceWithLengthHeader(out var incrementBytes, ref input_currptr, input + length))
                    return;

                // read member
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var memberByteArray, ref input_currptr, input + length))
                    return;

                //check if increment value is valid
                if (!NumUtils.TryParse(incrementBytes, out double incrValue))
                {
                    countDone = int.MaxValue;
                }
                else
                {
                    if (sortedSetDict.TryGetValue(memberByteArray, out double score))
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
                    while (!RespWriteUtils.WriteAsciiBulkString(sortedSetDict[memberByteArray].ToString(CultureInfo.InvariantCulture), ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    countDone = 1;
                }

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = countDone;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRank(byte* input, int length, ref SpanByteAndMemory output)
        {
            GetRank(input, length, ref output);
        }

        private void SortedSetRange(byte* input, int length, ref SpanByteAndMemory output)
        {
            //ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
            //ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
            var _input = (ObjectInputHeader*)input;
            int count = _input->count;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                // read min
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var minParamByteArray, ref input_currptr, input + length))
                    return;

                // read max
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var maxParamByteArray, ref input_currptr, input + length))
                    return;

                int countDone = 2;

                // read the rest of the arguments
                ZRangeOptions options = new();
                if (_input->header.SortedSetOp == SortedSetOperation.ZRANGEBYSCORE) options.ByScore = true;
                if (_input->header.SortedSetOp == SortedSetOperation.ZREVRANGE) options.Reverse = true;

                if (count > 2)
                {
                    int i = 0;
                    while (i < count - 2)
                    {
                        if (!RespReadUtils.TrySliceWithLengthHeader(out var token, ref input_currptr, input + length))
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
                            if (!RespReadUtils.TrySliceWithLengthHeader(out var offset, ref input_currptr, input + length) ||
                                !RespReadUtils.TrySliceWithLengthHeader(out var countLimit, ref input_currptr, input + length))
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
                        countDone = _input->count;
                        count = 0;
                    }

                    if (options.ByScore)
                    {

                        var scoredElements = GetElementsInRangeByScore(minValue, maxValue, minExclusive, maxExclusive, options.WithScores, options.Reverse, options.ValidLimit, false, options.Limit);

                        // write the size of the array reply
                        while (!RespWriteUtils.WriteArrayLength(options.WithScores ? scoredElements.Count * 2 : scoredElements.Count, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        foreach (var item in scoredElements)
                        {
                            while (!RespWriteUtils.WriteBulkString(item.Item2, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            if (options.WithScores)
                            {
                                while (!RespWriteUtils.WriteAsciiBulkString(item.Item1.ToString(CultureInfo.InvariantCulture), ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }
                        }
                        countDone = _input->count;
                    }
                    else
                    {  // byIndex
                        int minIndex = (int)minValue, maxIndex = (int)maxValue;
                        if (options.ValidLimit)
                        {
                            while (!RespWriteUtils.WriteError("ERR syntax error, LIMIT is only supported in BYSCORE or BYLEX."u8, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            countDone = _input->count;
                            count = 0;
                        }
                        else if (minValue > sortedSetDict.Count - 1)
                        {
                            // return empty list
                            while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            countDone = _input->count;
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

                            // calculate number of elements
                            int n = maxIndex - minIndex + 1;

                            var iterator = options.Reverse ? sortedSet.Reverse() : sortedSet;
                            iterator = iterator.Skip(minIndex).Take(n);

                            // write the size of the array reply
                            while (!RespWriteUtils.WriteArrayLength(options.WithScores ? n * 2 : n, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                            foreach (var item in iterator)
                            {
                                while (!RespWriteUtils.WriteBulkString(item.Item2, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                if (options.WithScores)
                                {
                                    while (!RespWriteUtils.WriteAsciiBulkString(item.Item1.ToString(CultureInfo.InvariantCulture), ref curr, end))
                                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                }
                            }
                            countDone = _input->count;
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
                        countDone = _input->count;
                        count = 0;
                    }
                    else
                    {
                        //write the size of the array reply
                        while (!RespWriteUtils.WriteArrayLength(options.WithScores ? elementsInLex.Count * 2 : elementsInLex.Count, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        foreach (var item in elementsInLex)
                        {
                            while (!RespWriteUtils.WriteBulkString(item.Item2, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            if (options.WithScores)
                            {
                                while (!RespWriteUtils.WriteAsciiBulkString(item.Item1.ToString(CultureInfo.InvariantCulture), ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }
                        }
                        countDone = _input->count;
                    }
                }

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = countDone;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRangeByScore(byte* input, int length, ref SpanByteAndMemory output)
        {
            SortedSetRange(input, length, ref output);
        }

        private void SortedSetReverseRange(byte* input, int length, ref SpanByteAndMemory output)
        {
            SortedSetRange(input, length, ref output);
        }

        private void SortedSetReverseRank(byte* input, int length, ref SpanByteAndMemory output)
        {
            GetRank(input, length, ref output, ascending: false);
        }

        private void SortedSetRemoveRangeByLex(byte* input, int length, byte* output)
        {
            GetRangeOrCountByLex(input, length, output, SortedSetOperation.ZREMRANGEBYLEX);
        }

        private void SortedSetRemoveRangeByRank(byte* input, int length, byte* output)
        {
            //ZREMRANGEBYRANK key start stop
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;

            int count = _input->count;
            *_output = default;

            //using minValue for partial execution detection
            _output->countDone = int.MinValue;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            if (!RespReadUtils.TrySliceWithLengthHeader(out var startBytes, ref input_currptr, input + length) ||
                !RespReadUtils.TrySliceWithLengthHeader(out var stopBytes, ref input_currptr, input + length))
            {
                return;
            }

            _output->bytesDone = (int)(input_currptr - input_startptr);
            _output->countDone = int.MaxValue;

            if (!NumUtils.TryParse(startBytes, out int start) ||
                !NumUtils.TryParse(stopBytes, out int stop))
            {
                return;
            }

            _output->countDone = 0;

            if (start > sortedSetDict.Count - 1)
            {
                _output->opsDone = 0;
                return;
            }

            //shift from the end of the set
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

            // calculate number of elements
            _output->opsDone = stop - start + 1;

            //using to list to avoid modified enumerator exception
            foreach (var item in sortedSet.Skip(start).Take(stop - start + 1).ToList())
            {
                if (sortedSetDict.TryGetValue(item.Item2, out var _key))
                {
                    sortedSetDict.Remove(item.Item2);
                    sortedSet.Remove((_key, item.Item2));

                    this.UpdateSize(item.Item2, false);
                }
            }
        }

        private void SortedSetRemoveRangeByScore(byte* input, int length, byte* output)
        {
            //ZREMRANGEBYSCORE key min max
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;

            int count = _input->count;
            *_output = default;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;

            // command could be partially executed
            _output->countDone = int.MinValue;

            // read min and max
            if (!RespReadUtils.TrySliceWithLengthHeader(out var minParamBytes, ref input_currptr, input + length) ||
                !RespReadUtils.TrySliceWithLengthHeader(out var maxParamBytes, ref input_currptr, input + length))
            {
                return;
            }

            if (!TryParseParameter(minParamBytes, out var minValue, out var minExclusive) ||
                !TryParseParameter(maxParamBytes, out var maxValue, out var maxExclusive))
            {
                _output->countDone = int.MaxValue;
            }
            else
            {
                var rem = GetElementsInRangeByScore(minValue, maxValue, minExclusive, maxExclusive, false, false, false, true);
                _output->opsDone = rem.Count;
                _output->countDone = 0;
            }
            _output->bytesDone = (int)(input_currptr - input_startptr);
        }

        private void SortedSetCountByLex(byte* input, int length, byte* output)
        {
            GetRangeOrCountByLex(input, length, output, SortedSetOperation.ZLEXCOUNT);
        }

        private void SortedSetPopMin(byte* input, ref SpanByteAndMemory output)
        {
            PopMinOrMaxCount(input, ref output, SortedSetOperation.ZPOPMIN);
        }

        private void SortedSetRandomMember(byte* input, int length, ref SpanByteAndMemory output)
        {
            var _input = (ObjectInputHeader*)input;

            int count = _input->count;

            bool withScores = _input->done == 1;

            if (count > 0 && count > sortedSet.Count)
                count = sortedSet.Count;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                var arrayLength = Math.Abs(withScores ? count * 2 : count);
                if (arrayLength > 1)
                {
                    // The count parameter can have a negative value, but the array length can't
                    while (!RespWriteUtils.WriteArrayLength(arrayLength, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }

                int[] indexes = default;

                if (count > 0)
                {
                    // The order of fields in the reply is not truly random
                    indexes = new HashSet<int>(Enumerable.Range(0, sortedSetDict.Count).OrderBy(x => Guid.NewGuid()).Take(count)).ToArray();
                }
                else
                {
                    // Repeating fields are possible.
                    // Exactly count fields, or an empty array is returned
                    // The order of fields in the reply is truly random.
                    indexes = new int[Math.Abs(count)];
                    for (int i = 0; i < indexes.Length; i++)
                        indexes[i] = RandomNumberGenerator.GetInt32(0, sortedSetDict.Count);
                }

                foreach (var item in indexes)
                {
                    var element = sortedSetDict.ElementAt(item);

                    while (!RespWriteUtils.WriteBulkString(element.Key, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    if (withScores)
                    {
                        while (!RespWriteUtils.WriteAsciiBulkString(element.Value.ToString(CultureInfo.InvariantCulture), ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }

                // Write bytes parsed from input and count done, into output footer
                _output.bytesDone = 0;
                _output.countDone = count;
                _output.opsDone = count;
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

        private void GetRangeOrCountByLex(byte* input, int length, byte* output, SortedSetOperation op)
        {
            //ZREMRANGEBYLEX key min max
            //ZLEXCOUNT key min max
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;
            var end = input + length;

            _output->opsDone = 0;

            //using minValue for partial execution detection
            _output->countDone = int.MinValue;

            // read min and max
            if (!RespReadUtils.TrySliceWithLengthHeader(out var minParamBytes, ref input_currptr, end) ||
                !RespReadUtils.TrySliceWithLengthHeader(out var maxParamBytes, ref input_currptr, end))
            {
                return;
            }

            var rem = GetElementsInRangeByLex(minParamBytes, maxParamBytes, false, false, op != SortedSetOperation.ZLEXCOUNT, out int count);

            _output->countDone = count;
            _output->opsDone = rem.Count;
            _output->bytesDone = (int)(input_currptr - input_startptr);
        }

        /// <summary>
        /// Gets the rank of a member of the sorted set
        /// in ascending or descending order
        /// </summary>
        /// <param name="input"></param>
        /// <param name="length"></param>
        /// <param name="output"></param>
        /// <param name="ascending"></param>
        private void GetRank(byte* input, int length, ref SpanByteAndMemory output, bool ascending = true)
        {
            //ZRANK key member
            var _input = (ObjectInputHeader*)input;
            var input_startptr = input + sizeof(ObjectInputHeader);
            var input_currptr = input_startptr;
            var withScore = false;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();
            var curr = ptr;
            var end = curr + output.Length;
            var error = false;

            ObjectOutputHeader _output = default;
            try
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var member, ref input_currptr, input + length))
                    return;

                if (_input->count == 3) // ZRANK key member WITHSCORE
                {
                    if (!RespReadUtils.TrySliceWithLengthHeader(out var token, ref input_currptr, input + length))
                        return;

                    if (token.EqualsUpperCaseSpanIgnoringCase("WITHSCORE"u8))
                    {
                        withScore = true;
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        error = true;
                    }
                }

                if (!error)
                {
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

                            while (!RespWriteUtils.WriteAsciiBulkString(score.ToString(CultureInfo.InvariantCulture), ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                        else
                        {
                            while (!RespWriteUtils.WriteInteger(rank, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        }
                    }
                }

                _output.bytesDone = (int)(input_currptr - input_startptr);
                _output.countDone = _input->count;
                _output.opsDone = 1;
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
                var iterator = sortedSet.GetViewBetween((sortedSet.Min.Item1, minValueChars.ToArray()), sortedSet.Max);

                // using ToList method so we avoid the Invalid operation ex. when removing
                foreach (var item in iterator.ToList())
                {
                    var inRange = new ReadOnlySpan<byte>(item.Item2).SequenceCompareTo(minValueChars);
                    if (inRange < 0 || (inRange == 0 && minValueExclusive))
                        continue;

                    var outRange = maxValueChars == default ? -1 : new ReadOnlySpan<byte>(item.Item2).SequenceCompareTo(maxValueChars);
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
        private void PopMinOrMaxCount(byte* input, ref SpanByteAndMemory output, SortedSetOperation op)
        {
            var _input = (ObjectInputHeader*)input;
            int count = _input->count;
            int prevDone = _input->done; // how many were previously done
            int countDone = 0;

            int totalLen = 0;

            if (sortedSet.Count < count)
                count = sortedSet.Count;

            bool isMemory = false;
            MemoryHandle ptrHandle = default;
            byte* ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            var inputCount = count;

            try
            {
                while (!RespWriteUtils.WriteArrayLength(count * 2, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                totalLen = (int)(curr - ptr);

                while (count > 0)
                {
                    if (countDone < prevDone) // skip processing previously done entries
                    {
                        countDone++;
                        count--;
                        continue;
                    }
                    var max = op == SortedSetOperation.ZPOPMAX ? sortedSet.Max : sortedSet.Min;
                    var success = sortedSet.Remove(max);
                    success = sortedSetDict.Remove(max.Item2);

                    this.UpdateSize(max.Item2, false);

                    while (!RespWriteUtils.WriteBulkString(max.Item2, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    while (!RespWriteUtils.WriteAsciiBulkString(max.Item1.ToString(CultureInfo.InvariantCulture), ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    countDone++;
                    count--;
                }

                // Write output
                _output.bytesDone = 0; // No reads done
                _output.countDone = countDone;
                // how many can be done based on the lenght of the SS
                _output.opsDone = inputCount;
            }
            finally
            {
                while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
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