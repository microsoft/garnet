// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
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
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var member, ref ptr, end))
                    return;

                if (c < _input->done)
                    continue;

                _output->countDone++;

                if (parsed)
                {
                    if (!sortedSetDict.TryGetValue(member, out var _scoreStored))
                    {
                        _output->opsDone++;
                        sortedSetDict.Add(member, score);
                        sortedSet.Add((score, member));

                        this.UpdateSize(member);
                    }
                    else if (_scoreStored != score)
                    {
                        sortedSetDict[member] = score;
                        var success = sortedSet.Remove((_scoreStored, member));
                        Debug.Assert(success);
                        success = sortedSet.Add((score, member));
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
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var value, ref ptr, end))
                    return;

                if (c < _input->done)
                    continue;

                _output->countDone++;

                if (sortedSetDict.TryGetValue(value, out var _key))
                {
                    _output->opsDone++;
                    sortedSetDict.Remove(value);
                    sortedSet.Remove((_key, value));

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
                    while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(score.ToString()), ref curr, end))
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
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var minParamByteArray, ref input_currptr, end))
                return;

            // read max
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var maxParamByteArray, ref input_currptr, end))
                return;

            //check if parameters are valid
            if (!TryParseParameter(minParamByteArray, out var minValue, out var minExclusive) || !TryParseParameter(maxParamByteArray, out var maxValue, out var maxExclusive))
            {
                count = Int32.MaxValue;
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
            _output.countDone = Int32.MinValue;
            try
            {
                // read increment
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var incrementByteArray, ref input_currptr, input + length))
                    return;

                // read member
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var memberByteArray, ref input_currptr, input + length))
                    return;

                //check if increment value is valid
                if (!double.TryParse(Encoding.ASCII.GetString(incrementByteArray), out var incrValue))
                {
                    countDone = Int32.MaxValue;
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
                    while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(sortedSetDict[memberByteArray].ToString()), ref curr, end))
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

        private void SortedSetRank(byte* input, int length, byte* output)
        {
            GetRank(input, length, output);
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
                        if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var token, ref input_currptr, input + length))
                            return;
                        switch (Encoding.ASCII.GetString(token).ToLower())
                        {
                            case "byscore":
                                options.ByScore = true;
                                break;
                            case "bylex":
                                options.ByLex = true;
                                break;
                            case "rev":
                                options.Reverse = true;
                                break;
                            case "limit":
                                // read the next two tokens
                                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var offset, ref input_currptr, input + length))
                                    return;
                                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var countLimit, ref input_currptr, input + length))
                                    return;
                                if (TryParseParameter(offset, out var offsetLimit, out bool _) && TryParseParameter(countLimit, out var countLimitNumber, out bool _))
                                {
                                    options.Limit = ((int)offsetLimit, (int)countLimitNumber);
                                    options.ValidLimit = true;
                                    i += 2;
                                }
                                break;
                            case "withscores":
                                options.WithScores = true;
                                break;
                            default:
                                break;
                        }
                        i++;
                    }
                }

                if (count >= 2 && ((!options.ByScore && !options.ByLex) || options.ByScore))
                {

                    if (!TryParseParameter(minParamByteArray, out var minValue, out var minExclusive) | !TryParseParameter(maxParamByteArray, out var maxValue, out var maxExclusive))
                    {
                        ReadOnlySpan<byte> errorMessage = "-ERR max or min value is not a float value.\r\n"u8;
                        while (!RespWriteUtils.WriteResponse(errorMessage, ref curr, end))
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
                                while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(item.Item1.ToString()), ref curr, end))
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
                            ReadOnlySpan<byte> errorMessage = "-ERR syntax error, LIMIT is only supported in BYSCORE or BYLEX.\r\n"u8;
                            while (!RespWriteUtils.WriteResponse(errorMessage, ref curr, end))
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
                                    while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(item.Item1.ToString()), ref curr, end))
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
                        ReadOnlySpan<byte> errorMessage = "-ERR max or min value not valid string range.\r\n"u8;
                        while (!RespWriteUtils.WriteResponse(errorMessage, ref curr, end))
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
                                while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(item.Item1.ToString()), ref curr, end))
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

        private void SortedSetReverseRank(byte* input, int length, byte* output)
        {
            GetRank(input, length, output, ascending: false);
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

            byte* startParam = null;
            int pstartsize = 0;

            byte* stopParam = null;
            int pstopsize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref startParam, ref pstartsize, ref input_currptr, input + length))
                return;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref stopParam, ref pstopsize, ref input_currptr, input + length))
                return;

            _output->bytesDone = (int)(input_currptr - input_startptr);
            _output->countDone = int.MaxValue;

            if (!NumUtils.TryBytesToInt(startParam, pstartsize, out int start) || !NumUtils.TryBytesToInt(stopParam, pstopsize, out int stop))
                return;

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

            // read min
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var minParamByteArray, ref input_currptr, input + length))
                return;

            // read max
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var maxParamByteArray, ref input_currptr, input + length))
                return;

            if (!TryParseParameter(minParamByteArray, out var minValue, out var minExclusive) || !TryParseParameter(maxParamByteArray, out double maxValue, out bool maxExclusive))
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
                        while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(element.Value.ToString()), ref curr, end))
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

            // read min
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var minParamByteArray, ref input_currptr, end))
                return;

            // read max
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var maxParamByteArray, ref input_currptr, end))
                return;

            var rem = GetElementsInRangeByLex(minParamByteArray, maxParamByteArray, false, false, op != SortedSetOperation.ZLEXCOUNT, out int count);

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
        private void GetRank(byte* input, int length, byte* output, bool ascending = true)
        {
            //ZRANK key member
            var _input = (ObjectInputHeader*)input;
            var _output = (ObjectOutputHeader*)output;

            var member = new Span<byte>(input + sizeof(ObjectInputHeader), _input->count).ToArray();

            *_output = default;

            if (!sortedSetDict.TryGetValue(member, out var score))
            {
                _output->opsDone = -1;
                return;
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
                _output->opsDone = ascending ? rank : (sortedSet.Count - rank) - 1;
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
        private List<(double, byte[])> GetElementsInRangeByLex(byte[] minParamByteArray, byte[] maxParamByteArray, bool doReverse, bool validLimit, bool rem, out int errorCode, (int, int) limit = default)
        {
            var elementsInLex = new List<(double, byte[])>();

            // parse boundaries
            if (!TryParseLexParameter(minParamByteArray, out var minValue) || !TryParseLexParameter(maxParamByteArray, out var maxValue))
            {
                errorCode = int.MaxValue;
                return elementsInLex;
            }

            try
            {
                var iterator = sortedSet.GetViewBetween((sortedSet.Min.Item1, minValue.chars), sortedSet.Max);

                // using ToList method so we avoid the Invalid operation ex. when removing
                foreach (var item in iterator.ToList())
                {
                    var inRange = new ReadOnlySpan<byte>(item.Item2).SequenceCompareTo(minValue.chars);
                    if (inRange < 0 || (inRange == 0 && minValue.exclusive))
                        continue;

                    var outRange = maxValue.chars == null ? -1 : new ReadOnlySpan<byte>(item.Item2).SequenceCompareTo(maxValue.chars);
                    if (outRange > 0 || (outRange == 0 && maxValue.exclusive))
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

                    while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(max.Item1.ToString()), ref curr, end))
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
        /// <param name="val"></param>
        /// <param name="valueDouble"></param>
        /// <param name="exclusive"></param>
        /// <returns></returns>
        private static bool TryParseParameter(byte[] val, out double valueDouble, out bool exclusive)
        {
            exclusive = false;
            var strVal = Encoding.ASCII.GetString(val);
            if (string.Compare("+inf", strVal, StringComparison.InvariantCultureIgnoreCase) == 0)
            {
                valueDouble = double.PositiveInfinity;
                return true;
            }
            else if (string.Compare("-inf", strVal, StringComparison.InvariantCultureIgnoreCase) == 0)
            {
                valueDouble = double.NegativeInfinity;
                return true;
            }

            // adjust for exclusion
            if (val[0] == '(')
            {
                strVal = strVal[1..];
                exclusive = true;
            }

            return double.TryParse(strVal, out valueDouble);
        }

        /// <summary>
        /// Helper method to parse parameter when using Lexicographical ranges
        /// </summary>
        /// <param name="val"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        private static bool TryParseLexParameter(byte[] val, out (byte[] chars, bool exclusive) limit)
        {
            switch ((char)val[0])
            {
                case '+':
                case '-':
                    limit = (null, false);
                    return true;
                case '[':
                    limit = (new Span<byte>(val)[1..].ToArray(), false);
                    return true;
                case '(':
                    limit = (new Span<byte>(val)[1..].ToArray(), true);
                    return true;
                default:
                    limit = default;
                    return false;
            }
        }

        #endregion
    }
}