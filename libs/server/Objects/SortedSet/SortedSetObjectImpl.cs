// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Garnet.common;

using Limit = (int offset, int count);

namespace Garnet.server
{
    /// <summary>
    /// Sorted Set - RESP specific operations
    /// </summary>
    public partial class SortedSetObject : GarnetObjectBase
    {
        /// <summary>
        /// Small struct to store options for ZRange command
        /// </summary>
        private struct ZRangeOptions
        {
            public bool ByScore { get; set; }
            public bool ByLex { get; set; }
            public bool Reverse { get; set; }
            public bool WithScores { get; set; }
            public bool ValidLimit { get; set; }
            public Limit Limit { get; set; }
        };

        private enum SpecialRanges : byte
        {
            None = 0,
            InfiniteMin = 1,
            InfiniteMax = 2
        }

        bool GetOptions(ref ObjectInput input, ref int currTokenIdx, out SortedSetAddOption options, ref RespMemoryWriter writer)
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
            if ((options & SortedSetAddOption.XX) == SortedSetAddOption.XX &&
                (options & SortedSetAddOption.NX) == SortedSetAddOption.NX)
                optionsError = CmdStrings.RESP_ERR_XX_NX_NOT_COMPATIBLE;

            // NX, GT & LT are mutually exclusive
            if (((options & SortedSetAddOption.GT) == SortedSetAddOption.GT &&
                 (options & SortedSetAddOption.LT) == SortedSetAddOption.LT) ||
               (((options & SortedSetAddOption.GT) == SortedSetAddOption.GT ||
                 (options & SortedSetAddOption.LT) == SortedSetAddOption.LT) &&
                (options & SortedSetAddOption.NX) == SortedSetAddOption.NX))
                optionsError = CmdStrings.RESP_ERR_GT_LT_NX_NOT_COMPATIBLE;

            // INCR supports only one score-element pair
            if ((options & SortedSetAddOption.INCR) == SortedSetAddOption.INCR &&
                (input.parseState.Count - currTokenIdx > 2))
                optionsError = CmdStrings.RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR;

            if (!optionsError.IsEmpty)
            {
                writer.WriteError(optionsError);
                return false;
            }

            // From here on we expect only score-element pairs
            // Remaining token count should be positive and even
            if (currTokenIdx == input.parseState.Count || (input.parseState.Count - currTokenIdx) % 2 != 0)
            {
                writer.WriteError(CmdStrings.RESP_SYNTAX_ERROR);
                return false;
            }

            return true;
        }

        private void SortedSetAdd(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            DeleteExpiredItems();

            var addedOrChanged = 0;
            double incrResult = 0;

            var options = SortedSetAddOption.None;
            var currTokenIdx = 0;
            var parsedOptions = false;

            var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            try
            {
                while (currTokenIdx < input.parseState.Count)
                {
                    // Try to parse a Score field
                    if (!input.parseState.TryGetDouble(currTokenIdx, out var score))
                    {
                        // Try to get and validate options before the Score field, if any
                        if (!parsedOptions)
                        {
                            parsedOptions = true;
                            if (!GetOptions(ref input, ref currTokenIdx, out options, ref writer))
                                return;
                            continue; // retry after parsing options
                        }
                        else
                        {
                            // Invalid Score encountered
                            writer.WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT);
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
                        if ((options & SortedSetAddOption.XX) == SortedSetAddOption.XX) continue;

                        incrResult = score;
                        sortedSetDict.Add(member, score);
                        if (sortedSet.Add((score, member)))
                            addedOrChanged++;

                        UpdateSize(memberSpan);
                    }
                    // Update existing member
                    else
                    {
                        // Update new score if INCR flag is set
                        if ((options & SortedSetAddOption.INCR) == SortedSetAddOption.INCR)
                        {
                            score += scoreStored;
                            incrResult = score;

                            if (double.IsNaN(score))
                            {
                                writer.WriteError(CmdStrings.RESP_ERR_GENERIC_SCORE_NAN);
                                return;
                            }
                        }

                        // No need for update
                        if (score == scoreStored)
                        {
                            _ = TryRemoveExpiration(member);
                            continue;
                        }

                        // Don't update existing member if NX flag is set
                        // or if GT/LT flag is set and existing score is higher/lower than new score, respectively
                        if ((options & SortedSetAddOption.NX) == SortedSetAddOption.NX ||
                            ((options & SortedSetAddOption.GT) == SortedSetAddOption.GT && scoreStored > score) ||
                            ((options & SortedSetAddOption.LT) == SortedSetAddOption.LT && scoreStored < score))
                        {
                            if ((options & SortedSetAddOption.INCR) == SortedSetAddOption.INCR)
                            {
                                writer.WriteNull();
                                return;
                            }
                            continue;
                        }

                        sortedSetDict[member] = score;
                        var success = sortedSet.Remove((scoreStored, member));
                        Debug.Assert(success);
                        success = sortedSet.Add((score, member));
                        _ = TryRemoveExpiration(member);
                        Debug.Assert(success);

                        // If CH flag is set, add changed member to final count
                        if ((options & SortedSetAddOption.CH) == SortedSetAddOption.CH)
                            addedOrChanged++;
                    }
                }

                if ((options & SortedSetAddOption.INCR) == SortedSetAddOption.INCR)
                {
                    writer.WriteDoubleNumeric(incrResult);
                }
                else
                {
                    writer.WriteInt32(addedOrChanged);
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        private void SortedSetRemove(ref ObjectInput input, ref ObjectOutput output)
        {
            DeleteExpiredItems();

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var value = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;
                var valueArray = value.ToArray();

                if (!sortedSetDict.Remove(valueArray, out var key))
                    continue;

                output.result1++;
                sortedSet.Remove((key, valueArray));
                _ = TryRemoveExpiration(valueArray);

                this.UpdateSize(value, false);
            }
        }

        private void SortedSetLength(ref ObjectOutput output)
        {
            // Check both objects
            Debug.Assert(sortedSetDict.Count == sortedSet.Count, "SortedSet object is not in sync.");
            output.result1 = Count();
        }

        private void SortedSetScore(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            // ZSCORE key member
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            var member = input.parseState.GetArgSliceByRef(0).ToArray();

            if (!TryGetScore(member, out var score))
            {
                writer.WriteNull();
            }
            else
            {
                writer.WriteDoubleNumeric(score);
            }
            output.result1 = 1;
        }

        private void SortedSetScores(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            // ZMSCORE key member
            var count = input.parseState.Count;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            writer.WriteArrayLength(count);

            for (var i = 0; i < count; i++)
            {
                var member = input.parseState.GetArgSliceByRef(i).ToArray();

                if (!TryGetScore(member, out var score))
                {
                    writer.WriteNull();
                }
                else
                {
                    writer.WriteDoubleNumeric(score);
                }
            }

            output.result1 = count;
        }

        private void SortedSetCount(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            // Read min & max
            var minParamSpan = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            var maxParamSpan = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            // Check if parameters are valid
            if (!TryParseParameter(minParamSpan, out var minValue, out var minExclusive) ||
                !TryParseParameter(maxParamSpan, out var maxValue, out var maxExclusive))
            {
                writer.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
                return;
            }

            // get the elements within the score range and write the result
            var count = 0;
            if (sortedSet.Count > 0 && minValue <= sortedSet.Max.Score)
            {
                foreach (var item in sortedSet.GetViewBetween((minValue, null), sortedSet.Max))
                {
                    if (IsExpired(item.Element))
                        continue;
                    if (item.Score > maxValue || (maxExclusive && item.Score == maxValue))
                        break;
                    if (minExclusive && item.Score == minValue)
                        continue;
                    count++;
                }
            }

            writer.WriteInt32(count);
        }

        private void SortedSetIncrement(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            DeleteExpiredItems();

            // It's useful to fix RESP2 in the internal API as that just reads back the output.
            if (input.arg2 > 0)
                respProtocolVersion = (byte)input.arg2;

            // ZINCRBY key increment member
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            // Try to read increment value
            if (!input.parseState.TryGetDouble(0, out var incrValue))
            {
                writer.WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT);
                return;
            }

            // Read member
            var member = input.parseState.GetArgSliceByRef(1).ToArray();

            if (sortedSetDict.TryGetValue(member, out var score))
            {
                var result = score + incrValue;

                if (double.IsNaN(result))
                {
                    writer.WriteError(CmdStrings.RESP_ERR_GENERIC_SCORE_NAN);
                    return;
                }

                sortedSetDict[member] = result;
                sortedSet.Remove((score, member));
                sortedSet.Add((result, member));
            }
            else
            {
                sortedSetDict.Add(member, incrValue);
                sortedSet.Add((incrValue, member));

                UpdateSize(member);
            }

            // Write the new score
            writer.WriteDoubleNumeric(sortedSetDict[member]);
        }

        private void SortedSetRange(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            //ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
            //ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
            //ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
            var rangeOpts = (SortedSetRangeOpts)input.arg2;
            var count = input.parseState.Count;
            var currIdx = 0;

            // Read min & max
            var minSpan = input.parseState.GetArgSliceByRef(currIdx++).ReadOnlySpan;
            var maxSpan = input.parseState.GetArgSliceByRef(currIdx++).ReadOnlySpan;

            // read the rest of the arguments
            ZRangeOptions options = new()
            {
                ByScore = (rangeOpts & SortedSetRangeOpts.ByScore) != 0,
                ByLex = (rangeOpts & SortedSetRangeOpts.ByLex) != 0,
                Reverse = (rangeOpts & SortedSetRangeOpts.Reverse) != 0,
                WithScores = (rangeOpts & SortedSetRangeOpts.WithScores) != 0 || (rangeOpts & SortedSetRangeOpts.Store) != 0
            };

            // The ZRANGESTORE code will read our output and store it, it's used to RESP2 output.
            // Since in that case isn't displayed to the user, we can override the version to let it work.
            if ((respProtocolVersion >= 3) && (rangeOpts & SortedSetRangeOpts.Store) != 0)
                respProtocolVersion = 2;

            var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            try
            {
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
                                writer.WriteError(CmdStrings.RESP_SYNTAX_ERROR);
                                return;
                            }

                            // Read the next two tokens
                            if (!input.parseState.TryGetInt(currIdx++, out var offset) ||
                                !input.parseState.TryGetInt(currIdx++, out var countLimit))
                            {
                                writer.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
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
                        writer.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
                        return;
                    }

                    if (options.ByScore)
                    {
                        var scoredElements = GetElementsInRangeByScore(minValue, maxValue, minExclusive, maxExclusive, options.WithScores, options.Reverse, options.ValidLimit, false, options.Limit);

                        WriteSortedSetResult(options.WithScores, scoredElements.Count, respProtocolVersion, scoredElements, ref writer);
                    }
                    else
                    {
                        // byIndex
                        var setCount = Count();
                        int minIndex = (int)minValue, maxIndex = (int)maxValue;
                        if (options.ValidLimit)
                        {
                            writer.WriteError(CmdStrings.RESP_ERR_LIMIT_NOT_SUPPORTED);
                            return;
                        }
                        else if (minValue > setCount - 1)
                        {
                            // return empty list
                            writer.WriteEmptyArray();
                            return;
                        }
                        else
                        {
                            //shift from the end of the set
                            if (minIndex < 0)
                            {
                                minIndex = setCount + minIndex;
                            }
                            if (maxIndex < 0)
                            {
                                maxIndex = setCount + maxIndex;
                            }
                            else if (maxIndex >= setCount)
                            {
                                maxIndex = setCount - 1;
                            }

                            // No elements to return if both indexes fall outside the range or min is higher than max
                            if ((minIndex < 0 && maxIndex < 0) || (minIndex > maxIndex))
                            {
                                writer.WriteEmptyArray();
                                return;
                            }
                            else
                            {
                                // Clamp minIndex to 0, if it is beyond the number of elements
                                minIndex = Math.Max(0, minIndex);

                                // calculate number of elements
                                var n = maxIndex - minIndex + 1;
                                var iterator = options.Reverse ? sortedSet.Reverse() : sortedSet;

                                if (expirationTimes is not null)
                                {
                                    iterator = iterator.Where(x => !IsExpired(x.Element));
                                }

                                iterator = iterator.Skip(minIndex).Take(n);

                                WriteSortedSetResult(options.WithScores, n, respProtocolVersion, iterator, ref writer);
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
                        writer.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING);
                    }
                    else
                    {
                        WriteSortedSetResult(options.WithScores, elementsInLex.Count, respProtocolVersion, elementsInLex, ref writer);
                    }
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        void WriteSortedSetResult(bool withScores, int count, byte respProtocolVersion, IEnumerable<(double, byte[])> iterator, ref RespMemoryWriter writer)
        {
            if (withScores && respProtocolVersion >= 3)
            {
                // write the size of the array reply
                writer.WriteArrayLength(count);

                foreach (var (score, element) in iterator)
                {
                    writer.WriteArrayLength(2);
                    writer.WriteBulkString(element);
                    writer.WriteDoubleNumeric(score);
                }
            }
            else
            {
                // write the size of the array reply
                writer.WriteArrayLength(withScores ? count * 2 : count);

                foreach (var (score, element) in iterator)
                {
                    writer.WriteBulkString(element);
                    if (withScores)
                    {
                        writer.WriteDoubleBulkString(score);
                    }
                }
            }
        }

        private void SortedSetRemoveRangeByRank(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            DeleteExpiredItems();

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            // ZREMRANGEBYRANK key start stop
            if (!input.parseState.TryGetInt(0, out var start) ||
                !input.parseState.TryGetInt(1, out var stop))
            {
                writer.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
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
                if (sortedSetDict.Remove(item.Element, out var key))
                {
                    sortedSet.Remove((key, item.Element));

                    UpdateSize(item.Element, false);
                }
                TryRemoveExpiration(item.Element);
            }

            // Write the number of elements
            writer.WriteInt32(elementCount);
        }

        private void SortedSetRemoveRangeByScore(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            DeleteExpiredItems();

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            // ZREMRANGEBYSCORE key min max
            // Read min and max
            var minParamBytes = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            var maxParamBytes = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

            if (!TryParseParameter(minParamBytes, out var minValue, out var minExclusive) ||
                !TryParseParameter(maxParamBytes, out var maxValue, out var maxExclusive))
            {
                writer.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
                return;
            }

            var elementCount = GetElementsInRangeByScore(minValue, maxValue, minExclusive, maxExclusive, false,
                false, false, true).Count;

            // Write the number of elements
            writer.WriteInt32(elementCount);
        }

        private void SortedSetRandomMember(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var count = input.arg1 >> 2;
            var withScores = (input.arg1 & 1) == 1;
            var includedCount = ((input.arg1 >> 1) & 1) == 1;
            var seed = input.arg2;
            var sortedSetCount = Count();

            if (count > 0 && count > sortedSetCount)
                count = sortedSetCount;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            // The count parameter can have a negative value, but the array length can't
            var arrayLength = Math.Abs((withScores && respProtocolVersion == 2) ? count * 2 : count);
            if (arrayLength > 1 || (arrayLength == 1 && includedCount))
            {
                writer.WriteArrayLength(arrayLength);
            }
            var indexCount = Math.Abs(count);

            var indexes = indexCount <= RandomUtils.IndexStackallocThreshold ?
                stackalloc int[RandomUtils.IndexStackallocThreshold].Slice(0, indexCount) : new int[indexCount];

            RandomUtils.PickKRandomIndexes(sortedSetCount, indexes, seed, count > 0);

            foreach (var item in indexes)
            {
                var (element, score) = ElementAt(item);

                if (withScores && (respProtocolVersion >= 3))
                    writer.WriteArrayLength(2);

                writer.WriteBulkString(element);

                if (withScores)
                {
                    writer.WriteDoubleNumeric(score);
                }
            }

            // Write count done into output footer
            output.result1 = count;
        }

        private void SortedSetRemoveOrCountRangeByLex(ref ObjectInput input, ref ObjectOutput output, SortedSetOperation op)
        {
            // ZREMRANGEBYLEX key min max
            // ZLEXCOUNT key min max

            // Using minValue for partial execution detection
            output.result1 = int.MinValue;

            var minParamBytes = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            var maxParamBytes = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

            var isRemove = op == SortedSetOperation.ZREMRANGEBYLEX;

            if (isRemove)
            {
                DeleteExpiredItems();
            }

            var rem = GetElementsInRangeByLex(minParamBytes, maxParamBytes, false, false, isRemove, out int errorCode);

            output.result1 = errorCode;
            if (errorCode == 0)
                output.result1 = rem.Count;
        }

        /// <summary>
        /// Gets the rank of a member of the sorted set
        /// in ascending or descending order
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="ascending"></param>
        private void SortedSetRank(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion, bool ascending = true)
        {
            //ZRANK key member
            var withScore = input.arg1 == 1;

            var member = input.parseState.GetArgSliceByRef(0).ToArray();

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            if (!TryGetScore(member, out var score))
            {
                writer.WriteNull();
            }
            else
            {
                var rank = 0;
                foreach (var item in sortedSet)
                {
                    if (IsExpired(item.Element))
                    {
                        continue;
                    }

                    if (item.Element.SequenceEqual(member))
                        break;
                    rank++;
                }

                if (!ascending)
                    rank = Count() - rank - 1;

                if (withScore)
                {
                    writer.WriteArrayLength(2); // Rank and score
                    writer.WriteInt32(rank);
                    writer.WriteDoubleNumeric(score);
                }
                else
                {
                    writer.WriteInt32(rank);
                }
            }
        }

        /// <summary>
        /// Removes and returns the element with the highest or lowest score from the sorted set.
        /// </summary>
        /// <param name="popMaxScoreElement">If true, pops the element with the highest score; otherwise, pops the element with the lowest score.</param>
        /// <returns>A tuple containing the score and the element as a byte array.</returns>
        public (double Score, byte[] Element) PopMinOrMax(bool popMaxScoreElement = false)
        {
            DeleteExpiredItems();

            if (sortedSet.Count == 0)
                return default;

            var element = popMaxScoreElement ? sortedSet.Max : sortedSet.Min;
            sortedSet.Remove(element);
            sortedSetDict.Remove(element.Element);
            TryRemoveExpiration(element.Element);
            this.UpdateSize(element.Element, false);

            return element;
        }

        /// <summary>
        /// Removes and returns up to COUNT members with the low or high score
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="op"></param>
        private void SortedSetPopMinOrMaxCount(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion, SortedSetOperation op)
        {
            DeleteExpiredItems();

            var count = input.arg1;
            var countDone = 0;
            var withHeader = true;

            if (count == -1)
            {
                withHeader = false;
                count = 1;
            }

            if (sortedSet.Count < count)
                count = sortedSet.Count;

            if (input.arg2 > 0)
                respProtocolVersion = (byte)input.arg2;

            // When the output will be read later by ProcessRespArrayOutputAsPairs we force RESP version to 2.
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            if (count == 0)
            {
                writer.WriteEmptyArray();
                output.result1 = 0;
                return;
            }

            if (withHeader)
            {
                if (respProtocolVersion >= 3)
                    writer.WriteArrayLength(count);
                else
                    writer.WriteArrayLength(count * 2);
            }

            while (count > 0)
            {
                var max = op == SortedSetOperation.ZPOPMAX ? sortedSet.Max : sortedSet.Min;
                sortedSet.Remove(max);
                sortedSetDict.Remove(max.Element);
                TryRemoveExpiration(max.Element);

                UpdateSize(max.Element, false);

                if (!withHeader || respProtocolVersion >= 3)
                    writer.WriteArrayLength(2);

                writer.WriteBulkString(max.Element);
                writer.WriteDoubleNumeric(max.Score);

                countDone++;
                count--;
            }

            output.result1 = countDone;
        }

        private void SortedSetPersist(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            DeleteExpiredItems();

            var numFields = input.parseState.Count;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            writer.WriteArrayLength(numFields);

            foreach (var item in input.parseState.Parameters)
            {
                var result = Persist(item.ToArray());
                writer.WriteInt32(result);
                output.result1++;
            }
        }

        private void SortedSetTimeToLive(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            DeleteExpiredItems();

            var isMilliseconds = input.arg1 == 1;
            var isTimestamp = input.arg2 == 1;
            var numFields = input.parseState.Count;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            writer.WriteArrayLength(numFields);

            foreach (var item in input.parseState.Parameters)
            {
                var result = GetExpiration(item.ToArray());

                if (result >= 0)
                {
                    if (isTimestamp && isMilliseconds)
                    {
                        result = ConvertUtils.UnixTimeInMillisecondsFromTicks(result);
                    }
                    else if (isTimestamp && !isMilliseconds)
                    {
                        result = ConvertUtils.UnixTimeInSecondsFromTicks(result);
                    }
                    else if (!isTimestamp && isMilliseconds)
                    {
                        result = ConvertUtils.MillisecondsFromDiffUtcNowTicks(result);
                    }
                    else if (!isTimestamp && !isMilliseconds)
                    {
                        result = ConvertUtils.SecondsFromDiffUtcNowTicks(result);
                    }
                }

                writer.WriteInt64(result);
                output.result1++;
            }
        }

        private void SortedSetExpire(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            DeleteExpiredItems();

            var expirationWithOption = new ExpirationWithOption(input.arg1, input.arg2);

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            writer.WriteArrayLength(input.parseState.Count);

            foreach (var item in input.parseState.Parameters)
            {
                var result = SetExpiration(item.ToArray(), expirationWithOption.ExpirationTimeInTicks, expirationWithOption.ExpireOption);
                writer.WriteInt32(result);
                output.result1++;
            }
        }

        private void SortedSetCollect(ref ObjectInput input, ref ObjectOutput output)
        {
            DeleteExpiredItems();

            output.result1 = 1;
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
            Limit limit = default)
        {
            var elementsInLex = new List<(double, byte[])>();

            // parse boundaries
            if (!TryParseLexParameter(minParamByteArray, out var minValueChars, out var minValueExclusive, out var minValueInfinity) ||
                !TryParseLexParameter(maxParamByteArray, out var maxValueChars, out var maxValueExclusive, out var maxValueInfinity))
            {
                errorCode = int.MaxValue;
                return elementsInLex;
            }

            if (doReverse)
            {
                var tmpMinValueChars = minValueChars;
                minValueChars = maxValueChars;
                maxValueChars = tmpMinValueChars;

                (maxValueInfinity, minValueInfinity) = (minValueInfinity, maxValueInfinity);
                (maxValueExclusive, minValueExclusive) = (minValueExclusive, maxValueExclusive);
            }

            if (minValueInfinity == SpecialRanges.InfiniteMax ||
                maxValueInfinity == SpecialRanges.InfiniteMin ||
                (validLimit && (limit.offset < 0 || limit.count == 0)))
            {
                errorCode = 0;
                return elementsInLex;
            }

            try
            {
                var iterator = sortedSet.GetViewBetween((sortedSet.Min.Score, minValueChars.ToArray()), sortedSet.Max);

                // using ToList method so we avoid the Invalid operation ex. when removing
                foreach (var item in iterator.ToList())
                {
                    if (IsExpired(item.Element))
                    {
                        continue;
                    }

                    if (minValueInfinity != SpecialRanges.InfiniteMin)
                    {
                        var inRange = new ReadOnlySpan<byte>(item.Element).SequenceCompareTo(minValueChars);
                        if (inRange < 0 || (inRange == 0 && minValueExclusive))
                            continue;
                    }

                    if (maxValueInfinity != SpecialRanges.InfiniteMax)
                    {
                        var outRange = new ReadOnlySpan<byte>(item.Element).SequenceCompareTo(maxValueChars);
                        if (outRange > 0 || (outRange == 0 && maxValueExclusive))
                            break;
                    }

                    if (rem)
                    {
                        if (sortedSetDict.Remove(item.Element, out var _key))
                        {
                            sortedSet.Remove((_key, item.Element));
                            TryRemoveExpiration(item.Element);

                            UpdateSize(item.Element, false);
                        }
                    }
                    elementsInLex.Add(item);
                }

                if (doReverse) elementsInLex.Reverse();

                if (validLimit)
                {
                    elementsInLex = [.. elementsInLex
                                        .Skip(limit.offset > 0 ? limit.offset : 0)
                                        .Take(limit.count >= 0 ? limit.count : elementsInLex.Count)];
                }
            }
            catch (ArgumentException)
            {
                // this exception is thrown when the SortedSet is empty or
                // when the supplied minimum is larger than the sortedset maximum.
                Debug.Assert(sortedSet.Count == 0 ||
                             new ReadOnlySpan<byte>(sortedSet.Max.Element).SequenceCompareTo(minValueChars) < 0);
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
        private List<(double, byte[])> GetElementsInRangeByScore(double minValue, double maxValue, bool minExclusive, bool maxExclusive, bool withScore, bool doReverse, bool validLimit, bool rem, Limit limit = default)
        {
            if (doReverse)
            {
                (minValue, maxValue) = (maxValue, minValue);
                (minExclusive, maxExclusive) = (maxExclusive, minExclusive);
            }

            List<(double, byte[])> scoredElements = new();
            if ((validLimit && (limit.offset < 0 || limit.count == 0)) ||
                (sortedSet.Max.Score < minValue))
            {
                return scoredElements;
            }

            foreach (var item in sortedSet.GetViewBetween((minValue, null), sortedSet.Max))
            {
                if (IsExpired(item.Element)) continue;
                if (item.Score > maxValue || (maxExclusive && item.Score == maxValue)) break;
                if (minExclusive && item.Score == minValue) continue;
                scoredElements.Add(item);
            }
            if (doReverse) scoredElements.Reverse();
            if (validLimit)
            {
                scoredElements = [.. scoredElements
                                 .Skip(limit.offset > 0 ? limit.offset : 0)
                                 .Take(limit.count >= 0 ? limit.count : scoredElements.Count)];
            }

            if (rem)
            {
                foreach (var item in scoredElements.ToList())
                {
                    if (sortedSetDict.Remove(item.Item2, out var _key))
                    {
                        sortedSet.Remove((_key, item.Item2));
                        TryRemoveExpiration(item.Item2);

                        this.UpdateSize(item.Item2, false);
                    }
                }
            }

            return scoredElements;
        }

        #endregion

        #region HelperMethods

        /// <summary>
        /// Helper method to parse parameters min and max and exclusions
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

            if (NumUtils.TryParseWithInfinity(val, out valueDouble))
            {
                if (exclusive && double.IsInfinity(valueDouble))
                    exclusive = false;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Helper method to parse parameter when using Lexicographical ranges
        /// </summary>
        private bool TryParseLexParameter(ReadOnlySpan<byte> val,
                                          out ReadOnlySpan<byte> limitChars,
                                          out bool limitExclusive,
                                          out SpecialRanges infinity)
        {
            limitChars = default;
            limitExclusive = false;
            infinity = SpecialRanges.None;

            switch (val[0])
            {
                case (byte)'-':
                    infinity = SpecialRanges.InfiniteMin;
                    return true;
                case (byte)'+':
                    infinity = SpecialRanges.InfiniteMax;
                    return true;
                case (byte)'[':
                    limitChars = val.Slice(1);
                    limitExclusive = false;
                    break;
                case (byte)'(':
                    limitChars = val.Slice(1);
                    limitExclusive = true;
                    break;
                default:
                    return false;
            }

            if (limitChars.Length == 1)
            {
                if ((limitChars[0] == '-') || (limitChars[0] == '+'))
                {
                    // Redis accepts [+ yet in practice seems to treat it as a minimum.
                    infinity = SpecialRanges.InfiniteMin;
                    limitChars = default;
                }
            }

            return true;
        }

        #endregion
    }
}