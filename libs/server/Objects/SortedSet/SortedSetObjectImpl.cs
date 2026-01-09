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

        internal enum SpecialRanges : byte
        {
            None = 0,
            InfiniteMin = 1,
            InfiniteMax = 2
        }

        private void SortedSetAdd(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();

            var addedOrChanged = 0;
            double incrResult = 0;

            var options = SortedSetAddOption.None;
            var currTokenIdx = 0;

            // Try to parse a Score field
            var isFirstScoreParsed = false;
            if (!input.parseState.TryGetDouble(currTokenIdx, out var score))
            {
                // Try to get and validate options before the Score field, if any
                options = input.parseState.GetSortedSetAddOptions(currTokenIdx, out var nextIdxStep);
                currTokenIdx += nextIdxStep;
            }
            else
            {
                isFirstScoreParsed = true;
                currTokenIdx++;
            }

            while (currTokenIdx < input.parseState.Count)
            {
                if (!isFirstScoreParsed)
                {
                    if (!input.parseState.TryGetDouble(currTokenIdx++, out score))
                    {
                        // Invalid Score encountered
                        writer.WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT);
                        output.OutputFlags |= OutputFlags.ValueUnchanged;
                        return;
                    }
                }
                else
                {
                    isFirstScoreParsed = false;
                }

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
                            output.OutputFlags |= OutputFlags.ValueUnchanged;
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
                            output.OutputFlags |= OutputFlags.ValueUnchanged;
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
                
                if (addedOrChanged == 0)
                    output.OutputFlags |= OutputFlags.ValueUnchanged;
            }
        }

        private void SortedSetRemove(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();

            var removedItems = 0;

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var value = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;
                var valueArray = value.ToArray();

                if (!sortedSetDict.Remove(valueArray, out var key))
                    continue;

                removedItems++;
                sortedSet.Remove((key, valueArray));
                _ = TryRemoveExpiration(valueArray);

                this.UpdateSize(value, false);
            }

            writer.WriteInt32(removedItems);

            if (removedItems == 0)
                output.OutputFlags |= OutputFlags.ValueUnchanged;

            output.Header.result1 = removedItems;
        }

        private void SortedSetLength(ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            // Check both objects
            Debug.Assert(sortedSetDict.Count == sortedSet.Count, "SortedSet object is not in sync.");

            var length = Count();
            writer.WriteInt64(length);
            output.Header.result1 = length;
        }

        private void SortedSetScore(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            // ZSCORE key member
            var member = input.parseState.GetArgSliceByRef(0).ToArray();

            if (!TryGetScore(member, out var score))
                writer.WriteNull();
            else
                writer.WriteDoubleNumeric(score);

            output.Header.result1 = 1;
        }

        private void SortedSetScores(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            // ZMSCORE key member
            var count = input.parseState.Count;

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

            output.Header.result1 = count;
        }

        private void SortedSetCount(ref ObjectInput input, ref RespMemoryWriter writer)
        {
            // Read min & max
            var parseSuccessful = input.parseState.TryGetSortedSetMinMaxParameter(0, out var minValue, out var minExclusive);
            Debug.Assert(parseSuccessful);
            parseSuccessful = input.parseState.TryGetSortedSetMinMaxParameter(1, out var maxValue, out var maxExclusive);
            Debug.Assert(parseSuccessful);

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

        private void SortedSetIncrement(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();

            // Read increment value
            var incrValue = input.parseState.GetDouble(0);

            // Read member
            var member = input.parseState.GetArgSliceByRef(1).ToArray();

            if (sortedSetDict.TryGetValue(member, out var score))
            {
                var result = score + incrValue;

                if (double.IsNaN(result))
                {
                    writer.WriteError(CmdStrings.RESP_ERR_GENERIC_SCORE_NAN);
                    output.OutputFlags |= OutputFlags.ValueUnchanged;
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

        private void SortedSetRange(ref ObjectInput input, ref RespMemoryWriter writer)
        {
            //ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
            //ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
            //ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
            var rangeOpts = (SortedSetRangeOptions)input.arg2;
            var count = input.parseState.Count;
            var currIdx = 0;

            // read the rest of the arguments
            ZRangeOptions options = new()
            {
                ByScore = (rangeOpts & SortedSetRangeOptions.ByScore) != 0,
                ByLex = (rangeOpts & SortedSetRangeOptions.ByLex) != 0,
                Reverse = (rangeOpts & SortedSetRangeOptions.Reverse) != 0,
                WithScores = (rangeOpts & SortedSetRangeOptions.WithScores) != 0 || (rangeOpts & SortedSetRangeOptions.Store) != 0
            };

            try
            {
                if ((rangeOpts & SortedSetRangeOptions.Limit) != 0)
                {
                    while (currIdx < count)
                    {
                        var tokenSpan = input.parseState.GetArgSliceByRef(currIdx++).ReadOnlySpan;

                        if (tokenSpan.EqualsUpperCaseSpanIgnoringCase("LIMIT"u8))
                        {
                            // Read the next two tokens
                            options.Limit = (input.parseState.GetInt(currIdx++), input.parseState.GetInt(currIdx++));
                            options.ValidLimit = true;
                        }
                    }
                }

                if (count >= 2 && ((!options.ByScore && !options.ByLex) || options.ByScore))
                {
                    // Read min & max
                    var parseSuccessful = input.parseState.TryGetSortedSetMinMaxParameter(0, out var minValue, out var minExclusive);
                    Debug.Assert(parseSuccessful);
                    parseSuccessful = input.parseState.TryGetSortedSetMinMaxParameter(1, out var maxValue, out var maxExclusive);
                    Debug.Assert(parseSuccessful);

                    if (options.ByScore)
                    {
                        var scoredElements = GetElementsInRangeByScore(minValue, maxValue, minExclusive, maxExclusive, options.WithScores, options.Reverse, options.ValidLimit, false, options.Limit);

                        WriteSortedSetResult(options.WithScores, scoredElements.Count, scoredElements, ref writer);
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

                                WriteSortedSetResult(options.WithScores, n, iterator, ref writer);
                            }
                        }
                    }
                }

                // by Lex
                if (count >= 2 && options.ByLex)
                {
                    var elementsInLex = GetElementsInRangeByLex(ref input.parseState, options.Reverse, options.ValidLimit, false, options.Limit);

                    WriteSortedSetResult(options.WithScores, elementsInLex.Count, elementsInLex, ref writer);
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        private void SortedSetRemoveRangeByRank(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();

            // ZREMRANGEBYRANK key start stop
            var start = input.parseState.GetInt(0);
            var stop = input.parseState.GetInt(1);

            if (start > sortedSetDict.Count - 1)
            {
                output.OutputFlags |= OutputFlags.ValueUnchanged;
                return;
            }

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

        private void SortedSetRemoveRangeByScore(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();

            // ZREMRANGEBYSCORE key min max
            // Read min and max
            var parseSuccessful = input.parseState.TryGetSortedSetMinMaxParameter(0, out var minValue, out var minExclusive);
            Debug.Assert(parseSuccessful);
            parseSuccessful = input.parseState.TryGetSortedSetMinMaxParameter(1, out var maxValue, out var maxExclusive);
            Debug.Assert(parseSuccessful);

            var elementCount = GetElementsInRangeByScore(minValue, maxValue, minExclusive, maxExclusive, false,
                false, false, true).Count;

            if (elementCount == 0)
                output.OutputFlags |= OutputFlags.ValueUnchanged;

            // Write the number of elements
            writer.WriteInt32(elementCount);
        }

        private void SortedSetRandomMember(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            var count = input.arg1 >> 2;
            var withScores = (input.arg1 & 1) == 1;
            var includedCount = ((input.arg1 >> 1) & 1) == 1;
            var seed = input.arg2;
            var sortedSetCount = Count();

            if (count > 0 && count > sortedSetCount)
                count = sortedSetCount;

            // The count parameter can have a negative value, but the array length can't
            var arrayLength = Math.Abs((withScores && !writer.resp3) ? count * 2 : count);
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

                if (withScores && writer.resp3)
                    writer.WriteArrayLength(2);

                writer.WriteBulkString(element);

                if (withScores)
                {
                    writer.WriteDoubleNumeric(score);
                }
            }

            // Write count done into output footer
            output.Header.result1 = count;
        }

        private void SortedSetRemoveOrCountRangeByLex(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            // ZREMRANGEBYLEX key min max
            // ZLEXCOUNT key min max

            var isRemove = input.header.SortedSetOp == SortedSetOperation.ZREMRANGEBYLEX;

            if (isRemove)
                DeleteExpiredItems();

            var rem = GetElementsInRangeByLex(ref input.parseState, false, false, isRemove);

            writer.WriteInt32(rem.Count);

            if (isRemove && rem.Count == 0)
                output.OutputFlags |= OutputFlags.ValueUnchanged;

            output.Header.result1 = rem.Count;
        }

        /// <summary>
        /// Gets the rank of a member of the sorted set
        /// in ascending or descending order
        /// </summary>
        /// <param name="input"></param>
        /// <param name="writer"></param>
        private void SortedSetRank(ref ObjectInput input, ref RespMemoryWriter writer)
        {
            //ZRANK key member
            var withScore = input.arg1 == 1;

            var member = input.parseState.GetArgSliceByRef(0).ToArray();

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

                if (input.header.SortedSetOp == SortedSetOperation.ZREVRANK)
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
            {
                return default;
            }

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
        /// <param name="writer"></param>
        private void SortedSetPopMinOrMaxCount(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();

            var count = input.arg1;
            var withHeader = true;

            if (count == -1)
            {
                withHeader = false;
                count = 1;
            }

            if (sortedSet.Count < count)
                count = sortedSet.Count;

            if (count == 0)
            {
                writer.WriteEmptyArray();
                output.Header.result1 = 0;
                output.OutputFlags |= OutputFlags.ValueUnchanged;
                return;
            }

            if (withHeader)
            {
                var length = writer.resp3 ? count : count * 2;
                writer.WriteArrayLength(length);
            }

            var countDone = 0;

            while (count > 0)
            {
                var max = input.header.SortedSetOp == SortedSetOperation.ZPOPMAX ? sortedSet.Max : sortedSet.Min;
                sortedSet.Remove(max);
                sortedSetDict.Remove(max.Element);
                TryRemoveExpiration(max.Element);

                UpdateSize(max.Element, false);

                if (!withHeader || writer.resp3)
                    writer.WriteArrayLength(2);

                writer.WriteBulkString(max.Element);
                writer.WriteDoubleNumeric(max.Score);

                countDone++;
                count--;
            }

            output.Header.result1 = countDone;
        }

        private void SortedSetPersist(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();
            var numFields = input.parseState.Count;

            writer.WriteArrayLength(numFields);

            foreach (var item in input.parseState.Parameters)
            {
                var result = Persist(item.ToArray());
                writer.WriteInt32(result);
                output.Header.result1++;
            }
        }

        private void SortedSetTimeToLive(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();

            var isMilliseconds = input.arg1 == 1;
            var isTimestamp = input.arg2 == 1;
            var numFields = input.parseState.Count;

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
                    else if (isTimestamp)
                    {
                        result = ConvertUtils.UnixTimeInSecondsFromTicks(result);
                    }
                    else if (isMilliseconds)
                    {
                        result = ConvertUtils.MillisecondsFromDiffUtcNowTicks(result);
                    }
                    else
                    {
                        result = ConvertUtils.SecondsFromDiffUtcNowTicks(result);
                    }
                }

                writer.WriteInt64(result);
                output.Header.result1++;
            }
        }

        private void SortedSetExpire(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();

            var expirationWithOption = new ExpirationWithOption(input.arg1, input.arg2);

            writer.WriteArrayLength(input.parseState.Count);

            foreach (var item in input.parseState.Parameters)
            {
                var result = SetExpiration(item.ToArray(), expirationWithOption.ExpirationTimeInTicks,
                    expirationWithOption.ExpireOption);
                writer.WriteInt32(result);
                output.Header.result1++;
            }
        }

        private void SortedSetCollect(ref ObjectOutput output)
        {
            DeleteExpiredItems();
            
            output.Header.result1 = 1;
        }

        #region CommonMethods

        /// <summary>
        /// Gets the elements that belong to the Range using lexicographical order
        /// </summary>
        /// <param name="parseState">Parse state</param>
        /// <param name="doReverse">Perfom reverse order</param>
        /// <param name="validLimit">Use a limit offset count</param>
        /// <param name="rem">Remove elements</param>
        /// <param name="limit">offset and count values</param>
        /// <returns></returns>
        private List<(double, byte[])> GetElementsInRangeByLex(
            ref SessionParseState parseState,
            bool doReverse,
            bool validLimit,
            bool rem,
            Limit limit = default)
        {
            var elementsInLex = new List<(double, byte[])>();

            // parse boundaries
            // Read min & max
            var parseSuccessful = parseState.TryGetSortedSetLexMinMaxParameter(0, out var minValueChars, out var minValueExclusive, out var minValueInfinity);
            Debug.Assert(parseSuccessful);
            parseSuccessful = parseState.TryGetSortedSetLexMinMaxParameter(1, out var maxValueChars, out var maxValueExclusive, out var maxValueInfinity);
            Debug.Assert(parseSuccessful);

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

        void WriteSortedSetResult(bool withScores, int count, IEnumerable<(double, byte[])> iterator, ref RespMemoryWriter writer)
        {
            if (withScores && writer.resp3)
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

        #endregion
    }
}