// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Sorted set methods with network layer access
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Adds all the specified members with the specified scores to the sorted set stored at key.
        /// Current members get the score updated and reordered.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetAdd<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 3)
                return AbortWithWrongNumberOfArguments("ZADD");

            // Get the key for SortedSet
            var currIdx = 0;
            var key = parseState.GetArgSliceByRef(currIdx++);
            var options = SortedSetAddOption.None;
            ReadOnlySpan<byte> error = default;

            // If the first argument is not a score field - parse and validate options
            if (!parseState.TryGetDouble(currIdx, out _))
            {
                options = parseState.GetSortedSetAddOptions(currIdx, out var nextIdxStep);

                // No options were parsed - invalid Score encountered
                if (nextIdxStep == 0)
                {
                    // Invalid Score encountered
                    error = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                }
                else
                {
                    currIdx += nextIdxStep;

                    // Validate ZADD options combination

                    // XX & NX are mutually exclusive
                    if ((options & SortedSetAddOption.XX) == SortedSetAddOption.XX &&
                        (options & SortedSetAddOption.NX) == SortedSetAddOption.NX)
                        error = CmdStrings.RESP_ERR_XX_NX_NOT_COMPATIBLE;

                    // NX, GT & LT are mutually exclusive
                    if (((options & SortedSetAddOption.GT) == SortedSetAddOption.GT &&
                         (options & SortedSetAddOption.LT) == SortedSetAddOption.LT) ||
                        (((options & SortedSetAddOption.GT) == SortedSetAddOption.GT ||
                          (options & SortedSetAddOption.LT) == SortedSetAddOption.LT) &&
                         (options & SortedSetAddOption.NX) == SortedSetAddOption.NX))
                        error = CmdStrings.RESP_ERR_GT_LT_NX_NOT_COMPATIBLE;

                    // INCR supports only one score-element pair
                    if ((options & SortedSetAddOption.INCR) == SortedSetAddOption.INCR &&
                        (parseState.Count - currIdx > 2))
                        error = CmdStrings.RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR;
                }
            }

            // From here on we expect only score-element pairs
            // Remaining token count should be positive and even
            if (error.IsEmpty && (currIdx == parseState.Count || (parseState.Count - currIdx) % 2 != 0))
                error = CmdStrings.RESP_SYNTAX_ERROR;

            if (!error.IsEmpty)
            {
                while (!RespWriteUtils.TryWriteError(error, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, startIdx: 1) { SortedSetOp = SortedSetOperation.ZADD };

            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetAdd(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
            }

            return true;
        }

        /// <summary>
        /// Removes the specified members from the sorted set stored at key.
        /// Non existing members are ignored.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRemove<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments("ZREM");
            }

            // Get the key for SortedSet
            var key = parseState.GetArgSliceByRef(0);

            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, startIdx: 1) { SortedSetOp = SortedSetOperation.ZREM };
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetRemove(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }
            return true;
        }

        /// <summary>
        /// Returns the sorted set cardinality (number of elements) of the sorted set
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetLength<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments("ZCARD");
            }

            // Get the key for SortedSet
            var key = parseState.GetArgSliceByRef(0);

            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState) { SortedSetOp = SortedSetOperation.ZCARD };

            var status = storageApi.SortedSetLength(key, ref input, out var output);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    while (!RespWriteUtils.TryWriteInt32(output.result1, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key, using byscore, bylex and rev modifiers.
        /// Min and max are range boundaries, where 0 is the first element, 1 is the next element and so on.
        /// There can also be negative numbers indicating offsets from the end of the sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and so on.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRange<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
            if (parseState.Count < 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(command));
            }

            var key = parseState.GetArgSliceByRef(0);

            // Try to parse command options and validate input
            if (!parseState.TryGetSortedSetRangeOptions(3, command, out var rangeOpts, out _, out _, out var error) ||
                !ValidateSortedSetRangeInput(1, rangeOpts, out error))
                return AbortWithErrorMessage(error);

            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, startIdx: 1, arg1: respProtocolVersion, arg2: (int)rangeOpts) { SortedSetOp = SortedSetOperation.ZRANGE };

            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetRange(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        private unsafe bool SortedSetRangeStore<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
            if (parseState.Count is < 4 or > 9)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ZRANGESTORE));
            }

            var dstKey = parseState.GetArgSliceByRef(0);
            var srcKey = parseState.GetArgSliceByRef(1);

            // Try to parse command options and validate input
            if (!parseState.TryGetSortedSetRangeOptions(4, RespCommand.ZRANGESTORE, out var options, out _, out _, out var error) ||
                !ValidateSortedSetRangeInput(2, options, out error))
                return AbortWithErrorMessage(error);

            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, startIdx: 2,
                arg1: respProtocolVersion, arg2: (int)options) { SortedSetOp = SortedSetOperation.ZRANGE };

            var status = storageApi.SortedSetRangeStore(dstKey, srcKey, ref input, out int result);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.TryWriteInt32(result, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the score of member in the sorted set at key.
        /// If member does not exist in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetScore<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //validation if minimum args
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArguments("ZSCORE");
            }

            // Get the key for SortedSet
            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, startIdx: 1,
                arg1: respProtocolVersion) { SortedSetOp = SortedSetOperation.ZSCORE };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetScore(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    WriteNull();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the score of member in the sorted set at key.
        /// If member does not exist in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetScores<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //validation if minimum args
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments("ZMSCORE");
            }

            // Get the key for SortedSet
            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, startIdx: 1) { SortedSetOp = SortedSetOperation.ZMSCORE };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetScores(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    // Write an empty array of count - 1 elements with null values.
                    while (!RespWriteUtils.TryWriteArrayLength(parseState.Count - 1, ref dcurr, dend))
                        SendAndReset();

                    for (var i = 0; i < parseState.Count - 1; i++)
                        WriteNull();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Removes and returns the first element from the sorted set stored at key,
        /// with the scores ordered from low to high (min) or high to low (max).
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetPop<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1 || parseState.Count > 2)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            // Get the key for SortedSet
            var key = parseState.GetArgSliceByRef(0);

            var popCount = -1;

            if (parseState.Count == 2)
            {
                // Read count
                if (!parseState.TryGetInt(1, out popCount) || (popCount < 0))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
                }
            }

            var op =
                command switch
                {
                    RespCommand.ZPOPMIN => SortedSetOperation.ZPOPMIN,
                    RespCommand.ZPOPMAX => SortedSetOperation.ZPOPMAX,
                    _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                };

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, arg1: popCount) { SortedSetOp = op };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetPop(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Removes and returns up to count members from the first non-empty sorted set key from the list of keys.
        /// </summary>
        private unsafe bool SortedSetMPop<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // ZMPOP requires at least 3 args: numkeys, key [key...], <MIN|MAX> [COUNT count]
            if (parseState.Count < 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ZMPOP));
            }

            // Get number of keys
            if (!parseState.TryGetInt(0, out var numKeys) || numKeys < 1)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (numKeys < 0)
            {
                return AbortWithErrorMessage(CmdStrings.GenericParamShouldBeGreaterThanZero, "numkeys");
            }

            // Validate we have enough arguments (no of keys + (MIN or MAX))
            if (parseState.Count < numKeys + 2)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            // Get MIN/MAX argument
            var orderArg = parseState.GetArgSliceByRef(numKeys + 1);
            var orderSpan = orderArg.ReadOnlySpan;
            var lowScoresFirst = true;

            if (orderSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MIN))
                lowScoresFirst = true;
            else if (orderSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MAX))
                lowScoresFirst = false;
            else
            {
                return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            // Parse optional COUNT argument
            var count = 1;
            if (parseState.Count > numKeys + 2)
            {
                if (parseState.Count != numKeys + 4)
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }

                var countArg = parseState.GetArgSliceByRef(numKeys + 2);
                if (!countArg.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.COUNT))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }

                if (!parseState.TryGetInt(numKeys + 3, out count) || count < 1)
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }

                if (count < 0)
                {
                    return AbortWithErrorMessage(CmdStrings.GenericParamShouldBeGreaterThanZero, "count");
                }
            }

            var keys = parseState.Parameters.Slice(1, numKeys);
            var status = storageApi.SortedSetMPop(keys, count, lowScoresFirst, out var poppedKey, out var pairs);

            switch (status)
            {
                case GarnetStatus.OK:
                    if (pairs == null || pairs.Length == 0)
                    {
                        // No elements found
                        WriteNull();
                    }
                    else
                    {
                        // Write array with 2 elements: key and array of elements
                        while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                            SendAndReset();

                        // Write key
                        while (!RespWriteUtils.TryWriteBulkString(poppedKey.ReadOnlySpan, ref dcurr, dend))
                            SendAndReset();

                        // Write array of member-score pairs
                        while (!RespWriteUtils.TryWriteArrayLength(pairs.Length, ref dcurr, dend))
                            SendAndReset();

                        foreach (var (member, score) in pairs)
                        {
                            while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                                SendAndReset();
                            while (!RespWriteUtils.TryWriteBulkString(member.ReadOnlySpan, ref dcurr, dend))
                                SendAndReset();

                            if (respProtocolVersion >= 3)
                            {
                                WriteDoubleNumeric(double.Parse(score.ReadOnlySpan));
                            }
                            else
                            {
                                while (!RespWriteUtils.TryWriteBulkString(score.ReadOnlySpan, ref dcurr, dend))
                                    SendAndReset();
                            }
                        }
                    }
                    break;

                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the number of elements in the sorted set at key with a score between min and max.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetCount<TGarnetApi>(ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 3)
            {
                return AbortWithWrongNumberOfArguments("ZCOUNT");
            }

            // Get the key for the Sorted Set
            var key = parseState.GetArgSliceByRef(0);

            // Validate min & max
            if (!parseState.TryGetSortedSetMinMaxParameter(1, out _, out _) ||
                !parseState.TryGetSortedSetMinMaxParameter(2, out _, out _))
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, startIdx: 1) { SortedSetOp = SortedSetOperation.ZCOUNT };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetCount(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// ZLEXCOUNT: Returns the number of elements in the sorted set with a value between min and max.
        /// When all the elements in a sorted set have the same score,
        /// this command forces lexicographical ordering.
        /// ZREMRANGEBYLEX: Removes all elements in the sorted set between the
        /// lexicographical range specified by min and max.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetLengthByValue<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 3)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            // Get the key
            var key = parseState.GetArgSliceByRef(0);

            var op =
                command switch
                {
                    RespCommand.ZREMRANGEBYLEX => SortedSetOperation.ZREMRANGEBYLEX,
                    RespCommand.ZLEXCOUNT => SortedSetOperation.ZLEXCOUNT,
                    _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                };

            // Validate min & max
            if (!parseState.TryGetSortedSetLexMinMaxParameter(1, out _, out _, out _) ||
                !parseState.TryGetSortedSetLexMinMaxParameter(2, out _, out _, out _))
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING);

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, startIdx: 1) { SortedSetOp = op };

            var status = op == SortedSetOperation.ZREMRANGEBYLEX ?
                storageApi.SortedSetRemoveRangeByLex(key, ref input, out var output) :
                storageApi.SortedSetLengthByValue(key, ref input, out output);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process response
                    if (output.result1 == int.MaxValue)
                    {
                        // Error in arguments
                        while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (output.result1 == int.MinValue)  // command partially executed
                        return false;
                    else
                        while (!RespWriteUtils.TryWriteInt32(output.result1, ref dcurr, dend))
                            SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Increments the score of member in the sorted set stored at key by increment.
        /// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetIncrement<TGarnetApi>(ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            //validation of required args
            if (parseState.Count != 3)
            {
                return AbortWithWrongNumberOfArguments("ZINCRBY");
            }

            // Get the key for the Sorted Set
            var key = parseState.GetArgSliceByRef(0);

            // Validate increment
            if (!parseState.TryGetDouble(1, out _))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_NOT_VALID_FLOAT);
            }

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, startIdx: 1) { SortedSetOp = SortedSetOperation.ZINCRBY };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetIncrement(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.NOTFOUND:
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// ZRANK: Returns the rank of member in the sorted set, the scores in the sorted set are ordered from low to high
        /// ZREVRANK: Returns the rank of member in the sorted set, with the scores ordered from high to low
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRank<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            // Get the key for SortedSet
            var key = parseState.GetArgSliceByRef(0);
            var includeWithScore = false;

            // Read WITHSCORE
            if (parseState.Count == 3)
            {
                var withScoreSlice = parseState.GetArgSliceByRef(2);

                if (!withScoreSlice.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHSCORE))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }

                includeWithScore = true;
            }

            var op =
                command switch
                {
                    RespCommand.ZRANK => SortedSetOperation.ZRANK,
                    RespCommand.ZREVRANK => SortedSetOperation.ZREVRANK,
                    _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                };

            // Prepare input
            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, startIdx: 1, arg1: includeWithScore ? 1 : 0) { SortedSetOp = op };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetRank(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    WriteNull();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// ZREMRANGEBYRANK: Removes all elements in the sorted set stored at key with rank between start and stop.
        /// Both start and stop are 0 -based indexes with 0 being the element with the lowest score.
        /// ZREMRANGEBYSCORE: Removes all elements in the sorted set stored at key with a score between min and max (inclusive by default).
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRemoveRange<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 3)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            // Get the key
            var key = parseState.GetArgSliceByRef(0);

            var op =
                command switch
                {
                    RespCommand.ZREMRANGEBYRANK => SortedSetOperation.ZREMRANGEBYRANK,
                    RespCommand.ZREMRANGEBYSCORE => SortedSetOperation.ZREMRANGEBYSCORE,
                    _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                };

            // Validate input
            if (op == SortedSetOperation.ZREMRANGEBYRANK)
            {
                if (!parseState.TryGetInt(1, out _) || 
                    !parseState.TryGetInt(2, out _))
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }
            else
            {
                if (!parseState.TryGetSortedSetMinMaxParameter(1, out _, out _) ||
                    !parseState.TryGetSortedSetMinMaxParameter(2, out _, out _))
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
            }


            // Prepare input
            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, startIdx: 1) { SortedSetOp = op };

            // Prepare output
            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetRemoveRange(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }
            return true;
        }

        /// <summary>
        /// Returns a random element from the sorted set key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetRandomMember<TGarnetApi>(ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1 || parseState.Count > 3)
            {
                return AbortWithWrongNumberOfArguments("ZRANDMEMBER");
            }

            // Get the key for the Sorted Set
            var key = parseState.GetArgSliceByRef(0);
            var paramCount = 1;
            var includeWithScores = false;
            var includedCount = false;

            if (parseState.Count >= 2)
            {
                // Read count
                if (!parseState.TryGetInt(1, out paramCount))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                includedCount = true;

                // Read withscores
                if (parseState.Count == 3)
                {
                    var withScoresSlice = parseState.GetArgSliceByRef(2);

                    if (!withScoresSlice.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHSCORES))
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }

                    includeWithScores = true;
                }
            }

            // Create a random seed
            var seed = Random.Shared.Next();

            // Prepare input
            var inputArg = (((paramCount << 1) | (includedCount ? 1 : 0)) << 1) | (includeWithScores ? 1 : 0);
            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, arg1: inputArg, arg2: seed) { SortedSetOp = SortedSetOperation.ZRANDMEMBER };

            var status = GarnetStatus.NOTFOUND;
            ObjectOutput output = default;

            // This prevents going to the backend if ZRANDMEMBER is called with a count of 0
            if (paramCount != 0)
            {
                // Prepare output
                output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));
                status = storageApi.SortedSetRandomMember(key, ref input, ref output);
            }

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    if (includedCount)
                    {
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_EMPTYLIST, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        WriteNull();
                    }
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }
            return true;
        }

        /// <summary>
        ///  Computes a difference operation  between the first and all successive sorted sets
        ///  and returns the result to the client.
        ///  The total number of input keys is specified.
        /// </summary>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        private unsafe bool SortedSetDifference<TGarnetApi>(ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments("ZDIFF");
            }

            // Number of keys
            if (!parseState.TryGetInt(0, out var nKeys))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (parseState.Count - 1 != nKeys && parseState.Count - 1 != nKeys + 1)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            var includeWithScores = false;
            var keys = new PinnedSpanByte[nKeys];

            for (var i = 1; i < nKeys + 1; i++)
            {
                keys[i - 1] = parseState.GetArgSliceByRef(i);
            }

            if (parseState.Count - 1 > nKeys)
            {
                var withScores = parseState.GetArgSliceByRef(parseState.Count - 1).ReadOnlySpan;

                if (!withScores.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHSCORES))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                includeWithScores = true;
            }

            var status = storageApi.SortedSetDifference(keys, out var result);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    if (result == null)
                    {
                        while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        // write the size of the array reply
                        while (!RespWriteUtils.TryWriteArrayLength(
                                includeWithScores && (respProtocolVersion == 2) ? result.Count * 2 : result.Count, ref dcurr, dend))
                            SendAndReset();

                        if (result != null)
                        {
                            foreach (var (score, element) in result)
                            {
                                if (respProtocolVersion >= 3 && includeWithScores)
                                    while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                                        SendAndReset();

                                while (!RespWriteUtils.TryWriteBulkString(element, ref dcurr, dend))
                                    SendAndReset();

                                if (includeWithScores)
                                {
                                    WriteDoubleNumeric(score);
                                }
                            }
                        }
                    }
                    break;
            }

            return true;
        }

        /// <summary>
        ///  Computes a difference operation  between the first and all successive sorted sets and store
        ///  and returns the result to the client.
        ///  The total number of input keys is specified.
        /// </summary>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        private unsafe bool SortedSetDifferenceStore<TGarnetApi>(ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ZDIFFSTORE));
            }

            // Number of keys
            if (!parseState.TryGetInt(1, out var nKeys))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (parseState.Count - 2 != nKeys)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            var destination = parseState.GetArgSliceByRef(0);
            var keys = parseState.Parameters.Slice(2, nKeys);

            var status = storageApi.SortedSetDifferenceStore(destination, keys, out var count);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    while (!RespWriteUtils.TryWriteInt32(count, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Computes an intersection operation between multiple sorted sets
        /// and returns the result to the client.
        /// </summary>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetIntersect<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ZINTER));
            }

            // Number of keys
            if (!parseState.TryGetInt(0, out var nKeys))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (nKeys < 1)
            {
                return AbortWithErrorMessage(CmdStrings.GenericErrAtLeastOneKey, nameof(RespCommand.ZINTER));
            }

            if (parseState.Count < nKeys + 1)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            var includeWithScores = false;
            double[] weights = null;
            var aggregateType = SortedSetAggregateType.Sum;
            var currentArg = nKeys + 1;

            // Read all the keys
            var keys = parseState.Parameters.Slice(1, nKeys);

            // Parse optional arguments
            while (currentArg < parseState.Count)
            {
                var arg = parseState.GetArgSliceByRef(currentArg).ReadOnlySpan;

                if (arg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHSCORES))
                {
                    includeWithScores = true;
                    currentArg++;
                }
                else if (arg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WEIGHTS))
                {
                    currentArg++;
                    if (currentArg + nKeys > parseState.Count)
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }

                    weights = new double[nKeys];
                    for (var i = 0; i < nKeys; i++)
                    {
                        if (!parseState.TryGetDouble(currentArg + i, out weights[i]))
                        {
                            return AbortWithErrorMessage(CmdStrings.GenericErrNotAFloat, "weight");
                        }
                    }
                    currentArg += nKeys;
                }
                else if (arg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.AGGREGATE))
                {
                    if (++currentArg >= parseState.Count || !parseState.TryGetSortedSetAggregateType(currentArg++, out aggregateType))
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }
                }
                else
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }
            }

            var status = storageApi.SortedSetIntersect(keys, weights, aggregateType, out var result);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    if (result == null || result.Count == 0)
                    {
                        while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                            SendAndReset();
                        break;
                    }

                    // write the size of the array reply
                    var arrayLength = result.Count;
                    if (includeWithScores && respProtocolVersion == 2)
                        arrayLength *= 2;
                    while (!RespWriteUtils.TryWriteArrayLength(arrayLength, ref dcurr, dend))
                        SendAndReset();

                    foreach (var (score, element) in result)
                    {
                        if (includeWithScores && respProtocolVersion >= 3)
                        {
                            while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                                SendAndReset();
                        }

                        while (!RespWriteUtils.TryWriteBulkString(element, ref dcurr, dend))
                            SendAndReset();

                        if (includeWithScores)
                        {
                            WriteDoubleNumeric(score);
                        }
                    }
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the cardinality of the intersection between multiple sorted sets.
        /// </summary>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetIntersectLength<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ZINTERCARD));
            }

            // Number of keys
            if (!parseState.TryGetInt(0, out var nKeys))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (nKeys < 1)
            {
                return AbortWithErrorMessage(CmdStrings.GenericErrAtLeastOneKey, nameof(RespCommand.ZINTERCARD));
            }

            if (parseState.Count < nKeys + 1)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            var keys = parseState.Parameters.Slice(1, nKeys);

            // Optional LIMIT argument
            int? limit = null;
            if (parseState.Count > nKeys + 1)
            {
                var limitArg = parseState.GetArgSliceByRef(nKeys + 1);
                if (!limitArg.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.LIMIT) || parseState.Count != nKeys + 3)
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }

                if (!parseState.TryGetInt(nKeys + 2, out var limitVal))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }

                if (limitVal < 0)
                {
                    return AbortWithErrorMessage(CmdStrings.GenericErrCantBeNegative, "LIMIT");
                }

                limit = limitVal;
            }

            var status = storageApi.SortedSetIntersectLength(keys, limit, out var count);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    while (!RespWriteUtils.TryWriteInt32(count, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Computes an intersection operation between multiple sorted sets and store
        /// the result in the destination key.
        /// The total number of input keys is specified.
        /// </summary>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetIntersectStore<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ZINTERSTORE));
            }

            // Number of keys
            if (!parseState.TryGetInt(1, out var nKeys))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            var destination = parseState.GetArgSliceByRef(0);
            var keys = parseState.Parameters.Slice(2, nKeys);

            double[] weights = null;
            var aggregateType = SortedSetAggregateType.Sum;
            var currentArg = nKeys + 2;

            // Parse optional arguments
            while (currentArg < parseState.Count)
            {
                var arg = parseState.GetArgSliceByRef(currentArg).ReadOnlySpan;

                if (arg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WEIGHTS))
                {
                    currentArg++;
                    if (currentArg + nKeys > parseState.Count)
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }

                    weights = new double[nKeys];
                    for (var i = 0; i < nKeys; i++)
                    {
                        if (!parseState.TryGetDouble(currentArg + i, out weights[i]))
                        {
                            return AbortWithErrorMessage(CmdStrings.GenericErrNotAFloat, "weight");
                        }
                    }
                    currentArg += nKeys;
                }
                else if (arg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.AGGREGATE))
                {
                    currentArg++;
                    if (currentArg >= parseState.Count)
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }

                    if (!parseState.TryGetSortedSetAggregateType(currentArg, out aggregateType))
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }
                    currentArg++;
                }
                else
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }
            }

            var status = storageApi.SortedSetIntersectStore(destination, keys, weights, aggregateType, out var count);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    while (!RespWriteUtils.TryWriteInt32(count, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        ///  Computes a union operation between multiple sorted sets and returns the result to the client.
        ///  The total number of input keys is specified.
        /// </summary>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetUnion<TGarnetApi>(ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ZUNION));
            }

            // Number of keys
            if (!parseState.TryGetInt(0, out var nKeys))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (nKeys < 1)
            {
                return AbortWithErrorMessage(CmdStrings.GenericErrAtLeastOneKey, nameof(RespCommand.ZUNION));
            }

            if (parseState.Count < nKeys + 1)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            var currentArg = nKeys + 1;
            var includeWithScores = false;
            double[] weights = null;
            var aggregateType = SortedSetAggregateType.Sum;

            // Parse optional arguments
            while (currentArg < parseState.Count)
            {
                var arg = parseState.GetArgSliceByRef(currentArg).ReadOnlySpan;
                if (arg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHSCORES))
                {
                    includeWithScores = true;
                    currentArg++;
                }
                else if (arg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WEIGHTS))
                {
                    currentArg++;
                    if (currentArg + nKeys > parseState.Count)
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }

                    weights = new double[nKeys];
                    for (var i = 0; i < nKeys; i++)
                    {
                        if (!parseState.TryGetDouble(currentArg + i, out weights[i]))
                        {
                            return AbortWithErrorMessage(CmdStrings.GenericErrNotAFloat, "weight");
                        }
                    }
                    currentArg += nKeys;
                }
                else if (arg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.AGGREGATE))
                {
                    currentArg++;
                    if (currentArg >= parseState.Count)
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }

                    if (!parseState.TryGetSortedSetAggregateType(currentArg, out aggregateType))
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }
                    currentArg++;
                }
                else
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }
            }

            // Read all the keys
            var keys = parseState.Parameters.Slice(1, nKeys);

            var status = storageApi.SortedSetUnion(keys, weights, aggregateType, out var result);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    if (result == null || result.Count == 0)
                    {
                        while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                            SendAndReset();
                        break;
                    }

                    // write the size of the array reply
                    var arrayLength = result.Count;
                    if (includeWithScores && respProtocolVersion == 2)
                        arrayLength *= 2;
                    while (!RespWriteUtils.TryWriteArrayLength(arrayLength, ref dcurr, dend))
                        SendAndReset();

                    foreach (var (score, element) in result)
                    {
                        if (includeWithScores && respProtocolVersion >= 3)
                        {
                            while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                                SendAndReset();
                        }

                        while (!RespWriteUtils.TryWriteBulkString(element, ref dcurr, dend))
                            SendAndReset();

                        if (includeWithScores)
                        {
                            WriteDoubleNumeric(score);
                        }
                    }
                    break;
            }

            return true;
        }

        /// <summary>
        /// Computes a union operation between multiple sorted sets and stores the result in destination.
        /// Returns the number of elements in the resulting sorted set at destination.
        /// </summary>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetUnionStore<TGarnetApi>(ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ZUNIONSTORE));
            }

            // Number of keys
            if (!parseState.TryGetInt(1, out var nKeys))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (nKeys < 1)
            {
                return AbortWithErrorMessage(CmdStrings.GenericErrAtLeastOneKey, nameof(RespCommand.ZUNIONSTORE));
            }

            if (parseState.Count < nKeys + 2)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            var currentArg = nKeys + 2;
            double[] weights = null;
            var aggregateType = SortedSetAggregateType.Sum;

            // Parse optional arguments
            while (currentArg < parseState.Count)
            {
                var arg = parseState.GetArgSliceByRef(currentArg).ReadOnlySpan;
                if (arg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WEIGHTS))
                {
                    currentArg++;
                    if (currentArg + nKeys > parseState.Count)
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }

                    weights = new double[nKeys];
                    for (var i = 0; i < nKeys; i++)
                    {
                        if (!parseState.TryGetDouble(currentArg + i, out weights[i]))
                        {
                            return AbortWithErrorMessage(CmdStrings.GenericErrNotAFloat, "weight");
                        }
                    }
                    currentArg += nKeys;
                }
                else if (arg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.AGGREGATE))
                {
                    currentArg++;
                    if (currentArg + 1 > parseState.Count)
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }

                    if (!parseState.TryGetSortedSetAggregateType(currentArg, out aggregateType))
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }
                    currentArg++;
                }
                else
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }
            }

            var destination = parseState.GetArgSliceByRef(0);
            var keys = parseState.Parameters.Slice(2, nKeys);

            var status = storageApi.SortedSetUnionStore(destination, keys, weights, aggregateType, out var count);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    while (!RespWriteUtils.TryWriteInt32(count, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// BZPOPMIN/BZPOPMAX key [key ...] timeout
        /// </summary>
        private unsafe bool SortedSetBlockingPop(RespCommand command)
        {
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            if (!parseState.TryGetTimeout(parseState.Count - 1, out var timeout, out var error))
            {
                return AbortWithErrorMessage(error);
            }

            var keysBytes = new byte[parseState.Count - 1][];

            for (var i = 0; i < keysBytes.Length; i++)
                keysBytes[i] = parseState.GetArgSliceByRef(i).ToArray();

            var result = storeWrapper.itemBroker.GetCollectionItemAsync(command, keysBytes, this, timeout).Result;

            if (result.IsForceUnblocked)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_UNBLOCKED_CLIENT_VIA_CLIENT_UNBLOCK, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (result.IsTypeMismatch)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (!result.Found)
            {
                WriteNull();
            }
            else
            {
                while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteBulkString(result.Key, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteBulkString(result.Item, ref dcurr, dend))
                    SendAndReset();

                WriteDoubleNumeric(result.Score);
            }

            return true;
        }

        /// <summary>
        /// BZMPOP timeout numkeys key [key ...] &lt;MIN | MAX&gt; [COUNT count]
        /// </summary>
        private unsafe bool SortedSetBlockingMPop()
        {
            if (parseState.Count < 4)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BZMPOP));
            }

            var currTokenId = 0;

            // Read timeout
            if (!parseState.TryGetTimeout(currTokenId++, out var timeout, out var error))
            {
                return AbortWithErrorMessage(error);
            }

            // Read count of keys
            if (!parseState.TryGetInt(currTokenId++, out var numKeys) || (numKeys <= 0))
            {
                return AbortWithErrorMessage(CmdStrings.GenericParamShouldBeGreaterThanZero, "numkeys");
            }

            // Should have MAX|MIN or it should contain COUNT + value
            if (parseState.Count != numKeys + 3 && parseState.Count != numKeys + 5)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }

            var keysBytes = new byte[numKeys][];
            for (var i = 0; i < keysBytes.Length; i++)
                keysBytes[i] = parseState.GetArgSliceByRef(currTokenId++).ToArray();

            var cmdArgs = new PinnedSpanByte[2];

            var orderArg = parseState.GetArgSliceByRef(currTokenId++);
            var orderSpan = orderArg.ReadOnlySpan;
            bool lowScoresFirst;

            if (orderSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MIN))
                lowScoresFirst = true;
            else if (orderSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MAX))
                lowScoresFirst = false;
            else
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }

            cmdArgs[0] = PinnedSpanByte.FromPinnedPointer((byte*)&lowScoresFirst, 1);

            var popCount = 1;

            if (parseState.Count == numKeys + 5)
            {
                var countKeyword = parseState.GetArgSliceByRef(currTokenId++);

                if (!countKeyword.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.COUNT))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                }

                if (!parseState.TryGetInt(currTokenId, out popCount) || popCount < 1)
                {
                    return AbortWithErrorMessage(CmdStrings.GenericParamShouldBeGreaterThanZero, "count");
                }
            }

            cmdArgs[1] = PinnedSpanByte.FromPinnedPointer((byte*)&popCount, sizeof(int));

            var result = storeWrapper.itemBroker.GetCollectionItemAsync(RespCommand.BZMPOP, keysBytes, this, timeout, cmdArgs).Result;

            if (result.IsForceUnblocked)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_UNBLOCKED_CLIENT_VIA_CLIENT_UNBLOCK, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (result.IsTypeMismatch)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (!result.Found)
            {
                WriteNull();
                return true;
            }

            // Write array with 2 elements: key and array of member-score pairs
            while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                SendAndReset();

            while (!RespWriteUtils.TryWriteBulkString(result.Key, ref dcurr, dend))
                SendAndReset();

            while (!RespWriteUtils.TryWriteArrayLength(result.Items.Length, ref dcurr, dend))
                SendAndReset();

            for (var i = 0; i < result.Items.Length; i++)
            {
                while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.TryWriteBulkString(result.Items[i], ref dcurr, dend))
                    SendAndReset();
                WriteDoubleNumeric(result.Scores[i]);
            }

            return true;
        }

        /// <summary>
        /// Sets an expiration time for a member in the SortedSet stored at key.
        /// ZEXPIRE key seconds [NX | XX | GT | LT] MEMBERS nummembers member [member ...]
        /// ZPEXPIRE key milliseconds [NX | XX | GT | LT] MEMBERS nummembers member [member ...]
        /// ZEXPIREAT key unix-time-seconds [NX | XX | GT | LT] MEMBERS nummembers member [member ...]
        /// ZPEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT] MEMBERS nummembers member [member ...]
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private unsafe bool SortedSetExpire<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count <= 4)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            var key = parseState.GetArgSliceByRef(0);

            if (!parseState.TryGetLong(1, out var expiration))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (expiration < 0)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_EXPIRE_TIME);
            }

            var currIdx = 2;
            if (parseState.TryGetExpireOption(currIdx, out var expireOption))
            {
                currIdx++; // If expire option is present, move to next argument else continue with the current argument
            }

            var fieldOption = parseState.GetArgSliceByRef(currIdx++);
            if (!fieldOption.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MEMBERS))
            {
                return AbortWithErrorMessage(CmdStrings.GenericErrMandatoryMissing, "MEMBERS");
            }

            if (!parseState.TryGetInt(currIdx++, out var numMembers))
            {
                return AbortWithErrorMessage(CmdStrings.GenericParamShouldBeGreaterThanZero, "numMembers");
            }

            if (parseState.Count != currIdx + numMembers)
            {
                return AbortWithErrorMessage(CmdStrings.GenericErrMustMatchNoOfArgs, "numMembers");
            }

            // Convert to expiration time in ticks
            var expirationTimeInTicks = command switch
            {
                RespCommand.ZEXPIRE => DateTimeOffset.UtcNow.AddSeconds(expiration).UtcTicks,
                RespCommand.ZPEXPIRE => DateTimeOffset.UtcNow.AddMilliseconds(expiration).UtcTicks,
                RespCommand.ZEXPIREAT => ConvertUtils.UnixTimestampInSecondsToTicks(expiration),
                _ => ConvertUtils.UnixTimestampInMillisecondsToTicks(expiration)
            };

            // Encode expiration time and expiration option and pass them into the input object
            var expirationWithOption = new ExpirationWithOption(expirationTimeInTicks, expireOption);

            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref parseState, startIdx: currIdx, 
                    arg1: expirationWithOption.WordHead, arg2: expirationWithOption.WordTail) { SortedSetOp = SortedSetOperation.ZEXPIRE };

            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetExpire(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteArrayLength(numMembers, ref dcurr, dend))
                        SendAndReset();
                    for (var i = 0; i < numMembers; i++)
                    {
                        while (!RespWriteUtils.TryWriteInt32(-2, ref dcurr, dend))
                            SendAndReset();
                    }
                    break;
                default:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
            }

            return true;
        }

        /// <summary>
        /// Returns the time to live (TTL) for the specified members in the SortedSet stored at the given key.
        /// ZTTL key MEMBERS nummembers member [member ...]
        /// ZPTTL key MEMBERS nummembers member [member ...]
        /// ZEXPIRETIME key MEMBERS nummembers member [member ...]
        /// ZPEXPIRETIME key MEMBERS nummembers member [member ...]
        /// </summary>
        /// <typeparam name="TGarnetApi">The type of the storage API.</typeparam>
        /// <param name="command">The RESP command indicating the type of TTL operation.</param>
        /// <param name="storageApi">The storage API instance to interact with the underlying storage.</param>
        /// <returns>True if the operation was successful; otherwise, false.</returns>
        /// <exception cref="GarnetException">Thrown when the object store is disabled.</exception>
        private unsafe bool SortedSetTimeToLive<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count <= 3)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            var key = parseState.GetArgSliceByRef(0);

            var fieldOption = parseState.GetArgSliceByRef(1);
            if (!fieldOption.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MEMBERS))
            {
                return AbortWithErrorMessage(CmdStrings.GenericErrMandatoryMissing, "MEMBERS");
            }

            if (!parseState.TryGetInt(2, out var numMembers))
            {
                return AbortWithErrorMessage(CmdStrings.GenericParamShouldBeGreaterThanZero, "numMembers");
            }

            if (parseState.Count != 3 + numMembers)
            {
                return AbortWithErrorMessage(CmdStrings.GenericErrMustMatchNoOfArgs, "numMembers");
            }

            var isMilliseconds = false;
            var isTimestamp = false;
            switch (command)
            {
                case RespCommand.ZPTTL:
                    isMilliseconds = true;
                    isTimestamp = false;
                    break;
                case RespCommand.ZEXPIRETIME:
                    isMilliseconds = false;
                    isTimestamp = true;
                    break;
                case RespCommand.ZPEXPIRETIME:
                    isMilliseconds = true;
                    isTimestamp = true;
                    break;
                default: // RespCommand.ZTTL
                    break;
            }

            var membersParseState = parseState.Slice(3, numMembers);

            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref membersParseState,
                arg1: isMilliseconds ? 1 : 0, arg2: isTimestamp ? 1 : 0) { SortedSetOp = SortedSetOperation.ZTTL };

            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetTimeToLive(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteArrayLength(numMembers, ref dcurr, dend))
                        SendAndReset();
                    for (var i = 0; i < numMembers; i++)
                    {
                        while (!RespWriteUtils.TryWriteInt32(-2, ref dcurr, dend))
                            SendAndReset();
                    }
                    break;
                default:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
            }

            return true;
        }

        /// <summary>
        /// Removes the expiration time from the specified members in the sorted set stored at the given key.
        /// ZPERSIST key MEMBERS nummembers member [member ...]
        /// </summary>
        /// <typeparam name="TGarnetApi">The type of the storage API.</typeparam>
        /// <param name="storageApi">The storage API instance to interact with the underlying storage.</param>
        /// <returns>True if the operation was successful; otherwise, false.</returns>
        /// <exception cref="GarnetException">Thrown when the object store is disabled.</exception>
        private unsafe bool SortedSetPersist<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count <= 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ZPERSIST));
            }

            var key = parseState.GetArgSliceByRef(0);

            var fieldOption = parseState.GetArgSliceByRef(1);
            if (!fieldOption.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MEMBERS))
            {
                return AbortWithErrorMessage(CmdStrings.GenericErrMandatoryMissing, "MEMBERS");
            }

            if (!parseState.TryGetInt(2, out var numMembers))
            {
                return AbortWithErrorMessage(CmdStrings.GenericParamShouldBeGreaterThanZero, "numMembers");
            }

            if (parseState.Count != 3 + numMembers)
            {
                return AbortWithErrorMessage(CmdStrings.GenericErrMustMatchNoOfArgs, "numMembers");
            }

            var membersParseState = parseState.Slice(3, numMembers);

            var input = new ObjectInput(GarnetObjectType.SortedSet, metaCommand, ref membersParseState) { SortedSetOp = SortedSetOperation.ZPERSIST };

            var output = ObjectOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.SortedSetPersist(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteArrayLength(numMembers, ref dcurr, dend))
                        SendAndReset();
                    for (var i = 0; i < numMembers; i++)
                    {
                        while (!RespWriteUtils.TryWriteInt32(-2, ref dcurr, dend))
                            SendAndReset();
                    }
                    break;
                default:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
            }

            return true;
        }

        private bool ValidateSortedSetRangeInput(int startIdx, SortedSetRangeOptions options, out ReadOnlySpan<byte> error)
        {
            error = default;
            if ((options & SortedSetRangeOptions.ByScore) == SortedSetRangeOptions.ByScore)
            {
                if (!parseState.TryGetSortedSetMinMaxParameter(startIdx, out _, out _) ||
                    !parseState.TryGetSortedSetMinMaxParameter(startIdx + 1, out _, out _))
                {
                    error = CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT;
                    return false;
                }
            }
            else if ((options & SortedSetRangeOptions.ByLex) == SortedSetRangeOptions.ByLex)
            {
                if (!parseState.TryGetSortedSetLexMinMaxParameter(startIdx, out _, out _, out _) ||
                    !parseState.TryGetSortedSetLexMinMaxParameter(startIdx + 1, out _, out _, out _))
                {
                    error = CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING;
                    return false;
                }
            }
            else
            {
                if (!parseState.TryGetInt(startIdx, out _) ||
                    !parseState.TryGetInt(startIdx + 1, out _))
                {
                    error = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                    return false;
                }
            }

            return true;
        }
    }
}