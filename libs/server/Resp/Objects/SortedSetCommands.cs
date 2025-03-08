﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
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
            {
                return AbortWithWrongNumberOfArguments("ZADD");
            }

            // Get the key for SortedSet
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZADD };
            var input = new ObjectInput(header, ref parseState, startIdx: 1);

            var outputFooter = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetAdd(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
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
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZREM };
            var input = new ObjectInput(header, ref parseState, startIdx: 1);

            var status = storageApi.SortedSetRemove(keyBytes, ref input, out var rmwOutput);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.TryWriteInt32(rmwOutput.result1, ref dcurr, dend))
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
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZCARD };
            var input = new ObjectInput(header);

            var status = storageApi.SortedSetLength(keyBytes, ref input, out var output);

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
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ZRANGE));
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var rangeOpts = SortedSetRangeOpts.None;

            switch (command)
            {
                case RespCommand.ZRANGE:
                    break;
                case RespCommand.ZREVRANGE:
                    rangeOpts = SortedSetRangeOpts.Reverse;
                    break;
                case RespCommand.ZRANGEBYLEX:
                    rangeOpts = SortedSetRangeOpts.ByLex;
                    break;
                case RespCommand.ZRANGEBYSCORE:
                    rangeOpts = SortedSetRangeOpts.ByScore;
                    break;
                case RespCommand.ZREVRANGEBYLEX:
                    rangeOpts = SortedSetRangeOpts.ByLex | SortedSetRangeOpts.Reverse;
                    break;
                case RespCommand.ZREVRANGEBYSCORE:
                    rangeOpts = SortedSetRangeOpts.ByScore | SortedSetRangeOpts.Reverse;
                    break;
                case RespCommand.ZRANGESTORE:
                    rangeOpts = SortedSetRangeOpts.Store;
                    break;
            }

            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZRANGE };
            var input = new ObjectInput(header, ref parseState, startIdx: 1, arg1: respProtocolVersion, arg2: (int)rangeOpts);

            var outputFooter = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetRange(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
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

            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZRANGE };
            var input = new ObjectInput(header, ref parseState, startIdx: 2, arg1: respProtocolVersion, arg2: (int)SortedSetRangeOpts.Store);

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
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZSCORE };
            var input = new ObjectInput(header, ref parseState, startIdx: 1, arg1: respProtocolVersion);

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetScore(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
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
        private unsafe bool SortedSetScores<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //validation if minimum args
            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments("ZMSCORE");
            }

            // Get the key for SortedSet
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZMSCORE };
            var input = new ObjectInput(header, ref parseState, startIdx: 1);

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetScores(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteArrayWithNullElements(parseState.Count - 1, ref dcurr, dend))
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
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var popCount = 1;

            if (parseState.Count == 2)
            {
                // Read count
                if (!parseState.TryGetInt(1, out popCount))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE, ref dcurr, dend))
                        SendAndReset();

                    return true;
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
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = op };
            var input = new ObjectInput(header, popCount);

            // Prepare output
            var outputFooter = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(SpanByte.FromPinnedPointer(dcurr, (int)(dend - dcurr))) };

            var status = storageApi.SortedSetPop(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
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
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericParamShouldBeGreaterThanZero, "numkeys")));
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
                    return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericParamShouldBeGreaterThanZero, "count")));
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
                        while (!RespWriteUtils.TryWriteNull(ref dcurr, dend))
                            SendAndReset();
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
                            while (!RespWriteUtils.TryWriteBulkString(score.ReadOnlySpan, ref dcurr, dend))
                                SendAndReset();
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
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZCOUNT };
            var input = new ObjectInput(header, ref parseState, startIdx: 1);

            // Prepare output
            var outputFooter = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(SpanByte.FromPinnedPointer(dcurr, (int)(dend - dcurr))) };

            var status = storageApi.SortedSetCount(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
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
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var op =
                command switch
                {
                    RespCommand.ZREMRANGEBYLEX => SortedSetOperation.ZREMRANGEBYLEX,
                    RespCommand.ZLEXCOUNT => SortedSetOperation.ZLEXCOUNT,
                    _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                };

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = op };
            var input = new ObjectInput(header, ref parseState, startIdx: 1);

            var status = op == SortedSetOperation.ZREMRANGEBYLEX ?
                storageApi.SortedSetRemoveRangeByLex(keyBytes, ref input, out var output) :
                storageApi.SortedSetLengthByValue(keyBytes, ref input, out output);

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
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZINCRBY };
            var input = new ObjectInput(header, ref parseState, startIdx: 1);

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetIncrement(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.NOTFOUND:
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
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
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();
            var includeWithScore = false;

            // Read WITHSCORE
            if (parseState.Count == 3)
            {
                var withScoreSlice = parseState.GetArgSliceByRef(2);

                if (!withScoreSlice.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHSCORE))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                        SendAndReset();

                    return true;
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
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = op };
            var input = new ObjectInput(header, ref parseState, startIdx: 1, arg1: includeWithScore ? 1 : 0);

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetRank(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
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
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            var op =
                command switch
                {
                    RespCommand.ZREMRANGEBYRANK => SortedSetOperation.ZREMRANGEBYRANK,
                    RespCommand.ZREMRANGEBYSCORE => SortedSetOperation.ZREMRANGEBYSCORE,
                    _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                };

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = op };
            var input = new ObjectInput(header, ref parseState, startIdx: 1);

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetRemoveRange(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
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
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();
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
                        while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                            SendAndReset();

                        return true;
                    }

                    includeWithScores = true;
                }
            }

            // Create a random seed
            var seed = Random.Shared.Next();

            // Prepare input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZRANDMEMBER };
            var inputArg = (((paramCount << 1) | (includedCount ? 1 : 0)) << 1) | (includeWithScores ? 1 : 0);
            var input = new ObjectInput(header, inputArg, seed);

            var status = GarnetStatus.NOTFOUND;
            GarnetObjectStoreOutput outputFooter = default;

            // This prevents going to the backend if ZRANDMEMBER is called with a count of 0
            if (paramCount != 0)
            {
                // Prepare GarnetObjectStore output
                outputFooter = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
                status = storageApi.SortedSetRandomMember(keyBytes, ref input, ref outputFooter);
            }

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.SpanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    var respBytes = includedCount ? CmdStrings.RESP_EMPTYLIST : CmdStrings.RESP_ERRNOTFOUND;
                    while (!RespWriteUtils.TryWriteDirect(respBytes, ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (parseState.Count - 1 != nKeys && parseState.Count - 1 != nKeys + 1)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            var includeWithScores = false;

            // Read all the keys
            if (parseState.Count <= 2)
            {
                //return empty array
                while (!RespWriteUtils.TryWriteArrayLength(0, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            var keys = new ArgSlice[nKeys];

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
                    // write the size of the array reply
                    var resultCount = result?.Count ?? 0;
                    while (!RespWriteUtils.TryWriteArrayLength(includeWithScores ? resultCount * 2 : resultCount, ref dcurr, dend))
                        SendAndReset();

                    if (result != null)
                    {
                        foreach (var (element, score) in result)
                        {
                            while (!RespWriteUtils.TryWriteBulkString(element, ref dcurr, dend))
                                SendAndReset();

                            if (includeWithScores)
                            {
                                while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref dcurr, dend))
                                    SendAndReset();
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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (parseState.Count - 2 != nKeys)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                    SendAndReset();
                return true;
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
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrAtLeastOneKey, nameof(RespCommand.ZINTER))));
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
                            return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrNotAFloat, "weight")));
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
                    while (!RespWriteUtils.TryWriteArrayLength(includeWithScores ? result.Count * 2 : result.Count, ref dcurr, dend))
                        SendAndReset();

                    foreach (var (element, score) in result)
                    {
                        while (!RespWriteUtils.TryWriteBulkString(element, ref dcurr, dend))
                            SendAndReset();

                        if (includeWithScores)
                        {
                            while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref dcurr, dend))
                                SendAndReset();
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
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrAtLeastOneKey, nameof(RespCommand.ZINTERCARD))));
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
                    return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrCantBeNegative, "LIMIT")));
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
                            return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrNotAFloat, "weight")));
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
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrAtLeastOneKey, nameof(RespCommand.ZUNION))));
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
                            return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrNotAFloat, "weight")));
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
                    while (!RespWriteUtils.TryWriteArrayLength(includeWithScores ? result.Count * 2 : result.Count, ref dcurr, dend))
                        SendAndReset();

                    foreach (var (element, score) in result)
                    {
                        while (!RespWriteUtils.TryWriteBulkString(element, ref dcurr, dend))
                            SendAndReset();

                        if (includeWithScores)
                        {
                            while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref dcurr, dend))
                                SendAndReset();
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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (nKeys < 1)
            {
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrAtLeastOneKey, nameof(RespCommand.ZUNIONSTORE))));
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
                            return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrNotAFloat, "weight")));
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
            if (storeWrapper.itemBroker == null)
                throw new GarnetException("Object store is disabled");

            if (parseState.Count < 2)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            if (!parseState.TryGetDouble(parseState.Count - 1, out var timeout))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
            }

            var keysBytes = new byte[parseState.Count - 1][];

            for (var i = 0; i < keysBytes.Length; i++)
            {
                keysBytes[i] = parseState.GetArgSliceByRef(i).SpanByte.ToByteArray();
            }

            var result = storeWrapper.itemBroker.GetCollectionItemAsync(command, keysBytes, this, timeout).Result;

            if (!result.Found)
            {
                while (!RespWriteUtils.TryWriteNull(ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteBulkString(result.Key, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteBulkString(result.Item, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteDoubleBulkString(result.Score, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// BZMPOP timeout numkeys key [key ...] &lt;MIN | MAX&gt; [COUNT count]
        /// </summary>
        private unsafe bool SortedSetBlockingMPop()
        {
            if (storeWrapper.itemBroker == null)
                throw new GarnetException("Object store is disabled");

            if (parseState.Count < 4)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BZMPOP));
            }

            var currTokenId = 0;

            // Read timeout
            if (!parseState.TryGetDouble(currTokenId++, out var timeout))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
            }

            // Read count of keys
            if (!parseState.TryGetInt(currTokenId++, out var numKeys))
            {
                var err = string.Format(CmdStrings.GenericParamShouldBeGreaterThanZero, "numkeys");
                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(err));
            }

            // Should have MAX|MIN or it should contain COUNT + value
            if (parseState.Count != numKeys + 3 && parseState.Count != numKeys + 5)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }

            var keysBytes = new byte[numKeys][];
            for (var i = 0; i < keysBytes.Length; i++)
            {
                keysBytes[i] = parseState.GetArgSliceByRef(currTokenId++).SpanByte.ToByteArray();
            }

            var cmdArgs = new ArgSlice[2];

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

            cmdArgs[0] = new ArgSlice((byte*)&lowScoresFirst, 1);

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
                    var err = string.Format(CmdStrings.GenericParamShouldBeGreaterThanZero, "count");
                    return AbortWithErrorMessage(Encoding.ASCII.GetBytes(err));
                }
            }

            cmdArgs[1] = new ArgSlice((byte*)&popCount, sizeof(int));

            var result = storeWrapper.itemBroker.GetCollectionItemAsync(RespCommand.BZMPOP, keysBytes, this, timeout, cmdArgs).Result;

            if (!result.Found)
            {
                while (!RespWriteUtils.TryWriteNull(ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Write array with 2 elements: key and array of member-score pairs
            while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                SendAndReset();

            while (!RespWriteUtils.TryWriteBulkString(result.Key, ref dcurr, dend))
                SendAndReset();

            while (!RespWriteUtils.TryWriteArrayLength(result.Items.Length, ref dcurr, dend))
                SendAndReset();

            for (var i = 0; i < result.Items.Length; i += 2)
            {
                while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.TryWriteBulkString(result.Items[i], ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.TryWriteDoubleBulkString(result.Scores[i], ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }
    }
}