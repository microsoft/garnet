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
            {
                return AbortWithWrongNumberOfArguments("ZADD");
            }

            if (parseState.Count % 2 != 1)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }

            // Get the key for SortedSet
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            if (NetworkSingleKeySlotVerify(keyBytes, false))
            {
                return true;
            }

            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZADD,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetAdd(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
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

            if (NetworkSingleKeySlotVerify(keyBytes, false))
            {
                return true;
            }

            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZREM,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            var status = storageApi.SortedSetRemove(keyBytes, ref input, out var rmwOutput);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.WriteInteger(rmwOutput.result1, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZCARD,
                },
            };

            var status = storageApi.SortedSetLength(keyBytes, ref input, out var output);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    while (!RespWriteUtils.WriteInteger(output.result1, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            var op =
                command switch
                {
                    RespCommand.ZRANGE => SortedSetOperation.ZRANGE,
                    RespCommand.ZREVRANGE => SortedSetOperation.ZREVRANGE,
                    RespCommand.ZRANGEBYSCORE => SortedSetOperation.ZRANGEBYSCORE,
                    RespCommand.ZREVRANGEBYSCORE => SortedSetOperation.ZREVRANGEBYSCORE,
                    _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                };

            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = op,
                },
                arg1 = respProtocolVersion,
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetRange(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZSCORE,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetScore(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZMSCORE,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetScores(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteArrayWithNullElements(parseState.Count - 1, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            if (NetworkSingleKeySlotVerify(keyBytes, false))
            {
                return true;
            }

            var popCount = 1;

            if (parseState.Count == 2)
            {
                // Read count
                if (!parseState.TryGetInt(1, out popCount))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE, ref dcurr, dend))
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
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = op,
                },
                arg1 = popCount,
            };

            // Prepare output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(SpanByte.FromPinnedPointer(dcurr, (int)(dend - dcurr))) };

            var status = storageApi.SortedSetPop(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZCOUNT,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            // Prepare output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(SpanByte.FromPinnedPointer(dcurr, (int)(dend - dcurr))) };

            var status = storageApi.SortedSetCount(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            if (NetworkSingleKeySlotVerify(keyBytes, command != RespCommand.ZREMRANGEBYLEX))
            {
                return true;
            }

            var op =
                command switch
                {
                    RespCommand.ZREMRANGEBYLEX => SortedSetOperation.ZREMRANGEBYLEX,
                    RespCommand.ZLEXCOUNT => SortedSetOperation.ZLEXCOUNT,
                    _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                };

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = op,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

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
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (output.result1 == int.MinValue)  // command partially executed
                        return false;
                    else
                        while (!RespWriteUtils.WriteInteger(output.result1, ref dcurr, dend))
                            SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            if (NetworkSingleKeySlotVerify(keyBytes, false))
            {
                return true;
            }

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZINCRBY,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetIncrement(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.NOTFOUND:
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            var includeWithScore = false;

            // Read WITHSCORE
            if (parseState.Count == 3)
            {
                var withScoreSlice = parseState.GetArgSliceByRef(2);

                if (!withScoreSlice.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHSCORE))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
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
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = op,
                },
                arg1 = includeWithScore ? 1 : 0,
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetRank(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            if (NetworkSingleKeySlotVerify(keyBytes, false))
            {
                return true;
            }

            var op =
                command switch
                {
                    RespCommand.ZREMRANGEBYRANK => SortedSetOperation.ZREMRANGEBYRANK,
                    RespCommand.ZREMRANGEBYSCORE => SortedSetOperation.ZREMRANGEBYSCORE,
                    _ => throw new Exception($"Unexpected {nameof(SortedSetOperation)}: {command}")
                };

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = op,
                },
                parseState = parseState,
                parseStateStartIdx = 1,
            };

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            var status = storageApi.SortedSetRemoveRange(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            if (NetworkSingleKeySlotVerify(keyBytes, true))
            {
                return true;
            }

            var paramCount = 1;
            var includeWithScores = false;
            var includedCount = false;

            if (parseState.Count >= 2)
            {
                // Read count
                if (!parseState.TryGetInt(1, out paramCount))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
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
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                            SendAndReset();

                        return true;
                    }

                    includeWithScores = true;
                }
            }

            // Create a random seed
            var seed = Random.Shared.Next();

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZRANDMEMBER,
                },
                arg1 = (((paramCount << 1) | (includedCount ? 1 : 0)) << 1) | (includeWithScores ? 1 : 0),
                arg2 = seed,
            };

            var status = GarnetStatus.NOTFOUND;
            GarnetObjectStoreOutput outputFooter = default;

            // This prevents going to the backend if ZRANDMEMBER is called with a count of 0
            if (paramCount != 0)
            {
                // Prepare GarnetObjectStore output
                outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
                status = storageApi.SortedSetRandomMember(keyBytes, ref input, ref outputFooter);
            }

            switch (status)
            {
                case GarnetStatus.OK:
                    ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    break;
                case GarnetStatus.NOTFOUND:
                    var respBytes = includedCount ? CmdStrings.RESP_EMPTYLIST : CmdStrings.RESP_ERRNOTFOUND;
                    while (!RespWriteUtils.WriteDirect(respBytes, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
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

            //number of keys
            if (!parseState.TryGetInt(0, out var nKeys))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (parseState.Count - 1 != nKeys && parseState.Count - 1 != nKeys + 1)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            var includeWithScores = false;

            // Read all the keys
            if (parseState.Count <= 2)
            {
                //return empty array
                while (!RespWriteUtils.WriteArrayLength(0, ref dcurr, dend))
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

                if (!withScores.SequenceEqual(CmdStrings.WITHSCORES))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                includeWithScores = true;
            }

            var status = storageApi.SortedSetDifference(keys, out var result);

            switch (status)
            {
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    // write the size of the array reply
                    var resultCount = result?.Count ?? 0;
                    while (!RespWriteUtils.WriteArrayLength(includeWithScores ? resultCount * 2 : resultCount, ref dcurr, dend))
                        SendAndReset();

                    if (result != null)
                    {
                        foreach (var (element, score) in result)
                        {
                            while (!RespWriteUtils.WriteBulkString(element, ref dcurr, dend))
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
    }
}