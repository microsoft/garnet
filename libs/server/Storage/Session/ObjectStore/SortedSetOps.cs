// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    sealed partial class StorageSession : IDisposable
    {

        /// <summary>
        /// Adds the specified member and score to the sorted set stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="score"></param>
        /// <param name="member"></param>
        /// <param name="zaddCount"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetAdd<TObjectContext>(ArgSlice key, ArgSlice score, ArgSlice member, out int zaddCount, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            zaddCount = 0;
            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, score, member);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZADD,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref outputFooter);

            if (status == GarnetStatus.OK)
            {
                zaddCount = TryProcessRespSimple64IntOutput(outputFooter, out var value) ? (int)value : default;
            }

            return status;
        }

        /// <summary>
        /// Adds all the specified members with the specified scores to the sorted set stored at key.
        /// Current members get the score updated and reordered.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="inputs"></param>
        /// <param name="zaddCount"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetAdd<TObjectContext>(ArgSlice key, (ArgSlice score, ArgSlice member)[] inputs, out int zaddCount, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            zaddCount = 0;

            if (inputs.Length == 0 || key.Length == 0)
                return GarnetStatus.OK;

            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.Initialize(ref parseStateBuffer, inputs.Length * 2);

            for (var i = 0; i < inputs.Length; i++)
            {
                parseStateBuffer[2 * i] = inputs[i].score;
                parseStateBuffer[(2 * i) + 1] = inputs[i].member;
            }

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZADD,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref outputFooter);

            if (status == GarnetStatus.OK)
            {
                zaddCount = TryProcessRespSimple64IntOutput(outputFooter, out var value) ? (int)value : default;
            }

            return status;
        }

        /// <summary>
        /// Removes the specified member from the sorted set stored at key.
        /// Non existing members are ignored.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="member"></param>
        /// <param name="zremCount"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetRemove<TObjectContext>(byte[] key, ArgSlice member, out int zremCount,
            ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long,
                ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            zremCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;

            parseState.InitializeWithArguments(ref parseStateBuffer, member);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZREM,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var status = RMWObjectStoreOperation(key, ref input, out var output, ref objectStoreContext);

            zremCount = output.result1;
            return status;
        }

        /// <summary>
        /// Removes the specified members from the sorted set stored at key.
        /// Non existing members are ignored.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <param name="zremCount"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetRemove<TObjectContext>(byte[] key, ArgSlice[] members, out int zremCount, ref TObjectContext objectStoreContext)
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            zremCount = 0;

            if (key.Length == 0 || members.Length == 0)
                return GarnetStatus.OK;

            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.Initialize(ref parseStateBuffer, members.Length);

            for (var i = 0; i < members.Length; i++)
            {
                parseStateBuffer[i] = members[i];
            }

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZREM,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var status = RMWObjectStoreOperation(key, ref input, out var output, ref objectStoreContext);

            zremCount = output.result1;
            return status;
        }

        /// <summary>
        /// Removes all elements in the range specified by min and max, having the same score.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="min"></param>
        /// <param name="max"></param>
        /// <param name="countRemoved"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetRemoveRangeByLex<TObjectContext>(ArgSlice key, string min, string max, out int countRemoved, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            countRemoved = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var minBytes = Encoding.ASCII.GetBytes(min);
            var maxBytes = Encoding.ASCII.GetBytes(max);

            GarnetStatus status;
            fixed (byte* ptr = minBytes)
            {
                fixed (byte* ptr2 = maxBytes)
                {
                    // Prepare the parse state
                    var minArgSlice = new ArgSlice(ptr, minBytes.Length);
                    var maxArgSlice = new ArgSlice(ptr2, maxBytes.Length);

                    var parseState = new SessionParseState();
                    ArgSlice[] parseStateBuffer = default;
                    parseState.InitializeWithArguments(ref parseStateBuffer, minArgSlice, maxArgSlice);

                    // Prepare the input
                    var input = new ObjectInput
                    {
                        header = new RespInputHeader
                        {
                            type = GarnetObjectType.SortedSet,
                            SortedSetOp = SortedSetOperation.ZREMRANGEBYLEX,
                        },
                        parseState = parseState,
                        parseStateStartIdx = 0,
                    };

                    status = RMWObjectStoreOperation(key.ToArray(), ref input, out var output, ref objectStoreContext);
                    countRemoved = output.result1;
                }
            }

            return status;
        }

        /// <summary>
        /// Removes all elements that have a score in the range specified by min and max.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="min"></param>
        /// <param name="max"></param>
        /// <param name="countRemoved"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetRemoveRangeByScore<TObjectContext>(ArgSlice key, string min, string max, out int countRemoved, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            countRemoved = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var minBytes = Encoding.ASCII.GetBytes(min);
            var maxBytes = Encoding.ASCII.GetBytes(max);

            GarnetStatus status;
            fixed (byte* ptr = minBytes)
            {
                fixed (byte* ptr2 = maxBytes)
                {
                    // Prepare the parse state
                    var minArgSlice = new ArgSlice(ptr, minBytes.Length);
                    var maxArgSlice = new ArgSlice(ptr2, maxBytes.Length);

                    var parseState = new SessionParseState();
                    ArgSlice[] parseStateBuffer = default;
                    parseState.InitializeWithArguments(ref parseStateBuffer, minArgSlice, maxArgSlice);

                    // Prepare the input
                    var input = new ObjectInput
                    {
                        header = new RespInputHeader
                        {
                            type = GarnetObjectType.SortedSet,
                            SortedSetOp = SortedSetOperation.ZREMRANGEBYSCORE,
                        },
                        parseState = parseState,
                        parseStateStartIdx = 0,
                    };

                    var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

                    status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref outputFooter);

                    if (status == GarnetStatus.OK)
                    {
                        countRemoved = TryProcessRespSimple64IntOutput(outputFooter, out var value) ? (int)value : default;
                    }
                }
            }

            return status;
        }

        /// <summary>
        /// Removes all elements with the index in the range specified by start and stop.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="start"></param>
        /// <param name="stop"></param>
        /// <param name="countRemoved"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetRemoveRangeByRank<TObjectContext>(ArgSlice key, int start, int stop, out int countRemoved, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            countRemoved = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var startBytes = Encoding.ASCII.GetBytes(start.ToString());
            var stopBytes = Encoding.ASCII.GetBytes(stop.ToString());

            GarnetStatus status;
            fixed (byte* ptr = startBytes)
            {
                fixed (byte* ptr2 = stopBytes)
                {
                    // Prepare the parse state
                    var startArgSlice = new ArgSlice(ptr, startBytes.Length);
                    var stopArgSlice = new ArgSlice(ptr2, stopBytes.Length);

                    var parseState = new SessionParseState();
                    ArgSlice[] parseStateBuffer = default;
                    parseState.InitializeWithArguments(ref parseStateBuffer, startArgSlice, stopArgSlice);

                    // Prepare the input
                    var input = new ObjectInput
                    {
                        header = new RespInputHeader
                        {
                            type = GarnetObjectType.SortedSet,
                            SortedSetOp = SortedSetOperation.ZREMRANGEBYRANK,
                        },
                        parseState = parseState,
                        parseStateStartIdx = 0,
                    };

                    var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

                    status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref outputFooter);

                    if (status == GarnetStatus.OK)
                    {
                        countRemoved = TryProcessRespSimple64IntOutput(outputFooter, out var value) ? (int)value : default;
                    }
                }
            }

            return status;
        }

        /// <summary>
        /// Removes and returns up to count members with the highest or lowest scores in the sorted set stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <param name="lowScoresFirst">When true return the lowest scores, otherwise the highest.</param>
        /// <param name="pairs"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetPop<TObjectContext>(ArgSlice key, int count, bool lowScoresFirst, out (ArgSlice member, ArgSlice score)[] pairs, ref TObjectContext objectStoreContext)
                where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            pairs = default;
            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = lowScoresFirst ? SortedSetOperation.ZPOPMIN : SortedSetOperation.ZPOPMAX,
                },
                arg1 = count,
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref outputFooter);

            //process output
            if (status == GarnetStatus.OK)
                pairs = ProcessRespArrayOutputAsPairs(outputFooter, out _);

            return status;
        }

        /// <summary>
        /// Increments the score of member in the sorted set stored at key by increment.
        /// Returns the new score of member.
        /// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="increment"></param>
        /// <param name="member"></param>
        /// <param name="newScore"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetIncrement<TObjectContext>(ArgSlice key, double increment, ArgSlice member, out double newScore, ref TObjectContext objectStoreContext)
                where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            newScore = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var incrementBytes = Encoding.ASCII.GetBytes(increment.ToString(CultureInfo.InvariantCulture));

            GarnetStatus status;
            fixed (byte* ptr = incrementBytes)
            {
                // Prepare the parse state
                var incrementArgSlice = new ArgSlice(ptr, incrementBytes.Length);

                var parseState = new SessionParseState();
                ArgSlice[] parseStateBuffer = default;
                parseState.InitializeWithArguments(ref parseStateBuffer, incrementArgSlice, member);

                // Prepare the input
                var input = new ObjectInput
                {
                    header = new RespInputHeader
                    {
                        type = GarnetObjectType.SortedSet,
                        SortedSetOp = SortedSetOperation.ZINCRBY,
                    },
                    parseState = parseState,
                    parseStateStartIdx = 0,
                };

                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
                status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref outputFooter);

                // Process output
                if (status == GarnetStatus.OK)
                {
                    var result = ProcessRespArrayOutput(outputFooter, out var error);
                    if (error == default)
                    {
                        // get the new score
                        _ = NumUtils.TryParse(result[0].ReadOnlySpan, out newScore);
                    }
                }
            }

            return status;
        }

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="zcardCount"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetLength<TObjectContext>(ArgSlice key, out int zcardCount, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            zcardCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = SortedSetOperation.ZCARD,
                },
            };

            var status = ReadObjectStoreOperation(key.ToArray(), ref input, out var output, ref objectStoreContext);

            zcardCount = output.result1;
            return status;
        }

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key, using byscore, bylex and rev modifiers.
        /// Min and max are range boundaries, where 0 is the first element, 1 is the next element and so on.
        /// There can also be negative numbers indicating offsets from the end of the sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and so on.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="min"></param>
        /// <param name="max"></param>
        /// <param name="sortedSetOrderOperation"></param>
        /// <param name="objectContext"></param>
        /// <param name="elements"></param>
        /// <param name="error"></param>
        /// <param name="withScores"></param>
        /// <param name="reverse"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetRange<TObjectContext>(ArgSlice key, ArgSlice min, ArgSlice max, SortedSetOrderOperation sortedSetOrderOperation, ref TObjectContext objectContext, out ArgSlice[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            elements = default;
            error = default;

            //min and max are required
            if (min.Length == 0 || max.Length == 0)
            {
                //error in arguments
                error = "Missing required min and max parameters";
                return GarnetStatus.NOTFOUND;
            }

            ReadOnlySpan<byte> operation = default;
            var sortedOperation = SortedSetOperation.ZRANGE;
            switch (sortedSetOrderOperation)
            {
                case SortedSetOrderOperation.ByScore:
                    sortedOperation = SortedSetOperation.ZRANGEBYSCORE;
                    operation = "BYSCORE"u8;
                    break;
                case SortedSetOrderOperation.ByLex:
                    sortedOperation = SortedSetOperation.ZRANGE;
                    operation = "BYLEX"u8;
                    break;
                case SortedSetOrderOperation.ByRank:
                    if (reverse)
                        sortedOperation = SortedSetOperation.ZREVRANGE;
                    operation = default;
                    break;
            }

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;

            var arguments = new List<ArgSlice> { min, max };

            // Operation order
            if (!operation.IsEmpty)
            {
                fixed (byte* ptrOp = operation)
                {
                    arguments.Add(new ArgSlice(ptrOp, operation.Length));
                }
            }

            // Reverse
            if (sortedOperation != SortedSetOperation.ZREVRANGE && reverse)
            {
                var reverseBytes = "REV"u8;
                fixed (byte* ptrOp = reverseBytes)
                {
                    arguments.Add(new ArgSlice(ptrOp, reverseBytes.Length));
                }
            }

            // Limit parameter
            if (limit != default && (sortedSetOrderOperation == SortedSetOrderOperation.ByScore || sortedSetOrderOperation == SortedSetOrderOperation.ByLex))
            {
                var limitBytes = "LIMIT"u8;
                fixed (byte* ptrOp = limitBytes)
                {
                    arguments.Add(new ArgSlice(ptrOp, limitBytes.Length));
                }

                // Offset
                var limitOffset = Encoding.ASCII.GetBytes(limit.Item1);
                fixed (byte* ptrOp = limitOffset)
                {
                    arguments.Add(new ArgSlice(ptrOp, limitOffset.Length));
                }

                // Count
                var limitCountLength = NumUtils.NumDigitsInLong(limit.Item2);
                var limitCountBytes = new byte[limitCountLength];
                fixed (byte* ptrCount = limitCountBytes)
                {
                    var ptr = ptrCount;
                    NumUtils.IntToBytes(limit.Item2, limitCountLength, ref ptr);
                    arguments.Add(new ArgSlice(ptrCount, limitCountLength));
                }
            }

            parseState.InitializeWithArguments(ref parseStateBuffer, [.. arguments]);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = sortedOperation,
                },
                arg1 = 2, // Default RESP server protocol version
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectContext, ref outputFooter);

            if (status == GarnetStatus.OK)
                elements = ProcessRespArrayOutput(outputFooter, out error);

            return status;
        }


        /// <summary>
        /// Computes the difference between the first and all successive sorted sets and returns resulting pairs.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="pairs"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetDifference(ArgSlice[] keys, out Dictionary<byte[], double> pairs)
        {
            pairs = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
                txnManager.Run(true);
            }

            var objectContext = txnManager.ObjectStoreLockableContext;

            try
            {
                var statusOp = GET(keys[0].ToArray(), out var firstObj, ref objectContext);
                if (statusOp == GarnetStatus.OK)
                {
                    if (firstObj.garnetObject is not SortedSetObject firstSortedSet)
                    {
                        return GarnetStatus.WRONGTYPE;
                    }
                    // read the rest of the keys
                    for (var item = 1; item < keys.Length; item++)
                    {
                        statusOp = GET(keys[item].ToArray(), out var nextObj, ref objectContext);
                        if (statusOp != GarnetStatus.OK)
                            continue;

                        if (nextObj.garnetObject is not SortedSetObject nextSortedSet)
                        {
                            pairs = default;
                            return GarnetStatus.WRONGTYPE;
                        }

                        if (pairs == default)
                            pairs = SortedSetObject.CopyDiff(firstSortedSet.Dictionary, nextSortedSet.Dictionary);
                        else
                            SortedSetObject.InPlaceDiff(pairs, nextSortedSet.Dictionary);
                    }
                }
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Returns the rank of member in the sorted set, the scores in the sorted set are ordered from high to low
        /// <param name="key">The key of the sorted set</param>
        /// <param name="member">The member to get the rank</param>
        /// <param name="reverse">If true, the rank is calculated from low to high</param>
        /// <param name="rank">The rank of the member (null if the member does not exist)</param>
        /// <param name="objectStoreContext"></param>
        /// </summary>
        public unsafe GarnetStatus SortedSetRank<TObjectContext>(ArgSlice key, ArgSlice member, bool reverse, out long? rank, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            rank = null;
            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, member);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.SortedSet,
                    SortedSetOp = reverse ? SortedSetOperation.ZREVRANK : SortedSetOperation.ZRANK,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            const int outputContainerSize = 32; // 3 for HEADER + CRLF + 20 for ascii long
            var outputContainer = stackalloc byte[outputContainerSize];
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(outputContainer, outputContainerSize) };

            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref outputFooter);

            if (status == GarnetStatus.OK)
            {
                Debug.Assert(*outputContainer == (byte)'$' || *outputContainer == (byte)':');
                if (*outputContainer == (byte)':')
                {
                    // member exists -> read the rank
                    var read = TryProcessRespSimple64IntOutput(outputFooter, out var value);
                    var rankValue = read ? (int)value : default;
                    Debug.Assert(read);
                    rank = rankValue;
                }
            }

            return status;
        }

        /// <summary>
        /// Adds all the specified members with the specified scores to the sorted set stored at key.
        /// Current members get the score updated and reordered.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetAdd<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        => RMWObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref output);

        /// <summary>
        /// Removes the specified members from the sorted set stored at key.
        /// Non existing members are ignored.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRemove<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperation(key, ref input, out output, ref objectStoreContext);

        /// <summary>
        /// Returns the number of members of the sorted set.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetLength<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperation(key, ref input, out output, ref objectStoreContext);

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key.
        /// Both start and stop are zero-based indexes, where 0 is the first element, 1 is the next element and so on.
        /// There can also be negative numbers indicating offsets from the end of the sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and so on.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRange<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref outputFooter);

        /// <summary>
        /// Returns the score of member in the sorted set at key.
        /// If member does not exist in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetScore<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref outputFooter);

        /// <summary>
        /// Returns the scores of members in the sorted set at key.
        /// For every member that does not exist in the sorted set, or if the key does not exist, nil is returned.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetScores<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref outputFooter);

        /// <summary>
        /// Removes and returns the first element from the sorted set stored at key,
        /// with the scores ordered from low to high (min) or high to low (max).
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetPop<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref outputFooter);

        /// <summary>
        /// Returns the number of elements in the sorted set at key with a score between min and max.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetCount<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref outputFooter);

        /// <summary>
        /// Removes all elements in the sorted set between the
        /// lexicographical range specified by min and max.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRemoveRangeByLex<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperation(key, ref input, out output, ref objectContext);

        /// <summary>
        /// Returns the number of elements in the sorted set with a value between min and max.
        /// When all the elements in a sorted set have the same score,
        /// this command forces lexicographical ordering.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetLengthByValue<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperation(key, ref input, out output, ref objectStoreContext);

        /// <summary>
        /// Increments the score of member in the sorted set stored at key by increment.
        /// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetIncrement<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref outputFooter);

        /// <summary>
        /// ZREMRANGEBYRANK: Removes all elements in the sorted set stored at key with rank between start and stop.
        /// Both start and stop are 0 -based indexes with 0 being the element with the lowest score.
        /// ZREMRANGEBYSCORE: Removes all elements in the sorted set stored at key with a score between min and max (inclusive by default).
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRemoveRange<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref outputFooter);

        /// <summary>
        /// Returns the rank of member in the sorted set, the scores in the sorted set are ordered from low to high
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRank<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref outputFooter);

        /// <summary>
        /// Returns a random member from the sorted set key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRandomMember<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref outputFooter);

        /// <summary>
        /// Iterates members of SortedSet key and their associated scores using a cursor,
        /// a match pattern and count parameters.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetScan<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
         where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
           => ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref outputFooter);
    }
}