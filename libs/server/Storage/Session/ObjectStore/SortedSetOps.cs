// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    sealed partial class StorageSession : IDisposable
    {
        private SingleWriterMultiReaderLock _zcollectTaskLock;

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
            parseState.InitializeWithArguments(score, member);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZADD };
            var input = new ObjectInput(header, ref parseState);

            var output = new GarnetObjectStoreOutput();

            var keyBytes = key.ToArray();
            var status = RMWObjectStoreOperationWithOutput(keyBytes, ref input, ref objectStoreContext, ref output);
            itemBroker.HandleCollectionUpdate(keyBytes);

            if (status == GarnetStatus.OK)
            {
                zaddCount = TryProcessRespSimple64IntOutput(output, out var value) ? (int)value : default;
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

            parseState.Initialize(inputs.Length * 2);

            for (var i = 0; i < inputs.Length; i++)
            {
                parseState.SetArguments(2 * i, inputs[i].score, inputs[i].member);
            }

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZADD };
            var input = new ObjectInput(header, ref parseState);

            var output = new GarnetObjectStoreOutput();

            var keyBytes = key.ToArray();
            var status = RMWObjectStoreOperationWithOutput(keyBytes, ref input, ref objectStoreContext, ref output);
            itemBroker.HandleCollectionUpdate(keyBytes);

            if (status == GarnetStatus.OK)
            {
                zaddCount = TryProcessRespSimple64IntOutput(output, out var value) ? (int)value : default;
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
            parseState.InitializeWithArgument(member);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZREM };
            var input = new ObjectInput(header, ref parseState);

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

            parseState.InitializeWithArguments(members);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZREM };
            var input = new ObjectInput(header, ref parseState);

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
        public unsafe GarnetStatus SortedSetRemoveRangeByLex<TObjectContext>(ArgSlice key, string min, string max,
            out int countRemoved, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long,
                ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            countRemoved = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Get buffer from scratch buffer manager
            var paramsSlice = scratchBufferBuilder.CreateArgSlice(min.Length + max.Length);
            var paramsSpan = paramsSlice.Span;

            // Store parameters to buffer
            var minSpan = paramsSpan.Slice(0, min.Length);
            Encoding.UTF8.GetBytes(min, minSpan);
            var minSlice = ArgSlice.FromPinnedSpan(minSpan);

            var maxSpan = paramsSpan.Slice(min.Length, max.Length);
            Encoding.UTF8.GetBytes(max, maxSpan);
            var maxSlice = ArgSlice.FromPinnedSpan(maxSpan);

            // Prepare the parse state
            parseState.InitializeWithArguments(minSlice, maxSlice);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZREMRANGEBYLEX };
            var input = new ObjectInput(header, ref parseState);

            var status = RMWObjectStoreOperation(key.ToArray(), ref input, out var output, ref objectStoreContext);
            countRemoved = output.result1;

            scratchBufferBuilder.RewindScratchBuffer(ref paramsSlice);

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
        public unsafe GarnetStatus SortedSetRemoveRangeByScore<TObjectContext>(ArgSlice key, string min, string max,
            out int countRemoved, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long,
                ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            countRemoved = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Get buffer from scratch buffer manager
            var paramsSlice = scratchBufferBuilder.CreateArgSlice(min.Length + max.Length);
            var paramsSpan = paramsSlice.Span;

            // Store parameters to buffer
            var minSpan = paramsSpan.Slice(0, min.Length);
            Encoding.UTF8.GetBytes(min, minSpan);
            var minSlice = ArgSlice.FromPinnedSpan(minSpan);

            var maxSpan = paramsSpan.Slice(min.Length, max.Length);
            Encoding.UTF8.GetBytes(max, maxSpan);
            var maxSlice = ArgSlice.FromPinnedSpan(maxSpan);

            // Prepare the parse state
            parseState.InitializeWithArguments(minSlice, maxSlice);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZREMRANGEBYSCORE };
            var input = new ObjectInput(header, ref parseState);

            var output = new GarnetObjectStoreOutput();

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext,
                ref output);

            scratchBufferBuilder.RewindScratchBuffer(ref paramsSlice);

            if (status == GarnetStatus.OK)
            {
                countRemoved = TryProcessRespSimple64IntOutput(output, out var value)
                    ? (int)value
                    : default;
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
        public unsafe GarnetStatus SortedSetRemoveRangeByRank<TObjectContext>(ArgSlice key, int start, int stop,
            out int countRemoved, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long,
                ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            countRemoved = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            GarnetStatus status;
            // Get parameter lengths
            var startLen = NumUtils.CountDigits(start);
            var stopLen = NumUtils.CountDigits(stop);

            // Get buffer from scratch buffer manager
            var paramsSlice = scratchBufferBuilder.CreateArgSlice(startLen + stopLen);
            var paramsSpan = paramsSlice.Span;

            // Store parameters to buffer
            var startSpan = paramsSpan.Slice(0, startLen);
            NumUtils.WriteInt64(start, startSpan);
            var startSlice = ArgSlice.FromPinnedSpan(startSpan);

            var stopSpan = paramsSpan.Slice(startLen, stopLen);
            NumUtils.WriteInt64(stop, stopSpan);
            var stopSlice = ArgSlice.FromPinnedSpan(stopSpan);

            parseState.InitializeWithArguments(startSlice, stopSlice);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZREMRANGEBYRANK };
            var input = new ObjectInput(header, ref parseState);

            var output = new GarnetObjectStoreOutput();

            status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext,
                ref output);

            scratchBufferBuilder.RewindScratchBuffer(ref paramsSlice);

            if (status == GarnetStatus.OK)
            {
                countRemoved = TryProcessRespSimple64IntOutput(output, out var value) ? (int)value : default;
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
            var op = lowScoresFirst ? SortedSetOperation.ZPOPMIN : SortedSetOperation.ZPOPMAX;
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = op };
            var input = new ObjectInput(header, count, 2);

            var output = new GarnetObjectStoreOutput();

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref output);

            //process output
            if (status == GarnetStatus.OK)
                pairs = ProcessRespArrayOutputAsPairs(output, out _);

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
        public unsafe GarnetStatus SortedSetIncrement<TObjectContext>(ArgSlice key, double increment, ArgSlice member,
            out double newScore, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long,
                ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            newScore = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var strIncr = increment.ToString(CultureInfo.InvariantCulture);
            var incrSlice = scratchBufferBuilder.CreateArgSlice(strIncr);
            Encoding.UTF8.GetBytes(strIncr, incrSlice.Span);

            // Prepare the parse state
            parseState.InitializeWithArguments(incrSlice, member);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZINCRBY };
            var input = new ObjectInput(header, ref parseState, arg2: 2);

            var output = new GarnetObjectStoreOutput();
            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext,
                ref output);

            // Process output
            if (status == GarnetStatus.OK)
            {
                var result = ProcessRespSingleTokenOutput(output);
                if (result.length > 0)
                {
                    var sbResult = result.ReadOnlySpan;
                    // get the new score
                    _ = NumUtils.TryParseWithInfinity(sbResult, out newScore);
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
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZCARD };
            var input = new ObjectInput(header);

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

            var rangeOpts = sortedSetOrderOperation switch
            {
                SortedSetOrderOperation.ByScore => SortedSetRangeOpts.ByScore,
                SortedSetOrderOperation.ByLex => SortedSetRangeOpts.ByLex,
                _ => SortedSetRangeOpts.None
            };

            var arguments = new List<ArgSlice> { min, max };

            if (reverse)
            {
                rangeOpts |= SortedSetRangeOpts.Reverse;
            }

            // Limit parameter
            if (limit != default && (sortedSetOrderOperation == SortedSetOrderOperation.ByScore || sortedSetOrderOperation == SortedSetOrderOperation.ByLex))
            {
                arguments.Add(scratchBufferBuilder.CreateArgSlice("LIMIT"u8));

                // Offset
                arguments.Add(scratchBufferBuilder.CreateArgSlice(limit.Item1));

                // Count
                var limitCountLength = NumUtils.CountDigits(limit.Item2);
                var limitCountSlice = scratchBufferBuilder.CreateArgSlice(limitCountLength);
                NumUtils.WriteInt64(limit.Item2, limitCountSlice.Span);
                arguments.Add(limitCountSlice);
            }

            parseState.InitializeWithArguments([.. arguments]);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZRANGE };
            var input = new ObjectInput(header, ref parseState, arg2: (int)rangeOpts);

            var output = new GarnetObjectStoreOutput();
            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectContext, ref output);

            for (var i = arguments.Count - 1; i > 1; i--)
            {
                var currSlice = arguments[i];
                scratchBufferBuilder.RewindScratchBuffer(ref currSlice);
            }

            if (status == GarnetStatus.OK)
                elements = ProcessRespArrayOutput(output, out error);

            return status;
        }


        /// <summary>
        /// Computes the difference between the first and all successive sorted sets and returns resulting pairs.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="pairs"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetDifference(ReadOnlySpan<ArgSlice> keys, out SortedSet<(double, byte[])> pairs)
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
                var status = SortedSetDifference(keys, ref objectContext, out var result);
                if (status == GarnetStatus.OK)
                {
                    pairs = new(SortedSetComparer.Instance);
                    foreach (var pair in result)
                    {
                        pairs.Add((pair.Value, pair.Key));
                    }
                }

                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        /// <summary>
        /// Computes the difference between the first and all successive sorted sets and store resulting pairs in the destination key.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="destinationKey"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetDifferenceStore(ArgSlice destinationKey, ReadOnlySpan<ArgSlice> keys, out int count)
        {
            count = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(destinationKey, true, LockType.Exclusive);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
                _ = txnManager.Run(true);
            }

            var objectContext = txnManager.ObjectStoreLockableContext;

            try
            {
                var status = SortedSetDifference(keys, ref objectContext, out var pairs);

                if (status != GarnetStatus.OK)
                {
                    return GarnetStatus.WRONGTYPE;
                }

                count = pairs?.Count ?? 0;
                if (count > 0)
                {
                    SortedSetObject newSetObject = new();
                    foreach (var (element, score) in pairs)
                    {
                        newSetObject.Add(element, score);
                    }

                    var destinationKeyBytes = destinationKey.ToArray();
                    _ = SET(destinationKeyBytes, newSetObject, ref objectContext);
                    itemBroker.HandleCollectionUpdate(destinationKeyBytes);
                }
                else
                {
                    _ = EXPIRE(destinationKey, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None,
                        ref lockableContext, ref objectContext);
                }

                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
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
            parseState.InitializeWithArgument(member);

            // Prepare the input
            var op = reverse ? SortedSetOperation.ZREVRANK : SortedSetOperation.ZRANK;
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = op };
            var input = new ObjectInput(header, ref parseState);

            const int outputContainerSize = 32; // 3 for HEADER + CRLF + 20 for ascii long
            var outputContainer = stackalloc byte[outputContainerSize];
            var output = new GarnetObjectStoreOutput(new(outputContainer, outputContainerSize));

            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref output);

            if (status == GarnetStatus.OK)
            {
                Debug.Assert(*outputContainer == (byte)'$' || *outputContainer == (byte)':');
                if (*outputContainer == (byte)':')
                {
                    // member exists -> read the rank
                    var read = TryProcessRespSimple64IntOutput(output, out var value);
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
        {
            var status = RMWObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref output);
            itemBroker.HandleCollectionUpdate(key);
            return status;
        }

        /// <summary>
        /// ZRANGESTORE - Stores a range of sorted set elements into a destination key.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="dstKey">The destination key where the range will be stored.</param>
        /// <param name="srcKey">The source key from which the range will be taken.</param>
        /// <param name="input">The input object containing range parameters.</param>
        /// <param name="result">The result of the operation, indicating the number of elements stored.</param>
        /// <param name="objectStoreContext">The context of the object store.</param>
        /// <returns>Returns a GarnetStatus indicating the success or failure of the operation.</returns>
        public unsafe GarnetStatus SortedSetRangeStore<TObjectContext>(ArgSlice dstKey, ArgSlice srcKey, ref ObjectInput input, out int result, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (txnManager.ObjectStoreLockableContext.Session is null)
                ThrowObjectStoreUninitializedException();

            result = 0;

            if (dstKey.Length == 0 || srcKey.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(dstKey, true, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(srcKey, true, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var objectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                SpanByteAndMemory rangeOutputMem = default;
                var rangeOutput = new GarnetObjectStoreOutput(rangeOutputMem);

                var status = SortedSetRange(srcKey.ToArray(), ref input, ref rangeOutput, ref objectStoreLockableContext);
                rangeOutputMem = rangeOutput.SpanByteAndMemory;

                if (status == GarnetStatus.WRONGTYPE)
                {
                    return GarnetStatus.WRONGTYPE;
                }

                if (status == GarnetStatus.NOTFOUND)
                {
                    // Expire/Delete the destination key if the source key is not found
                    _ = EXPIRE(dstKey, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None, ref lockableContext, ref objectStoreLockableContext);
                    return GarnetStatus.OK;
                }

                Debug.Assert(!rangeOutputMem.IsSpanByte, "Output should not be in SpanByte format when the status is OK");

                var rangeOutputHandler = rangeOutputMem.Memory.Memory.Pin();
                try
                {
                    var rangeOutPtr = (byte*)rangeOutputHandler.Pointer;
                    ref var currOutPtr = ref rangeOutPtr;
                    var endOutPtr = rangeOutPtr + rangeOutputMem.Length;

                    var destinationKey = dstKey.ToArray();
                    objectStoreLockableContext.Delete(ref destinationKey);

                    RespReadUtils.TryReadUnsignedArrayLength(out var arrayLen, ref currOutPtr, endOutPtr);
                    Debug.Assert(arrayLen % 2 == 0, "Should always contain element and its score");
                    result = arrayLen / 2;

                    if (result > 0)
                    {
                        parseState.Initialize(arrayLen); // 2 elements per pair (result * 2)

                        for (int j = 0; j < result; j++)
                        {
                            // Read member/element into parse state
                            parseState.Read((2 * j) + 1, ref currOutPtr, endOutPtr);
                            // Read score into parse state
                            parseState.Read(2 * j, ref currOutPtr, endOutPtr);
                        }

                        var zAddInput = new ObjectInput(new RespInputHeader
                        {
                            type = GarnetObjectType.SortedSet,
                            SortedSetOp = SortedSetOperation.ZADD,
                        }, ref parseState);

                        var zAddOutput = new GarnetObjectStoreOutput();
                        RMWObjectStoreOperationWithOutput(destinationKey, ref zAddInput, ref objectStoreLockableContext, ref zAddOutput);
                        itemBroker.HandleCollectionUpdate(destinationKey);
                    }
                }
                finally
                {
                    rangeOutputHandler.Dispose();
                }
                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

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
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRange<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref output);

        /// <summary>
        /// Returns the score of member in the sorted set at key.
        /// If member does not exist in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetScore<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref output);

        /// <summary>
        /// Returns the scores of members in the sorted set at key.
        /// For every member that does not exist in the sorted set, or if the key does not exist, nil is returned.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetScores<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref output);

        /// <summary>
        /// Removes and returns the first element from the sorted set stored at key,
        /// with the scores ordered from low to high (min) or high to low (max).
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetPop<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref output);

        /// <summary>
        /// Returns the number of elements in the sorted set at key with a score between min and max.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="minScore"></param>
        /// <param name="maxScore"></param>
        /// <param name="numElements"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetCount<TObjectContext>(ArgSlice key, ArgSlice minScore, ArgSlice maxScore, out int numElements, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            numElements = 0;
            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(minScore, maxScore);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZCOUNT };
            var input = new ObjectInput(header, ref parseState);

            const int outputContainerSize = 32; // 3 for HEADER + CRLF + 20 for ascii long
            var outputContainer = stackalloc byte[outputContainerSize];
            var output = new GarnetObjectStoreOutput(new(outputContainer, outputContainerSize));

            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectContext, ref output);

            if (status == GarnetStatus.OK)
            {
                Debug.Assert(*outputContainer == (byte)':');
                var read = TryProcessRespSimple64IntOutput(output, out var value);
                numElements = read ? (int)value : default;
                Debug.Assert(read);
            }
            return status;
        }

        /// <summary>
        /// Returns the number of elements in the sorted set at key with a score between min and max.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetCount<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref output);

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
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetIncrement<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref output);

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
        public GarnetStatus SortedSetRemoveRange<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns the rank of member in the sorted set, the scores in the sorted set are ordered from low to high
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRank<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns a random member from the sorted set key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRandomMember<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectContext, ref output);

        /// <summary>
        /// Iterates members of SortedSet key and their associated scores using a cursor,
        /// a match pattern and count parameters.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetScan<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
         where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
           => ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref output);

        public GarnetStatus SortedSetUnion(ReadOnlySpan<ArgSlice> keys, double[] weights, SortedSetAggregateType aggregateType, out SortedSet<(double, byte[])> pairs)
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
                var status = SortedSetUnion(keys, ref objectContext, out var result, weights, aggregateType);
                if (status == GarnetStatus.OK)
                {
                    pairs = new(SortedSetComparer.Instance);

                    foreach (var pair in result)
                    {
                        pairs.Add((pair.Value, pair.Key));
                    }
                }

                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        public GarnetStatus SortedSetUnionStore(ArgSlice destinationKey, ReadOnlySpan<ArgSlice> keys, double[] weights, SortedSetAggregateType aggregateType, out int count)
        {
            count = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(destinationKey, true, LockType.Exclusive);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
                _ = txnManager.Run(true);
            }

            var objectContext = txnManager.ObjectStoreLockableContext;

            try
            {
                var status = SortedSetUnion(keys, ref objectContext, out var pairs, weights, aggregateType);

                if (status == GarnetStatus.WRONGTYPE)
                {
                    return GarnetStatus.WRONGTYPE;
                }

                count = pairs?.Count ?? 0;

                if (count > 0)
                {
                    SortedSetObject newSortedSetObject = new();
                    foreach (var (element, score) in pairs)
                    {
                        newSortedSetObject.Add(element, score);
                    }

                    var destinationKeyBytes = destinationKey.ToArray();
                    _ = SET(destinationKeyBytes, newSortedSetObject, ref objectContext);
                    itemBroker.HandleCollectionUpdate(destinationKeyBytes);
                }
                else
                {
                    _ = EXPIRE(destinationKey, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None,
                        ref lockableContext, ref objectContext);
                }

                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        private GarnetStatus SortedSetUnion<TObjectContext>(ReadOnlySpan<ArgSlice> keys, ref TObjectContext objectContext,
            out Dictionary<byte[], double> pairs, double[] weights = null, SortedSetAggregateType aggregateType = SortedSetAggregateType.Sum)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            pairs = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            // Get the first sorted set
            var status = GET(keys[0].ToArray(), out var firstObj, ref objectContext);

            if (status == GarnetStatus.WRONGTYPE)
            {
                return GarnetStatus.WRONGTYPE;
            }

            Dictionary<byte[], double> sortedSetDictionary = null;

            if (status == GarnetStatus.OK)
            {
                if (firstObj.GarnetObject is not SortedSetObject firstSortedSet)
                {
                    return GarnetStatus.WRONGTYPE;
                }
                sortedSetDictionary = firstSortedSet.Dictionary;
            }

            // Initialize pairs with the first set
            if (weights is null)
            {
                pairs = sortedSetDictionary is null ? new Dictionary<byte[], double>(ByteArrayComparer.Instance) : new Dictionary<byte[], double>(sortedSetDictionary, ByteArrayComparer.Instance);
            }
            else
            {
                pairs = new Dictionary<byte[], double>(ByteArrayComparer.Instance);
                if (sortedSetDictionary is not null)
                {
                    foreach (var (key, score) in sortedSetDictionary)
                    {
                        pairs[key] = weights[0] * score;
                    }
                }
            }

            // Process remaining sets
            for (var i = 1; i < keys.Length; i++)
            {
                status = GET(keys[i].ToArray(), out var nextObj, ref objectContext);
                if (status == GarnetStatus.WRONGTYPE)
                    return GarnetStatus.WRONGTYPE;
                if (status != GarnetStatus.OK)
                    continue;

                if (nextObj.GarnetObject is not SortedSetObject nextSortedSet)
                {
                    pairs = default;
                    return GarnetStatus.WRONGTYPE;
                }

                foreach (var (key, score) in nextSortedSet.Dictionary)
                {
                    var weightedScore = weights is null ? score : score * weights[i];
                    if (pairs.TryGetValue(key, out var existingScore))
                    {
                        pairs[key] = aggregateType switch
                        {
                            SortedSetAggregateType.Sum => existingScore + weightedScore,
                            SortedSetAggregateType.Min => Math.Min(existingScore, weightedScore),
                            SortedSetAggregateType.Max => Math.Max(existingScore, weightedScore),
                            _ => existingScore + weightedScore // Default to SUM
                        };
                    }
                    else
                    {
                        pairs[key] = weightedScore;
                    }
                }
            }

            return GarnetStatus.OK;
        }

        private GarnetStatus SortedSetDifference<TObjectContext>(ReadOnlySpan<ArgSlice> keys, ref TObjectContext objectContext, out Dictionary<byte[], double> pairs)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            pairs = default;

            var statusOp = GET(keys[0].ToArray(), out var firstObj, ref objectContext);
            if (statusOp == GarnetStatus.WRONGTYPE)
            {
                return GarnetStatus.WRONGTYPE;
            }

            if (statusOp == GarnetStatus.NOTFOUND)
            {
                pairs = new Dictionary<byte[], double>(ByteArrayComparer.Instance);
                return GarnetStatus.OK;
            }

            if (firstObj.GarnetObject is not SortedSetObject firstSortedSet)
            {
                return GarnetStatus.WRONGTYPE;
            }

            pairs = SortedSetObject.CopyDiff(firstSortedSet, null);
            if (keys.Length == 1)
            {
                return GarnetStatus.OK;
            }

            // read the rest of the keys
            for (var item = 1; item < keys.Length; item++)
            {
                statusOp = GET(keys[item].ToArray(), out var nextObj, ref objectContext);
                if (statusOp == GarnetStatus.WRONGTYPE)
                    return GarnetStatus.WRONGTYPE;
                if (statusOp != GarnetStatus.OK)
                    continue;

                if (nextObj.GarnetObject is not SortedSetObject nextSortedSet)
                {
                    pairs = default;
                    return GarnetStatus.WRONGTYPE;
                }

                SortedSetObject.InPlaceDiff(pairs, nextSortedSet);
            }

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Removes and returns up to count members and their scores from the first sorted set that contains a member.
        /// </summary>
        public unsafe GarnetStatus SortedSetMPop(ReadOnlySpan<ArgSlice> keys, int count, bool lowScoresFirst, out ArgSlice poppedKey, out (ArgSlice member, ArgSlice score)[] pairs)
        {
            if (txnManager.ObjectStoreLockableContext.Session is null)
                ThrowObjectStoreUninitializedException();

            pairs = default;
            poppedKey = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                foreach (var key in keys)
                    txnManager.SaveKeyEntryToLock(key, true, LockType.Exclusive);
                txnManager.Run(true);
            }

            var storeLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                // Try popping from each key until we find one with members
                foreach (var key in keys)
                {
                    if (key.Length == 0) continue;

                    var status = SortedSetPop(key, count, lowScoresFirst, out pairs, ref storeLockableContext);
                    if (status == GarnetStatus.OK && pairs != null && pairs.Length > 0)
                    {
                        poppedKey = key;
                        return status;
                    }

                    if (status != GarnetStatus.OK && status != GarnetStatus.NOTFOUND)
                    {
                        return status;
                    }
                }

                return GarnetStatus.OK;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        /// <summary>
        /// Computes the cardinality of the intersection of multiple sorted sets.
        /// </summary>
        public GarnetStatus SortedSetIntersectLength(ReadOnlySpan<ArgSlice> keys, int? limit, out int count)
        {
            count = 0;

            var status = SortedSetIntersect(keys, null, SortedSetAggregateType.Sum, out var pairs);
            if (status == GarnetStatus.OK && pairs != null)
            {
                count = limit > 0 ? Math.Min(pairs.Count, limit.Value) : pairs.Count;
            }

            return status;
        }

        /// <summary>
        /// Computes the intersection of multiple sorted sets and stores the resulting sorted set at destinationKey.
        /// </summary>
        public GarnetStatus SortedSetIntersectStore(ArgSlice destinationKey, ReadOnlySpan<ArgSlice> keys, double[] weights, SortedSetAggregateType aggregateType, out int count)
        {
            count = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(destinationKey, true, LockType.Exclusive);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
                _ = txnManager.Run(true);
            }

            var objectContext = txnManager.ObjectStoreLockableContext;

            try
            {
                var status = SortedSetIntersection(keys, weights, aggregateType, ref objectContext, out var pairs);

                if (status != GarnetStatus.OK)
                {
                    return GarnetStatus.WRONGTYPE;
                }

                count = pairs?.Count ?? 0;

                if (count > 0)
                {
                    SortedSetObject newSortedSetObject = new();
                    foreach (var (element, score) in pairs)
                    {
                        newSortedSetObject.Add(element, score);
                    }

                    var destinationKeyBytes = destinationKey.ToArray();
                    _ = SET(destinationKeyBytes, newSortedSetObject, ref objectContext);
                    itemBroker.HandleCollectionUpdate(destinationKeyBytes);
                }
                else
                {
                    _ = EXPIRE(destinationKey, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None,
                        ref lockableContext, ref objectContext);
                }

                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        /// <summary>
        /// Computes the intersection of multiple sorted sets and returns the result with optional weights and aggregate type.
        /// </summary>
        public GarnetStatus SortedSetIntersect(ReadOnlySpan<ArgSlice> keys, double[] weights, SortedSetAggregateType aggregateType, out SortedSet<(double, byte[])> pairs)
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
                var status = SortedSetIntersection(keys, weights, aggregateType, ref objectContext, out var result);
                if (status == GarnetStatus.OK)
                {
                    pairs = new(SortedSetComparer.Instance);
                    if (result != null)
                    {
                        foreach (var pair in result)
                        {
                            pairs.Add((pair.Value, pair.Key));
                        }
                    }
                }

                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        /// <summary>
        /// Computes the intersection of multiple sorted sets and returns the result with optional weights and aggregate type.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="keys">The keys of the sorted sets to intersect.</param>
        /// <param name="weights">The weights to apply to each sorted set's scores. If null, no weights are applied.</param>
        /// <param name="aggregateType">The type of aggregation to use (Sum, Min, Max).</param>
        /// <param name="objectContext">The object context.</param>
        /// <param name="pairs">The resulting dictionary of intersected elements and their scores.</param>
        /// <returns></returns>
        private GarnetStatus SortedSetIntersection<TObjectContext>(ReadOnlySpan<ArgSlice> keys, double[] weights, SortedSetAggregateType aggregateType, ref TObjectContext objectContext, out Dictionary<byte[], double> pairs)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            pairs = default;

            var statusOp = GET(keys[0].ToArray(), out var firstObj, ref objectContext);

            if (statusOp == GarnetStatus.WRONGTYPE)
            {
                return GarnetStatus.WRONGTYPE;
            }

            if (statusOp == GarnetStatus.NOTFOUND)
            {
                pairs = new Dictionary<byte[], double>(ByteArrayComparer.Instance);
                return GarnetStatus.OK;
            }

            if (firstObj.GarnetObject is not SortedSetObject firstSortedSet)
            {
                return GarnetStatus.WRONGTYPE;
            }

            // Initialize result with first set
            if (weights is null)
            {
                pairs = keys.Length == 1 ? firstSortedSet.Dictionary : new Dictionary<byte[], double>(firstSortedSet.Dictionary, ByteArrayComparer.Instance);
            }
            else
            {
                pairs = new Dictionary<byte[], double>(ByteArrayComparer.Instance);
                foreach (var kvp in firstSortedSet.Dictionary)
                {
                    pairs[kvp.Key] = kvp.Value * weights[0];
                }
            }

            if (keys.Length == 1)
            {
                return GarnetStatus.OK;
            }

            // Intersect with remaining sets
            for (var i = 1; i < keys.Length; i++)
            {
                statusOp = GET(keys[i].ToArray(), out var nextObj, ref objectContext);
                if (statusOp == GarnetStatus.WRONGTYPE)
                    return GarnetStatus.WRONGTYPE;
                if (statusOp != GarnetStatus.OK)
                {
                    pairs = default;
                    return GarnetStatus.OK;
                }

                if (nextObj.GarnetObject is not SortedSetObject nextSortedSet)
                {
                    pairs = default;
                    return GarnetStatus.WRONGTYPE;
                }

                foreach (var kvp in pairs)
                {
                    if (!nextSortedSet.TryGetScore(kvp.Key, out var score))
                    {
                        pairs.Remove(kvp.Key);
                        continue;
                    }

                    var weightedScore = weights is null ? score : score * weights[i];
                    pairs[kvp.Key] = aggregateType switch
                    {
                        SortedSetAggregateType.Sum => kvp.Value + weightedScore,
                        SortedSetAggregateType.Min => Math.Min(kvp.Value, weightedScore),
                        SortedSetAggregateType.Max => Math.Max(kvp.Value, weightedScore),
                        _ => kvp.Value + weightedScore // Default to SUM
                    };

                    // That's what the references do. Arguably we're doing bug compatible behaviour here.
                    if (double.IsNaN(pairs[kvp.Key]))
                    {
                        pairs[kvp.Key] = 0;
                    }
                }

                // If intersection becomes empty, we can stop early
                if (pairs.Count == 0)
                {
                    break;
                }
            }

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Sets the expiration time for the specified key.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="key">The key for which to set the expiration time.</param>
        /// <param name="input">The input object containing the operation details.</param>
        /// <param name="output">The output footer object to store the result.</param>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        public GarnetStatus SortedSetExpire<TObjectContext>(ArgSlice key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            return RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectContext, ref output);
        }

        /// <summary>
        /// Sets the expiration time for the specified key and fields in a sorted set.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="key">The key of the sorted set.</param>
        /// <param name="members">The members within the sorted set to set the expiration time for.</param>
        /// <param name="expireAt">The expiration time as a DateTimeOffset.</param>
        /// <param name="expireOption">The expiration option to use.</param>
        /// <param name="results">The results of the operation, indicating the number of fields that were successfully set to expire.</param>
        /// <param name="objectContext">The context of the object store.</param>
        /// <returns>Returns a GarnetStatus indicating the success or failure of the operation.</returns>
        public GarnetStatus SortedSetExpire<TObjectContext>(ArgSlice key, ReadOnlySpan<ArgSlice> members, DateTimeOffset expireAt, ExpireOption expireOption, out int[] results, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            results = default;
            var expirationTimeInTicks = expireAt.UtcTicks;

            var expirationWithOption = new ExpirationWithOption(expirationTimeInTicks, expireOption);

            parseState.InitializeWithArguments(members);

            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZEXPIRE };
            var innerInput = new ObjectInput(header, ref parseState, arg1: expirationWithOption.WordHead, arg2: expirationWithOption.WordTail);

            var output = new GarnetObjectStoreOutput();
            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref innerInput, ref objectContext, ref output);

            if (status == GarnetStatus.OK)
            {
                results = ProcessRespIntegerArrayOutput(output, out _);
            }

            return status;
        }

        /// <summary>
        /// Returns the time-to-live (TTL) of a SortedSet member.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="key">The key of the hash.</param>
        /// <param name="input">The input object containing the operation details.</param>
        /// <param name="output">The output footer object to store the result.</param>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        public GarnetStatus SortedSetTimeToLive<TObjectContext>(ArgSlice key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            return ReadObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectContext, ref output);
        }

        /// <summary>
        /// Returns the time-to-live (TTL) of a SortedSet member.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="key">The key of the sorted set.</param>
        /// <param name="members">The members within the sorted set to get the TTL for.</param>
        /// <param name="expireIn">The array of TimeSpan representing the TTL for each member.</param>
        /// <param name="objectContext">The context of the object store.</param>
        /// <returns>Returns a GarnetStatus indicating the success or failure of the operation.</returns>
        public GarnetStatus SortedSetTimeToLive<TObjectContext>(ArgSlice key, ReadOnlySpan<ArgSlice> members, out TimeSpan[] expireIn, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            expireIn = default;
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZTTL };
            parseState.Initialize(members.Length);
            parseState.SetArguments(0, members);
            var isMilliseconds = 1;
            var isTimestamp = 0;
            var innerInput = new ObjectInput(header, ref parseState, arg1: isMilliseconds, arg2: isTimestamp);

            var output = new GarnetObjectStoreOutput();
            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), ref innerInput, ref objectContext, ref output);

            if (status == GarnetStatus.OK)
            {
                expireIn = ProcessRespInt64ArrayOutput(output, out _).Select(x => TimeSpan.FromMilliseconds(x < 0 ? 0 : x)).ToArray();
            }

            return status;
        }

        /// <summary>
        /// Removes the expiration time from a SortedSet member, making it persistent.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="key">The key of the SortedSet.</param>
        /// <param name="input">The input object containing the operation details.</param>
        /// <param name="output">The output footer object to store the result.</param>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        public GarnetStatus SortedSetPersist<TObjectContext>(ArgSlice key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectContext, ref output);

        /// <summary>
        /// Removes the expiration time from the specified members in the sorted set stored at the given key.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="key">The key of the sorted set.</param>
        /// <param name="members">The members whose expiration time will be removed.</param>
        /// <param name="results">The results of the operation, indicating the number of members whose expiration time was successfully removed.</param>
        /// <param name="objectContext">The context of the object store.</param>
        /// <returns>Returns a GarnetStatus indicating the success or failure of the operation.</returns>
        public GarnetStatus SortedSetPersist<TObjectContext>(ArgSlice key, ReadOnlySpan<ArgSlice> members, out int[] results, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            results = default;
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZPERSIST };
            parseState.Initialize(members.Length);
            parseState.SetArguments(0, members);
            var innerInput = new ObjectInput(header, ref parseState);
            var output = new GarnetObjectStoreOutput();

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref innerInput, ref objectContext, ref output);

            if (status == GarnetStatus.OK)
            {
                results = ProcessRespIntegerArrayOutput(output, out _);
            }

            return status;
        }

        /// <summary>
        /// Collects SortedSet keys and performs a specified operation on them.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="keys">The keys to collect.</param>
        /// <param name="input">The input object containing the operation details.</param>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        /// <remarks>
        /// If the first key is "*", all SortedSet keys are scanned in batches and the operation is performed on each key.
        /// Otherwise, the operation is performed on the specified keys.
        /// </remarks>
        public GarnetStatus SortedSetCollect<TObjectContext>(ReadOnlySpan<ArgSlice> keys, ref ObjectInput input, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            if (keys[0].ReadOnlySpan.SequenceEqual("*"u8))
            {
                return ObjectCollect(keys[0], CmdStrings.ZSET, _zcollectTaskLock, ref input, ref objectContext);
            }

            foreach (var key in keys)
            {
                RMWObjectStoreOperation(key.ToArray(), ref input, out _, ref objectContext);
            }

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Collects SortedSet keys and performs a specified operation on them.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        /// <remarks>
        /// If the first key is "*", all SortedSet keys are scanned in batches and the operation is performed on each key.
        /// Otherwise, the operation is performed on the specified keys.
        /// </remarks>
        public GarnetStatus SortedSetCollect<TObjectContext>(ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            return SortedSetCollect([], ref objectContext);
        }

        /// <summary>
        /// Collects SortedSet keys and performs a specified operation on them.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="keys">The keys to collect.</param>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        /// <remarks>
        /// If the first key is "*", all SortedSet keys are scanned in batches and the operation is performed on each key.
        /// Otherwise, the operation is performed on the specified keys.
        /// </remarks>
        public GarnetStatus SortedSetCollect<TObjectContext>(ReadOnlySpan<ArgSlice> keys, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZCOLLECT };
            var innerInput = new ObjectInput(header);

            if (keys.IsEmpty)
            {
                return SortedSetCollect([ArgSlice.FromPinnedSpan("*"u8)], ref innerInput, ref objectContext);
            }

            return SortedSetCollect(keys, ref innerInput, ref objectContext);
        }
    }
}