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
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetAdd<TObjectContext>(PinnedSpanByte key, PinnedSpanByte score, PinnedSpanByte member, out int zaddCount, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            zaddCount = 0;
            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(score, member);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState) { SortedSetOp = SortedSetOperation.ZADD };

            var output = new ObjectOutput();

            var status = RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);
            itemBroker?.HandleCollectionUpdate(key.ToArray());

            if (status == GarnetStatus.OK)
                zaddCount = TryProcessRespSimple64IntOutput(output, out var value) ? (int)value : default;

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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetAdd<TObjectContext>(PinnedSpanByte key, (PinnedSpanByte score, PinnedSpanByte member)[] inputs, out int zaddCount, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            zaddCount = 0;

            if (inputs.Length == 0 || key.Length == 0)
                return GarnetStatus.OK;

            parseState.Initialize(inputs.Length * 2);

            for (var i = 0; i < inputs.Length; i++)
            {
                parseState.SetArguments(2 * i, isMetaArg: false, inputs[i].score, inputs[i].member);
            }

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState) { SortedSetOp = SortedSetOperation.ZADD };

            var output = new ObjectOutput();

            var status = RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);
            itemBroker?.HandleCollectionUpdate(key.ToArray());

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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetRemove<TObjectContext>(PinnedSpanByte key, PinnedSpanByte member, out int zremCount,
            ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            zremCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArgument(member);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState) { SortedSetOp = SortedSetOperation.ZREM };

            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);

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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetRemove<TObjectContext>(PinnedSpanByte key, PinnedSpanByte[] members, out int zremCount, ref TObjectContext objectContext)
           where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            zremCount = 0;

            if (key.Length == 0 || members.Length == 0)
                return GarnetStatus.OK;

            parseState.InitializeWithArguments(members);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState) { SortedSetOp = SortedSetOperation.ZREM };

            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);

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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetRemoveRangeByLex<TObjectContext>(PinnedSpanByte key, string min, string max,
            out int countRemoved, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
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
            var minSlice = PinnedSpanByte.FromPinnedSpan(minSpan);

            var maxSpan = paramsSpan.Slice(min.Length, max.Length);
            Encoding.UTF8.GetBytes(max, maxSpan);
            var maxSlice = PinnedSpanByte.FromPinnedSpan(maxSpan);

            // Prepare the parse state
            parseState.InitializeWithArguments(minSlice, maxSlice);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState) { SortedSetOp = SortedSetOperation.ZREMRANGEBYLEX };

            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);
            countRemoved = output.result1;

            scratchBufferBuilder.RewindScratchBuffer(paramsSlice);

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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetRemoveRangeByScore<TObjectContext>(PinnedSpanByte key, string min, string max,
            out int countRemoved, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
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
            var minSlice = PinnedSpanByte.FromPinnedSpan(minSpan);

            var maxSpan = paramsSpan.Slice(min.Length, max.Length);
            Encoding.UTF8.GetBytes(max, maxSpan);
            var maxSlice = PinnedSpanByte.FromPinnedSpan(maxSpan);

            // Prepare the parse state
            parseState.InitializeWithArguments(minSlice, maxSlice);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState) { SortedSetOp = SortedSetOperation.ZREMRANGEBYSCORE };

            var output = new ObjectOutput();

            var status = RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext,
                ref output);

            scratchBufferBuilder.RewindScratchBuffer(paramsSlice);

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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetRemoveRangeByRank<TObjectContext>(PinnedSpanByte key, int start, int stop,
            out int countRemoved, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
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
            var startSlice = PinnedSpanByte.FromPinnedSpan(startSpan);

            var stopSpan = paramsSpan.Slice(startLen, stopLen);
            NumUtils.WriteInt64(stop, stopSpan);
            var stopSlice = PinnedSpanByte.FromPinnedSpan(stopSpan);

            parseState.InitializeWithArguments(startSlice, stopSlice);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState) { SortedSetOp = SortedSetOperation.ZREMRANGEBYRANK };

            var output = new ObjectOutput();

            status = RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext,
                ref output);

            scratchBufferBuilder.RewindScratchBuffer(paramsSlice);

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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetPop<TObjectContext>(PinnedSpanByte key, int count, bool lowScoresFirst, out (PinnedSpanByte member, PinnedSpanByte score)[] pairs, ref TObjectContext objectContext)
                where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            pairs = default;
            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var op = lowScoresFirst ? SortedSetOperation.ZPOPMIN : SortedSetOperation.ZPOPMAX;
            var input = new ObjectInput(GarnetObjectType.SortedSet, arg1: count, arg2: 2) { SortedSetOp = op };

            var output = new ObjectOutput();

            var status = RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetIncrement<TObjectContext>(PinnedSpanByte key, double increment, PinnedSpanByte member,
            out double newScore, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
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
            var input = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState, arg2: 2) { SortedSetOp = SortedSetOperation.ZINCRBY };

            var output = new ObjectOutput { SpanByteAndMemory = new SpanByteAndMemory(null) };
            var status = RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext,
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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus SortedSetLength<TObjectContext>(PinnedSpanByte key, out int zcardCount, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            zcardCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZCARD };

            var status = ReadObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);

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
        public unsafe GarnetStatus SortedSetRange<TObjectContext>(PinnedSpanByte key, PinnedSpanByte min, PinnedSpanByte max, SortedSetOrderOperation sortedSetOrderOperation, ref TObjectContext objectContext, out PinnedSpanByte[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
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
                SortedSetOrderOperation.ByScore => SortedSetRangeOptions.ByScore,
                SortedSetOrderOperation.ByLex => SortedSetRangeOptions.ByLex,
                _ => SortedSetRangeOptions.None
            };

            var arguments = new List<PinnedSpanByte> { min, max };

            if (reverse)
            {
                rangeOpts |= SortedSetRangeOptions.Reverse;
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

                rangeOpts |= SortedSetRangeOptions.Limit;
            }

            parseState.InitializeWithArguments([.. arguments]);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState, arg2: (int)rangeOpts) { SortedSetOp = SortedSetOperation.ZRANGE };

            var output = new ObjectOutput { SpanByteAndMemory = new SpanByteAndMemory(null) };
            var status = ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

            for (var i = arguments.Count - 1; i > 1; i--)
            {
                var currSlice = arguments[i];
                scratchBufferBuilder.RewindScratchBuffer(currSlice);
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
        public unsafe GarnetStatus SortedSetDifference(ReadOnlySpan<PinnedSpanByte> keys, out SortedSet<(double, byte[])> pairs)
        {
            pairs = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                txnManager.Run(true);
            }

            var objectContext = txnManager.ObjectTransactionalContext;

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
        public GarnetStatus SortedSetDifferenceStore(PinnedSpanByte destinationKey, ReadOnlySpan<PinnedSpanByte> keys, out int count)
        {
            count = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object | TransactionStoreTypes.Unified);
                txnManager.SaveKeyEntryToLock(destinationKey, LockType.Exclusive);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                _ = txnManager.Run(true);
            }

            var objectContext = txnManager.ObjectTransactionalContext;
            var unifiedContext = txnManager.UnifiedTransactionalContext;

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

                    _ = SET(destinationKey, newSetObject, ref objectContext);
                    itemBroker?.HandleCollectionUpdate(destinationKey.ToArray());
                }
                else
                {
                    _ = EXPIRE(destinationKey, TimeSpan.Zero, out _, ExpireOption.None, ref unifiedContext);
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
        /// <param name="objectContext"></param>
        /// </summary>
        public unsafe GarnetStatus SortedSetRank<TObjectContext>(PinnedSpanByte key, PinnedSpanByte member, bool reverse, out long? rank, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            rank = null;
            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArgument(member);

            // Prepare the input
            var op = reverse ? SortedSetOperation.ZREVRANK : SortedSetOperation.ZRANK;
            var input = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState) { SortedSetOp = op };

            const int outputContainerSize = 32; // 3 for HEADER + CRLF + 20 for ascii long
            var outputContainer = stackalloc byte[outputContainerSize];
            var output = ObjectOutput.FromPinnedPointer(outputContainer, outputContainerSize);

            var status = ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetAdd<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
        where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);
            itemBroker?.HandleCollectionUpdate(key.ToArray());
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
        /// <param name="objectContext">The context of the object store.</param>
        /// <returns>Returns a GarnetStatus indicating the success or failure of the operation.</returns>
        public unsafe GarnetStatus SortedSetRangeStore<TObjectContext>(PinnedSpanByte dstKey, PinnedSpanByte srcKey, ref ObjectInput input, out int result, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            if (txnManager.ObjectTransactionalContext.Session is null)
                ThrowObjectStoreUninitializedException();

            result = 0;

            if (dstKey.Length == 0 || srcKey.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object | TransactionStoreTypes.Unified);
                txnManager.SaveKeyEntryToLock(dstKey, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(srcKey, LockType.Shared);
                _ = txnManager.Run(true);
            }

            // SetObject
            var ssObjectTransactionalContext = txnManager.ObjectTransactionalContext;
            var ssUnifiedTransactionalContext = txnManager.UnifiedTransactionalContext;

            try
            {
                SpanByteAndMemory rangeOutputMem = default;
                var rangeOutput = new ObjectOutput(rangeOutputMem);
                var status = SortedSetRange(srcKey, ref input, ref rangeOutput, ref ssObjectTransactionalContext);
                rangeOutputMem = rangeOutput.SpanByteAndMemory;

                if (status == GarnetStatus.WRONGTYPE)
                    return GarnetStatus.WRONGTYPE;

                if (status == GarnetStatus.NOTFOUND)
                {
                    // Expire/Delete the destination key if the source key is not found
                    _ = EXPIRE(dstKey, TimeSpan.Zero, out _, ExpireOption.None, ref ssUnifiedTransactionalContext);
                    return GarnetStatus.OK;
                }

                Debug.Assert(!rangeOutputMem.IsSpanByte, "Output should not be in SpanByte format when the status is OK");

                var rangeOutputHandler = rangeOutputMem.Memory.Memory.Pin();
                try
                {
                    var rangeOutPtr = (byte*)rangeOutputHandler.Pointer;
                    ref var currOutPtr = ref rangeOutPtr;
                    var endOutPtr = rangeOutPtr + rangeOutputMem.Length;

                    var destinationKey = dstKey.ReadOnlySpan;
                    ssObjectTransactionalContext.Delete(destinationKey);

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

                        var zAddInput = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState) { SortedSetOp = SortedSetOperation.ZADD };

                        var zAddOutput = new ObjectOutput();
                        RMWObjectStoreOperationWithOutput(destinationKey, ref zAddInput, ref ssObjectTransactionalContext, ref zAddOutput);
                        itemBroker?.HandleCollectionUpdate(destinationKey.ToArray());
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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRemove<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out output, ref objectContext);

        /// <summary>
        /// Returns the number of members of the sorted set.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetLength<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperation(key.ReadOnlySpan, ref input, out output, ref objectContext);

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key.
        /// Both start and stop are zero-based indexes, where 0 is the first element, 1 is the next element and so on.
        /// There can also be negative numbers indicating offsets from the end of the sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and so on.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRange<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns the score of member in the sorted set at key.
        /// If member does not exist in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetScore<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns the scores of members in the sorted set at key.
        /// For every member that does not exist in the sorted set, or if the key does not exist, nil is returned.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetScores<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Removes and returns the first element from the sorted set stored at key,
        /// with the scores ordered from low to high (min) or high to low (max).
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetPop<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

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
        public unsafe GarnetStatus SortedSetCount<TObjectContext>(PinnedSpanByte key, PinnedSpanByte minScore, PinnedSpanByte maxScore, out int numElements, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            numElements = 0;
            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(minScore, maxScore);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState) { SortedSetOp = SortedSetOperation.ZCOUNT };

            const int outputContainerSize = 32; // 3 for HEADER + CRLF + 20 for ascii long
            var outputContainer = stackalloc byte[outputContainerSize];
            var output = ObjectOutput.FromPinnedPointer(outputContainer, outputContainerSize);

            var status = ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

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
        public GarnetStatus SortedSetCount<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

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
        public GarnetStatus SortedSetRemoveRangeByLex<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out output, ref objectContext);

        /// <summary>
        /// Returns the number of elements in the sorted set with a value between min and max.
        /// When all the elements in a sorted set have the same score,
        /// this command forces lexicographical ordering.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetLengthByValue<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperation(key.ReadOnlySpan, ref input, out output, ref objectContext);

        /// <summary>
        /// Increments the score of member in the sorted set stored at key by increment.
        /// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetIncrement<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

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
        public GarnetStatus SortedSetRemoveRange<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns the rank of member in the sorted set, the scores in the sorted set are ordered from low to high
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRank<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns a random member from the sorted set key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRandomMember<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Iterates members of SortedSet key and their associated scores using a cursor,
        /// a match pattern and count parameters.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetScan<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
         where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
           => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        public GarnetStatus SortedSetUnion(ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out SortedSet<(double, byte[])> pairs)
        {
            pairs = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                txnManager.Run(true);
            }

            var objectContext = txnManager.ObjectTransactionalContext;

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

        public GarnetStatus SortedSetUnionStore(PinnedSpanByte destinationKey, ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out int count)
        {
            count = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object | TransactionStoreTypes.Unified);
                txnManager.SaveKeyEntryToLock(destinationKey, LockType.Exclusive);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                _ = txnManager.Run(true);
            }

            var objectContext = txnManager.ObjectTransactionalContext;
            var unifiedContext = txnManager.UnifiedTransactionalContext;

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

                    _ = SET(destinationKey, newSortedSetObject, ref objectContext);
                    itemBroker?.HandleCollectionUpdate(destinationKey.ToArray());
                }
                else
                {
                    _ = EXPIRE(destinationKey, TimeSpan.Zero, out _, ExpireOption.None, ref unifiedContext);
                }

                return status;
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }
        }

        private GarnetStatus SortedSetUnion<TObjectContext>(ReadOnlySpan<PinnedSpanByte> keys, ref TObjectContext objectContext,
            out Dictionary<byte[], double> pairs, double[] weights = null, SortedSetAggregateType aggregateType = SortedSetAggregateType.Sum)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            pairs = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            // Get the first sorted set
            var status = GET(keys[0], out var firstObj, ref objectContext);
            if (status == GarnetStatus.WRONGTYPE)
                return GarnetStatus.WRONGTYPE;

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
                status = GET(keys[i], out var nextObj, ref objectContext);
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

        private GarnetStatus SortedSetDifference<TObjectContext>(ReadOnlySpan<PinnedSpanByte> keys, ref TObjectContext objectContext, out Dictionary<byte[], double> pairs)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            pairs = default;

            var statusOp = GET(keys[0], out var firstObj, ref objectContext);
            if (statusOp == GarnetStatus.WRONGTYPE)
                return GarnetStatus.WRONGTYPE;

            if (statusOp == GarnetStatus.NOTFOUND)
            {
                pairs = new Dictionary<byte[], double>(ByteArrayComparer.Instance);
                return GarnetStatus.OK;
            }

            if (firstObj.GarnetObject is not SortedSetObject firstSortedSet)
                return GarnetStatus.WRONGTYPE;

            pairs = SortedSetObject.CopyDiff(firstSortedSet, null);
            if (keys.Length == 1)
            {
                return GarnetStatus.OK;
            }

            // read the rest of the keys
            for (var item = 1; item < keys.Length; item++)
            {
                statusOp = GET(keys[item], out var nextObj, ref objectContext);
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
        public unsafe GarnetStatus SortedSetMPop(ReadOnlySpan<PinnedSpanByte> keys, int count, bool lowScoresFirst, out PinnedSpanByte poppedKey, out (PinnedSpanByte member, PinnedSpanByte score)[] pairs)
        {
            if (txnManager.ObjectTransactionalContext.Session is null)
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
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object);
                foreach (var key in keys)
                    txnManager.SaveKeyEntryToLock(key, LockType.Exclusive);
                txnManager.Run(true);
            }

            var transactionalContext = txnManager.ObjectTransactionalContext;

            try
            {
                // Try popping from each key until we find one with members
                foreach (var key in keys)
                {
                    if (key.Length == 0) continue;

                    var status = SortedSetPop(key, count, lowScoresFirst, out pairs, ref transactionalContext);
                    if (status == GarnetStatus.OK && pairs != null && pairs.Length > 0)
                    {
                        poppedKey = key;
                        return status;
                    }

                    if (status != GarnetStatus.OK && status != GarnetStatus.NOTFOUND)
                        return status;
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
        public GarnetStatus SortedSetIntersectLength(ReadOnlySpan<PinnedSpanByte> keys, int? limit, out int count)
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
        public GarnetStatus SortedSetIntersectStore(PinnedSpanByte destinationKey, ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out int count)
        {
            count = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object | TransactionStoreTypes.Unified);
                txnManager.SaveKeyEntryToLock(destinationKey, LockType.Exclusive);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                _ = txnManager.Run(true);
            }

            var objectContext = txnManager.ObjectTransactionalContext;
            var unifiedContext = txnManager.UnifiedTransactionalContext;

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

                    _ = SET(destinationKey, newSortedSetObject, ref objectContext);
                    itemBroker?.HandleCollectionUpdate(destinationKey.ToArray());
                }
                else
                {
                    _ = EXPIRE(destinationKey, TimeSpan.Zero, out _, ExpireOption.None, ref unifiedContext);
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
        public GarnetStatus SortedSetIntersect(ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out SortedSet<(double, byte[])> pairs)
        {
            pairs = default;

            if (keys.Length == 0)
                return GarnetStatus.OK;

            var createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object);
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, LockType.Shared);
                txnManager.Run(true);
            }

            var objectContext = txnManager.ObjectTransactionalContext;

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
        private GarnetStatus SortedSetIntersection<TObjectContext>(ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, ref TObjectContext objectContext, out Dictionary<byte[], double> pairs)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            pairs = default;

            var statusOp = GET(keys[0], out var firstObj, ref objectContext);
            if (statusOp == GarnetStatus.WRONGTYPE)
                return GarnetStatus.WRONGTYPE;

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
                return GarnetStatus.OK;

            // Intersect with remaining sets
            for (var i = 1; i < keys.Length; i++)
            {
                statusOp = GET(keys[i], out var nextObj, ref objectContext);
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
        /// <param name="output">The output  object to store the result.</param>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        public GarnetStatus SortedSetExpire<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
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
        public GarnetStatus SortedSetExpire<TObjectContext>(PinnedSpanByte key, ReadOnlySpan<PinnedSpanByte> members, DateTimeOffset expireAt, ExpireOption expireOption, out int[] results, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            results = default;
            var expirationTimeInTicks = expireAt.UtcTicks;

            var expirationWithOption = new ExpirationWithOption(expirationTimeInTicks, expireOption);

            parseState.InitializeWithArguments(members);

            var innerInput = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState, 
                arg1: expirationWithOption.WordHead, arg2: expirationWithOption.WordTail) { SortedSetOp = SortedSetOperation.ZEXPIRE };

            var output = new ObjectOutput();
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
        /// <param name="output">The output  object to store the result.</param>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        public GarnetStatus SortedSetTimeToLive<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns the time-to-live (TTL) of a SortedSet member.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="key">The key of the sorted set.</param>
        /// <param name="members">The members within the sorted set to get the TTL for.</param>
        /// <param name="expireIn">The array of TimeSpan representing the TTL for each member.</param>
        /// <param name="objectContext">The context of the object store.</param>
        /// <returns>Returns a GarnetStatus indicating the success or failure of the operation.</returns>
        public GarnetStatus SortedSetTimeToLive<TObjectContext>(PinnedSpanByte key, ReadOnlySpan<PinnedSpanByte> members, out TimeSpan[] expireIn, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            expireIn = default;
            parseState.Initialize(members.Length);
            parseState.SetArguments(0, isMetaArg: false, members);
            var isMilliseconds = 1;
            var isTimestamp = 0;
            var innerInput = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState, arg1: isMilliseconds, arg2: isTimestamp) { SortedSetOp = SortedSetOperation.ZTTL };

            var output = new ObjectOutput();
            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), ref innerInput, ref objectContext, ref output);

            if (status == GarnetStatus.OK)
                expireIn = [.. ProcessRespInt64ArrayOutput(output, out _).Select(x => TimeSpan.FromMilliseconds(x < 0 ? 0 : x))];
            return status;
        }

        /// <summary>
        /// Removes the expiration time from a SortedSet member, making it persistent.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="key">The key of the SortedSet.</param>
        /// <param name="input">The input object containing the operation details.</param>
        /// <param name="output">The output  object to store the result.</param>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        public GarnetStatus SortedSetPersist<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
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
        public GarnetStatus SortedSetPersist<TObjectContext>(PinnedSpanByte key, ReadOnlySpan<PinnedSpanByte> members, out int[] results, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            results = default;
            parseState.Initialize(members.Length);
            parseState.SetArguments(0, isMetaArg: false, members);
            var innerInput = new ObjectInput(GarnetObjectType.SortedSet, RespMetaCommand.None, ref parseState) { SortedSetOp = SortedSetOperation.ZPERSIST };
            var output = new ObjectOutput();

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
        public GarnetStatus SortedSetCollect<TObjectContext>(ReadOnlySpan<PinnedSpanByte> keys, ref ObjectInput input, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
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
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
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
        public GarnetStatus SortedSetCollect<TObjectContext>(ReadOnlySpan<PinnedSpanByte> keys, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var innerInput = new ObjectInput(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZCOLLECT };

            if (keys.IsEmpty)
            {
                return SortedSetCollect([PinnedSpanByte.FromPinnedSpan("*"u8)], ref innerInput, ref objectContext);
            }

            return SortedSetCollect(keys, ref innerInput, ref objectContext);
        }
    }
}