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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            zaddCount = 0;
            if (key.Length == 0)
                return GarnetStatus.OK;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, score, member);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.SortedSet;
            rmwInput->header.flags = 0;
            rmwInput->header.SortedSetOp = SortedSetOperation.ZADD;
            rmwInput->arg1 = 1;

            var status = RMWObjectStoreOperation(key.ToArray(), input, out var output, ref objectStoreContext);

            zaddCount = output.result1;
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            zaddCount = 0;

            if (inputs.Length == 0 || key.Length == 0)
                return GarnetStatus.OK;

            // Prepare header in buffer
            var rmwInput = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
            rmwInput->header.type = GarnetObjectType.SortedSet;
            rmwInput->header.flags = 0;
            rmwInput->header.SortedSetOp = SortedSetOperation.ZADD;
            rmwInput->arg1 = inputs.Length;

            int inputLength = sizeof(ObjectInputHeader);
            foreach (var (score, member) in inputs)
            {
                var tmp = scratchBufferManager.FormatScratchAsResp(0, score, member);
                inputLength += tmp.Length;
            }
            var input = scratchBufferManager.GetSliceFromTail(inputLength);

            var status = RMWObjectStoreOperation(key.ToArray(), input, out var output, ref objectStoreContext);

            zaddCount = output.result1;
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
        public unsafe GarnetStatus SortedSetRemove<TObjectContext>(byte[] key, ArgSlice member, out int zremCount, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            zremCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var _inputSlice = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, member);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)_inputSlice.ptr;
            rmwInput->header.type = GarnetObjectType.SortedSet;
            rmwInput->header.flags = 0;
            rmwInput->header.SortedSetOp = SortedSetOperation.ZREM;
            rmwInput->arg1 = 1;

            var status = RMWObjectStoreOperation(key, _inputSlice, out var output, ref objectStoreContext);

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
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            zremCount = 0;

            if (key.Length == 0 || members.Length == 0)
                return GarnetStatus.OK;

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
            rmwInput->header.type = GarnetObjectType.SortedSet;
            rmwInput->header.flags = 0;
            rmwInput->header.SortedSetOp = SortedSetOperation.ZREM;
            rmwInput->arg1 = members.Length;

            var inputLength = sizeof(ObjectInputHeader);
            foreach (var member in members)
            {
                var tmp = scratchBufferManager.FormatScratchAsResp(0, member);
                inputLength += tmp.Length;
            }
            var input = scratchBufferManager.GetSliceFromTail(inputLength);

            var status = RMWObjectStoreOperation(key, input, out var output, ref objectStoreContext);

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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
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
                    var minArgSlice = new ArgSlice(ptr, minBytes.Length);
                    var maxArgSlice = new ArgSlice(ptr2, maxBytes.Length);
                    var _inputSlice = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, minArgSlice, maxArgSlice);

                    // Prepare header in input buffer
                    var rmwInput = (ObjectInputHeader*)_inputSlice.ptr;
                    rmwInput->header.type = GarnetObjectType.SortedSet;
                    rmwInput->header.flags = 0;
                    rmwInput->header.SortedSetOp = SortedSetOperation.ZREMRANGEBYLEX;
                    rmwInput->arg1 = 3;

                    status = RMWObjectStoreOperation(key.ToArray(), _inputSlice, out var output, ref objectStoreContext);
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
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
                    var minArgSlice = new ArgSlice(ptr, minBytes.Length);
                    var maxArgSlice = new ArgSlice(ptr2, maxBytes.Length);
                    var _inputSlice = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, minArgSlice, maxArgSlice);

                    // Prepare header in input buffer
                    var rmwInput = (ObjectInputHeader*)_inputSlice.ptr;
                    rmwInput->header.type = GarnetObjectType.SortedSet;
                    rmwInput->header.flags = 0;
                    rmwInput->header.SortedSetOp = SortedSetOperation.ZREMRANGEBYSCORE;
                    rmwInput->arg1 = 3;

                    status = RMWObjectStoreOperation(key.ToArray(), _inputSlice, out var output, ref objectStoreContext);
                    countRemoved = output.result1;
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
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
                    var startArgSlice = new ArgSlice(ptr, startBytes.Length);
                    var stopArgSlice = new ArgSlice(ptr2, stopBytes.Length);
                    var _inputSlice = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, startArgSlice, stopArgSlice);

                    // Prepare header in input buffer
                    var rmwInput = (ObjectInputHeader*)_inputSlice.ptr;
                    rmwInput->header.type = GarnetObjectType.SortedSet;
                    rmwInput->header.flags = 0;
                    rmwInput->header.SortedSetOp = SortedSetOperation.ZREMRANGEBYRANK;
                    rmwInput->arg1 = 3;

                    status = RMWObjectStoreOperation(key.ToArray(), _inputSlice, out var output, ref objectStoreContext);
                    countRemoved = output.result1;
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
        public unsafe GarnetStatus SortedSetPop<TObjectContext>(ArgSlice key, int count, bool lowScoresFirst, out (ArgSlice score, ArgSlice member)[] pairs, ref TObjectContext objectStoreContext)
                where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            pairs = default;
            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare header in input buffer
            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);

            var inputPtr = (ObjectInputHeader*)input.ptr;
            inputPtr->header.type = GarnetObjectType.SortedSet;
            inputPtr->header.flags = 0;
            inputPtr->header.SortedSetOp = lowScoresFirst ? SortedSetOperation.ZPOPMIN : SortedSetOperation.ZPOPMAX;
            inputPtr->arg1 = count;

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), input, ref objectStoreContext, ref outputFooter);

            //process output
            //if (status == GarnetStatus.OK)
            ProcessRespArrayOutput(outputFooter, out _);

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
                where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            newScore = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var incrementBytes = Encoding.ASCII.GetBytes(increment.ToString(CultureInfo.InvariantCulture));

            GarnetStatus status;
            fixed (byte* ptr = incrementBytes)
            {
                var incrementArgSlice = new ArgSlice(ptr, incrementBytes.Length);
                var _inputSlice = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, incrementArgSlice, member);

                // Prepare header in input buffer
                var rmwInput = (ObjectInputHeader*)_inputSlice.ptr;
                rmwInput->header.type = GarnetObjectType.SortedSet;
                rmwInput->header.flags = 0;
                rmwInput->header.SortedSetOp = SortedSetOperation.ZINCRBY;
                rmwInput->arg1 = 3;

                var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
                status = RMWObjectStoreOperationWithOutput(key.ToArray(), _inputSlice, ref objectStoreContext, ref outputFooter);

                //Process output
                string error = default;
                if (status == GarnetStatus.OK)
                {
                    var result = ProcessRespArrayOutput(outputFooter, out error);
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            zcardCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);
            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.SortedSet;
            rmwInput->header.flags = 0;
            rmwInput->header.SortedSetOp = SortedSetOperation.ZCARD;
            rmwInput->arg1 = 1;

            var status = ReadObjectStoreOperation(key.ToArray(), input, out ObjectOutputHeader output, ref objectStoreContext);

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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            elements = default;
            error = default;

            //min and max are required
            if (min.Length == 0 || max.Length == 0)
            {
                //error in arguments
                error = "Missins required min and max parameters";
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

            // Prepare header in input buffer
            var inputPtr = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
            inputPtr->header.type = GarnetObjectType.SortedSet;
            inputPtr->header.flags = 0;
            inputPtr->header.SortedSetOp = sortedOperation;
            inputPtr->arg1 = 2 + (operation != default ? 1 : 0) + (sortedOperation != SortedSetOperation.ZREVRANGE && reverse ? 1 : 0) + (limit != default ? 3 : 0);

            var inputLength = sizeof(ObjectInputHeader);

            // min and max parameters
            var tmp = scratchBufferManager.FormatScratchAsResp(0, min, max);
            inputLength += tmp.Length;

            //operation order
            if (operation != default)
            {
                fixed (byte* ptrOp = operation)
                {
                    tmp = scratchBufferManager.FormatScratchAsResp(0, new ArgSlice(ptrOp, operation.Length));
                }
                inputLength += tmp.Length;
            }

            //reverse
            if (sortedOperation != SortedSetOperation.ZREVRANGE && reverse)
            {
                ReadOnlySpan<byte> reverseBytes = "REV"u8;
                fixed (byte* ptrOp = reverseBytes)
                {
                    tmp = scratchBufferManager.FormatScratchAsResp(0, new ArgSlice(ptrOp, reverseBytes.Length));
                }
                inputLength += tmp.Length;
            }

            //limit parameter
            if (limit != default && (sortedSetOrderOperation == SortedSetOrderOperation.ByScore || sortedSetOrderOperation == SortedSetOrderOperation.ByLex))
            {
                ReadOnlySpan<byte> limitBytes = "LIMIT"u8;
                fixed (byte* ptrOp = limitBytes)
                {
                    tmp = scratchBufferManager.FormatScratchAsResp(0, new ArgSlice(ptrOp, limitBytes.Length));
                }
                inputLength += tmp.Length;

                //offset
                var limitOffset = Encoding.ASCII.GetBytes(limit.Item1);
                fixed (byte* ptrOp = limitOffset)
                {
                    tmp = scratchBufferManager.FormatScratchAsResp(0, new ArgSlice(ptrOp, limitOffset.Length));
                    inputLength += tmp.Length;
                }

                //count
                var limitCountLength = NumUtils.NumDigitsInLong(limit.Item2);
                var limitCountBytes = new byte[limitCountLength];
                fixed (byte* ptrCount = limitCountBytes)
                {
                    byte* ptr = (byte*)ptrCount;
                    NumUtils.IntToBytes(limit.Item2, limitCountLength, ref ptr);
                    tmp = scratchBufferManager.FormatScratchAsResp(0, new ArgSlice(ptrCount, limitCountLength));
                    inputLength += tmp.Length;
                }
            }

            var input = scratchBufferManager.GetSliceFromTail(inputLength);
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), input, ref objectContext, ref outputFooter);

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

            bool createTransaction = false;

            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                foreach (var item in keys)
                    txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
                txnManager.Run(true);
            }

            var objectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                var statusOp = GET(keys[0].ToArray(), out GarnetObjectStoreOutput firstObj, ref objectStoreLockableContext);
                if (statusOp == GarnetStatus.OK)
                {
                    if (firstObj.garnetObject is not SortedSetObject firstSortedSet)
                    {
                        return GarnetStatus.WRONGTYPE;
                    }
                    // read the rest of the keys
                    for (var item = 1; item < keys.Length; item++)
                    {
                        statusOp = GET(keys[item].ToArray(), out GarnetObjectStoreOutput nextObj, ref objectStoreLockableContext);
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
        /// Iterates members of SortedSet key and their associated scores using a cursor,
        /// a match pattern and count parameters
        /// </summary>
        /// <param name="key">The key of the sorted set</param>
        /// <param name="cursor">The value of the cursor</param>
        /// <param name="match">The pattern to match the members</param>
        /// <param name="count">Limit number for the response</param>
        /// <param name="items">The list of items for the response</param>
        /// <param name="objectStoreContext"></param>
        public unsafe GarnetStatus SortedSetScan<TObjectContext>(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            items = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            if (String.IsNullOrEmpty(match))
                match = "*";

            // Prepare header in input buffer
            // Header + ObjectScanCountLimit
            var inputSize = ObjectInputHeader.Size + sizeof(int);
            var rmwInput = scratchBufferManager.CreateArgSlice(inputSize).ptr;
            ((ObjectInputHeader*)rmwInput)->header.type = GarnetObjectType.SortedSet;
            ((ObjectInputHeader*)rmwInput)->header.flags = 0;
            ((ObjectInputHeader*)rmwInput)->header.SortedSetOp = SortedSetOperation.ZSCAN;

            // Number of tokens in the input after the header (match, value, count, value)
            ((ObjectInputHeader*)rmwInput)->arg1 = 4;
            ((ObjectInputHeader*)rmwInput)->arg2 = (int)cursor;
            rmwInput += ObjectInputHeader.Size;

            // Object Input Limit
            (*(int*)rmwInput) = ObjectScanCountLimit;
            int inputLength = sizeof(ObjectInputHeader) + sizeof(int);

            ArgSlice tmp;
            // Write match
            var matchPatternValue = Encoding.ASCII.GetBytes(match.Trim());
            fixed (byte* matchKeywordPtr = CmdStrings.MATCH, matchPatterPtr = matchPatternValue)
            {
                tmp = scratchBufferManager.FormatScratchAsResp(0, new ArgSlice(matchKeywordPtr, CmdStrings.MATCH.Length),
                            new ArgSlice(matchPatterPtr, matchPatternValue.Length));
            }
            inputLength += tmp.Length;

            // Write count
            int lengthCountNumber = NumUtils.NumDigits(count);
            byte[] countBytes = new byte[lengthCountNumber];

            fixed (byte* countPtr = CmdStrings.COUNT, countValuePtr = countBytes)
            {
                byte* countValuePtr2 = countValuePtr;
                NumUtils.IntToBytes(count, lengthCountNumber, ref countValuePtr2);

                tmp = scratchBufferManager.FormatScratchAsResp(0, new ArgSlice(countPtr, CmdStrings.COUNT.Length),
                          new ArgSlice(countValuePtr, countBytes.Length));
            }
            inputLength += tmp.Length;

            var input = scratchBufferManager.GetSliceFromTail(inputLength);

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), input, ref objectStoreContext, ref outputFooter);

            items = default;
            if (status == GarnetStatus.OK)
                items = ProcessRespArrayOutput(outputFooter, out _, isScanOutput: true);

            return status;

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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            rank = null;
            if (key.Length == 0)
                return GarnetStatus.OK;

            var inputSlice = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, member);
            var rawInput = (ObjectInputHeader*)inputSlice.ptr;
            rawInput->header.type = GarnetObjectType.SortedSet;
            rawInput->header.flags = 0;
            rawInput->header.SortedSetOp = reverse ? SortedSetOperation.ZREVRANK : SortedSetOperation.ZRANK;
            rawInput->arg1 = 1;

            const int outputContainerSize = 32; // 3 for HEADER + CRLF + 20 for ascii long
            var outputContainer = stackalloc byte[outputContainerSize];
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(outputContainer, outputContainerSize) };

            var status = ReadObjectStoreOperationWithOutput(key.ToArray(), inputSlice, ref objectStoreContext, ref outputFooter);

            if (status == GarnetStatus.OK)
            {
                Debug.Assert(*outputContainer == (byte)'$' || *outputContainer == (byte)':');
                if (*outputContainer == (byte)':')
                {
                    // member exists -> read the rank
                    bool read = RespReadUtils.Read64Int(out var rankValue, ref outputContainer, &outputContainer[outputContainerSize]);
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
        public GarnetStatus SortedSetAdd<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        => RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);

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
        public GarnetStatus SortedSetRemove<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);

        /// <summary>
        /// Returns the number of members of the sorted set.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetLength<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => ReadObjectStoreOperation(key, input, out output, ref objectStoreContext);

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
        public GarnetStatus SortedSetRange<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => ReadObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

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
        public GarnetStatus SortedSetScore<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => ReadObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

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
        public GarnetStatus SortedSetScores<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => ReadObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

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
        public GarnetStatus SortedSetPop<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => RMWObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

        /// <summary>
        /// Returns the number of elements in the sorted set at key with a score between min and max.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetCount<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => ReadObjectStoreOperation(key, input, out output, ref objectContext);

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
        public GarnetStatus SortedSetRemoveRangeByLex<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => RMWObjectStoreOperation(key, input, out output, ref objectContext);

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
        public GarnetStatus SortedSetLengthByValue<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => ReadObjectStoreOperation(key, input, out output, ref objectStoreContext);

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
        public GarnetStatus SortedSetIncrement<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => RMWObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

        /// <summary>
        /// ZREMRANGEBYRANK: Removes all elements in the sorted set stored at key with rank between start and stop.
        /// Both start and stop are 0 -based indexes with 0 being the element with the lowest score.
        /// ZREMRANGEBYSCORE: Removes all elements in the sorted set stored at key with a score between min and max (inclusive by default).
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRemoveRange<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => RMWObjectStoreOperation(key, input, out output, ref objectContext);

        /// <summary>
        /// Returns the rank of member in the sorted set, the scores in the sorted set are ordered from low to high
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRank<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => ReadObjectStoreOperationWithOutput(key, input, ref objectContext, ref outputFooter);

        /// <summary>
        /// Returns a random member from the sorted set key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus SortedSetRandomMember<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => ReadObjectStoreOperationWithOutput(key, input, ref objectContext, ref outputFooter);

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
        public GarnetStatus SortedSetScan<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
         where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
           => ReadObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);
    }
}