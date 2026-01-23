// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi<TStringContext, TObjectContext, TUnifiedContext> : IGarnetApi, IGarnetWatchApi
        where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
    {
        #region SortedSet Methods

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd(PinnedSpanByte key, PinnedSpanByte score, PinnedSpanByte member, out int zaddCount)
            => storageSession.SortedSetAdd(key, score, member, out zaddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd(PinnedSpanByte key, (PinnedSpanByte score, PinnedSpanByte member)[] inputs, out int zaddCount)
            => storageSession.SortedSetAdd(key, inputs, out zaddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetAdd(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRangeStore(PinnedSpanByte dstKey, PinnedSpanByte srcKey, ref ObjectInput input, out int result)
            => storageSession.SortedSetRangeStore(dstKey, srcKey, ref input, out result, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemove(PinnedSpanByte key, PinnedSpanByte member, out int zremCount)
            => storageSession.SortedSetRemove(key, member, out zremCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemove(PinnedSpanByte key, PinnedSpanByte[] members, out int zaddCount)
            => storageSession.SortedSetRemove(key, members, out zaddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemove(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetRemove(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetLength(PinnedSpanByte key, out int len)
            => storageSession.SortedSetLength(key, out len, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetLength(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetLength(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRange(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetRange(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetScore(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetScore(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetScores(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetScores(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetPop(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetPop(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetMPop(ReadOnlySpan<PinnedSpanByte> keys, int count, bool lowScoresFirst, out PinnedSpanByte poppedKey, out (PinnedSpanByte member, PinnedSpanByte score)[] pairs)
            => storageSession.SortedSetMPop(keys, count, lowScoresFirst, out poppedKey, out pairs);

        /// <inheritdoc />
        public GarnetStatus SortedSetPop(PinnedSpanByte key, out (PinnedSpanByte member, PinnedSpanByte score)[] pairs, int count = 1, bool lowScoresFirst = true)
            => storageSession.SortedSetPop(key, count, lowScoresFirst, out pairs, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetCount(PinnedSpanByte key, PinnedSpanByte minScore, PinnedSpanByte maxScore, out int numElements)
            => storageSession.SortedSetCount(key, minScore, maxScore, out numElements, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetCount(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetCount(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetLengthByValue(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetLengthByValue(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByLex(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetRemoveRangeByLex(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByLex(PinnedSpanByte key, string min, string max, out int countRemoved)
            => storageSession.SortedSetRemoveRangeByLex(key, min, max, out countRemoved, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByScore(PinnedSpanByte key, string min, string max, out int countRemoved)
            => storageSession.SortedSetRemoveRangeByScore(key, min, max, out countRemoved, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByRank(PinnedSpanByte key, int start, int stop, out int countRemoved)
            => storageSession.SortedSetRemoveRangeByRank(key, start, stop, out countRemoved, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetIncrement(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetIncrement(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetIncrement(PinnedSpanByte key, double increment, PinnedSpanByte member, out double newScore)
            => storageSession.SortedSetIncrement(key, increment, member, out newScore, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRange(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetRemoveRange(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRank(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetRank(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRank(PinnedSpanByte key, PinnedSpanByte member, bool reverse, out long? rank)
            => storageSession.SortedSetRank(key, member, reverse, out rank, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRandomMember(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetRandomMember(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRange(PinnedSpanByte key, PinnedSpanByte min, PinnedSpanByte max, SortedSetOrderOperation sortedSetOrderOperation, out PinnedSpanByte[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
            => storageSession.SortedSetRange(key, min, max, sortedSetOrderOperation, ref objectContext, out elements, out error, withScores, reverse, limit);

        /// <inheritdoc />
        public GarnetStatus SortedSetDifference(PinnedSpanByte[] keys, out SortedSet<(double, byte[])> pairs)
            => storageSession.SortedSetDifference(keys, out pairs);

        /// <inheritdoc />
        public GarnetStatus SortedSetUnion(ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out SortedSet<(double, byte[])> pairs)
            => storageSession.SortedSetUnion(keys, weights, aggregateType, out pairs);

        /// <inheritdoc />
        public GarnetStatus SortedSetDifferenceStore(PinnedSpanByte destinationKey, ReadOnlySpan<PinnedSpanByte> keys, out int count)
            => storageSession.SortedSetDifferenceStore(destinationKey, keys, out count);

        /// <inheritdoc />
        public GarnetStatus SortedSetUnionStore(PinnedSpanByte destinationKey, ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, ref MetaCommandInfo metaCommandInfo, out int count)
            => storageSession.SortedSetUnionStore(destinationKey, keys, weights, aggregateType, ref metaCommandInfo, out count);

        /// <inheritdoc />
        public GarnetStatus SortedSetScan(PinnedSpanByte key, long cursor, string match, int count, out PinnedSpanByte[] items)
            => storageSession.ObjectScan(GarnetObjectType.SortedSet, key, cursor, match, count, out items, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetIntersect(ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out SortedSet<(double, byte[])> pairs)
            => storageSession.SortedSetIntersect(keys, weights, aggregateType, out pairs);

        /// <inheritdoc />
        public GarnetStatus SortedSetIntersectLength(ReadOnlySpan<PinnedSpanByte> keys, int? limit, out int count)
            => storageSession.SortedSetIntersectLength(keys, limit, out count);

        /// <inheritdoc />
        public GarnetStatus SortedSetIntersectStore(PinnedSpanByte destinationKey, ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out int count)
            => storageSession.SortedSetIntersectStore(destinationKey, keys, weights, aggregateType, out count);

        /// <inheritdoc />
        public GarnetStatus SortedSetExpire(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetExpire(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetExpire(PinnedSpanByte key, ReadOnlySpan<PinnedSpanByte> members, DateTimeOffset expireAt, ExpireOption expireOption, out int[] results)
            => storageSession.SortedSetExpire(key, members, expireAt, expireOption, out results, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetPersist(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetPersist(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetPersist(PinnedSpanByte key, ReadOnlySpan<PinnedSpanByte> members, out int[] results)
            => storageSession.SortedSetPersist(key, members, out results, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetTimeToLive(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SortedSetTimeToLive(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetTimeToLive(PinnedSpanByte key, ReadOnlySpan<PinnedSpanByte> members, out TimeSpan[] expireIn)
            => storageSession.SortedSetTimeToLive(key, members, out expireIn, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetCollect(ReadOnlySpan<PinnedSpanByte> keys, ref ObjectInput input)
            => storageSession.SortedSetCollect(keys, ref input, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetCollect()
            => storageSession.SortedSetCollect(ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetCollect(ReadOnlySpan<PinnedSpanByte> keys)
            => storageSession.SortedSetCollect(keys, ref objectContext);

        #endregion

        #region Geospatial commands

        /// <inheritdoc />
        public GarnetStatus GeoAdd(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.GeoAdd(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus GeoCommands(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.GeoCommands(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus GeoSearchReadOnly(PinnedSpanByte key, ref GeoSearchOptions opts,
                                      ref ObjectInput input, ref SpanByteAndMemory output)
            => storageSession.GeoSearchReadOnly(key, ref opts, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus GeoSearchStore(PinnedSpanByte key, PinnedSpanByte destinationKey, ref GeoSearchOptions opts,
                                           ref ObjectInput input, ref SpanByteAndMemory output)
            => storageSession.GeoSearchStore(key, destinationKey, ref opts, ref input, ref output, ref objectContext);
        #endregion

        #region List Methods

        #region PUSHPOP

        /// <inheritdoc />
        public GarnetStatus ListRightPush(PinnedSpanByte key, PinnedSpanByte element, out int itemsCount, bool whenExists = false)
            => storageSession.ListPush(key, element, whenExists ? ListOperation.RPUSHX : ListOperation.RPUSH, out itemsCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRightPush(PinnedSpanByte key, PinnedSpanByte[] elements, out int itemsCount, bool whenExists = false)
            => storageSession.ListPush(key, elements, whenExists ? ListOperation.RPUSHX : ListOperation.RPUSH, out itemsCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRightPush(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
             => storageSession.ListPush(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPush(PinnedSpanByte key, PinnedSpanByte[] elements, out int itemsCount, bool onlyWhenExists = false)
            => storageSession.ListPush(key, elements, onlyWhenExists ? ListOperation.LPUSHX : ListOperation.LPUSH, out itemsCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPush(PinnedSpanByte key, PinnedSpanByte element, out int count, bool onlyWhenExists = false)
            => storageSession.ListPush(key, element, onlyWhenExists ? ListOperation.LPUSHX : ListOperation.LPUSH, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPush(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.ListPush(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListPosition(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.ListPosition(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPop(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.ListPop(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public unsafe GarnetStatus ListLeftPop(PinnedSpanByte key, out PinnedSpanByte element)
             => storageSession.ListPop(key, ListOperation.LPOP, ref objectContext, out element);

        /// <inheritdoc />
        public GarnetStatus ListLeftPop(PinnedSpanByte key, int count, out PinnedSpanByte[] poppedElements)
             => storageSession.ListPop(key, count, ListOperation.LPOP, ref objectContext, out poppedElements);

        /// <inheritdoc />
        public GarnetStatus ListLeftPop(PinnedSpanByte[] keys, int count, out PinnedSpanByte poppedKey, out PinnedSpanByte[] poppedElements)
            => storageSession.ListPopMultiple(keys, OperationDirection.Left, count, ref objectContext, out poppedKey, out poppedElements);

        /// <inheritdoc />
        public GarnetStatus ListRightPop(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.ListPop(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public unsafe GarnetStatus ListRightPop(PinnedSpanByte key, out PinnedSpanByte element)
            => storageSession.ListPop(key, ListOperation.RPOP, ref objectContext, out element);

        /// <inheritdoc />
        public GarnetStatus ListRightPop(PinnedSpanByte key, int count, out PinnedSpanByte[] poppedElements)
            => storageSession.ListPop(key, count, ListOperation.RPOP, ref objectContext, out poppedElements);

        /// <inheritdoc />
        public GarnetStatus ListRightPop(PinnedSpanByte[] keys, int count, out PinnedSpanByte poppedKey, out PinnedSpanByte[] poppedElements)
            => storageSession.ListPopMultiple(keys, OperationDirection.Right, count, ref objectContext, out poppedKey, out poppedElements);

        #endregion

        /// <inheritdoc />
        public GarnetStatus ListLength(PinnedSpanByte key, out int count)
            => storageSession.ListLength(key, ref objectContext, out count);

        /// <inheritdoc />
        public GarnetStatus ListLength(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.ListLength(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListMove(PinnedSpanByte source, PinnedSpanByte destination, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] element)
            => storageSession.ListMove(source, destination, sourceDirection, destinationDirection, out element);

        /// <inheritdoc />
        public bool ListTrim(PinnedSpanByte key, int start, int stop)
            => storageSession.ListTrim(key, start, stop, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListTrim(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.ListTrim(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRange(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.ListRange(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListInsert(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.ListInsert(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListIndex(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
             => storageSession.ListIndex(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRemove(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.ListRemove(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListSet(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.ListSet(key, ref input, ref output, ref objectContext);

        #endregion

        #region Set Methods

        /// <inheritdoc />
        public GarnetStatus SetAdd(PinnedSpanByte key, PinnedSpanByte member, out int saddCount)
            => storageSession.SetAdd(key, member, out saddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetAdd(PinnedSpanByte key, PinnedSpanByte[] members, out int saddCount)
            => storageSession.SetAdd(key, members, out saddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetAdd(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SetAdd(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRemove(PinnedSpanByte key, PinnedSpanByte member, out int sremCount)
            => storageSession.SetRemove(key, member, out sremCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRemove(PinnedSpanByte key, PinnedSpanByte[] members, out int sremCount)
            => storageSession.SetRemove(key, members, out sremCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRemove(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SetRemove(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetLength(PinnedSpanByte key, out int count)
            => storageSession.SetLength(key, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetLength(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SetLength(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetMembers(PinnedSpanByte key, out PinnedSpanByte[] members)
            => storageSession.SetMembers(key, out members, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetMembers(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SetMembers(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetIsMember(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SetIsMember(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetIsMember(PinnedSpanByte key, PinnedSpanByte[] members, out int[] result)
            => storageSession.SetIsMember(key, members, out result, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetPop(PinnedSpanByte key, out PinnedSpanByte member)
            => storageSession.SetPop(key, out member, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetPop(PinnedSpanByte key, int count, out PinnedSpanByte[] members)
            => storageSession.SetPop(key, count, out members, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetPop(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SetPop(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRandomMember(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.SetRandomMember(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetScan(PinnedSpanByte key, long cursor, string match, int count, out PinnedSpanByte[] items)
            => storageSession.ObjectScan(GarnetObjectType.Set, key, cursor, match, count, out items, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetMove(PinnedSpanByte sourceKey, PinnedSpanByte destinationKey, PinnedSpanByte member, out int smoveResult)
            => storageSession.SetMove(sourceKey, destinationKey, member, out smoveResult);

        public GarnetStatus SetUnion(PinnedSpanByte[] keys, out HashSet<byte[]> output)
            => storageSession.SetUnion(keys, out output);

        /// <inheritdoc />
        public GarnetStatus SetUnionStore(PinnedSpanByte key, PinnedSpanByte[] keys, out int count)
            => storageSession.SetUnionStore(key, keys, out count);

        /// <inheritdoc />
        public GarnetStatus SetDiff(PinnedSpanByte[] keys, out HashSet<byte[]> members)
            => storageSession.SetDiff(keys, out members);

        /// <inheritdoc />
        public GarnetStatus SetDiffStore(PinnedSpanByte key, PinnedSpanByte[] keys, out int count)
            => storageSession.SetDiffStore(key, keys, out count);

        /// <inheritdoc />
        public GarnetStatus SetIntersect(PinnedSpanByte[] keys, out HashSet<byte[]> output)
            => storageSession.SetIntersect(keys, out output);

        /// <inheritdoc />
        public GarnetStatus SetIntersectLength(ReadOnlySpan<PinnedSpanByte> keys, int? limit, out int count)
            => storageSession.SetIntersectLength(keys, limit, out count);

        /// <inheritdoc />
        public GarnetStatus SetIntersectStore(PinnedSpanByte key, PinnedSpanByte[] keys, out int count)
            => storageSession.SetIntersectStore(key, keys, out count);

        #endregion

        #region Hash Methods

        /// <inheritdoc />
        public GarnetStatus HashSet(PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value, out int count)
            => storageSession.HashSet(key, field, value, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashSetWhenNotExists(PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value, out int count)
            => storageSession.HashSet(key, field, value, out count, ref objectContext, nx: true);

        /// <inheritdoc />
        public GarnetStatus HashSet(PinnedSpanByte key, (PinnedSpanByte field, PinnedSpanByte value)[] elements, out int count)
         => storageSession.HashSet(key, elements, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashSet(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output)
            => storageSession.HashSet(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashDelete(PinnedSpanByte key, PinnedSpanByte field, out int count)
        => storageSession.HashDelete(key, field, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashDelete(PinnedSpanByte key, PinnedSpanByte[] fields, out int count)
        => storageSession.HashDelete(key, fields, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGet(PinnedSpanByte key, PinnedSpanByte field, out PinnedSpanByte value)
         => storageSession.HashGet(key, field, out value, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetAll(PinnedSpanByte key, out PinnedSpanByte[] values)
        => storageSession.HashGetAll(key, out values, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGet(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        => storageSession.HashGet(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetAll(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.HashGetAll(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetMultiple(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.HashGetMultiple(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetMultiple(PinnedSpanByte key, PinnedSpanByte[] fields, out PinnedSpanByte[] values)
            => storageSession.HashGetMultiple(key, fields, out values, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashLength(PinnedSpanByte key, out int count)
        => storageSession.HashLength(key, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashLength(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.HashLength(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashStrLength(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.HashStrLength(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashExists(PinnedSpanByte key, PinnedSpanByte field, out bool exists)
            => storageSession.HashExists(key, field, out exists, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashExists(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output)
            => storageSession.HashExists(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashRandomField(PinnedSpanByte key, out PinnedSpanByte field)
        => storageSession.HashRandomField(key, out field, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashRandomField(PinnedSpanByte key, int count, bool withValues, out PinnedSpanByte[] fields)
        => storageSession.HashRandomField(key, count, withValues, out fields, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashRandomField(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.HashRandomField(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashDelete(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.HashDelete(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashKeys(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.HashKeys(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashVals(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.HashVals(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashIncrement(PinnedSpanByte key, PinnedSpanByte input, out OutputHeader output)
            => storageSession.HashIncrement(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashIncrement(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.HashIncrement(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashExpire(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.HashExpire(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashPersist(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.HashPersist(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashScan(PinnedSpanByte key, long cursor, string match, int count, out PinnedSpanByte[] items)
            => storageSession.ObjectScan(GarnetObjectType.Hash, key, cursor, match, count, out items, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashTimeToLive(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.HashTimeToLive(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashCollect(ReadOnlySpan<PinnedSpanByte> keys, ref ObjectInput input)
            => storageSession.HashCollect(keys, ref input, ref objectContext);

        #endregion
    }
}