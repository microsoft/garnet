// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = ObjectAllocator<IGarnetObject, StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>;

    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi<TContext, TObjectContext> : IGarnetApi, IGarnetWatchApi
        where TContext : ITsavoriteContext<SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        where TObjectContext : ITsavoriteContext<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
    {
        #region SortedSet Methods

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd(ArgSlice key, ArgSlice score, ArgSlice member, out int zaddCount)
            => storageSession.SortedSetAdd(key, score, member, out zaddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd(ArgSlice key, (ArgSlice score, ArgSlice member)[] inputs, out int zaddCount)
            => storageSession.SortedSetAdd(key, inputs, out zaddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            => storageSession.SortedSetAdd(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRangeStore(ArgSlice dstKey, ArgSlice srcKey, ref ObjectInput input, out int result)
            => storageSession.SortedSetRangeStore(dstKey, srcKey, ref input, out result, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemove(ArgSlice key, ArgSlice member, out int zremCount)
            => storageSession.SortedSetRemove(key.SpanByte, member, out zremCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemove(ArgSlice key, ArgSlice[] members, out int zaddCount)
            => storageSession.SortedSetRemove(key.SpanByte, members, out zaddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemove(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SortedSetRemove(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetLength(ArgSlice key, out int len)
            => storageSession.SortedSetLength(key, out len, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetLength(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SortedSetLength(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRange(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetRange(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetScore(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetScore(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetScores(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetScores(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetPop(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetPop(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetMPop(ReadOnlySpan<ArgSlice> keys, int count, bool lowScoresFirst, out ArgSlice poppedKey, out (ArgSlice member, ArgSlice score)[] pairs)
            => storageSession.SortedSetMPop(keys, count, lowScoresFirst, out poppedKey, out pairs);

        /// <inheritdoc />
        public GarnetStatus SortedSetPop(ArgSlice key, out (ArgSlice member, ArgSlice score)[] pairs, int count = 1, bool lowScoresFirst = true)
            => storageSession.SortedSetPop(key, count, lowScoresFirst, out pairs, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetCount(ArgSlice key, ArgSlice minScore, ArgSlice maxScore, out int numElements)
            => storageSession.SortedSetCount(key, minScore, maxScore, out numElements, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetCount(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            => storageSession.SortedSetCount(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetLengthByValue(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SortedSetLengthByValue(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByLex(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SortedSetRemoveRangeByLex(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByLex(ArgSlice key, string min, string max, out int countRemoved)
            => storageSession.SortedSetRemoveRangeByLex(key, min, max, out countRemoved, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByScore(ArgSlice key, string min, string max, out int countRemoved)
            => storageSession.SortedSetRemoveRangeByScore(key, min, max, out countRemoved, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByRank(ArgSlice key, int start, int stop, out int countRemoved)
            => storageSession.SortedSetRemoveRangeByRank(key, start, stop, out countRemoved, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetIncrement(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetIncrement(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetIncrement(ArgSlice key, double increment, ArgSlice member, out double newScore)
            => storageSession.SortedSetIncrement(key, increment, member, out newScore, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRange(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetRemoveRange(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRank(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetRank(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRank(ArgSlice key, ArgSlice member, bool reverse, out long? rank)
            => storageSession.SortedSetRank(key, member, reverse, out rank, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRandomMember(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetRandomMember(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRange(ArgSlice key, ArgSlice min, ArgSlice max, SortedSetOrderOperation sortedSetOrderOperation, out ArgSlice[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
            => storageSession.SortedSetRange(key, min, max, sortedSetOrderOperation, ref objectContext, out elements, out error, withScores, reverse, limit);

        /// <inheritdoc />
        public GarnetStatus SortedSetDifference(ArgSlice[] keys, out Dictionary<byte[], double> pairs)
            => storageSession.SortedSetDifference(keys, out pairs);

        /// <inheritdoc />
        public GarnetStatus SortedSetUnion(ReadOnlySpan<ArgSlice> keys, double[] weights, SortedSetAggregateType aggregateType, out Dictionary<byte[], double> pairs)
            => storageSession.SortedSetUnion(keys, weights, aggregateType, out pairs);

        /// <inheritdoc />
        public GarnetStatus SortedSetDifferenceStore(ArgSlice destinationKey, ReadOnlySpan<ArgSlice> keys, out int count)
            => storageSession.SortedSetDifferenceStore(destinationKey, keys, out count);

        public GarnetStatus SortedSetUnionStore(ArgSlice destinationKey, ReadOnlySpan<ArgSlice> keys, double[] weights, SortedSetAggregateType aggregateType, out int count)
            => storageSession.SortedSetUnionStore(destinationKey, keys, weights, aggregateType, out count);

        /// <inheritdoc />
        public GarnetStatus SortedSetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            => storageSession.ObjectScan(GarnetObjectType.SortedSet, key, cursor, match, count, out items, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetIntersect(ReadOnlySpan<ArgSlice> keys, double[] weights, SortedSetAggregateType aggregateType, out Dictionary<byte[], double> pairs)
            => storageSession.SortedSetIntersect(keys, weights, aggregateType, out pairs);

        /// <inheritdoc />
        public GarnetStatus SortedSetIntersectLength(ReadOnlySpan<ArgSlice> keys, int? limit, out int count)
            => storageSession.SortedSetIntersectLength(keys, limit, out count);

        /// <inheritdoc />
        public GarnetStatus SortedSetIntersectStore(ArgSlice destinationKey, ReadOnlySpan<ArgSlice> keys, double[] weights, SortedSetAggregateType aggregateType, out int count)
            => storageSession.SortedSetIntersectStore(destinationKey, keys, weights, aggregateType, out count);

        #endregion

        #region Geospatial commands

        /// <inheritdoc />
        public GarnetStatus GeoAdd(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.GeoAdd(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus GeoCommands(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.GeoCommands(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus GeoSearchStore(ArgSlice key, ArgSlice destinationKey, ref ObjectInput input, ref SpanByteAndMemory output)
            => storageSession.GeoSearchStore(key, destinationKey, ref input, ref output, ref objectContext);

        #endregion

        #region List Methods

        #region PUSHPOP

        /// <inheritdoc />
        public GarnetStatus ListRightPush(ArgSlice key, ArgSlice element, out int itemsCount, bool whenExists = false)
            => storageSession.ListPush(key, element, whenExists ? ListOperation.RPUSHX : ListOperation.RPUSH, out itemsCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRightPush(ArgSlice key, ArgSlice[] elements, out int itemsCount, bool whenExists = false)
            => storageSession.ListPush(key, elements, whenExists ? ListOperation.RPUSHX : ListOperation.RPUSH, out itemsCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRightPush(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
             => storageSession.ListPush(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPush(ArgSlice key, ArgSlice[] elements, out int itemsCount, bool onlyWhenExists = false)
            => storageSession.ListPush(key, elements, onlyWhenExists ? ListOperation.LPUSHX : ListOperation.LPUSH, out itemsCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPush(ArgSlice key, ArgSlice element, out int count, bool onlyWhenExists = false)
            => storageSession.ListPush(key, element, onlyWhenExists ? ListOperation.LPUSHX : ListOperation.LPUSH, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPush(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.ListPush(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListPosition(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.ListPosition(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPop(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.ListPop(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public unsafe GarnetStatus ListLeftPop(ArgSlice key, out ArgSlice element)
             => storageSession.ListPop(key, ListOperation.LPOP, ref objectContext, out element);

        /// <inheritdoc />
        public GarnetStatus ListLeftPop(ArgSlice key, int count, out ArgSlice[] poppedElements)
             => storageSession.ListPop(key, count, ListOperation.LPOP, ref objectContext, out poppedElements);

        /// <inheritdoc />
        public GarnetStatus ListLeftPop(ArgSlice[] keys, int count, out ArgSlice poppedKey, out ArgSlice[] poppedElements)
            => storageSession.ListPopMultiple(keys, OperationDirection.Left, count, ref objectContext, out poppedKey, out poppedElements);

        /// <inheritdoc />
        public GarnetStatus ListRightPop(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.ListPop(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public unsafe GarnetStatus ListRightPop(ArgSlice key, out ArgSlice element)
            => storageSession.ListPop(key, ListOperation.RPOP, ref objectContext, out element);

        /// <inheritdoc />
        public GarnetStatus ListRightPop(ArgSlice key, int count, out ArgSlice[] poppedElements)
            => storageSession.ListPop(key, count, ListOperation.RPOP, ref objectContext, out poppedElements);

        /// <inheritdoc />
        public GarnetStatus ListRightPop(ArgSlice[] keys, int count, out ArgSlice poppedKey, out ArgSlice[] poppedElements)
            => storageSession.ListPopMultiple(keys, OperationDirection.Right, count, ref objectContext, out poppedKey, out poppedElements);

        #endregion

        /// <inheritdoc />
        public GarnetStatus ListLength(ArgSlice key, out int count)
            => storageSession.ListLength(key, ref objectContext, out count);

        /// <inheritdoc />
        public GarnetStatus ListLength(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.ListLength(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListMove(ArgSlice source, ArgSlice destination, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] element)
            => storageSession.ListMove(source, destination, sourceDirection, destinationDirection, out element);

        /// <inheritdoc />
        public bool ListTrim(ArgSlice key, int start, int stop)
            => storageSession.ListTrim(key, start, stop, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListTrim(SpanByte key, ref ObjectInput input)
            => storageSession.ListTrim(key, ref input, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRange(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.ListRange(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListInsert(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.ListInsert(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListIndex(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
             => storageSession.ListIndex(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRemove(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.ListRemove(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListSet(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.ListSet(key, ref input, ref outputFooter, ref objectContext);

        #endregion

        #region Set Methods

        /// <inheritdoc />
        public GarnetStatus SetAdd(ArgSlice key, ArgSlice member, out int saddCount)
            => storageSession.SetAdd(key, member, out saddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetAdd(ArgSlice key, ArgSlice[] members, out int saddCount)
            => storageSession.SetAdd(key, members, out saddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetAdd(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SetAdd(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRemove(ArgSlice key, ArgSlice member, out int sremCount)
            => storageSession.SetRemove(key, member, out sremCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRemove(ArgSlice key, ArgSlice[] members, out int sremCount)
            => storageSession.SetRemove(key, members, out sremCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRemove(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SetRemove(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetLength(ArgSlice key, out int count)
            => storageSession.SetLength(key, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetLength(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SetLength(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetMembers(ArgSlice key, out ArgSlice[] members)
            => storageSession.SetMembers(key, out members, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetMembers(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SetMembers(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetIsMember(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SetIsMember(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetIsMember(ArgSlice key, ArgSlice[] members, out int[] result)
            => storageSession.SetIsMember(key, members, out result, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetPop(ArgSlice key, out ArgSlice member)
            => storageSession.SetPop(key, out member, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetPop(ArgSlice key, int count, out ArgSlice[] members)
            => storageSession.SetPop(key, count, out members, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetPop(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SetPop(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRandomMember(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SetRandomMember(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            => storageSession.ObjectScan(GarnetObjectType.Set, key, cursor, match, count, out items, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetMove(ArgSlice sourceKey, ArgSlice destinationKey, ArgSlice member, out int smoveResult)
            => storageSession.SetMove(sourceKey, destinationKey, member, out smoveResult);

        public GarnetStatus SetUnion(ArgSlice[] keys, out HashSet<byte[]> output)
            => storageSession.SetUnion(keys, out output);

        /// <inheritdoc />
        public GarnetStatus SetUnionStore(SpanByte key, ArgSlice[] keys, out int count)
            => storageSession.SetUnionStore(key, keys, out count);

        /// <inheritdoc />
        public GarnetStatus SetDiff(ArgSlice[] keys, out HashSet<byte[]> members)
            => storageSession.SetDiff(keys, out members);

        /// <inheritdoc />
        public GarnetStatus SetDiffStore(SpanByte key, ArgSlice[] keys, out int count)
            => storageSession.SetDiffStore(key, keys, out count);

        /// <inheritdoc />
        public GarnetStatus SetIntersect(ArgSlice[] keys, out HashSet<byte[]> output)
            => storageSession.SetIntersect(keys, out output);

        /// <inheritdoc />
        public GarnetStatus SetIntersectLength(ReadOnlySpan<ArgSlice> keys, int? limit, out int count)
            => storageSession.SetIntersectLength(keys, limit, out count);

        /// <inheritdoc />
        public GarnetStatus SetIntersectStore(SpanByte key, ArgSlice[] keys, out int count)
            => storageSession.SetIntersectStore(key, keys, out count);

        #endregion

        #region Hash Methods

        /// <inheritdoc />
        public GarnetStatus HashSet(ArgSlice key, ArgSlice field, ArgSlice value, out int count)
            => storageSession.HashSet(key, field, value, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashSetWhenNotExists(ArgSlice key, ArgSlice field, ArgSlice value, out int count)
            => storageSession.HashSet(key, field, value, out count, ref objectContext, nx: true);

        /// <inheritdoc />
        public GarnetStatus HashSet(ArgSlice key, (ArgSlice field, ArgSlice value)[] elements, out int count)
         => storageSession.HashSet(key, elements, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashSet(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.HashSet(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashDelete(ArgSlice key, ArgSlice field, out int count)
        => storageSession.HashDelete(key, field, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashDelete(ArgSlice key, ArgSlice[] fields, out int count)
        => storageSession.HashDelete(key, fields, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGet(ArgSlice key, ArgSlice field, out ArgSlice value)
         => storageSession.HashGet(key, field, out value, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetAll(ArgSlice key, out ArgSlice[] values)
        => storageSession.HashGetAll(key, out values, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGet(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        => storageSession.HashGet(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetAll(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashGetAll(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetMultiple(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashGetMultiple(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetMultiple(ArgSlice key, ArgSlice[] fields, out ArgSlice[] values)
            => storageSession.HashGetMultiple(key, fields, out values, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashLength(ArgSlice key, out int count)
        => storageSession.HashLength(key, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashLength(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.HashLength(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashStrLength(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.HashStrLength(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashExists(ArgSlice key, ArgSlice field, out bool exists)
            => storageSession.HashExists(key, field, out exists, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashExists(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.HashExists(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashRandomField(ArgSlice key, out ArgSlice field)
        => storageSession.HashRandomField(key, out field, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashRandomField(ArgSlice key, int count, bool withValues, out ArgSlice[] fields)
        => storageSession.HashRandomField(key, count, withValues, out fields, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashRandomField(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashRandomField(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashDelete(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.HashDelete(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashKeys(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashKeys(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashVals(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashVals(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashIncrement(SpanByte key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.HashIncrement(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashIncrement(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashIncrement(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashExpire(ArgSlice key, long expireAt, bool isMilliseconds, ExpireOption expireOption, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashExpire(key, expireAt, isMilliseconds, expireOption, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashPersist(ArgSlice key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashPersist(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            => storageSession.ObjectScan(GarnetObjectType.Hash, key, cursor, match, count, out items, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashTimeToLive(ArgSlice key, bool isMilliseconds, bool isTimestamp, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashTimeToLive(key, isMilliseconds, isTimestamp, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashCollect(ReadOnlySpan<ArgSlice> keys, ref ObjectInput input)
            => storageSession.HashCollect(keys, ref input, ref objectContext);

        #endregion
    }
}