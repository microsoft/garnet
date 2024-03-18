// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{

    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi<TContext, TObjectContext> : IGarnetApi, IGarnetWatchApi
        where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        #region SortedSet Methods

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd(byte[] key, ArgSlice input, out int zaddCount)
        {
            var status = storageSession.SortedSetAdd(key, input, out var output, ref objectContext);
            zaddCount = output.countDone;
            return status;
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd(ArgSlice key, ArgSlice score, ArgSlice member, out int zaddCount)
            => storageSession.SortedSetAdd(key, score, member, out zaddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd(ArgSlice key, (ArgSlice score, ArgSlice member)[] inputs, out int zaddCount)
            => storageSession.SortedSetAdd(key, inputs, out zaddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.SortedSetAdd(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemove(ArgSlice key, ArgSlice member, out int zremCount)
            => storageSession.SortedSetRemove(key.Bytes, member, out zremCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemove(ArgSlice key, ArgSlice[] members, out int zaddCount)
            => storageSession.SortedSetRemove(key.Bytes, members, out zaddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemove(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.SortedSetRemove(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetLength(ArgSlice key, out int len)
            => storageSession.SortedSetLength(key, out len, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetLength(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.SortedSetLength(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRange(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetRange(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetScore(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetScore(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetPop(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetPop(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetPop(ArgSlice key, out (ArgSlice score, ArgSlice member)[] pairs, int count = 1, bool lowScoresFirst = true)
            => storageSession.SortedSetPop(key, count, lowScoresFirst, out pairs, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetCount(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.SortedSetCount(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetLengthByValue(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.SortedSetLengthByValue(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByLex(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.SortedSetRemoveRangeByLex(key, input, out output, ref objectContext);

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
        public GarnetStatus SortedSetIncrement(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetIncrement(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetIncrement(ArgSlice key, double increment, ArgSlice member, out double newScore)
            => storageSession.SortedSetIncrement(key, increment, member, out newScore, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRange(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.SortedSetRemoveRange(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRank(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.SortedSetRank(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRandomMember(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetRandomMember(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRange(ArgSlice key, ArgSlice min, ArgSlice max, SortedSetOrderOperation sortedSetOrderOperation, out ArgSlice[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
            => storageSession.SortedSetRange(key, min, max, sortedSetOrderOperation, ref objectContext, out elements, out error, withScores, reverse, limit);

        /// <inheritdoc />
        public GarnetStatus SortedSetDifference(ArgSlice[] keys, out Dictionary<byte[], double> pairs)
            => storageSession.SortedSetDifference(keys, out pairs);

        /// <inheritdoc />
        public GarnetStatus SortedSetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            => storageSession.SortedSetScan(key, cursor, match, count, out items, ref objectContext);

        #endregion

        #region Geospatial commands

        /// <inheritdoc />
        public GarnetStatus GeoAdd(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.GeoAdd(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus GeoCommands(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.GeoCommands(key, input, ref outputFooter, ref objectContext);

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
        public GarnetStatus ListRightPush(byte[] key, ArgSlice input, out ObjectOutputHeader output)
             => storageSession.ListPush(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPush(ArgSlice key, ArgSlice[] elements, out int itemsCount, bool onlyWhenExists = false)
            => storageSession.ListPush(key, elements, onlyWhenExists ? ListOperation.LPUSHX : ListOperation.LPUSH, out itemsCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPush(ArgSlice key, ArgSlice element, out int count, bool onlyWhenExists = false)
            => storageSession.ListPush(key, element, onlyWhenExists ? ListOperation.LPUSHX : ListOperation.LPUSH, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPush(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.ListPush(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPop(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.ListPop(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public unsafe GarnetStatus ListLeftPop(ArgSlice key, out ArgSlice element)
             => storageSession.ListPop(key, ListOperation.LPOP, ref objectContext, out element);

        /// <inheritdoc />
        public GarnetStatus ListLeftPop(ArgSlice key, int count, out ArgSlice[] poppedElements)
             => storageSession.ListPop(key, count, ListOperation.LPOP, ref objectContext, out poppedElements);

        /// <inheritdoc />
        public GarnetStatus ListRightPop(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.ListPop(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public unsafe GarnetStatus ListRightPop(ArgSlice key, out ArgSlice element)
            => storageSession.ListPop(key, ListOperation.RPOP, ref objectContext, out element);

        /// <inheritdoc />
        public GarnetStatus ListRightPop(ArgSlice key, int count, out ArgSlice[] poppedElements)
            => storageSession.ListPop(key, count, ListOperation.RPOP, ref objectContext, out poppedElements);

        #endregion

        /// <inheritdoc />
        public GarnetStatus ListLength(ArgSlice key, out int count)
            => storageSession.ListLength(key, ref objectContext, out count);

        /// <inheritdoc />
        public GarnetStatus ListLength(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.ListLength(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public bool ListMove(ArgSlice source, ArgSlice destination, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] element)
            => storageSession.ListMove(source, destination, sourceDirection, destinationDirection, out element);

        /// <inheritdoc />
        public bool ListTrim(ArgSlice key, int start, int stop)
            => storageSession.ListTrim(key, start, stop, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListTrim(byte[] key, ArgSlice input)
            => storageSession.ListTrim(key, input, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRange(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.ListRange(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListInsert(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.ListInsert(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListIndex(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
             => storageSession.ListIndex(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRemove(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.ListRemove(key, input, out output, ref objectContext);

        #endregion

        #region Set Methods

        /// <inheritdoc />
        public GarnetStatus SetAdd(ArgSlice key, ArgSlice member, out int saddCount)
            => storageSession.SetAdd(key, member, out saddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetAdd(ArgSlice key, ArgSlice[] members, out int saddCount)
            => storageSession.SetAdd(key, members, out saddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetAdd(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.SetAdd(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRemove(ArgSlice key, ArgSlice member, out int sremCount)
            => storageSession.SetRemove(key, member, out sremCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRemove(ArgSlice key, ArgSlice[] members, out int sremCount)
            => storageSession.SetRemove(key, members, out sremCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRemove(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.SetRemove(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetLength(ArgSlice key, out int count)
            => storageSession.SetLength(key, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetLength(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.SetLength(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetMembers(ArgSlice key, out ArgSlice[] members)
            => storageSession.SetMembers(key, out members, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetMembers(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SetMembers(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetPop(ArgSlice key, out ArgSlice member)
            => storageSession.SetPop(key, out member, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetPop(ArgSlice key, int count, out ArgSlice[] members)
            => storageSession.SetPop(key, count, out members, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetPop(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SetPop(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            => storageSession.SetScan(key, cursor, match, count, out items, ref objectContext);

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
        public GarnetStatus HashSet(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.HashSet(key, input, out output, ref objectContext);

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
        public GarnetStatus HashGet(ArgSlice key, ArgSlice[] fields, out ArgSlice[] values)
        => storageSession.HashGet(key, fields, out values, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetAll(ArgSlice key, out ArgSlice[] values)
        => storageSession.HashGetAll(key, out values, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGet(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        => storageSession.HashGet(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashLength(ArgSlice key, out int count)
        => storageSession.HashLength(key, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashLength(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.HashLength(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashExists(ArgSlice key, ArgSlice field, out bool exists)
            => storageSession.HashExists(key, field, out exists, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashExists(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.HashExists(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashRandomField(ArgSlice key, out ArgSlice field)
        => storageSession.HashRandomField(key, out field, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashRandomField(ArgSlice key, int count, bool withValues, out ArgSlice[] fields)
        => storageSession.HashRandomField(key, count, withValues, out fields, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashRandomField(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashGet(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashDelete(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.HashDelete(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashKeys(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashKeys(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashVals(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashVals(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashIncrement(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.HashIncrement(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashIncrement(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashIncrement(key, input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashScan(ArgSlice key, long cursor, string match, long count, out ArgSlice[] items)
            => storageSession.HashScan(key, cursor, match, count, out items, ref objectContext);

        #endregion
    }

}