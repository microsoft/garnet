// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi<TKeyLocker, TEpochGuard> : IGarnetApi<TKeyLocker, TEpochGuard>, IGarnetWatchApi<TKeyLocker, TEpochGuard>
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
    {
        #region SortedSet Methods

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetAdd(ArgSlice key, ArgSlice score, ArgSlice member, out int zaddCount)
            => storageSession.SortedSetAdd<TKeyLocker, TEpochGuard>(key, score, member, out zaddCount);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetAdd(ArgSlice key, (ArgSlice score, ArgSlice member)[] inputs, out int zaddCount)
            => storageSession.SortedSetAdd<TKeyLocker, TEpochGuard>(key, inputs, out zaddCount);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetAdd(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            => storageSession.SortedSetAdd<TKeyLocker, TEpochGuard>(key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemove(ArgSlice key, ArgSlice member, out int zremCount)
            => storageSession.SortedSetRemove<TKeyLocker, TEpochGuard>(key.ToArray(), member, out zremCount);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemove(ArgSlice key, ArgSlice[] members, out int zaddCount)
            => storageSession.SortedSetRemove<TKeyLocker, TEpochGuard>(key.ToArray(), members, out zaddCount);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemove(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SortedSetRemove<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetLength(ArgSlice key, out int len)
            => storageSession.SortedSetLength<TKeyLocker, TEpochGuard>(key, out len);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SortedSetLength<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRange(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetRange<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetScore(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetScore<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetScores(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetScores<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetPop(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetPop<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetPop(ArgSlice key, out (ArgSlice member, ArgSlice score)[] pairs, int count = 1, bool lowScoresFirst = true)
            => storageSession.SortedSetPop<TKeyLocker, TEpochGuard>(key, count, lowScoresFirst, out pairs);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetCount(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            => storageSession.SortedSetCount<TKeyLocker, TEpochGuard>(key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetLengthByValue(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SortedSetLengthByValue<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemoveRangeByLex(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SortedSetRemoveRangeByLex<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemoveRangeByLex(ArgSlice key, string min, string max, out int countRemoved)
            => storageSession.SortedSetRemoveRangeByLex<TKeyLocker, TEpochGuard>(key, min, max, out countRemoved);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemoveRangeByScore(ArgSlice key, string min, string max, out int countRemoved)
            => storageSession.SortedSetRemoveRangeByScore<TKeyLocker, TEpochGuard>(key, min, max, out countRemoved);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemoveRangeByRank(ArgSlice key, int start, int stop, out int countRemoved)
            => storageSession.SortedSetRemoveRangeByRank<TKeyLocker, TEpochGuard>(key, start, stop, out countRemoved);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetIncrement(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetIncrement<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetIncrement(ArgSlice key, double increment, ArgSlice member, out double newScore)
            => storageSession.SortedSetIncrement<TKeyLocker, TEpochGuard>(key, increment, member, out newScore);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemoveRange(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetRemoveRange<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRank(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetRank<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRank(ArgSlice key, ArgSlice member, bool reverse, out long? rank)
            => storageSession.SortedSetRank<TKeyLocker, TEpochGuard>(key, member, reverse, out rank);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRandomMember(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SortedSetRandomMember<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRange(ArgSlice key, ArgSlice min, ArgSlice max, SortedSetOrderOperation sortedSetOrderOperation, out ArgSlice[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
            => storageSession.SortedSetRange<TKeyLocker, TEpochGuard>(key, min, max, sortedSetOrderOperation, out elements, out error, withScores, reverse, limit);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetDifference(ArgSlice[] keys, out Dictionary<byte[], double> pairs)
            => storageSession.SortedSetDifference(keys, out pairs);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            => storageSession.ObjectScan<TKeyLocker, TEpochGuard>(GarnetObjectType.SortedSet, key, cursor, match, count, out items);

        #endregion

        #region Geospatial commands

        /// <inheritdoc />
        public readonly GarnetStatus GeoAdd(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.GeoAdd<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus GeoCommands(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.GeoCommands<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        #endregion

        #region List Methods

        #region PUSHPOP

        /// <inheritdoc />
        public readonly GarnetStatus ListRightPush(ArgSlice key, ArgSlice element, out int itemsCount, bool whenExists = false)
            => storageSession.ListPush<TKeyLocker, TEpochGuard>(key, element, whenExists ? ListOperation.RPUSHX : ListOperation.RPUSH, out itemsCount);

        /// <inheritdoc />
        public readonly GarnetStatus ListRightPush(ArgSlice key, ArgSlice[] elements, out int itemsCount, bool whenExists = false)
            => storageSession.ListPush<TKeyLocker, TEpochGuard>(key, elements, whenExists ? ListOperation.RPUSHX : ListOperation.RPUSH, out itemsCount);

        /// <inheritdoc />
        public readonly GarnetStatus ListRightPush(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
             => storageSession.ListPush<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus ListLeftPush(ArgSlice key, ArgSlice[] elements, out int itemsCount, bool onlyWhenExists = false)
            => storageSession.ListPush<TKeyLocker, TEpochGuard>(key, elements, onlyWhenExists ? ListOperation.LPUSHX : ListOperation.LPUSH, out itemsCount);

        /// <inheritdoc />
        public readonly GarnetStatus ListLeftPush(ArgSlice key, ArgSlice element, out int count, bool onlyWhenExists = false)
            => storageSession.ListPush<TKeyLocker, TEpochGuard>(key, element, onlyWhenExists ? ListOperation.LPUSHX : ListOperation.LPUSH, out count);

        /// <inheritdoc />
        public readonly GarnetStatus ListLeftPush(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.ListPush<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus ListLeftPop(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.ListPop<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly unsafe GarnetStatus ListLeftPop(ArgSlice key, out ArgSlice element)
             => storageSession.ListPop<TKeyLocker, TEpochGuard>(key, ListOperation.LPOP, out element);

        /// <inheritdoc />
        public readonly GarnetStatus ListLeftPop(ArgSlice key, int count, out ArgSlice[] poppedElements)
             => storageSession.ListPop<TKeyLocker, TEpochGuard>(key, count, ListOperation.LPOP, out poppedElements);

        /// <inheritdoc />
        public readonly GarnetStatus ListLeftPop(ArgSlice[] keys, int count, out ArgSlice poppedKey, out ArgSlice[] poppedElements)
            => storageSession.ListPopMultiple<TKeyLocker, TEpochGuard>(keys, OperationDirection.Left, count, out poppedKey, out poppedElements);

        /// <inheritdoc />
        public readonly GarnetStatus ListRightPop(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.ListPop<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly unsafe GarnetStatus ListRightPop(ArgSlice key, out ArgSlice element)
            => storageSession.ListPop<TKeyLocker, TEpochGuard>(key, ListOperation.RPOP, out element);

        /// <inheritdoc />
        public readonly GarnetStatus ListRightPop(ArgSlice key, int count, out ArgSlice[] poppedElements)
            => storageSession.ListPop<TKeyLocker, TEpochGuard>(key, count, ListOperation.RPOP, out poppedElements);

        /// <inheritdoc />
        public readonly GarnetStatus ListRightPop(ArgSlice[] keys, int count, out ArgSlice poppedKey, out ArgSlice[] poppedElements)
            => storageSession.ListPopMultiple<TKeyLocker, TEpochGuard>(keys, OperationDirection.Right, count, out poppedKey, out poppedElements);

        #endregion

        /// <inheritdoc />
        public readonly GarnetStatus ListLength(ArgSlice key, out int count)
            => storageSession.ListLength<TKeyLocker, TEpochGuard>(key, out count);

        /// <inheritdoc />
        public readonly GarnetStatus ListLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.ListLength<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus ListMove(ArgSlice source, ArgSlice destination, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] element)
            => storageSession.ListMove(source, destination, sourceDirection, destinationDirection, out element);

        /// <inheritdoc />
        public readonly bool ListTrim(ArgSlice key, int start, int stop)
            => storageSession.ListTrim<TKeyLocker, TEpochGuard>(key, start, stop);

        /// <inheritdoc />
        public readonly GarnetStatus ListTrim(byte[] key, ref ObjectInput input)
            => storageSession.ListTrim<TKeyLocker, TEpochGuard>(key, ref input);

        /// <inheritdoc />
        public readonly GarnetStatus ListRange(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.ListRange<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus ListInsert(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.ListInsert<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus ListIndex(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
             => storageSession.ListIndex<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus ListRemove(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.ListRemove<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus ListSet(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)

            => storageSession.ListSet<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        #endregion

        #region Set Methods

        /// <inheritdoc />
        public readonly GarnetStatus SetAdd(ArgSlice key, ArgSlice member, out int saddCount)
            => storageSession.SetAdd<TKeyLocker, TEpochGuard>(key, member, out saddCount);

        /// <inheritdoc />
        public readonly GarnetStatus SetAdd(ArgSlice key, ArgSlice[] members, out int saddCount)
            => storageSession.SetAdd<TKeyLocker, TEpochGuard>(key, members, out saddCount);

        /// <inheritdoc />
        public readonly GarnetStatus SetAdd(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SetAdd<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SetRemove(ArgSlice key, ArgSlice member, out int sremCount)
            => storageSession.SetRemove<TKeyLocker, TEpochGuard>(key, member, out sremCount);

        /// <inheritdoc />
        public readonly GarnetStatus SetRemove(ArgSlice key, ArgSlice[] members, out int sremCount)
            => storageSession.SetRemove<TKeyLocker, TEpochGuard>(key, members, out sremCount);

        /// <inheritdoc />
        public readonly GarnetStatus SetRemove(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SetRemove<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SetLength(ArgSlice key, out int count)
            => storageSession.SetLength<TKeyLocker, TEpochGuard>(key, out count);

        /// <inheritdoc />
        public readonly GarnetStatus SetLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.SetLength<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SetMembers(ArgSlice key, out ArgSlice[] members)
            => storageSession.SetMembers<TKeyLocker, TEpochGuard>(key, out members);

        /// <inheritdoc />
        public readonly GarnetStatus SetMembers(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SetMembers<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SetIsMember(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SetIsMember<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SetPop(ArgSlice key, out ArgSlice member)
            => storageSession.SetPop<TKeyLocker, TEpochGuard>(key, out member);

        /// <inheritdoc />
        public readonly GarnetStatus SetPop(ArgSlice key, int count, out ArgSlice[] members)
            => storageSession.SetPop<TKeyLocker, TEpochGuard>(key, count, out members);

        /// <inheritdoc />
        public readonly GarnetStatus SetPop(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SetPop<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SetRandomMember(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.SetRandomMember<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            => storageSession.ObjectScan<TKeyLocker, TEpochGuard>(GarnetObjectType.Set, key, cursor, match, count, out items);

        /// <inheritdoc />
        public readonly GarnetStatus SetMove(ArgSlice sourceKey, ArgSlice destinationKey, ArgSlice member, out int smoveResult)
            => storageSession.SetMove(sourceKey, destinationKey, member, out smoveResult);

        public readonly GarnetStatus SetUnion(ArgSlice[] keys, out HashSet<byte[]> output)
            => storageSession.SetUnion(keys, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SetUnionStore(byte[] key, ArgSlice[] keys, out int count)
            => storageSession.SetUnionStore(key, keys, out count);

        /// <inheritdoc />
        public readonly GarnetStatus SetDiff(ArgSlice[] keys, out HashSet<byte[]> members)
            => storageSession.SetDiff(keys, out members);

        /// <inheritdoc />
        public readonly GarnetStatus SetDiffStore(byte[] key, ArgSlice[] keys, out int count)
            => storageSession.SetDiffStore(key, keys, out count);

        /// <inheritdoc />
        public readonly GarnetStatus SetIntersect(ArgSlice[] keys, out HashSet<byte[]> output)
            => storageSession.SetIntersect(keys, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SetIntersectStore(byte[] key, ArgSlice[] keys, out int count)
            => storageSession.SetIntersectStore(key, keys, out count);

        #endregion

        #region Hash Methods

        /// <inheritdoc />
        public readonly GarnetStatus HashSet(ArgSlice key, ArgSlice field, ArgSlice value, out int count)
            => storageSession.HashSet<TKeyLocker, TEpochGuard>(key, field, value, out count);

        /// <inheritdoc />
        public readonly GarnetStatus HashSetWhenNotExists(ArgSlice key, ArgSlice field, ArgSlice value, out int count)
            => storageSession.HashSet<TKeyLocker, TEpochGuard>(key, field, value, out count, nx: true);

        /// <inheritdoc />
        public readonly GarnetStatus HashSet(ArgSlice key, (ArgSlice field, ArgSlice value)[] elements, out int count)
            => storageSession.HashSet<TKeyLocker, TEpochGuard>(key, elements, out count);

        /// <inheritdoc />
        public readonly GarnetStatus HashSet(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.HashSet<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus HashDelete(ArgSlice key, ArgSlice field, out int count)
            => storageSession.HashDelete<TKeyLocker, TEpochGuard>(key, field, out count);

        /// <inheritdoc />
        public readonly GarnetStatus HashDelete(ArgSlice key, ArgSlice[] fields, out int count)
            => storageSession.HashDelete<TKeyLocker, TEpochGuard>(key, fields, out count);

        /// <inheritdoc />
        public readonly GarnetStatus HashGet(ArgSlice key, ArgSlice field, out ArgSlice value)
            => storageSession.HashGet<TKeyLocker, TEpochGuard>(key, field, out value);

        /// <inheritdoc />
        public readonly GarnetStatus HashGetAll(ArgSlice key, out ArgSlice[] values)
            => storageSession.HashGetAll<TKeyLocker, TEpochGuard>(key, out values);

        /// <inheritdoc />
        public readonly GarnetStatus HashGet(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashGet<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashGetAll(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashGetAll<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashGetMultiple(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashGetMultiple<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashGetMultiple(ArgSlice key, ArgSlice[] fields, out ArgSlice[] values)
            => storageSession.HashGetMultiple<TKeyLocker, TEpochGuard>(key, fields, out values);

        /// <inheritdoc />
        public readonly GarnetStatus HashLength(ArgSlice key, out int count)
            => storageSession.HashLength<TKeyLocker, TEpochGuard>(key, out count);

        /// <inheritdoc />
        public readonly GarnetStatus HashLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.HashLength<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus HashStrLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.HashStrLength<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus HashExists(ArgSlice key, ArgSlice field, out bool exists)
            => storageSession.HashExists<TKeyLocker, TEpochGuard>(key, field, out exists);

        /// <inheritdoc />
        public readonly GarnetStatus HashExists(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.HashExists<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus HashRandomField(ArgSlice key, out ArgSlice field)
            => storageSession.HashRandomField<TKeyLocker, TEpochGuard>(key, out field);

        /// <inheritdoc />
        public readonly GarnetStatus HashRandomField(ArgSlice key, int count, bool withValues, out ArgSlice[] fields)
            => storageSession.HashRandomField<TKeyLocker, TEpochGuard>(key, count, withValues, out fields);

        /// <inheritdoc />
        public readonly GarnetStatus HashRandomField(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashRandomField<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashDelete(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            => storageSession.HashDelete<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus HashKeys(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashKeys<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashVals(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashVals<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashIncrement(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            => storageSession.HashIncrement<TKeyLocker, TEpochGuard>(key, input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus HashIncrement(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            => storageSession.HashIncrement<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            => storageSession.ObjectScan<TKeyLocker, TEpochGuard>(GarnetObjectType.Hash, key, cursor, match, count, out items);

        #endregion
    }

}