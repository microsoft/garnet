// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Garnet API implementation
    /// </summary>
    internal partial struct GarnetApi : IGarnetApi, IGarnetWatchApi
    {
        #region SortedSet Methods

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice score, ArgSlice member, out int zaddCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetAdd(key, score, member, out zaddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd<TKeyLocker, TEpochGuard>(ArgSlice key, (ArgSlice score, ArgSlice member)[] inputs, out int zaddCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetAdd(key, inputs, out zaddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetAdd<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetAdd(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemove<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice member, out int zremCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemove(key.ToArray(), member, out zremCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemove<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] members, out int zaddCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemove(key.ToArray(), members, out zaddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemove<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemove(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int len)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetLength(key, out len, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetLength(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRange<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRange(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetScore<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetScore(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetScores<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetScores(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetPop<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetPop(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetPop<TKeyLocker, TEpochGuard>(ArgSlice key, out (ArgSlice member, ArgSlice score)[] pairs, int count = 1, bool lowScoresFirst = true)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetPop(key, count, lowScoresFirst, out pairs, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetCount<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetCount(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetLengthByValue<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetLengthByValue(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByLex<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemoveRangeByLex(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByLex<TKeyLocker, TEpochGuard>(ArgSlice key, string min, string max, out int countRemoved)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemoveRangeByLex(key, min, max, out countRemoved, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByScore<TKeyLocker, TEpochGuard>(ArgSlice key, string min, string max, out int countRemoved)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemoveRangeByScore(key, min, max, out countRemoved, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRangeByRank<TKeyLocker, TEpochGuard>(ArgSlice key, int start, int stop, out int countRemoved)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemoveRangeByRank(key, start, stop, out countRemoved, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetIncrement<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetIncrement(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetIncrement<TKeyLocker, TEpochGuard>(ArgSlice key, double increment, ArgSlice member, out double newScore)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetIncrement(key, increment, member, out newScore, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRemoveRange<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemoveRange(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRank<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRank(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRank<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice member, bool reverse, out long? rank)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRank(key, member, reverse, out rank, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRandomMember<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRandomMember(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SortedSetRange<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice min, ArgSlice max, SortedSetOrderOperation sortedSetOrderOperation, out ArgSlice[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRange(key, min, max, sortedSetOrderOperation, ref objectContext, out elements, out error, withScores, reverse, limit);

        /// <inheritdoc />
        public GarnetStatus SortedSetDifference<TKeyLocker, TEpochGuard>(ArgSlice[] keys, out Dictionary<byte[], double> pairs)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetDifference(keys, out pairs);

        /// <inheritdoc />
        public GarnetStatus SortedSetScan<TKeyLocker, TEpochGuard>(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ObjectScan(GarnetObjectType.SortedSet, key, cursor, match, count, out items, ref objectContext);

        #endregion

        #region Geospatial commands

        /// <inheritdoc />
        public GarnetStatus GeoAdd<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GeoAdd(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus GeoCommands<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GeoCommands(key, ref input, ref outputFooter, ref objectContext);

        #endregion

        #region List Methods

        #region PUSHPOP

        /// <inheritdoc />
        public GarnetStatus ListRightPush<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice element, out int itemsCount, bool whenExists = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPush(key, element, whenExists ? ListOperation.RPUSHX : ListOperation.RPUSH, out itemsCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRightPush<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] elements, out int itemsCount, bool whenExists = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPush(key, elements, whenExists ? ListOperation.RPUSHX : ListOperation.RPUSH, out itemsCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRightPush<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => storageSession.ListPush(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPush<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] elements, out int itemsCount, bool onlyWhenExists = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPush(key, elements, onlyWhenExists ? ListOperation.LPUSHX : ListOperation.LPUSH, out itemsCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPush<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice element, out int count, bool onlyWhenExists = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPush(key, element, onlyWhenExists ? ListOperation.LPUSHX : ListOperation.LPUSH, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPush<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPush(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListLeftPop<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPop(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public unsafe GarnetStatus ListLeftPop<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice element)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => storageSession.ListPop(key, ListOperation.LPOP, ref objectContext, out element);

        /// <inheritdoc />
        public GarnetStatus ListLeftPop<TKeyLocker, TEpochGuard>(ArgSlice key, int count, out ArgSlice[] poppedElements)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => storageSession.ListPop(key, count, ListOperation.LPOP, ref objectContext, out poppedElements);

        /// <inheritdoc />
        public GarnetStatus ListLeftPop<TKeyLocker, TEpochGuard>(ArgSlice[] keys, int count, out ArgSlice poppedKey, out ArgSlice[] poppedElements)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPopMultiple(keys, OperationDirection.Left, count, ref objectContext, out poppedKey, out poppedElements);

        /// <inheritdoc />
        public GarnetStatus ListRightPop<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPop(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public unsafe GarnetStatus ListRightPop<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice element)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPop(key, ListOperation.RPOP, ref objectContext, out element);

        /// <inheritdoc />
        public GarnetStatus ListRightPop<TKeyLocker, TEpochGuard>(ArgSlice key, int count, out ArgSlice[] poppedElements)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPop(key, count, ListOperation.RPOP, ref objectContext, out poppedElements);

        /// <inheritdoc />
        public GarnetStatus ListRightPop<TKeyLocker, TEpochGuard>(ArgSlice[] keys, int count, out ArgSlice poppedKey, out ArgSlice[] poppedElements)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPopMultiple(keys, OperationDirection.Right, count, ref objectContext, out poppedKey, out poppedElements);

        #endregion

        /// <inheritdoc />
        public GarnetStatus ListLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListLength(key, ref objectContext, out count);

        /// <inheritdoc />
        public GarnetStatus ListLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListLength(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListMove<TKeyLocker, TEpochGuard>(ArgSlice source, ArgSlice destination, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] element)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListMove(source, destination, sourceDirection, destinationDirection, out element);

        /// <inheritdoc />
        public bool ListTrim(ArgSlice key, int start, int stop)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListTrim(key, start, stop, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListTrim<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListTrim(key, ref input, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRange<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListRange(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListInsert<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListInsert(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListIndex<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => storageSession.ListIndex(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListRemove<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListRemove(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus ListSet<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListSet(key, ref input, ref outputFooter, ref objectContext);

        #endregion

        #region Set Methods

        /// <inheritdoc />
        public GarnetStatus SetAdd<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice member, out int saddCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetAdd(key, member, out saddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetAdd<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] members, out int saddCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetAdd(key, members, out saddCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetAdd<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetAdd(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRemove<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice member, out int sremCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetRemove(key, member, out sremCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRemove<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] members, out int sremCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetRemove(key, members, out sremCount, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRemove<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetRemove(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetLength(key, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetLength(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetMembers<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice[] members)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetMembers(key, out members, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetMembers<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetMembers(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetIsMember<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetIsMember(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetPop<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice member)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetPop(key, out member, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetPop<TKeyLocker, TEpochGuard>(ArgSlice key, int count, out ArgSlice[] members)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetPop(key, count, out members, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetPop<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetPop(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetRandomMember<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetRandomMember(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetScan<TKeyLocker, TEpochGuard>(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ObjectScan(GarnetObjectType.Set, key, cursor, match, count, out items, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SetMove<TKeyLocker, TEpochGuard>(ArgSlice sourceKey, ArgSlice destinationKey, ArgSlice member, out int smoveResult)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetMove(sourceKey, destinationKey, member, out smoveResult);

        public GarnetStatus SetUnion<TKeyLocker, TEpochGuard>(ArgSlice[] keys, out HashSet<byte[]> output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetUnion(keys, out output);

        /// <inheritdoc />
        public GarnetStatus SetUnionStore<TKeyLocker, TEpochGuard>(byte[] key, ArgSlice[] keys, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetUnionStore(key, keys, out count);

        /// <inheritdoc />
        public GarnetStatus SetDiff<TKeyLocker, TEpochGuard>(ArgSlice[] keys, out HashSet<byte[]> members)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetDiff(keys, out members);

        /// <inheritdoc />
        public GarnetStatus SetDiffStore<TKeyLocker, TEpochGuard>(byte[] key, ArgSlice[] keys, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetDiffStore(key, keys, out count);

        /// <inheritdoc />
        public GarnetStatus SetIntersect<TKeyLocker, TEpochGuard>(ArgSlice[] keys, out HashSet<byte[]> output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetIntersect(keys, out output);

        /// <inheritdoc />
        public GarnetStatus SetIntersectStore<TKeyLocker, TEpochGuard>(byte[] key, ArgSlice[] keys, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetIntersectStore(key, keys, out count);

        #endregion

        #region Hash Methods

        /// <inheritdoc />
        public GarnetStatus HashSet<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, ArgSlice value, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashSet(key, field, value, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashSetWhenNotExists<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, ArgSlice value, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashSet(key, field, value, out count, ref objectContext, nx: true);

        /// <inheritdoc />
        public GarnetStatus HashSet<TKeyLocker, TEpochGuard>(ArgSlice key, (ArgSlice field, ArgSlice value)[] elements, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashSet(key, elements, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashSet<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashSet(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashDelete<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashDelete(key, field, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashDelete<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] fields, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashDelete(key, fields, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGet<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, out ArgSlice value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashGet(key, field, out value, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetAll<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice[] values)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashGetAll(key, out values, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGet<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashGet(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetAll<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashGetAll(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetMultiple<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashGetMultiple(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashGetMultiple<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] fields, out ArgSlice[] values)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashGetMultiple(key, fields, out values, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashLength(key, out count, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashLength(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashStrLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashStrLength(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashExists<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, out bool exists)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashExists(key, field, out exists, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashExists<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashExists(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashRandomField<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice field)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashRandomField(key, out field, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashRandomField<TKeyLocker, TEpochGuard>(ArgSlice key, int count, bool withValues, out ArgSlice[] fields)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashRandomField(key, count, withValues, out fields, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashRandomField<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashRandomField(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashDelete<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashDelete(key, ref input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashKeys<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashKeys(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashVals<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashVals(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashIncrement<TKeyLocker, TEpochGuard>(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashIncrement(key, input, out output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashIncrement<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashIncrement(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus HashScan<TKeyLocker, TEpochGuard>(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ObjectScan(GarnetObjectType.Hash, key, cursor, match, count, out items, ref objectContext);

        #endregion
    }

}