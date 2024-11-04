// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi : IGarnetApi, IGarnetWatchApi
    {
        #region SortedSet Methods

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetAdd<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice score, ArgSlice member, out int zaddCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetAdd<TKeyLocker, TEpochGuard>(key, score, member, out zaddCount);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetAdd<TKeyLocker, TEpochGuard>(ArgSlice key, (ArgSlice score, ArgSlice member)[] inputs, out int zaddCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetAdd<TKeyLocker, TEpochGuard>(key, inputs, out zaddCount);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetAdd<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetAdd<TKeyLocker, TEpochGuard>(key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemove<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice member, out int zremCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemove<TKeyLocker, TEpochGuard>(key.ToArray(), member, out zremCount);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemove<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] members, out int zaddCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemove<TKeyLocker, TEpochGuard>(key.ToArray(), members, out zaddCount);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemove<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemove<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int len)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetLength<TKeyLocker, TEpochGuard>(key, out len);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetLength<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRange<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRange<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetScore<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetScore<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetScores<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetScores<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetPop<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetPop<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetPop<TKeyLocker, TEpochGuard>(ArgSlice key, out (ArgSlice member, ArgSlice score)[] pairs, int count = 1, bool lowScoresFirst = true)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetPop<TKeyLocker, TEpochGuard>(key, count, lowScoresFirst, out pairs);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetCount<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetCount<TKeyLocker, TEpochGuard>(key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetLengthByValue<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetLengthByValue<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemoveRangeByLex<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemoveRangeByLex<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemoveRangeByLex<TKeyLocker, TEpochGuard>(ArgSlice key, string min, string max, out int countRemoved)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemoveRangeByLex<TKeyLocker, TEpochGuard>(key, min, max, out countRemoved);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemoveRangeByScore<TKeyLocker, TEpochGuard>(ArgSlice key, string min, string max, out int countRemoved)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemoveRangeByScore<TKeyLocker, TEpochGuard>(key, min, max, out countRemoved);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemoveRangeByRank<TKeyLocker, TEpochGuard>(ArgSlice key, int start, int stop, out int countRemoved)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemoveRangeByRank<TKeyLocker, TEpochGuard>(key, start, stop, out countRemoved);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetIncrement<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetIncrement<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetIncrement<TKeyLocker, TEpochGuard>(ArgSlice key, double increment, ArgSlice member, out double newScore)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetIncrement<TKeyLocker, TEpochGuard>(key, increment, member, out newScore);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRemoveRange<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRemoveRange<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRank<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRank<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRank<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice member, bool reverse, out long? rank)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRank<TKeyLocker, TEpochGuard>(key, member, reverse, out rank);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRandomMember<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRandomMember<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRange<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice min, ArgSlice max, SortedSetOrderOperation sortedSetOrderOperation, out ArgSlice[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SortedSetRange<TKeyLocker, TEpochGuard>(key, min, max, sortedSetOrderOperation, out elements, out error, withScores, reverse, limit);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetDifference(ArgSlice[] keys, out Dictionary<byte[], double> pairs)
            => storageSession.SortedSetDifference(keys, out pairs);

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetScan<TKeyLocker, TEpochGuard>(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ObjectScan<TKeyLocker, TEpochGuard>(GarnetObjectType.SortedSet, key, cursor, match, count, out items);

        #endregion

        #region Geospatial commands

        /// <inheritdoc />
        public readonly GarnetStatus GeoAdd<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GeoAdd<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus GeoCommands<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GeoCommands<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        #endregion

        #region List Methods

        #region PUSHPOP

        /// <inheritdoc />
        public readonly GarnetStatus ListRightPush<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice element, out int itemsCount, bool whenExists = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPush<TKeyLocker, TEpochGuard>(key, element, whenExists ? ListOperation.RPUSHX : ListOperation.RPUSH, out itemsCount);

        /// <inheritdoc />
        public readonly GarnetStatus ListRightPush<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] elements, out int itemsCount, bool whenExists = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPush<TKeyLocker, TEpochGuard>(key, elements, whenExists ? ListOperation.RPUSHX : ListOperation.RPUSH, out itemsCount);

        /// <inheritdoc />
        public readonly GarnetStatus ListRightPush<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => storageSession.ListPush<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus ListLeftPush<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] elements, out int itemsCount, bool onlyWhenExists = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPush<TKeyLocker, TEpochGuard>(key, elements, onlyWhenExists ? ListOperation.LPUSHX : ListOperation.LPUSH, out itemsCount);

        /// <inheritdoc />
        public readonly GarnetStatus ListLeftPush<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice element, out int count, bool onlyWhenExists = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPush<TKeyLocker, TEpochGuard>(key, element, onlyWhenExists ? ListOperation.LPUSHX : ListOperation.LPUSH, out count);

        /// <inheritdoc />
        public readonly GarnetStatus ListLeftPush<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPush<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus ListLeftPop<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPop<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly unsafe GarnetStatus ListLeftPop<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice element)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => storageSession.ListPop<TKeyLocker, TEpochGuard>(key, ListOperation.LPOP, out element);

        /// <inheritdoc />
        public readonly GarnetStatus ListLeftPop<TKeyLocker, TEpochGuard>(ArgSlice key, int count, out ArgSlice[] poppedElements)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => storageSession.ListPop<TKeyLocker, TEpochGuard>(key, count, ListOperation.LPOP, out poppedElements);

        /// <inheritdoc />
        public readonly GarnetStatus ListLeftPop<TKeyLocker, TEpochGuard>(ArgSlice[] keys, int count, out ArgSlice poppedKey, out ArgSlice[] poppedElements)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPopMultiple<TKeyLocker, TEpochGuard>(keys, OperationDirection.Left, count, out poppedKey, out poppedElements);

        /// <inheritdoc />
        public readonly GarnetStatus ListRightPop<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPop<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly unsafe GarnetStatus ListRightPop<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice element)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPop<TKeyLocker, TEpochGuard>(key, ListOperation.RPOP, out element);

        /// <inheritdoc />
        public readonly GarnetStatus ListRightPop<TKeyLocker, TEpochGuard>(ArgSlice key, int count, out ArgSlice[] poppedElements)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPop<TKeyLocker, TEpochGuard>(key, count, ListOperation.RPOP, out poppedElements);

        /// <inheritdoc />
        public readonly GarnetStatus ListRightPop<TKeyLocker, TEpochGuard>(ArgSlice[] keys, int count, out ArgSlice poppedKey, out ArgSlice[] poppedElements)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListPopMultiple<TKeyLocker, TEpochGuard>(keys, OperationDirection.Right, count, out poppedKey, out poppedElements);

        #endregion

        /// <inheritdoc />
        public readonly GarnetStatus ListLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListLength<TKeyLocker, TEpochGuard>(key, out count);

        /// <inheritdoc />
        public readonly GarnetStatus ListLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListLength<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus ListMove(ArgSlice source, ArgSlice destination, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] element)
            => storageSession.ListMove(source, destination, sourceDirection, destinationDirection, out element);

        /// <inheritdoc />
        public readonly bool ListTrim<TKeyLocker, TEpochGuard>(ArgSlice key, int start, int stop)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListTrim<TKeyLocker, TEpochGuard>(key, start, stop);

        /// <inheritdoc />
        public readonly GarnetStatus ListTrim<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListTrim<TKeyLocker, TEpochGuard>(key, ref input);

        /// <inheritdoc />
        public readonly GarnetStatus ListRange<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListRange<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus ListInsert<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListInsert<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus ListIndex<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => storageSession.ListIndex<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus ListRemove<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListRemove<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus ListSet<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ListSet<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        #endregion

        #region Set Methods

        /// <inheritdoc />
        public readonly GarnetStatus SetAdd<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice member, out int saddCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetAdd<TKeyLocker, TEpochGuard>(key, member, out saddCount);

        /// <inheritdoc />
        public readonly GarnetStatus SetAdd<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] members, out int saddCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetAdd<TKeyLocker, TEpochGuard>(key, members, out saddCount);

        /// <inheritdoc />
        public readonly GarnetStatus SetAdd<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetAdd<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SetRemove<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice member, out int sremCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetRemove<TKeyLocker, TEpochGuard>(key, member, out sremCount);

        /// <inheritdoc />
        public readonly GarnetStatus SetRemove<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] members, out int sremCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetRemove<TKeyLocker, TEpochGuard>(key, members, out sremCount);

        /// <inheritdoc />
        public readonly GarnetStatus SetRemove<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetRemove<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SetLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetLength<TKeyLocker, TEpochGuard>(key, out count);

        /// <inheritdoc />
        public readonly GarnetStatus SetLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetLength<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus SetMembers<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice[] members)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetMembers<TKeyLocker, TEpochGuard>(key, out members);

        /// <inheritdoc />
        public readonly GarnetStatus SetMembers<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetMembers<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SetIsMember<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetIsMember<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SetPop<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice member)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetPop<TKeyLocker, TEpochGuard>(key, out member);

        /// <inheritdoc />
        public readonly GarnetStatus SetPop<TKeyLocker, TEpochGuard>(ArgSlice key, int count, out ArgSlice[] members)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetPop<TKeyLocker, TEpochGuard>(key, count, out members);

        /// <inheritdoc />
        public readonly GarnetStatus SetPop<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetPop<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SetRandomMember<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SetRandomMember<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus SetScan<TKeyLocker, TEpochGuard>(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
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
        public readonly GarnetStatus HashSet<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, ArgSlice value, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashSet<TKeyLocker, TEpochGuard>(key, field, value, out count);

        /// <inheritdoc />
        public readonly GarnetStatus HashSetWhenNotExists<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, ArgSlice value, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashSet<TKeyLocker, TEpochGuard>(key, field, value, out count, nx: true);

        /// <inheritdoc />
        public readonly GarnetStatus HashSet<TKeyLocker, TEpochGuard>(ArgSlice key, (ArgSlice field, ArgSlice value)[] elements, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashSet<TKeyLocker, TEpochGuard>(key, elements, out count);

        /// <inheritdoc />
        public readonly GarnetStatus HashSet<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashSet<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus HashDelete<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashDelete<TKeyLocker, TEpochGuard>(key, field, out count);

        /// <inheritdoc />
        public readonly GarnetStatus HashDelete<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] fields, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashDelete<TKeyLocker, TEpochGuard>(key, fields, out count);

        /// <inheritdoc />
        public readonly GarnetStatus HashGet<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, out ArgSlice value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashGet<TKeyLocker, TEpochGuard>(key, field, out value);

        /// <inheritdoc />
        public readonly GarnetStatus HashGetAll<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice[] values)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashGetAll<TKeyLocker, TEpochGuard>(key, out values);

        /// <inheritdoc />
        public readonly GarnetStatus HashGet<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashGet<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashGetAll<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashGetAll<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashGetMultiple<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashGetMultiple<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashGetMultiple<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] fields, out ArgSlice[] values)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashGetMultiple<TKeyLocker, TEpochGuard>(key, fields, out values);

        /// <inheritdoc />
        public readonly GarnetStatus HashLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashLength<TKeyLocker, TEpochGuard>(key, out count);

        /// <inheritdoc />
        public readonly GarnetStatus HashLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashLength<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus HashStrLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashStrLength<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus HashExists<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, out bool exists)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashExists<TKeyLocker, TEpochGuard>(key, field, out exists);

        /// <inheritdoc />
        public readonly GarnetStatus HashExists<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashExists<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus HashRandomField<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice field)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashRandomField<TKeyLocker, TEpochGuard>(key, out field);

        /// <inheritdoc />
        public readonly GarnetStatus HashRandomField<TKeyLocker, TEpochGuard>(ArgSlice key, int count, bool withValues, out ArgSlice[] fields)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashRandomField<TKeyLocker, TEpochGuard>(key, count, withValues, out fields);

        /// <inheritdoc />
        public readonly GarnetStatus HashRandomField<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashRandomField<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashDelete<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashDelete<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus HashKeys<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashKeys<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashVals<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashVals<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashIncrement<TKeyLocker, TEpochGuard>(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashIncrement<TKeyLocker, TEpochGuard>(key, input, out output);

        /// <inheritdoc />
        public readonly GarnetStatus HashIncrement<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HashIncrement<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <inheritdoc />
        public readonly GarnetStatus HashScan<TKeyLocker, TEpochGuard>(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.ObjectScan<TKeyLocker, TEpochGuard>(GarnetObjectType.Hash, key, cursor, match, count, out items);

        #endregion
    }

}