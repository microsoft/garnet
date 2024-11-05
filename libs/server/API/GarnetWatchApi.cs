// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Garnet API implementation for watch purposes
    /// </summary>
    struct GarnetWatchApi : IGarnetReadApi
    {
        readonly GarnetApi garnetApi;

        public GarnetWatchApi(GarnetApi garnetApi)
        {
            this.garnetApi = garnetApi;
        }

        #region GET
        /// <inheritdoc />
        public readonly GarnetStatus GET<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.GET<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus GETForMemoryResult<TKeyLocker, TEpochGuard>(ArgSlice key, out MemoryResult<byte> value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Main);
            return garnetApi.GETForMemoryResult<TKeyLocker, TEpochGuard>(key, out value);
        }

        /// <inheritdoc />
        public readonly GarnetStatus GET<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Main);
            return garnetApi.GET<TKeyLocker, TEpochGuard>(key, out value);
        }

        /// <inheritdoc />
        public readonly GarnetStatus GET<TKeyLocker, TEpochGuard>(byte[] key, out GarnetObjectStoreOutput value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.GET<TKeyLocker, TEpochGuard>(key, out value);
        }
        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public readonly GarnetStatus GETRANGE<TKeyLocker, TEpochGuard>(ref SpanByte key, int sliceStart, int sliceLength, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.GETRANGE<TKeyLocker, TEpochGuard>(ref key, sliceStart, sliceLength, ref output);
        }
        #endregion

        #region TTL
        /// <inheritdoc />
        public readonly GarnetStatus TTL<TKeyLocker, TEpochGuard>(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(new ArgSlice(ref key), storeType);
            return garnetApi.TTL<TKeyLocker, TEpochGuard>(ref key, storeType, ref output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus PTTL<TKeyLocker, TEpochGuard>(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(new ArgSlice(ref key), storeType);
            return garnetApi.PTTL<TKeyLocker, TEpochGuard>(ref key, storeType, ref output);
        }

        #endregion

        #region SortedSet Methods

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int zcardCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SortedSetLength<TKeyLocker, TEpochGuard>(key, out zcardCount);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SortedSetLength<TKeyLocker, TEpochGuard>(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetCount<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SortedSetCount<TKeyLocker, TEpochGuard>(key, ref input, ref output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetLengthByValue<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SortedSetLengthByValue<TKeyLocker, TEpochGuard>(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRandomMember<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SortedSetRandomMember<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRange<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SortedSetRange<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetScore<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SortedSetScore<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetScores<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SortedSetScores<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRank<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SortedSetRank<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRank<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice member, bool reverse, out long? rank)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SortedSetRank<TKeyLocker, TEpochGuard>(key, member, reverse, out rank);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRange<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice min, ArgSlice max, SortedSetOrderOperation sortedSetOrderOperation, out ArgSlice[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SortedSetRange<TKeyLocker, TEpochGuard>(key, min, max, sortedSetOrderOperation, out elements, out error, withScores, reverse, limit);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetDifference<TKeyLocker, TEpochGuard>(ArgSlice[] keys, out Dictionary<byte[], double> pairs)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            foreach (var key in keys)
                garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);

            return garnetApi.SortedSetDifference(keys, out pairs);
        }

        /// <inheritdoc />
        public readonly GarnetStatus GeoCommands<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.GeoCommands<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetScan<TKeyLocker, TEpochGuard>(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SortedSetScan<TKeyLocker, TEpochGuard>(key, cursor, match, count, out items);
        }

        #endregion

        #region List Methods

        /// <inheritdoc />
        public readonly GarnetStatus ListLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.ListLength<TKeyLocker, TEpochGuard>(key, out count);
        }

        /// <inheritdoc />
        public readonly GarnetStatus ListLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.ListLength<TKeyLocker, TEpochGuard>(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus ListRange<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.ListRange<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus ListIndex<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.ListIndex<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        #endregion

        #region Set Methods

        /// <inheritdoc />
        public readonly GarnetStatus SetLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int scardCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SetLength<TKeyLocker, TEpochGuard>(key, out scardCount);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SetLength<TKeyLocker, TEpochGuard>(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetMembers<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice[] members)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SetMembers<TKeyLocker, TEpochGuard>(key, out members);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetIsMember<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SetIsMember<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetMembers<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SetMembers<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetScan<TKeyLocker, TEpochGuard>(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SetScan<TKeyLocker, TEpochGuard>(key, cursor, match, count, out items);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetUnion<TKeyLocker, TEpochGuard>(ArgSlice[] keys, out HashSet<byte[]> output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            foreach (var key in keys)
                garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SetUnion(keys, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetIntersect<TKeyLocker, TEpochGuard>(ArgSlice[] keys, out HashSet<byte[]> output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            foreach (var key in keys)
                garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SetIntersect(keys, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetDiff<TKeyLocker, TEpochGuard>(ArgSlice[] keys, out HashSet<byte[]> output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            foreach (var key in keys)
                garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.SetDiff(keys, out output);
        }
        #endregion

        #region Hash Methods

        /// <inheritdoc />
        public readonly GarnetStatus HashGet<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, out ArgSlice value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashGet<TKeyLocker, TEpochGuard>(key, field, out value);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashGetMultiple<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] fields, out ArgSlice[] values)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashGetMultiple<TKeyLocker, TEpochGuard>(key, fields, out values);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashGetAll<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice[] values)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashGetAll<TKeyLocker, TEpochGuard>(key, out values);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashLength<TKeyLocker, TEpochGuard>(key, out count);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashExists<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, out bool exists)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashExists<TKeyLocker, TEpochGuard>(key, field, out exists);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashRandomField<TKeyLocker, TEpochGuard>(ArgSlice key, int count, bool withValues, out ArgSlice[] fields)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashRandomField<TKeyLocker, TEpochGuard>(key, count, withValues, out fields);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashRandomField<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice field)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashRandomField<TKeyLocker, TEpochGuard>(key, out field);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashRandomField<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashRandomField<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashGet<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashGet<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        public readonly GarnetStatus HashGetAll<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashGetAll<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        public readonly GarnetStatus HashGetMultiple<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashGetMultiple<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashStrLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashStrLength<TKeyLocker, TEpochGuard>(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashExists<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashExists<TKeyLocker, TEpochGuard>(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashKeys<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashKeys<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashVals<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashVals<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashLength<TKeyLocker, TEpochGuard>(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashScan<TKeyLocker, TEpochGuard>(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Object);
            return garnetApi.HashScan<TKeyLocker, TEpochGuard>(key, cursor, match, count, out items);
        }

        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public readonly GarnetStatus StringGetBit<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringGetBit<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus StringGetBit<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice offset, out bool bValue)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Main);
            return garnetApi.StringGetBit<TKeyLocker, TEpochGuard>(key, offset, out bValue);
        }

        /// <inheritdoc />
        public readonly GarnetStatus StringBitCount<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringBitCount<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus StringBitCount<TKeyLocker, TEpochGuard>(ArgSlice key, long start, long end, out long result, bool useBitInterval = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Main);
            return garnetApi.StringBitCount<TKeyLocker, TEpochGuard>(key, start, end, out result, useBitInterval);
        }

        /// <inheritdoc />
        public readonly GarnetStatus StringBitPosition<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringBitPosition<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus StringBitFieldReadOnly<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringBitFieldReadOnly<TKeyLocker, TEpochGuard>(ref key, ref input, secondaryCommand, ref output);
        }

        #endregion

        #region HLL Methods

        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogLength<TKeyLocker, TEpochGuard>(Span<ArgSlice> keys, ref SpanByte input, out long count, out bool error)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            foreach (var key in keys)
                garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Main);
            return garnetApi.HyperLogLogLength<TKeyLocker, TEpochGuard>(keys, ref input, out count, out error);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogLength<TKeyLocker, TEpochGuard>(Span<ArgSlice> keys, out long count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            foreach (var key in keys)
                garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.Main);
            return garnetApi.HyperLogLogLength<TKeyLocker, TEpochGuard>(keys, out count);
        }

        #endregion

        #region Server Methods

        /// <inheritdoc />
        public readonly List<byte[]> GetDbKeys(ArgSlice pattern)
        {
            return garnetApi.GetDbKeys(pattern);
        }

        /// <inheritdoc />
        public readonly int GetDbSize()
        {
            return garnetApi.GetDbSize();
        }

        /// <inheritdoc />
        public readonly bool DbScan(ArgSlice patternB, bool allKeys, long cursor, out long cursorStore, out List<byte[]> keys, long count = 10, ReadOnlySpan<byte> type = default)
        {
            return garnetApi.DbScan(patternB, allKeys, cursor, out cursorStore, out keys, count, type);
        }

        /// <inheritdoc />
        public readonly bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
            => garnetApi.IterateMainStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public readonly ITsavoriteScanIterator<SpanByte, SpanByte> IterateMainStore()
            => garnetApi.IterateMainStore();

        /// <inheritdoc />
        public readonly bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<byte[], IGarnetObject>
            => garnetApi.IterateObjectStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public readonly ITsavoriteScanIterator<byte[], IGarnetObject> IterateObjectStore()
            => garnetApi.IterateObjectStore();

        #endregion

        #region Common Methods

        public readonly GarnetStatus ObjectScan<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            garnetApi.WATCH<TKeyLocker, TEpochGuard>(key, StoreType.All);
            return garnetApi.ObjectScan<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
        }

        #endregion
    }
}