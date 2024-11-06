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
    struct GarnetWatchApi<TKeyLocker, TEpochGuard, TGarnetApi> : IGarnetReadApi<TKeyLocker, TEpochGuard>
        where TKeyLocker : struct, IKeyLocker
        where TEpochGuard : struct, IGarnetEpochGuard
        where TGarnetApi : IGarnetReadApi<TKeyLocker, TEpochGuard>, IGarnetWatchApi<TKeyLocker, TEpochGuard>
    {
        readonly TGarnetApi garnetApi;

        public GarnetWatchApi(TGarnetApi garnetApi)
        {
            this.garnetApi = garnetApi;
        }

        #region GET
        /// <inheritdoc />
        public readonly GarnetStatus GET(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.GET(ref key, ref input, ref output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus GETForMemoryResult(ArgSlice key, out MemoryResult<byte> value)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.GETForMemoryResult(key, out value);
        }

        /// <inheritdoc />
        public readonly GarnetStatus GET(ArgSlice key, out ArgSlice value)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.GET(key, out value);
        }

        /// <inheritdoc />
        public readonly GarnetStatus GET(byte[] key, out GarnetObjectStoreOutput value)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.GET(key, out value);
        }
        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public readonly GarnetStatus GETRANGE(ref SpanByte key, int sliceStart, int sliceLength, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.GETRANGE(ref key, sliceStart, sliceLength, ref output);
        }
        #endregion

        #region TTL
        /// <inheritdoc />
        public readonly GarnetStatus TTL(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), storeType);
            return garnetApi.TTL(ref key, storeType, ref output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus PTTL(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), storeType);
            return garnetApi.PTTL(ref key, storeType, ref output);
        }

        #endregion

        #region SortedSet Methods

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetLength(ArgSlice key, out int zcardCount)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLength(key, out zcardCount);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetCount(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetCount(key, ref input, ref output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetLengthByValue(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLengthByValue(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRandomMember(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRandomMember(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRange(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRange(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetScore(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScore(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetScores(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScores(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRank(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRank(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRank(ArgSlice key, ArgSlice member, bool reverse, out long? rank)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRank(key, member, reverse, out rank);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetRange(ArgSlice key, ArgSlice min, ArgSlice max, SortedSetOrderOperation sortedSetOrderOperation, out ArgSlice[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRange(key, min, max, sortedSetOrderOperation, out elements, out error, withScores, reverse, limit);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetDifference(ArgSlice[] keys, out Dictionary<byte[], double> pairs)
        {
            foreach (var key in keys)
                garnetApi.WATCH(key, StoreType.Object);

            return garnetApi.SortedSetDifference(keys, out pairs);
        }

        /// <inheritdoc />
        public readonly GarnetStatus GeoCommands(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.GeoCommands(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SortedSetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScan(key, cursor, match, count, out items);
        }

        #endregion

        #region List Methods

        /// <inheritdoc />
        public readonly GarnetStatus ListLength(ArgSlice key, out int count)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListLength(key, out count);
        }

        /// <inheritdoc />
        public readonly GarnetStatus ListLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus ListRange(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListRange(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus ListIndex(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListIndex(key, ref input, ref outputFooter);
        }

        #endregion

        #region Set Methods

        /// <inheritdoc />
        public readonly GarnetStatus SetLength(ArgSlice key, out int scardCount)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetLength(key, out scardCount);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetMembers(ArgSlice key, out ArgSlice[] members)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetMembers(key, out members);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetIsMember(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetIsMember(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetMembers(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetMembers(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetScan(key, cursor, match, count, out items);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetUnion(ArgSlice[] keys, out HashSet<byte[]> output)
        {
            foreach (var key in keys)
                garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetUnion(keys, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetIntersect(ArgSlice[] keys, out HashSet<byte[]> output)
        {
            foreach (var key in keys)
                garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetIntersect(keys, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus SetDiff(ArgSlice[] keys, out HashSet<byte[]> output)
        {
            foreach (var key in keys)
                garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetDiff(keys, out output);
        }
        #endregion

        #region Hash Methods

        /// <inheritdoc />
        public readonly GarnetStatus HashGet(ArgSlice key, ArgSlice field, out ArgSlice value)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGet(key, field, out value);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashGetMultiple(ArgSlice key, ArgSlice[] fields, out ArgSlice[] values)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetMultiple(key, fields, out values);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashGetAll(ArgSlice key, out ArgSlice[] values)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetAll(key, out values);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashLength(ArgSlice key, out int count)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashLength(key, out count);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashExists(ArgSlice key, ArgSlice field, out bool exists)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashExists(key, field, out exists);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashRandomField(ArgSlice key, int count, bool withValues, out ArgSlice[] fields)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashRandomField(key, count, withValues, out fields);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashRandomField(ArgSlice key, out ArgSlice field)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashRandomField(key, out field);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashRandomField(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashRandomField(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashGet(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGet(key, ref input, ref outputFooter);
        }

        public readonly GarnetStatus HashGetAll(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetAll(key, ref input, ref outputFooter);
        }

        public readonly GarnetStatus HashGetMultiple(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetMultiple(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashStrLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashStrLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashExists(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashExists(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashKeys(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashKeys(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashVals(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashVals(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HashScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashScan(key, cursor, match, count, out items);
        }

        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public readonly GarnetStatus StringGetBit(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringGetBit(ref key, ref input, ref output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus StringGetBit(ArgSlice key, ArgSlice offset, out bool bValue)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringGetBit(key, offset, out bValue);
        }

        /// <inheritdoc />
        public readonly GarnetStatus StringBitCount(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringBitCount(ref key, ref input, ref output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus StringBitCount(ArgSlice key, long start, long end, out long result, bool useBitInterval = false)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringBitCount(key, start, end, out result, useBitInterval);
        }

        /// <inheritdoc />
        public readonly GarnetStatus StringBitPosition(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringBitPosition(ref key, ref input, ref output);
        }

        /// <inheritdoc />
        public readonly GarnetStatus StringBitFieldReadOnly(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringBitFieldReadOnly(ref key, ref input, secondaryCommand, ref output);
        }

        #endregion

        #region HLL Methods

        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogLength(Span<ArgSlice> keys, ref SpanByte input, out long count, out bool error)
        {
            foreach (var key in keys)
                garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.HyperLogLogLength(keys, ref input, out count, out error);
        }

        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogLength(Span<ArgSlice> keys, out long count)
        {
            foreach (var key in keys)
                garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.HyperLogLogLength(keys, out count);
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

        public readonly GarnetStatus ObjectScan(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.All);
            return garnetApi.ObjectScan(key, ref input, ref outputFooter);
        }

        #endregion
    }
}