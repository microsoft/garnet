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
    struct GarnetWatchApi<TGarnetApi> : IGarnetReadApi
        where TGarnetApi : IGarnetReadApi, IGarnetWatchApi
    {
        TGarnetApi garnetApi;

        public GarnetWatchApi(TGarnetApi garnetApi)
        {
            this.garnetApi = garnetApi;
        }

        #region GET
        /// <inheritdoc />
        public GarnetStatus GET(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.GET(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus GETForMemoryResult(ArgSlice key, out MemoryResult<byte> value)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.GETForMemoryResult(key, out value);
        }

        /// <inheritdoc />
        public GarnetStatus GET(ArgSlice key, out ArgSlice value)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.GET(key, out value);
        }

        /// <inheritdoc />
        public GarnetStatus GET(SpanByte key, out GarnetObjectStoreOutput value)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.GET(key, out value);
        }
        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public GarnetStatus GETRANGE(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.GETRANGE(key, ref input, ref output);
        }
        #endregion

        #region TTL
        /// <inheritdoc />
        public GarnetStatus TTL(SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, storeType);
            return garnetApi.TTL(key, storeType, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus PTTL(SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, storeType);
            return garnetApi.PTTL(key, storeType, ref output);
        }

        #endregion

        #region EXPIRETIME

        /// <inheritdoc />
        public GarnetStatus EXPIRETIME(SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, storeType);
            return garnetApi.EXPIRETIME(key, storeType, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus PEXPIRETIME(SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, storeType);
            return garnetApi.PEXPIRETIME(key, storeType, ref output);
        }

        #endregion

        #region SortedSet Methods

        /// <inheritdoc />
        public GarnetStatus SortedSetLength(ArgSlice key, out int zcardCount)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLength(key, out zcardCount);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetLength(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetCount(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetCount(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetLengthByValue(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLengthByValue(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRandomMember(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRandomMember(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRange(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRange(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetScore(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScore(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetScores(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScores(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRank(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRank(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRank(ArgSlice key, ArgSlice member, bool reverse, out long? rank)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRank(key, member, reverse, out rank);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRange(ArgSlice key, ArgSlice min, ArgSlice max, SortedSetOrderOperation sortedSetOrderOperation, out ArgSlice[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRange(key, min, max, sortedSetOrderOperation, out elements, out error, withScores, reverse, limit);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetDifference(ArgSlice[] keys, out Dictionary<byte[], double> pairs)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SortedSetDifference(keys, out pairs);
        }

        /// <inheritdoc />
        public GarnetStatus GeoCommands(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.GeoCommands(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScan(key, cursor, match, count, out items);
        }

        #endregion

        #region List Methods

        /// <inheritdoc />
        public GarnetStatus ListLength(ArgSlice key, out int count)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListLength(key, out count);
        }

        /// <inheritdoc />
        public GarnetStatus ListLength(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus ListRange(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListRange(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus ListIndex(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListIndex(key, ref input, ref outputFooter);
        }

        #endregion

        #region Set Methods

        /// <inheritdoc />
        public GarnetStatus SetLength(ArgSlice key, out int scardCount)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetLength(key, out scardCount);
        }

        /// <inheritdoc />
        public GarnetStatus SetLength(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SetMembers(ArgSlice key, out ArgSlice[] members)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetMembers(key, out members);
        }

        /// <inheritdoc />
        public GarnetStatus SetIsMember(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetIsMember(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus SetIsMember(ArgSlice key, ArgSlice[] members, out int[] result)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetIsMember(key, members, out result);
        }

        /// <inheritdoc />
        public GarnetStatus SetMembers(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetMembers(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus SetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetScan(key, cursor, match, count, out items);
        }

        /// <inheritdoc />
        public GarnetStatus SetUnion(ArgSlice[] keys, out HashSet<byte[]> output)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SetUnion(keys, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SetIntersect(ArgSlice[] keys, out HashSet<byte[]> output)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SetIntersect(keys, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SetDiff(ArgSlice[] keys, out HashSet<byte[]> output)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SetDiff(keys, out output);
        }
        #endregion

        #region Hash Methods

        /// <inheritdoc />
        public GarnetStatus HashGet(ArgSlice key, ArgSlice field, out ArgSlice value)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGet(key, field, out value);
        }

        /// <inheritdoc />
        public GarnetStatus HashGetMultiple(ArgSlice key, ArgSlice[] fields, out ArgSlice[] values)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetMultiple(key, fields, out values);
        }

        /// <inheritdoc />
        public GarnetStatus HashGetAll(ArgSlice key, out ArgSlice[] values)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetAll(key, out values);
        }

        /// <inheritdoc />
        public GarnetStatus HashLength(ArgSlice key, out int count)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashLength(key, out count);
        }

        /// <inheritdoc />
        public GarnetStatus HashExists(ArgSlice key, ArgSlice field, out bool exists)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashExists(key, field, out exists);
        }

        /// <inheritdoc />
        public GarnetStatus HashRandomField(ArgSlice key, int count, bool withValues, out ArgSlice[] fields)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashRandomField(key, count, withValues, out fields);
        }

        /// <inheritdoc />
        public GarnetStatus HashRandomField(ArgSlice key, out ArgSlice field)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashRandomField(key, out field);
        }

        /// <inheritdoc />
        public GarnetStatus HashRandomField(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashRandomField(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus HashGet(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGet(key, ref input, ref outputFooter);
        }

        public GarnetStatus HashGetAll(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetAll(key, ref input, ref outputFooter);
        }

        public GarnetStatus HashGetMultiple(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetMultiple(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus HashStrLength(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashStrLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus HashExists(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashExists(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus HashKeys(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashKeys(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus HashVals(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashVals(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus HashLength(SpanByte key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus HashScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashScan(key, cursor, match, count, out items);
        }

        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public GarnetStatus StringGetBit(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringGetBit(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus StringGetBit(ArgSlice key, ArgSlice offset, out bool bValue)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringGetBit(key, offset, out bValue);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitCount(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringBitCount(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitCount(ArgSlice key, long start, long end, out long result, bool useBitInterval = false)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringBitCount(key, start, end, out result, useBitInterval);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitPosition(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringBitPosition(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitFieldReadOnly(SpanByte key, ref RawStringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringBitFieldReadOnly(key, ref input, secondaryCommand, ref output);
        }

        #endregion

        #region HLL Methods

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(ref RawStringInput input, out long count, out bool error)
        {
            for (var i = 0; i < input.parseState.Count; i++)
            {
                var key = input.parseState.GetArgSliceByRef(i);
                garnetApi.WATCH(key, StoreType.Main);
            }

            return garnetApi.HyperLogLogLength(ref input, out count, out error);
        }

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(Span<ArgSlice> keys, out long count)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Main);
            }
            return garnetApi.HyperLogLogLength(keys, out count);
        }

        #endregion

        #region Server Methods

        /// <inheritdoc />
        public List<byte[]> GetDbKeys(ArgSlice pattern)
        {
            return garnetApi.GetDbKeys(pattern);
        }

        /// <inheritdoc />
        public int GetDbSize()
        {
            return garnetApi.GetDbSize();
        }

        /// <inheritdoc />
        public bool DbScan(ArgSlice patternB, bool allKeys, long cursor, out long cursorStore, out List<byte[]> keys, long count = 10, ReadOnlySpan<byte> type = default)
        {
            return garnetApi.DbScan(patternB, allKeys, cursor, out cursorStore, out keys, count, type);
        }

        /// <inheritdoc />
        public bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<SpanByte>
            => garnetApi.IterateMainStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public ITsavoriteScanIterator<SpanByte> IterateMainStore()
            => garnetApi.IterateMainStore();

        /// <inheritdoc />
        public bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<IGarnetObject>
            => garnetApi.IterateObjectStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public ITsavoriteScanIterator<IGarnetObject> IterateObjectStore()
            => garnetApi.IterateObjectStore();

        #endregion

        #region Common Methods

        public GarnetStatus ObjectScan(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.All);
            return garnetApi.ObjectScan(key, ref input, ref outputFooter);
        }

        /// <inheritdoc />
        public int GetScratchBufferOffset()
            => garnetApi.GetScratchBufferOffset();

        /// <inheritdoc />
        public bool ResetScratchBuffer(int offset)
            => garnetApi.ResetScratchBuffer(offset);

        #endregion
    }
}