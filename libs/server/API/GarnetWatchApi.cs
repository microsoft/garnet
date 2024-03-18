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
        public GarnetStatus GET(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.GET(ref key, ref input, ref output);
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
        public GarnetStatus GET(byte[] key, out GarnetObjectStoreOutput value)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.GET(key, out value);
        }
        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public GarnetStatus GETRANGE(ref SpanByte key, int sliceStart, int sliceLength, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.GETRANGE(ref key, sliceStart, sliceLength, ref output);
        }
        #endregion

        #region TTL
        /// <inheritdoc />
        public GarnetStatus TTL(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), storeType);
            return garnetApi.TTL(ref key, storeType, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus PTTL(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), storeType);
            return garnetApi.PTTL(ref key, storeType, ref output);
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
        public GarnetStatus SortedSetLength(byte[] key, ArgSlice input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLength(key, input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetCount(byte[] key, ArgSlice input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetCount(key, input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetLengthByValue(byte[] key, ArgSlice input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLengthByValue(key, input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRandomMember(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRandomMember(key, input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRange(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRange(key, input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetScore(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScore(key, input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRank(byte[] key, ArgSlice input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRank(key, input, out output);
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
        public GarnetStatus GeoCommands(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.GeoCommands(key, input, ref outputFooter);
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
        public GarnetStatus ListLength(byte[] key, ArgSlice input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListLength(key, input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus ListRange(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListRange(key, input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus ListIndex(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListIndex(key, input, ref outputFooter);
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
        public GarnetStatus SetLength(byte[] key, ArgSlice input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetLength(key, input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SetMembers(ArgSlice key, out ArgSlice[] members)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetMembers(key, out members);
        }

        /// <inheritdoc />
        public GarnetStatus SetMembers(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetMembers(key, input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus SetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetScan(key, cursor, match, count, out items);
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
        public GarnetStatus HashGet(ArgSlice key, ArgSlice[] fields, out ArgSlice[] values)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGet(key, fields, out values);
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
        public GarnetStatus HashRandomField(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashRandomField(key, input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus HashGet(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGet(key, input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus HashExists(byte[] key, ArgSlice input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashExists(key, input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus HashKeys(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashKeys(key, input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus HashVals(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashVals(key, input, ref outputFooter);
        }

        /// <inheritdoc />
        public GarnetStatus HashLength(byte[] key, ArgSlice input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashLength(key, input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus HashScan(ArgSlice key, long cursor, string match, long count, out ArgSlice[] items)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashScan(key, cursor, match, count, out items);
        }

        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public GarnetStatus StringGetBit(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringGetBit(ref key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus StringGetBit(ArgSlice key, ArgSlice offset, out bool bValue)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringGetBit(key, offset, out bValue);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitCount(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringBitCount(ref key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitCount(ArgSlice key, long start, long end, out long result, bool useBitInterval = false)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringBitCount(key, start, end, out result, useBitInterval);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitPosition(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringBitPosition(ref key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitFieldReadOnly(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringBitFieldReadOnly(ref key, ref input, secondaryCommand, ref output);
        }

        #endregion

        #region HLL Methods

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(ArgSlice[] keys, ref SpanByte input, out long count, out bool error)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Main);
            }
            return garnetApi.HyperLogLogLength(keys, ref input, out count, out error);
        }

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(ArgSlice[] keys, out long count)
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
        public bool DbScan(ArgSlice patternB, bool allKeys, long cursor, out long cursorStore, out List<byte[]> keys, long count = 10, Span<byte> type = default)
        {
            return garnetApi.DbScan(patternB, allKeys, cursor, out cursorStore, out keys, count, type);
        }

        /// <inheritdoc />
        public bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
            => garnetApi.IterateMainStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public ITsavoriteScanIterator<SpanByte, SpanByte> IterateMainStore()
            => garnetApi.IterateMainStore();

        /// <inheritdoc />
        public bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<byte[], IGarnetObject>
            => garnetApi.IterateObjectStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public ITsavoriteScanIterator<byte[], IGarnetObject> IterateObjectStore()
            => garnetApi.IterateObjectStore();

        #endregion

        #region Common Methods

        public GarnetStatus ObjectScan(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
        {
            garnetApi.WATCH(key, StoreType.All);
            return garnetApi.ObjectScan(key, input, ref outputFooter);
        }

        #endregion
    }
}