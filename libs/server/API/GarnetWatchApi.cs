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
        public GarnetStatus GET(ArgSlice key, ref RawStringInput input, ref SpanByteAndMemory output)
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
        public GarnetStatus GET(byte[] key, out GarnetObjectStoreOutput value)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.GET(key, out value);
        }

        /// <inheritdoc />
        public GarnetStatus LCS(ArgSlice key1, ArgSlice key2, ref SpanByteAndMemory output, bool lenOnly = false, bool withIndices = false, bool withMatchLen = false, int minMatchLen = 0)
        {
            garnetApi.WATCH(key1, StoreType.Object);
            garnetApi.WATCH(key2, StoreType.Object);
            return garnetApi.LCS(key1, key2, ref output, lenOnly, withIndices, withMatchLen, minMatchLen);
        }
        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public GarnetStatus GETRANGE(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.GETRANGE(ref key, ref input, ref output);
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

        #region EXPIRETIME

        /// <inheritdoc />
        public GarnetStatus EXPIRETIME(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), storeType);
            return garnetApi.EXPIRETIME(ref key, storeType, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus PEXPIRETIME(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), storeType);
            return garnetApi.PEXPIRETIME(ref key, storeType, ref output);
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
        public GarnetStatus SortedSetLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetCount(ArgSlice key, ArgSlice minScore, ArgSlice maxScore, out int numElements)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetCount(key, minScore, maxScore, out numElements);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetCount(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetCount(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetLengthByValue(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLengthByValue(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRandomMember(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRandomMember(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRange(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRange(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetScore(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScore(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetScores(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScores(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRank(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRank(key, ref input, ref output);
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
        public GarnetStatus SortedSetDifference(ArgSlice[] keys, out SortedSet<(double, byte[])> pairs)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SortedSetDifference(keys, out pairs);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetUnion(ReadOnlySpan<ArgSlice> keys, double[] weights, SortedSetAggregateType aggregateType, out SortedSet<(double, byte[])> pairs)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SortedSetUnion(keys, weights, aggregateType, out pairs);
        }

        /// <inheritdoc />
        public GarnetStatus GeoCommands(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.GeoCommands(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus GeoSearchReadOnly(ArgSlice key, ref GeoSearchOptions opts,
                                      ref ObjectInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.GeoSearchReadOnly(key, ref opts, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScan(key, cursor, match, count, out items);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetIntersect(ReadOnlySpan<ArgSlice> keys, double[] weights, SortedSetAggregateType aggregateType, out SortedSet<(double, byte[])> pairs)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SortedSetIntersect(keys, weights, aggregateType, out pairs);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetIntersectLength(ReadOnlySpan<ArgSlice> keys, int? limit, out int count)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SortedSetIntersectLength(keys, limit, out count);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetTimeToLive(ArgSlice key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetTimeToLive(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetTimeToLive(ArgSlice key, ReadOnlySpan<ArgSlice> members, out TimeSpan[] expireIn)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetTimeToLive(key, members, out expireIn);
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
        public GarnetStatus ListLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus ListRange(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListRange(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus ListIndex(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListIndex(key, ref input, ref output);
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
        public GarnetStatus SetLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
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
        public GarnetStatus SetIsMember(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetIsMember(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SetIsMember(ArgSlice key, ArgSlice[] members, out int[] result)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetIsMember(key, members, out result);
        }

        /// <inheritdoc />
        public GarnetStatus SetMembers(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetMembers(key, ref input, ref output);
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

        public GarnetStatus SetIntersectLength(ReadOnlySpan<ArgSlice> keys, int? limit, out int count)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SetIntersectLength(keys, limit, out count);
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
        public GarnetStatus HashRandomField(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashRandomField(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus HashGet(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGet(key, ref input, ref output);
        }

        public GarnetStatus HashGetAll(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetAll(key, ref input, ref output);
        }

        public GarnetStatus HashGetMultiple(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetMultiple(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus HashStrLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashStrLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus HashExists(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashExists(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus HashKeys(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashKeys(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus HashVals(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashVals(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus HashLength(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
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

        /// <inheritdoc />
        public GarnetStatus HashTimeToLive(ArgSlice key, bool isMilliseconds, bool isTimestamp, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashTimeToLive(key, isMilliseconds, isTimestamp, ref input, ref output);
        }

        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public GarnetStatus StringGetBit(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
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
        public GarnetStatus StringBitCount(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
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
        public GarnetStatus StringBitPosition(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringBitPosition(ref key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitFieldReadOnly(ref SpanByte key, ref RawStringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(new ArgSlice(ref key), StoreType.Main);
            return garnetApi.StringBitFieldReadOnly(ref key, ref input, secondaryCommand, ref output);
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
        public bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, ref long cursor, long untilAddress = -1, long maxAddress = long.MaxValue, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
            => garnetApi.IterateMainStore(ref scanFunctions, ref cursor, untilAddress, maxAddress: maxAddress, includeTombstones: includeTombstones);

        /// <inheritdoc />
        public ITsavoriteScanIterator<SpanByte, SpanByte> IterateMainStore()
            => garnetApi.IterateMainStore();

        /// <inheritdoc />
        public bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, ref long cursor, long untilAddress = -1, long maxAddress = long.MaxValue, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions<byte[], IGarnetObject>
            => garnetApi.IterateObjectStore(ref scanFunctions, ref cursor, untilAddress, maxAddress: maxAddress, includeTombstones: includeTombstones);

        /// <inheritdoc />
        public ITsavoriteScanIterator<byte[], IGarnetObject> IterateObjectStore()
            => garnetApi.IterateObjectStore();

        #endregion

        #region Common Methods

        public GarnetStatus ObjectScan(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            garnetApi.WATCH(key, StoreType.All);
            return garnetApi.ObjectScan(key, ref input, ref output);
        }

        /// <inheritdoc />
        public int GetScratchBufferOffset()
            => garnetApi.GetScratchBufferOffset();

        /// <inheritdoc />
        public bool ResetScratchBuffer(int offset)
            => garnetApi.ResetScratchBuffer(offset);

        #endregion

        #region Vector Sets
        /// <inheritdoc/>
        public GarnetStatus VectorSetValueSimilarity(ArgSlice key, VectorValueType valueType, ArgSlice value, int count, float delta, int searchExplorationFactor, ArgSlice filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.VectorSetValueSimilarity(key, valueType, value, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes, out result);
        }

        /// <inheritdoc/>
        public GarnetStatus VectorSetElementSimilarity(ArgSlice key, ArgSlice element, int count, float delta, int searchExplorationFactor, ArgSlice filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.VectorSetElementSimilarity(key, element, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes, out result);
        }

        /// <inheritdoc/>
        public GarnetStatus VectorSetEmbedding(ArgSlice key, ArgSlice element, ref SpanByteAndMemory outputDistances)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.VectorSetEmbedding(key, element, ref outputDistances);
        }

        /// <inheritdoc/>
        public GarnetStatus VectorSetDimensions(ArgSlice key, out int dimensions)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.VectorSetDimensions(key, out dimensions);
        }

        /// <inheritdoc/>
        GarnetStatus IGarnetReadApi.VectorSetInfo(ArgSlice key, out VectorQuantType quantType, out VectorDistanceMetricType distanceMetricType, out uint vectorDimensions, out uint reducedDimensions, out uint buildExplorationFactor, out uint numberOfLinks, out long size)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.VectorSetInfo(key, out quantType, out distanceMetricType, out vectorDimensions, out reducedDimensions, out buildExplorationFactor, out numberOfLinks, out size);
        }

        #endregion
    }
}