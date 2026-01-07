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
        public GarnetStatus GET(PinnedSpanByte key, ref StringInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.GET(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus GETForMemoryResult(PinnedSpanByte key, out MemoryResult<byte> value)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.GETForMemoryResult(key, out value);
        }

        /// <inheritdoc />
        public GarnetStatus GET(PinnedSpanByte key, out PinnedSpanByte value)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.GET(key, out value);
        }

        /// <inheritdoc />
        public GarnetStatus GET(PinnedSpanByte key, out ObjectOutput value)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.GET(key, out value);
        }

        /// <inheritdoc />
        public GarnetStatus LCS(PinnedSpanByte key1, PinnedSpanByte key2, ref SpanByteAndMemory output, bool lenOnly = false, bool withIndices = false, bool withMatchLen = false, int minMatchLen = 0)
        {
            garnetApi.WATCH(key1, StoreType.Object);
            garnetApi.WATCH(key2, StoreType.Object);
            return garnetApi.LCS(key1, key2, ref output, lenOnly, withIndices, withMatchLen, minMatchLen);
        }
        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public GarnetStatus GETRANGE(PinnedSpanByte key, ref StringInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.GETRANGE(key, ref input, ref output);
        }
        #endregion

        #region TTL
        /// <inheritdoc />
        public GarnetStatus TTL(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
        {
            garnetApi.WATCH(key, StoreType.All);
            return garnetApi.TTL(key, ref input, ref output);
        }

        #endregion

        #region EXPIRETIME

        /// <inheritdoc />
        public GarnetStatus EXPIRETIME(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
        {
            garnetApi.WATCH(key, StoreType.All);
            return garnetApi.EXPIRETIME(key, ref input, ref output);
        }

        #endregion

        #region SortedSet Methods

        /// <inheritdoc />
        public GarnetStatus SortedSetLength(PinnedSpanByte key, out int zcardCount)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLength(key, out zcardCount);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetLength(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetCount(PinnedSpanByte key, PinnedSpanByte minScore, PinnedSpanByte maxScore, out int numElements)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetCount(key, minScore, maxScore, out numElements);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetCount(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetCount(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetLengthByValue(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetLengthByValue(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRandomMember(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRandomMember(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRange(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRange(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetScore(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScore(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetScores(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScores(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRank(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRank(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRank(PinnedSpanByte key, PinnedSpanByte member, bool reverse, out long? rank)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRank(key, member, reverse, out rank);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetRange(PinnedSpanByte key, PinnedSpanByte min, PinnedSpanByte max, SortedSetOrderOperation sortedSetOrderOperation, out PinnedSpanByte[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetRange(key, min, max, sortedSetOrderOperation, out elements, out error, withScores, reverse, limit);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetDifference(PinnedSpanByte[] keys, out SortedSet<(double, byte[])> pairs)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SortedSetDifference(keys, out pairs);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetUnion(ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out SortedSet<(double, byte[])> pairs)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SortedSetUnion(keys, weights, aggregateType, out pairs);
        }

        /// <inheritdoc />
        public GarnetStatus GeoCommands(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.GeoCommands(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus GeoSearchReadOnly(PinnedSpanByte key, ref GeoSearchOptions opts,
                                      ref ObjectInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.GeoSearchReadOnly(key, ref opts, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetScan(PinnedSpanByte key, long cursor, string match, int count, out PinnedSpanByte[] items)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetScan(key, cursor, match, count, out items);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetIntersect(ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out SortedSet<(double, byte[])> pairs)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SortedSetIntersect(keys, weights, aggregateType, out pairs);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetIntersectLength(ReadOnlySpan<PinnedSpanByte> keys, int? limit, out int count)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SortedSetIntersectLength(keys, limit, out count);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetTimeToLive(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetTimeToLive(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SortedSetTimeToLive(PinnedSpanByte key, ReadOnlySpan<PinnedSpanByte> members, out TimeSpan[] expireIn)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SortedSetTimeToLive(key, members, out expireIn);
        }

        #endregion

        #region List Methods

        /// <inheritdoc />
        public GarnetStatus ListLength(PinnedSpanByte key, out int count)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListLength(key, out count);
        }

        /// <inheritdoc />
        public GarnetStatus ListLength(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus ListRange(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListRange(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus ListIndex(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.ListIndex(key, ref input, ref output);
        }

        #endregion

        #region Set Methods

        /// <inheritdoc />
        public GarnetStatus SetLength(PinnedSpanByte key, out int scardCount)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetLength(key, out scardCount);
        }

        /// <inheritdoc />
        public GarnetStatus SetLength(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SetMembers(PinnedSpanByte key, out PinnedSpanByte[] members)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetMembers(key, out members);
        }

        /// <inheritdoc />
        public GarnetStatus SetIsMember(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetIsMember(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SetIsMember(PinnedSpanByte key, PinnedSpanByte[] members, out int[] result)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetIsMember(key, members, out result);
        }

        /// <inheritdoc />
        public GarnetStatus SetMembers(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetMembers(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus SetScan(PinnedSpanByte key, long cursor, string match, int count, out PinnedSpanByte[] items)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.SetScan(key, cursor, match, count, out items);
        }

        /// <inheritdoc />
        public GarnetStatus SetUnion(PinnedSpanByte[] keys, out HashSet<byte[]> output)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SetUnion(keys, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SetIntersect(PinnedSpanByte[] keys, out HashSet<byte[]> output)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SetIntersect(keys, out output);
        }

        /// <inheritdoc />
        public GarnetStatus SetDiff(PinnedSpanByte[] keys, out HashSet<byte[]> output)
        {
            foreach (var key in keys)
            {
                garnetApi.WATCH(key, StoreType.Object);
            }
            return garnetApi.SetDiff(keys, out output);
        }

        public GarnetStatus SetIntersectLength(ReadOnlySpan<PinnedSpanByte> keys, int? limit, out int count)
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
        public GarnetStatus HashGet(PinnedSpanByte key, PinnedSpanByte field, out PinnedSpanByte value)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGet(key, field, out value);
        }

        /// <inheritdoc />
        public GarnetStatus HashGetMultiple(PinnedSpanByte key, PinnedSpanByte[] fields, out PinnedSpanByte[] values)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetMultiple(key, fields, out values);
        }

        /// <inheritdoc />
        public GarnetStatus HashGetAll(PinnedSpanByte key, out PinnedSpanByte[] values)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetAll(key, out values);
        }

        /// <inheritdoc />
        public GarnetStatus HashLength(PinnedSpanByte key, out int count)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashLength(key, out count);
        }

        /// <inheritdoc />
        public GarnetStatus HashExists(PinnedSpanByte key, PinnedSpanByte field, out bool exists)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashExists(key, field, out exists);
        }

        /// <inheritdoc />
        public GarnetStatus HashRandomField(PinnedSpanByte key, int count, bool withValues, out PinnedSpanByte[] fields)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashRandomField(key, count, withValues, out fields);
        }

        /// <inheritdoc />
        public GarnetStatus HashRandomField(PinnedSpanByte key, out PinnedSpanByte field)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashRandomField(key, out field);
        }

        /// <inheritdoc />
        public GarnetStatus HashRandomField(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashRandomField(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus HashGet(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGet(key, ref input, ref output);
        }

        public GarnetStatus HashGetAll(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetAll(key, ref input, ref output);
        }

        public GarnetStatus HashGetMultiple(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashGetMultiple(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus HashStrLength(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashStrLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus HashExists(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashExists(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus HashKeys(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashKeys(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus HashVals(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashVals(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus HashLength(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashLength(key, ref input, out output);
        }

        /// <inheritdoc />
        public GarnetStatus HashScan(PinnedSpanByte key, long cursor, string match, int count, out PinnedSpanByte[] items)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashScan(key, cursor, match, count, out items);
        }

        /// <inheritdoc />
        public GarnetStatus HashTimeToLive(PinnedSpanByte key, bool isMilliseconds, bool isTimestamp, ref ObjectInput input, ref ObjectOutput output)
        {
            garnetApi.WATCH(key, StoreType.Object);
            return garnetApi.HashTimeToLive(key, isMilliseconds, isTimestamp, ref input, ref output);
        }

        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public GarnetStatus StringGetBit(PinnedSpanByte key, ref StringInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringGetBit(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus StringGetBit(PinnedSpanByte key, PinnedSpanByte offset, out bool bValue)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringGetBit(key, offset, out bValue);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitCount(PinnedSpanByte key, ref StringInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringBitCount(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitCount(PinnedSpanByte key, long start, long end, out long result, bool useBitInterval = false)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringBitCount(key, start, end, out result, useBitInterval);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitPosition(PinnedSpanByte key, ref StringInput input, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringBitPosition(key, ref input, ref output);
        }

        /// <inheritdoc />
        public GarnetStatus StringBitFieldReadOnly(PinnedSpanByte key, ref StringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.StringBitFieldReadOnly(key, ref input, secondaryCommand, ref output);
        }

        #endregion

        #region HLL Methods

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(ref StringInput input, out long count, out bool error)
        {
            for (var i = 0; i < input.parseState.Count; i++)
            {
                var key = input.parseState.GetArgSliceByRef(i);
                garnetApi.WATCH(key, StoreType.Main);
            }

            return garnetApi.HyperLogLogLength(ref input, out count, out error);
        }

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(Span<PinnedSpanByte> keys, out long count)
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
        public List<byte[]> GetDbKeys(PinnedSpanByte pattern)
        {
            return garnetApi.GetDbKeys(pattern);
        }

        /// <inheritdoc />
        public int GetDbSize()
        {
            return garnetApi.GetDbSize();
        }

        /// <inheritdoc />
        public bool DbScan(PinnedSpanByte patternB, bool allKeys, long cursor, out long cursorStore, out List<byte[]> keys, long count = 10, ReadOnlySpan<byte> type = default)
        {
            return garnetApi.DbScan(patternB, allKeys, cursor, out cursorStore, out keys, count, type);
        }

        /// <inheritdoc />
        public bool IterateStore<TScanFunctions>(ref TScanFunctions scanFunctions, ref long cursor, long untilAddress = -1, long maxAddress = long.MaxValue, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions
            => garnetApi.IterateStore(ref scanFunctions, ref cursor, untilAddress, maxAddress: maxAddress, includeTombstones: includeTombstones);

        /// <inheritdoc />
        public ITsavoriteScanIterator IterateStore()
            => garnetApi.IterateStore();

        #endregion

        #region Common Methods

        public GarnetStatus ObjectScan(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
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
        public GarnetStatus VectorSetValueSimilarity(PinnedSpanByte key, VectorValueType valueType, PinnedSpanByte value, int count, float delta, int searchExplorationFactor, PinnedSpanByte filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.VectorSetValueSimilarity(key, valueType, value, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes, out result);
        }

        /// <inheritdoc/>
        public GarnetStatus VectorSetElementSimilarity(PinnedSpanByte key, PinnedSpanByte element, int count, float delta, int searchExplorationFactor, PinnedSpanByte filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.VectorSetElementSimilarity(key, element, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes, out result);
        }

        /// <inheritdoc/>
        public GarnetStatus VectorSetEmbedding(PinnedSpanByte key, PinnedSpanByte element, ref SpanByteAndMemory outputDistances)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.VectorSetEmbedding(key, element, ref outputDistances);
        }

        /// <inheritdoc/>
        public GarnetStatus VectorSetDimensions(PinnedSpanByte key, out int dimensions)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.VectorSetDimensions(key, out dimensions);
        }

        /// <inheritdoc/>
        GarnetStatus IGarnetReadApi.VectorSetInfo(PinnedSpanByte key, out VectorQuantType quantType, out uint vectorDimensions, out uint reducedDimensions, out uint buildExplorationFactor, out uint numberOfLinks, out long size)
        {
            garnetApi.WATCH(key, StoreType.Main);
            return garnetApi.VectorSetInfo(key, out quantType, out vectorDimensions, out reducedDimensions, out buildExplorationFactor, out numberOfLinks, out size);
        }
        #endregion
    }
}