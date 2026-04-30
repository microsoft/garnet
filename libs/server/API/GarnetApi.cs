// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    // See TransactionManager.cs for aliases BasicGarnetApi and TransactionalGarnetApi

    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi<TStringContext, TObjectContext, TUnifiedContext> : IGarnetApi, IGarnetWatchApi
        where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
    {
        readonly StorageSession storageSession;
        TStringContext stringContext;
        TObjectContext objectContext;
        TUnifiedContext unifiedContext;

        internal GarnetApi(StorageSession storageSession, TStringContext stringContext, TObjectContext objectContext, TUnifiedContext unifiedContext)
        {
            this.storageSession = storageSession;
            this.stringContext = stringContext;
            this.objectContext = objectContext;
            this.unifiedContext = unifiedContext;
        }

        #region WATCH
        /// <inheritdoc />
        public void WATCH(PinnedSpanByte key, StoreType type)
            => storageSession.WATCH(key, type);
        #endregion

        #region GET
        /// <inheritdoc />
        public GarnetStatus GET(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.GET(key, ref input, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus GET_WithPending(PinnedSpanByte key, ref StringInput input, ref StringOutput output, long ctx, out bool pending)
            => storageSession.GET_WithPending(key.ReadOnlySpan, ref input, ref output, ctx, out pending, ref stringContext);

        /// <inheritdoc />
        public bool GET_CompletePending((GarnetStatus, StringOutput)[] outputArr, bool wait = false)
            => storageSession.GET_CompletePending(outputArr, wait, ref stringContext);

        public bool GET_CompletePending(out CompletedOutputIterator<StringInput, StringOutput, long> completedOutputs, bool wait)
            => storageSession.GET_CompletePending(out completedOutputs, wait, ref stringContext);

        /// <inheritdoc />
        public unsafe GarnetStatus GETForMemoryResult(PinnedSpanByte key, out MemoryResult<byte> value)
            => storageSession.GET(key, out value, ref stringContext);

        /// <inheritdoc />
        public unsafe GarnetStatus GET(PinnedSpanByte key, out PinnedSpanByte value)
            => storageSession.GET(key, out value, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus GET(PinnedSpanByte key, out ObjectOutput value)
            => storageSession.GET(key, out value, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus LCS(PinnedSpanByte key1, PinnedSpanByte key2, ref StringOutput output, bool lenOnly = false, bool withIndices = false, bool withMatchLen = false, int minMatchLen = 0)
            => storageSession.LCS(key1, key2, ref output, lenOnly, withIndices, withMatchLen, minMatchLen);
        #endregion

        #region GETEX

        /// <inheritdoc />
        public GarnetStatus GETEX(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.GETEX(key, ref input, ref output, ref stringContext);

        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public GarnetStatus GETRANGE(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.GETRANGE(key, ref input, ref output, ref stringContext);
        #endregion

        #region SET
        /// <inheritdoc />
        public GarnetStatus SET(PinnedSpanByte key, PinnedSpanByte value)
            => storageSession.SET(key, value, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus SET(PinnedSpanByte key, ref StringInput input, PinnedSpanByte value)
            => storageSession.SET(key, ref input, value, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus SET_Conditional(PinnedSpanByte key, ref StringInput input)
            => storageSession.SET_Conditional(key, ref input, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus SET_Conditional(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.SET_Conditional(key, ref input, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus SET_ETagConditional(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.SET_Conditional(key, ref input, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus DEL_ETagConditional(PinnedSpanByte key, ref StringInput input)
            => storageSession.DEL_Conditional(key, ref input, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus SET(PinnedSpanByte key, Memory<byte> value)
            => storageSession.SET(key, value, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus SET(PinnedSpanByte key, IGarnetObject value)
            => storageSession.SET(key, value, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SET<TSourceLogRecord>(in TSourceLogRecord srcLogRecord)
            where TSourceLogRecord : ISourceLogRecord
            => storageSession.SET(in srcLogRecord, ref unifiedContext);

        /// <inheritdoc />
        public GarnetStatus SET<TSourceLogRecord>(PinnedSpanByte key, ref UnifiedInput input, in TSourceLogRecord srcLogRecord)
            where TSourceLogRecord : ISourceLogRecord
            => storageSession.SET(key, ref input, in srcLogRecord, ref unifiedContext);

        #endregion

        #region SETEX
        /// <inheritdoc />
        public unsafe GarnetStatus SETEX(PinnedSpanByte key, PinnedSpanByte value, PinnedSpanByte expiryMs)
            => storageSession.SETEX(key, value, expiryMs, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus SETEX(PinnedSpanByte key, PinnedSpanByte value, TimeSpan expiry)
            => storageSession.SETEX(key, value, expiry, ref stringContext);

        #endregion

        #region SETRANGE

        /// <inheritdoc />
        public GarnetStatus SETRANGE(PinnedSpanByte key, ref StringInput input, ref PinnedSpanByte output)
            => storageSession.SETRANGE(key, ref input, ref output, ref stringContext);

        #endregion

        #region MSETNX
        /// <inheritdoc />
        public GarnetStatus MSET_Conditional(ref StringInput input) =>
            storageSession.MSET_Conditional(ref input, ref stringContext);
        #endregion

        #region APPEND

        /// <inheritdoc />
        public GarnetStatus APPEND(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.APPEND(key, ref input, ref output, ref stringContext);

        /// <inheritdoc />    
        public GarnetStatus APPEND(PinnedSpanByte key, PinnedSpanByte value, ref PinnedSpanByte output)
            => storageSession.APPEND(key, value, ref output, ref stringContext);

        #endregion

        #region RENAME
        /// <inheritdoc />
        public GarnetStatus RENAME(PinnedSpanByte oldKey, PinnedSpanByte newKey)
            => storageSession.RENAME(oldKey, newKey);

        /// <inheritdoc />
        public GarnetStatus RENAMENX(PinnedSpanByte oldKey, PinnedSpanByte newKey, out int result)
            => storageSession.RENAMENX(oldKey, newKey, out result);
        #endregion

        #region Increment (INCR, INCRBY, DECR, DECRBY)
        /// <inheritdoc />
        public GarnetStatus Increment(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.Increment(key, ref input, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus Increment(PinnedSpanByte key, out long output, long incrementCount = 1)
            => storageSession.Increment(key, out output, incrementCount, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus Decrement(PinnedSpanByte key, out long output, long decrementCount = 1)
            => Increment(key, out output, -decrementCount);

        /// <inheritdoc />
        public GarnetStatus IncrementByFloat(PinnedSpanByte key, ref StringOutput output, double val)
        {
            SessionParseState parseState = default;

            var input = new StringInput(RespCommand.INCRBYFLOAT, ref parseState, BitConverter.DoubleToInt64Bits(val));
            _ = Increment(key, ref input, ref output);
            return GarnetStatus.OK;
        }

        /// <inheritdoc />
        public GarnetStatus IncrementByFloat(PinnedSpanByte key, out double output, double val)
        {
            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatDoubleLength + 1];
            var stringOutput = StringOutput.FromPinnedSpan(outputBuffer);

            _ = IncrementByFloat(key, ref stringOutput, val);

            if (!stringOutput.HasError)
            {
                _ = NumUtils.TryReadDouble(stringOutput.SpanByteAndMemory.Span, out output);
            }
            else
            {
                output = (stringOutput.OutputFlags & StringOutputFlags.NaNOrInfinityError) != 0 ? double.NaN : 0;
            }

            return GarnetStatus.OK;
        }
        #endregion

        #region GETDEL
        /// <inheritdoc />
        public GarnetStatus GETDEL(PinnedSpanByte key, ref StringOutput output)
            => storageSession.GETDEL(key, ref output, ref stringContext);
        #endregion

        #region Advanced ops
        /// <inheritdoc />
        public GarnetStatus RMW_MainStore(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.RMW_MainStore(key.ReadOnlySpan, ref input, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus Read_MainStore(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus RMW_ObjectStore(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.RMW_ObjectStore(key.ReadOnlySpan, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus Read_ObjectStore(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => storageSession.Read_ObjectStore(key.ReadOnlySpan, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus RMW_UnifiedStore(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
            => storageSession.RMW_UnifiedStore(key.ReadOnlySpan, ref input, ref output, ref unifiedContext);

        /// <inheritdoc />
        public GarnetStatus Read_UnifiedStore(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
            => storageSession.Read_UnifiedStore(key.ReadOnlySpan, ref input, ref output, ref unifiedContext);

        /// <inheritdoc />
        public void ReadWithPrefetch<TBatch>(ref TBatch batch, long userContext = default)
            where TBatch : IReadArgBatch<FixedSpanByteKey, StringInput, StringOutput>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => storageSession.ReadWithPrefetch(ref batch, ref stringContext, userContext);
        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public GarnetStatus StringSetBit(PinnedSpanByte key, PinnedSpanByte offset, bool bit, out bool previous)
           => storageSession.StringSetBit(key, offset, bit, out previous, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus StringSetBit(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
           => storageSession.StringSetBit(key, ref input, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus StringGetBit(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.StringGetBit(key, ref input, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus StringGetBit(PinnedSpanByte key, PinnedSpanByte offset, out bool bValue)
            => storageSession.StringGetBit(key, offset, out bValue, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus StringBitCount(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.StringBitCount(key, ref input, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus StringBitCount(PinnedSpanByte key, long start, long end, out long result, bool useBitInterval = false)
             => storageSession.StringBitCount(key, start, end, useBitInterval, out result, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus StringBitOperation(ref StringInput input, BitmapOperation bitOp, out long result)
            => storageSession.StringBitOperation(ref input, bitOp, out result);

        /// <inheritdoc />
        public GarnetStatus StringBitOperation(BitmapOperation bitop, PinnedSpanByte destinationKey, PinnedSpanByte[] keys, out long result)
            => storageSession.StringBitOperation(bitop, destinationKey, keys, out result);

        /// <inheritdoc />
        public GarnetStatus StringBitPosition(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.StringBitPosition(key, ref input, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus StringBitField(PinnedSpanByte key, ref StringInput input, RespCommand secondaryCommand, ref StringOutput output)
            => storageSession.StringBitField(key, ref input, secondaryCommand, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus StringBitFieldReadOnly(PinnedSpanByte key, ref StringInput input, RespCommand secondaryCommand, ref StringOutput output)
            => storageSession.StringBitFieldReadOnly(key, ref input, secondaryCommand, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus StringBitField(PinnedSpanByte key, List<BitFieldCmdArgs> commandArguments, out List<long?> result)
            => storageSession.StringBitField(key, commandArguments, out result, ref stringContext);

        #endregion

        #region HyperLogLog Methods
        /// <inheritdoc />
        public GarnetStatus HyperLogLogAdd(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
            => storageSession.HyperLogLogAdd(key, ref input, ref output, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogAdd(PinnedSpanByte key, string[] elements, out bool updated)
            => storageSession.HyperLogLogAdd(key, elements, out updated, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(ref StringInput input, out long count, out bool error)
            => storageSession.HyperLogLogLength(ref input, out count, out error, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(Span<PinnedSpanByte> keys, out long count)
            => storageSession.HyperLogLogLength(keys, out count, ref stringContext);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogMerge(ref StringInput input, out bool error)
            => storageSession.HyperLogLogMerge(ref input, out error);
        #endregion

        #region Server Methods

        /// <inheritdoc />
        public List<byte[]> GetDbKeys(PinnedSpanByte pattern)
            => storageSession.DBKeys(pattern);

        /// <inheritdoc />
        public int GetDbSize()
            => storageSession.DbSize();

        /// <inheritdoc />
        public readonly bool DbScan(PinnedSpanByte patternB, bool allKeys, long cursor, out long storeCursor, out List<byte[]> Keys, long count = 10, ReadOnlySpan<byte> type = default)
            => storageSession.DbScan(patternB, allKeys, cursor, out storeCursor, out Keys, count, type);

        /// <inheritdoc />
        public readonly bool IterateStore<TScanFunctions>(ref TScanFunctions scanFunctions, ref long cursor, long untilAddress = -1, long maxAddress = long.MaxValue, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions
            => storageSession.IterateStore(ref scanFunctions, ref cursor, untilAddress, maxAddress: maxAddress, includeTombstones: includeTombstones);

        /// <inheritdoc />
        public readonly ITsavoriteScanIterator IterateStore()
            => storageSession.IterateStore();

        #endregion

        #region Common Methods

        /// <inheritdoc />
        public GarnetStatus ObjectScan(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
         => storageSession.ObjectScan(key.ReadOnlySpan, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public void ResetScratchBuffer()
            => storageSession.scratchBufferAllocator.Reset();
        #endregion

        #region VectorSet commands

        /// <inheritdoc />
        public unsafe GarnetStatus VectorSetAdd(PinnedSpanByte key, int reduceDims, VectorValueType valueType, PinnedSpanByte values, PinnedSpanByte element, VectorQuantType quantizer, int buildExplorationFactor, PinnedSpanByte attributes, int numLinks, VectorDistanceMetricType distanceMetric, out VectorManagerResult result, out ReadOnlySpan<byte> errorMsg)
        => storageSession.VectorSetAdd(key, reduceDims, valueType, values, element, quantizer, buildExplorationFactor, attributes, numLinks, distanceMetric, out result, out errorMsg);

        /// <inheritdoc />
        public unsafe GarnetStatus VectorSetRemove(PinnedSpanByte key, PinnedSpanByte element)
        => storageSession.VectorSetRemove(key, element);

        /// <inheritdoc />
        public unsafe GarnetStatus VectorSetValueSimilarity(PinnedSpanByte key, VectorValueType valueType, PinnedSpanByte values, int count, float delta, int searchExplorationFactor, PinnedSpanByte filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result, ref SpanByteAndMemory filterBitmap)
        => storageSession.VectorSetValueSimilarity(key, valueType, values, count, delta, searchExplorationFactor, filter.ReadOnlySpan, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes, out result, ref filterBitmap);

        /// <inheritdoc />
        public unsafe GarnetStatus VectorSetElementSimilarity(PinnedSpanByte key, PinnedSpanByte element, int count, float delta, int searchExplorationFactor, PinnedSpanByte filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result, ref SpanByteAndMemory filterBitmap)
        => storageSession.VectorSetElementSimilarity(key, element.ReadOnlySpan, count, delta, searchExplorationFactor, filter.ReadOnlySpan, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes, out result, ref filterBitmap);

        /// <inheritdoc/>
        public unsafe GarnetStatus VectorSetEmbedding(PinnedSpanByte key, PinnedSpanByte element, ref SpanByteAndMemory outputDistances)
        => storageSession.VectorSetEmbedding(key, element.ReadOnlySpan, ref outputDistances);

        /// <inheritdoc/>
        public unsafe GarnetStatus VectorSetDimensions(PinnedSpanByte key, out int dimensions)
        => storageSession.VectorSetDimensions(key, out dimensions);

        /// <inheritdoc/>
        public unsafe GarnetStatus VectorSetInfo(PinnedSpanByte key, out VectorQuantType quantType, out VectorDistanceMetricType distanceMetricType, out uint vectorDimensions, out uint reducedDimensions, out uint buildExplorationFactor, out uint numberOfLinks, out long size)
        => storageSession.VectorSetInfo(key, out quantType, out distanceMetricType, out vectorDimensions, out reducedDimensions, out buildExplorationFactor, out numberOfLinks, out size);

        /// <inheritdoc/>
        public unsafe GarnetStatus VectorSetGetAttribute(PinnedSpanByte key, PinnedSpanByte element, ref SpanByteAndMemory outputAttributes)
        => storageSession.VectorSetGetAttribute(key, element, ref outputAttributes);

        #endregion

        #region RangeIndex
        /// <inheritdoc />
        public GarnetStatus RangeIndexCreate(PinnedSpanByte key, byte storageBackend,
            ulong cacheSize, uint minRecordSize, uint maxRecordSize, uint maxKeyLen, uint leafPageSize,
            out RangeIndexResult result, out ReadOnlySpan<byte> errorMsg)
            => storageSession.RangeIndexCreate(key, storageBackend, cacheSize, minRecordSize, maxRecordSize, maxKeyLen, leafPageSize, out result, out errorMsg);

        /// <inheritdoc />
        public GarnetStatus RangeIndexSet(PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value,
            out RangeIndexResult result, out ReadOnlySpan<byte> errorMsg)
            => storageSession.RangeIndexSet(key, field, value, out result, out errorMsg);

        /// <inheritdoc />
        public GarnetStatus RangeIndexGet(PinnedSpanByte key, PinnedSpanByte field,
            ref StringOutput output, out RangeIndexResult result)
            => storageSession.RangeIndexGet(key, field, ref output, out result);

        /// <inheritdoc />
        public GarnetStatus RangeIndexDel(PinnedSpanByte key, PinnedSpanByte field,
            out RangeIndexResult result)
            => storageSession.RangeIndexDel(key, field, out result);

        /// <inheritdoc />
        public GarnetStatus RangeIndexScan(PinnedSpanByte key, PinnedSpanByte startKey, int count,
            BfTreeInterop.ScanReturnField returnField, ref StringOutput output,
            out int recordCount, out RangeIndexResult result)
            => storageSession.RangeIndexScan(key, startKey, count, returnField, ref output, out recordCount, out result);

        /// <inheritdoc />
        public GarnetStatus RangeIndexRange(PinnedSpanByte key, PinnedSpanByte startKey, PinnedSpanByte endKey,
            BfTreeInterop.ScanReturnField returnField, ref StringOutput output,
            out int recordCount, out RangeIndexResult result)
            => storageSession.RangeIndexRange(key, startKey, endKey, returnField, ref output, out recordCount, out result);

        /// <inheritdoc />
        public GarnetStatus RangeIndexExists(PinnedSpanByte key, out bool exists)
            => storageSession.RangeIndexExists(key, out exists);

        /// <inheritdoc />
        public GarnetStatus RangeIndexConfig(PinnedSpanByte key,
            out byte storageBackend, out ulong cacheSize, out uint minRecordSize,
            out uint maxRecordSize, out uint maxKeyLen, out uint leafPageSize,
            out RangeIndexResult result)
            => storageSession.RangeIndexConfig(key, out storageBackend, out cacheSize, out minRecordSize,
                out maxRecordSize, out maxKeyLen, out leafPageSize, out result);

        /// <inheritdoc />
        public GarnetStatus RangeIndexMetrics(PinnedSpanByte key,
            out nint treeHandle, out bool isLive, out bool isFlushed, out bool isRecovered,
            out RangeIndexResult result)
            => storageSession.RangeIndexMetrics(key, out treeHandle, out isLive, out isFlushed, out isRecovered, out result);
        #endregion
    }
}