// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

global using BasicGarnetApi = Garnet.server.GarnetApi<
        Tsavorite.core.BasicContext<Garnet.server.RawStringInput, Tsavorite.core.SpanByteAndMemory, long, Garnet.server.MainSessionFunctions,
            /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
        Tsavorite.core.BasicContext<Garnet.server.ObjectInput, Garnet.server.GarnetObjectStoreOutput, long, Garnet.server.ObjectSessionFunctions,
            /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
        Tsavorite.core.BasicContext<Garnet.server.UnifiedStoreInput, Garnet.server.GarnetUnifiedStoreOutput, long, Garnet.server.UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>>;
global using ConsistentReadGarnetApi = Garnet.server.GarnetApi<
        Tsavorite.core.ConsistentReadContext<Garnet.server.RawStringInput, Tsavorite.core.SpanByteAndMemory, long, Garnet.server.MainSessionFunctions,
            /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
        Tsavorite.core.ConsistentReadContext<Garnet.server.ObjectInput, Garnet.server.GarnetObjectStoreOutput, long, Garnet.server.ObjectSessionFunctions,
            /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
        Tsavorite.core.ConsistentReadContext<Garnet.server.UnifiedStoreInput, Garnet.server.GarnetUnifiedStoreOutput, long, Garnet.server.UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>>;
global using StoreAllocator = Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>;
global using StoreFunctions = Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>;
global using TransactionalConsistentReadGarnetApi = Garnet.server.GarnetApi<
        Tsavorite.core.TransactionalConsistentReadContext<Garnet.server.RawStringInput, Tsavorite.core.SpanByteAndMemory, long, Garnet.server.MainSessionFunctions,
            /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
        Tsavorite.core.TransactionalConsistentReadContext<Garnet.server.ObjectInput, Garnet.server.GarnetObjectStoreOutput, long, Garnet.server.ObjectSessionFunctions,
            /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
        Tsavorite.core.TransactionalConsistentReadContext<Garnet.server.UnifiedStoreInput, Garnet.server.GarnetUnifiedStoreOutput, long, Garnet.server.UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>>;
global using TransactionalGarnetApi = Garnet.server.GarnetApi<
        Tsavorite.core.TransactionalContext<Garnet.server.RawStringInput, Tsavorite.core.SpanByteAndMemory, long, Garnet.server.MainSessionFunctions,
            /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
        Tsavorite.core.TransactionalContext<Garnet.server.ObjectInput, Garnet.server.GarnetObjectStoreOutput, long, Garnet.server.ObjectSessionFunctions,
            /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
        Tsavorite.core.TransactionalContext<Garnet.server.UnifiedStoreInput, Garnet.server.GarnetUnifiedStoreOutput, long, Garnet.server.UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>>;

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
    public partial struct GarnetApi<TContext, TObjectContext, TUnifiedContext> : IGarnetApi, IGarnetWatchApi
        where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
    {
        readonly StorageSession storageSession;
        TContext context;
        TObjectContext objectContext;
        TUnifiedContext unifiedContext;

        internal GarnetApi(StorageSession storageSession, TContext context, TObjectContext objectContext, TUnifiedContext unifiedContext)
        {
            this.storageSession = storageSession;
            this.context = context;
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
        public GarnetStatus GET(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.GET(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus GET_WithPending(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, long ctx, out bool pending)
            => storageSession.GET_WithPending(key.ReadOnlySpan, ref input, ref output, ctx, out pending, ref context);

        /// <inheritdoc />
        public bool GET_CompletePending((GarnetStatus, SpanByteAndMemory)[] outputArr, bool wait = false)
            => storageSession.GET_CompletePending(outputArr, wait, ref context);

        public bool GET_CompletePending(out CompletedOutputIterator<RawStringInput, SpanByteAndMemory, long> completedOutputs, bool wait)
            => storageSession.GET_CompletePending(out completedOutputs, wait, ref context);

        /// <inheritdoc />
        public unsafe GarnetStatus GETForMemoryResult(PinnedSpanByte key, out MemoryResult<byte> value)
            => storageSession.GET(key, out value, ref context);

        /// <inheritdoc />
        public unsafe GarnetStatus GET(PinnedSpanByte key, out PinnedSpanByte value)
            => storageSession.GET(key, out value, ref context);

        /// <inheritdoc />
        public GarnetStatus GET(PinnedSpanByte key, out GarnetObjectStoreOutput value)
            => storageSession.GET(key, out value, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus LCS(PinnedSpanByte key1, PinnedSpanByte key2, ref SpanByteAndMemory output, bool lenOnly = false, bool withIndices = false, bool withMatchLen = false, int minMatchLen = 0)
            => storageSession.LCS(key1, key2, ref output, lenOnly, withIndices, withMatchLen, minMatchLen);
        #endregion

        #region GETEX

        /// <inheritdoc />
        public GarnetStatus GETEX(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.GETEX(key, ref input, ref output, ref context);

        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public GarnetStatus GETRANGE(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.GETRANGE(key, ref input, ref output, ref context);
        #endregion

        #region SET
        /// <inheritdoc />
        public GarnetStatus SET(PinnedSpanByte key, PinnedSpanByte value)
            => storageSession.SET(key, value, ref context);

        /// <inheritdoc />
        public GarnetStatus SET(PinnedSpanByte key, ref RawStringInput input, PinnedSpanByte value)
            => storageSession.SET(key, ref input, value, ref context);

        /// <inheritdoc />
        public GarnetStatus SET_Conditional(PinnedSpanByte key, ref RawStringInput input)
            => storageSession.SET_Conditional(key, ref input, ref context);

        /// <inheritdoc />
        public GarnetStatus DEL_Conditional(PinnedSpanByte key, ref RawStringInput input)
            => storageSession.DEL_Conditional(key, ref input, ref context);

        /// <inheritdoc />
        public GarnetStatus SET_Conditional(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.SET_Conditional(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus SET(PinnedSpanByte key, Memory<byte> value)
            => storageSession.SET(key, value, ref context);

        /// <inheritdoc />
        public GarnetStatus SET(PinnedSpanByte key, IGarnetObject value)
            => storageSession.SET(key, value, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus SET<TSourceLogRecord>(in TSourceLogRecord srcLogRecord)
            where TSourceLogRecord : ISourceLogRecord
            => storageSession.SET(in srcLogRecord, ref unifiedContext);

        /// <inheritdoc />
        public GarnetStatus SET<TSourceLogRecord>(PinnedSpanByte key, ref UnifiedStoreInput input, in TSourceLogRecord srcLogRecord)
            where TSourceLogRecord : ISourceLogRecord
            => storageSession.SET(key, ref input, in srcLogRecord, ref unifiedContext);

        #endregion

        #region SETEX
        /// <inheritdoc />
        public unsafe GarnetStatus SETEX(PinnedSpanByte key, PinnedSpanByte value, PinnedSpanByte expiryMs)
            => storageSession.SETEX(key, value, expiryMs, ref context);

        /// <inheritdoc />
        public GarnetStatus SETEX(PinnedSpanByte key, PinnedSpanByte value, TimeSpan expiry)
            => storageSession.SETEX(key, value, expiry, ref context);

        #endregion

        #region SETRANGE

        /// <inheritdoc />
        public GarnetStatus SETRANGE(PinnedSpanByte key, ref RawStringInput input, ref PinnedSpanByte output)
            => storageSession.SETRANGE(key, ref input, ref output, ref context);

        #endregion

        #region MSETNX
        /// <inheritdoc />
        public GarnetStatus MSET_Conditional(ref RawStringInput input) =>
            storageSession.MSET_Conditional(ref input, ref context);
        #endregion

        #region APPEND

        /// <inheritdoc />
        public GarnetStatus APPEND(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.APPEND(key, ref input, ref output, ref context);

        /// <inheritdoc />    
        public GarnetStatus APPEND(PinnedSpanByte key, PinnedSpanByte value, ref PinnedSpanByte output)
            => storageSession.APPEND(key, value, ref output, ref context);

        #endregion

        #region RENAME
        /// <inheritdoc />
        public GarnetStatus RENAME(PinnedSpanByte oldKey, PinnedSpanByte newKey, bool withEtag = false)
            => storageSession.RENAME(oldKey, newKey, withEtag);

        /// <inheritdoc />
        public GarnetStatus RENAMENX(PinnedSpanByte oldKey, PinnedSpanByte newKey, out int result, bool withEtag = false)
            => storageSession.RENAMENX(oldKey, newKey, out result, withEtag);
        #endregion

        #region Increment (INCR, INCRBY, DECR, DECRBY)
        /// <inheritdoc />
        public GarnetStatus Increment(PinnedSpanByte key, ref RawStringInput input, ref PinnedSpanByte output)
            => storageSession.Increment(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus Increment(PinnedSpanByte key, out long output, long incrementCount = 1)
            => storageSession.Increment(key, out output, incrementCount, ref context);

        /// <inheritdoc />
        public GarnetStatus Decrement(PinnedSpanByte key, out long output, long decrementCount = 1)
            => Increment(key, out output, -decrementCount);

        /// <inheritdoc />
        public GarnetStatus IncrementByFloat(PinnedSpanByte key, ref PinnedSpanByte output, double val)
        {
            SessionParseState parseState = default;

            var input = new RawStringInput(RespCommand.INCRBYFLOAT, ref parseState, BitConverter.DoubleToInt64Bits(val));
            _ = Increment(key, ref input, ref output);

            if (output.Length != NumUtils.MaximumFormatDoubleLength + 1)
                return GarnetStatus.OK;

            var errorFlag = (OperationError)output.Span[0];

            switch (errorFlag)
            {
                case OperationError.INVALID_TYPE:
                case OperationError.NAN_OR_INFINITY:
                    return GarnetStatus.WRONGTYPE;
                default:
                    throw new GarnetException($"Invalid OperationError {errorFlag}");
            }
        }

        /// <inheritdoc />
        public GarnetStatus IncrementByFloat(PinnedSpanByte key, out double output, double val)
        {
            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatDoubleLength + 1];
            var _output = PinnedSpanByte.FromPinnedSpan(outputBuffer);
            var status = IncrementByFloat(key, ref _output, val);

            switch (status)
            {
                case GarnetStatus.OK:
                    _ = NumUtils.TryReadDouble(_output.ReadOnlySpan, out output);
                    break;
                case GarnetStatus.WRONGTYPE:
                default:
                    var errorFlag = (OperationError)_output.Span[0];
                    output = errorFlag == OperationError.NAN_OR_INFINITY ? double.NaN : 0;
                    break;
            }

            return status;
        }
        #endregion

        #region GETDEL
        /// <inheritdoc />
        public GarnetStatus GETDEL(PinnedSpanByte key, ref SpanByteAndMemory output)
            => storageSession.GETDEL(key, ref output, ref context);
        #endregion

        #region Advanced ops
        /// <inheritdoc />
        public GarnetStatus RMW_MainStore(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.RMW_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus Read_MainStore(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus RMW_ObjectStore(PinnedSpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            => storageSession.RMW_ObjectStore(key.ReadOnlySpan, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus Read_ObjectStore(PinnedSpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            => storageSession.Read_ObjectStore(key.ReadOnlySpan, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus RMW_UnifiedStore(PinnedSpanByte key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output)
            => storageSession.RMW_UnifiedStore(key.ReadOnlySpan, ref input, ref output, ref unifiedContext);

        /// <inheritdoc />
        public GarnetStatus Read_UnifiedStore(PinnedSpanByte key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output)
            => storageSession.Read_UnifiedStore(key.ReadOnlySpan, ref input, ref output, ref unifiedContext);
        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public GarnetStatus StringSetBit(PinnedSpanByte key, PinnedSpanByte offset, bool bit, out bool previous)
           => storageSession.StringSetBit(key, offset, bit, out previous, ref context);

        /// <inheritdoc />
        public GarnetStatus StringSetBit(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
           => storageSession.StringSetBit(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringGetBit(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.StringGetBit(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringGetBit(PinnedSpanByte key, PinnedSpanByte offset, out bool bValue)
            => storageSession.StringGetBit(key, offset, out bValue, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitCount(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.StringBitCount(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitCount(PinnedSpanByte key, long start, long end, out long result, bool useBitInterval = false)
             => storageSession.StringBitCount(key, start, end, useBitInterval, out result, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitOperation(ref RawStringInput input, BitmapOperation bitOp, out long result)
            => storageSession.StringBitOperation(ref input, bitOp, out result);

        /// <inheritdoc />
        public GarnetStatus StringBitOperation(BitmapOperation bitop, PinnedSpanByte destinationKey, PinnedSpanByte[] keys, out long result)
            => storageSession.StringBitOperation(bitop, destinationKey, keys, out result);

        /// <inheritdoc />
        public GarnetStatus StringBitPosition(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.StringBitPosition(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitField(PinnedSpanByte key, ref RawStringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output)
            => storageSession.StringBitField(key, ref input, secondaryCommand, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitFieldReadOnly(PinnedSpanByte key, ref RawStringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output)
            => storageSession.StringBitFieldReadOnly(key, ref input, secondaryCommand, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitField(PinnedSpanByte key, List<BitFieldCmdArgs> commandArguments, out List<long?> result)
            => storageSession.StringBitField(key, commandArguments, out result, ref context);

        #endregion

        #region HyperLogLog Methods
        /// <inheritdoc />
        public GarnetStatus HyperLogLogAdd(PinnedSpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.HyperLogLogAdd(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogAdd(PinnedSpanByte key, string[] elements, out bool updated)
            => storageSession.HyperLogLogAdd(key, elements, out updated, ref context);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(ref RawStringInput input, out long count, out bool error)
            => storageSession.HyperLogLogLength(ref input, out count, out error, ref context);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(Span<PinnedSpanByte> keys, out long count)
            => storageSession.HyperLogLogLength(keys, out count, ref context);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogMerge(ref RawStringInput input, out bool error)
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
        public GarnetStatus ObjectScan(PinnedSpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
         => storageSession.ObjectScan(key.ReadOnlySpan, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public int GetScratchBufferOffset()
            => storageSession.scratchBufferBuilder.ScratchBufferOffset;

        /// <inheritdoc />
        public bool ResetScratchBuffer(int offset)
            => storageSession.scratchBufferBuilder.ResetScratchBuffer(offset);
        #endregion
    }
}