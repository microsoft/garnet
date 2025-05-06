// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using ObjectStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    // See TransactionManager.cs for aliases BasicGarnetApi and TransactionalGarnetApi

    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi<TContext, TObjectContext> : IGarnetApi, IGarnetWatchApi
        where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
    {
        readonly StorageSession storageSession;
        TContext context;
        TObjectContext objectContext;

        internal GarnetApi(StorageSession storageSession, TContext context, TObjectContext objectContext)
        {
            this.storageSession = storageSession;
            this.context = context;
            this.objectContext = objectContext;
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

        #region TTL

        /// <inheritdoc />
        public GarnetStatus TTL(PinnedSpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.TTL(key, storeType, ref output, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus PTTL(PinnedSpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.TTL(key, storeType, ref output, ref context, ref objectContext, milliseconds: true);

        #endregion

        #region EXPIRETIME

        /// <inheritdoc />
        public GarnetStatus EXPIRETIME(PinnedSpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.EXPIRETIME(key, storeType, ref output, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus PEXPIRETIME(PinnedSpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.EXPIRETIME(key, storeType, ref output, ref context, ref objectContext, milliseconds: true);

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
        public GarnetStatus SET<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, StoreType storeType)
            where TSourceLogRecord : ISourceLogRecord
            => storageSession.SET(ref srcLogRecord, storeType, ref context, ref objectContext);
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
        public GarnetStatus RENAME(PinnedSpanByte oldKey, PinnedSpanByte newKey, bool withEtag = false, StoreType storeType = StoreType.All)
            => storageSession.RENAME(oldKey, newKey, storeType, withEtag);

        /// <inheritdoc />
        public GarnetStatus RENAMENX(PinnedSpanByte oldKey, PinnedSpanByte newKey, out int result, bool withEtag = false, StoreType storeType = StoreType.All)
            => storageSession.RENAMENX(oldKey, newKey, storeType, out result, withEtag);
        #endregion

        #region EXISTS
        /// <inheritdoc />
        public GarnetStatus EXISTS(PinnedSpanByte key, StoreType storeType = StoreType.All)
            => storageSession.EXISTS(key, storeType, ref context, ref objectContext);
        #endregion

        #region EXPIRE
        /// <inheritdoc />
        public unsafe GarnetStatus EXPIRE(PinnedSpanByte key, ref RawStringInput input, out bool timeoutSet, StoreType storeType = StoreType.All)
            => storageSession.EXPIRE(key, ref input, out timeoutSet, storeType, ref context, ref objectContext);

        /// <inheritdoc />
        public unsafe GarnetStatus EXPIRE(PinnedSpanByte key, PinnedSpanByte expiryMs, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIRE(key, expiryMs, out timeoutSet, storeType, expireOption, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus EXPIRE(PinnedSpanByte key, TimeSpan expiry, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIRE(key, expiry, out timeoutSet, storeType, expireOption, ref context, ref objectContext);
        #endregion

        #region EXPIREAT

        /// <inheritdoc />
        public GarnetStatus EXPIREAT(PinnedSpanByte key, long expiryTimestamp, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIREAT(key, expiryTimestamp, out timeoutSet, storeType, expireOption, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus PEXPIREAT(PinnedSpanByte key, long expiryTimestamp, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
             => storageSession.EXPIREAT(key, expiryTimestamp, out timeoutSet, storeType, expireOption, ref context, ref objectContext, milliseconds: true);

        #endregion

        #region PERSIST
        /// <inheritdoc />
        public unsafe GarnetStatus PERSIST(PinnedSpanByte key, StoreType storeType = StoreType.All)
            => storageSession.PERSIST(key, storeType, ref context, ref objectContext);
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
        #endregion

        #region DELETE
        /// <inheritdoc />
        public GarnetStatus DELETE(PinnedSpanByte key, StoreType storeType = StoreType.All)
            => storageSession.DELETE(key, storeType, ref context, ref objectContext);
        #endregion

        #region GETDEL
        /// <inheritdoc />
        public GarnetStatus GETDEL(PinnedSpanByte key, ref SpanByteAndMemory output)
            => storageSession.GETDEL(key, ref output, ref context);
        #endregion

        #region TYPE

        /// <inheritdoc />
        public GarnetStatus GetKeyType(PinnedSpanByte key, out string typeName)
            => storageSession.GetKeyType(key, out typeName, ref context, ref objectContext);

        #endregion

        #region MEMORY

        /// <inheritdoc />
        public GarnetStatus MemoryUsageForKey(PinnedSpanByte key, out long memoryUsage, int samples = 0)
            => storageSession.MemoryUsageForKey(key, out memoryUsage, ref context, ref objectContext, samples);

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
        public readonly bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions
            => storageSession.IterateMainStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public readonly ITsavoriteScanIterator IterateMainStore()
            => storageSession.IterateMainStore();

        /// <inheritdoc />
        public readonly bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions
            => storageSession.IterateObjectStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public readonly ITsavoriteScanIterator IterateObjectStore()
            => storageSession.IterateObjectStore();

        #endregion

        #region Common Methods

        /// <inheritdoc />
        public GarnetStatus ObjectScan(PinnedSpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
         => storageSession.ObjectScan(key.ReadOnlySpan, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public int GetScratchBufferOffset()
            => storageSession.scratchBufferManager.ScratchBufferOffset;

        /// <inheritdoc />
        public bool ResetScratchBuffer(int offset)
            => storageSession.scratchBufferManager.ResetScratchBuffer(offset);
        #endregion
    }
}