// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = ObjectAllocator<IGarnetObject, StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>;

    // See TransactionManager.cs for aliases BasicGarnetApi and TransactionalGarnetApi

    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi<TContext, TObjectContext> : IGarnetApi, IGarnetWatchApi
        where TContext : ITsavoriteContext<SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        where TObjectContext : ITsavoriteContext<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
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
        public void WATCH(ArgSlice key, StoreType type)
            => storageSession.WATCH(key, type);

        /// <inheritdoc />
        public void WATCH(SpanByte key, StoreType type)
            => storageSession.WATCH(key, type);
        #endregion

        #region GET
        /// <inheritdoc />
        public GarnetStatus GET(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.GET(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus GET_WithPending(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, long ctx, out bool pending)
            => storageSession.GET_WithPending(key, ref input, ref output, ctx, out pending, ref context);

        /// <inheritdoc />
        public bool GET_CompletePending((GarnetStatus, SpanByteAndMemory)[] outputArr, bool wait = false)
            => storageSession.GET_CompletePending(outputArr, wait, ref context);

        public bool GET_CompletePending(out CompletedOutputIterator<SpanByte, RawStringInput, SpanByteAndMemory, long> completedOutputs, bool wait)
            => storageSession.GET_CompletePending(out completedOutputs, wait, ref context);

        /// <inheritdoc />
        public unsafe GarnetStatus GETForMemoryResult(ArgSlice key, out MemoryResult<byte> value)
            => storageSession.GET(key, out value, ref context);

        /// <inheritdoc />
        public unsafe GarnetStatus GET(ArgSlice key, out ArgSlice value)
            => storageSession.GET(key, out value, ref context);

        /// <inheritdoc />
        public GarnetStatus GET(SpanByte key, out GarnetObjectStoreOutput value)
            => storageSession.GET(key, out value, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus LCS(ArgSlice key1, ArgSlice key2, ref SpanByteAndMemory output, bool lenOnly = false, bool withIndices = false, bool withMatchLen = false, int minMatchLen = 0)
            => storageSession.LCS(key1, key2, ref output, lenOnly, withIndices, withMatchLen, minMatchLen);
        #endregion

        #region GETEX

        /// <inheritdoc />
        public GarnetStatus GETEX(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.GETEX(key, ref input, ref output, ref context);

        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public GarnetStatus GETRANGE(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.GETRANGE(key, ref input, ref output, ref context);
        #endregion

        #region TTL

        /// <inheritdoc />
        public GarnetStatus TTL(SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.TTL(key, storeType, ref output, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus PTTL(SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.TTL(key, storeType, ref output, ref context, ref objectContext, milliseconds: true);

        #endregion

        #region EXPIRETIME

        /// <inheritdoc />
        public GarnetStatus EXPIRETIME(SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.EXPIRETIME(key, storeType, ref output, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus PEXPIRETIME(SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.EXPIRETIME(key, storeType, ref output, ref context, ref objectContext, milliseconds: true);

        #endregion

        #region SET
        /// <inheritdoc />
        public GarnetStatus SET(SpanByte key, SpanByte value)
            => storageSession.SET(key, value, ref context);

        /// <inheritdoc />
        public GarnetStatus SET(SpanByte key, ref RawStringInput input, SpanByte value)
            => storageSession.SET(key, ref input, value, ref context);

        /// <inheritdoc />
        public GarnetStatus SET_Conditional(SpanByte key, ref RawStringInput input)
            => storageSession.SET_Conditional(key, ref input, ref context);

        /// <inheritdoc />
        public GarnetStatus SET_Conditional(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.SET_Conditional(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus SET(ArgSlice key, Memory<byte> value)
            => storageSession.SET(key, value, ref context);

        /// <inheritdoc />
        public GarnetStatus SET(ArgSlice key, ArgSlice value)
            => storageSession.SET(key, value, ref context);

        /// <inheritdoc />
        public GarnetStatus SET(SpanByte key, IGarnetObject value)
            => storageSession.SET(key, value, ref objectContext);
        #endregion

        #region SETEX
        /// <inheritdoc />
        public unsafe GarnetStatus SETEX(ArgSlice key, ArgSlice value, ArgSlice expiryMs)
            => storageSession.SETEX(key, value, expiryMs, ref context);

        /// <inheritdoc />
        public GarnetStatus SETEX(ArgSlice key, ArgSlice value, TimeSpan expiry)
            => storageSession.SETEX(key, value, expiry, ref context);

        #endregion

        #region SETRANGE

        /// <inheritdoc />
        public GarnetStatus SETRANGE(ArgSlice key, ref RawStringInput input, ref ArgSlice output)
            => storageSession.SETRANGE(key, ref input, ref output, ref context);

        #endregion

        #region MSETNX
        /// <inheritdoc />
        public GarnetStatus MSET_Conditional(ref RawStringInput input) =>
            storageSession.MSET_Conditional(ref input, ref context);
        #endregion

        #region APPEND

        /// <inheritdoc />
        public GarnetStatus APPEND(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.APPEND(key, ref input, ref output, ref context);

        /// <inheritdoc />    
        public GarnetStatus APPEND(ArgSlice key, ArgSlice value, ref ArgSlice output)
            => storageSession.APPEND(key, value, ref output, ref context);

        #endregion

        #region RENAME
        /// <inheritdoc />
        public GarnetStatus RENAME(ArgSlice oldKey, ArgSlice newKey, bool withEtag = false, StoreType storeType = StoreType.All)
            => storageSession.RENAME(oldKey, newKey, storeType, withEtag);

        /// <inheritdoc />
        public GarnetStatus RENAMENX(ArgSlice oldKey, ArgSlice newKey, out int result, bool withEtag = false, StoreType storeType = StoreType.All)
            => storageSession.RENAMENX(oldKey, newKey, storeType, out result, withEtag);
        #endregion

        #region EXISTS
        /// <inheritdoc />
        public GarnetStatus EXISTS(ArgSlice key, StoreType storeType = StoreType.All)
            => storageSession.EXISTS(key, storeType, ref context, ref objectContext);
        #endregion

        #region EXPIRE
        /// <inheritdoc />
        public unsafe GarnetStatus EXPIRE(ArgSlice key, ref RawStringInput input, out bool timeoutSet, StoreType storeType = StoreType.All)
            => storageSession.EXPIRE(key, ref input, out timeoutSet, storeType, ref context, ref objectContext);

        /// <inheritdoc />
        public unsafe GarnetStatus EXPIRE(ArgSlice key, ArgSlice expiryMs, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIRE(key, expiryMs, out timeoutSet, storeType, expireOption, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus EXPIRE(ArgSlice key, TimeSpan expiry, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIRE(key, expiry, out timeoutSet, storeType, expireOption, ref context, ref objectContext);
        #endregion

        #region EXPIREAT

        /// <inheritdoc />
        public GarnetStatus EXPIREAT(ArgSlice key, long expiryTimestamp, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIREAT(key, expiryTimestamp, out timeoutSet, storeType, expireOption, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus PEXPIREAT(ArgSlice key, long expiryTimestamp, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
             => storageSession.EXPIREAT(key, expiryTimestamp, out timeoutSet, storeType, expireOption, ref context, ref objectContext, milliseconds: true);

        #endregion

        #region PERSIST
        /// <inheritdoc />
        public unsafe GarnetStatus PERSIST(ArgSlice key, StoreType storeType = StoreType.All)
            => storageSession.PERSIST(key, storeType, ref context, ref objectContext);
        #endregion

        #region Increment (INCR, INCRBY, DECR, DECRBY)
        /// <inheritdoc />
        public GarnetStatus Increment(ArgSlice key, ref RawStringInput input, ref ArgSlice output)
            => storageSession.Increment(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus Increment(ArgSlice key, out long output, long incrementCount = 1)
            => storageSession.Increment(key, out output, incrementCount, ref context);

        /// <inheritdoc />
        public GarnetStatus Decrement(ArgSlice key, out long output, long decrementCount = 1)
            => Increment(key, out output, -decrementCount);
        #endregion

        #region DELETE
        /// <inheritdoc />
        public GarnetStatus DELETE(ArgSlice key, StoreType storeType = StoreType.All)
            => storageSession.DELETE(key, storeType, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus DELETE(SpanByte key, StoreType storeType = StoreType.All)
            => storageSession.DELETE(key, storeType, ref context, ref objectContext);
        #endregion

        #region GETDEL
        /// <inheritdoc />
        public GarnetStatus GETDEL(SpanByte key, ref SpanByteAndMemory output)
            => storageSession.GETDEL(key, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus GETDEL(ArgSlice key, ref SpanByteAndMemory output)
            => storageSession.GETDEL(key, ref output, ref context);
        #endregion

        #region TYPE

        /// <inheritdoc />
        public GarnetStatus GetKeyType(ArgSlice key, out string typeName)
            => storageSession.GetKeyType(key, out typeName, ref context, ref objectContext);

        #endregion

        #region MEMORY

        /// <inheritdoc />
        public GarnetStatus MemoryUsageForKey(ArgSlice key, out long memoryUsage, int samples = 0)
            => storageSession.MemoryUsageForKey(key, out memoryUsage, ref context, ref objectContext, samples);

        #endregion

        #region Advanced ops
        /// <inheritdoc />
        public GarnetStatus RMW_MainStore(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.RMW_MainStore(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus Read_MainStore(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.Read_MainStore(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus RMW_ObjectStore(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            => storageSession.RMW_ObjectStore(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus Read_ObjectStore(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            => storageSession.Read_ObjectStore(key, ref input, ref output, ref objectContext);
        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public GarnetStatus StringSetBit(ArgSlice key, ArgSlice offset, bool bit, out bool previous)
           => storageSession.StringSetBit(key, offset, bit, out previous, ref context);

        /// <inheritdoc />
        public GarnetStatus StringSetBit(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
           => storageSession.StringSetBit(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringGetBit(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.StringGetBit(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringGetBit(ArgSlice key, ArgSlice offset, out bool bValue)
            => storageSession.StringGetBit(key, offset, out bValue, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitCount(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.StringBitCount(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitCount(ArgSlice key, long start, long end, out long result, bool useBitInterval = false)
             => storageSession.StringBitCount(key, start, end, useBitInterval, out result, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitOperation(ref RawStringInput input, BitmapOperation bitOp, out long result)
            => storageSession.StringBitOperation(ref input, bitOp, out result);

        /// <inheritdoc />
        public GarnetStatus StringBitOperation(BitmapOperation bitop, ArgSlice destinationKey, ArgSlice[] keys, out long result)
            => storageSession.StringBitOperation(bitop, destinationKey, keys, out result);

        /// <inheritdoc />
        public GarnetStatus StringBitPosition(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.StringBitPosition(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitField(SpanByte key, ref RawStringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output)
            => storageSession.StringBitField(key, ref input, secondaryCommand, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitFieldReadOnly(SpanByte key, ref RawStringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output)
            => storageSession.StringBitFieldReadOnly(key, ref input, secondaryCommand, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitField(ArgSlice key, List<BitFieldCmdArgs> commandArguments, out List<long?> result)
            => storageSession.StringBitField(key, commandArguments, out result, ref context);

        #endregion

        #region HyperLogLog Methods
        /// <inheritdoc />
        public GarnetStatus HyperLogLogAdd(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.HyperLogLogAdd(key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogAdd(ArgSlice key, string[] elements, out bool updated)
            => storageSession.HyperLogLogAdd(key, elements, out updated, ref context);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(ref RawStringInput input, out long count, out bool error)
            => storageSession.HyperLogLogLength(ref input, out count, out error, ref context);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(Span<ArgSlice> keys, out long count)
            => storageSession.HyperLogLogLength(keys, out count, ref context);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogMerge(ref RawStringInput input, out bool error)
            => storageSession.HyperLogLogMerge(ref input, out error);
        #endregion

        #region Server Methods

        /// <inheritdoc />
        public List<byte[]> GetDbKeys(ArgSlice pattern)
            => storageSession.DBKeys(pattern);

        /// <inheritdoc />
        public int GetDbSize()
            => storageSession.DbSize();

        /// <inheritdoc />
        public bool DbScan(ArgSlice patternB, bool allKeys, long cursor, out long storeCursor, out List<byte[]> Keys, long count = 10, ReadOnlySpan<byte> type = default)
            => storageSession.DbScan(patternB, allKeys, cursor, out storeCursor, out Keys, count, type);

        /// <inheritdoc />
        public bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<SpanByte>
            => storageSession.IterateMainStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public ITsavoriteScanIterator<SpanByte> IterateMainStore()
            => storageSession.IterateMainStore();

        /// <inheritdoc />
        public bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<IGarnetObject>
            => storageSession.IterateObjectStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public ITsavoriteScanIterator<IGarnetObject> IterateObjectStore()
            => storageSession.IterateObjectStore();

        #endregion

        #region Common Methods

        /// <inheritdoc />
        public GarnetStatus ObjectScan(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
         => storageSession.ObjectScan(key, ref input, ref outputFooter, ref objectContext);

        /// <inheritdoc />
        public int GetScratchBufferOffset()
            => storageSession.scratchBufferManager.ScratchBufferOffset;

        /// <inheritdoc />
        public bool ResetScratchBuffer(int offset)
            => storageSession.scratchBufferManager.ResetScratchBuffer(offset);
        #endregion
    }
}