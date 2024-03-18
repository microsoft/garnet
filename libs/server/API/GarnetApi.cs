// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    // Example aliases:
    //   using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;
    //   using LockableGarnetApi = GarnetApi<LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;

    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi<TContext, TObjectContext> : IGarnetApi, IGarnetWatchApi
        where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
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
        public void WATCH(byte[] key, StoreType type)
            => storageSession.WATCH(key, type);
        #endregion

        #region GET
        /// <inheritdoc />
        public GarnetStatus GET(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.GET(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus GET_WithPending(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, long ctx, out bool pending)
            => storageSession.GET_WithPending(ref key, ref input, ref output, ctx, out pending, ref context);

        /// <inheritdoc />
        public bool GET_CompletePending((GarnetStatus, SpanByteAndMemory)[] outputArr, bool wait = false)
            => storageSession.GET_CompletePending(outputArr, wait, ref context);

        /// <inheritdoc />
        public unsafe GarnetStatus GETForMemoryResult(ArgSlice key, out MemoryResult<byte> value)
            => storageSession.GET(key, out value, ref context);

        /// <inheritdoc />
        public unsafe GarnetStatus GET(ArgSlice key, out ArgSlice value)
            => storageSession.GET(key, out value, ref context);

        /// <inheritdoc />
        public GarnetStatus GET(byte[] key, out GarnetObjectStoreOutput value)
            => storageSession.GET(key, out value, ref objectContext);
        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public GarnetStatus GETRANGE(ref SpanByte key, int sliceStart, int sliceLength, ref SpanByteAndMemory output)
            => storageSession.GETRANGE(ref key, sliceStart, sliceLength, ref output, ref context);
        #endregion

        #region TTL

        /// <inheritdoc />
        public GarnetStatus TTL(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.TTL(ref key, storeType, ref output, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus PTTL(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.TTL(ref key, storeType, ref output, ref context, ref objectContext, milliseconds: true);

        #endregion

        #region SET
        /// <inheritdoc />
        public GarnetStatus SET(ref SpanByte key, ref SpanByte value)
            => storageSession.SET(ref key, ref value, ref context);

        /// <inheritdoc />
        public GarnetStatus SET_Conditional(ref SpanByte key, ref SpanByte input)
            => storageSession.SET_Conditional(ref key, ref input, ref context);

        /// <inheritdoc />
        public GarnetStatus SET_Conditional(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.SET_Conditional(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus SET(ArgSlice key, Memory<byte> value)
            => storageSession.SET(key, value, ref context);

        /// <inheritdoc />
        public GarnetStatus SET(ArgSlice key, ArgSlice value)
            => storageSession.SET(key, value, ref context);

        /// <inheritdoc />
        public GarnetStatus SET(byte[] key, IGarnetObject value)
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
        public GarnetStatus SETRANGE(ArgSlice key, ArgSlice value, int offset, ref ArgSlice output)
            => storageSession.SETRANGE(key, value, offset, ref output, ref context);

        #endregion

        #region APPEND

        /// <inheritdoc />
        public GarnetStatus APPEND(ref SpanByte key, ref SpanByte value, ref SpanByteAndMemory output)
            => storageSession.APPEND(ref key, ref value, ref output, ref context);

        /// <inheritdoc />    
        public GarnetStatus APPEND(ArgSlice key, ArgSlice value, ref ArgSlice output)
            => storageSession.APPEND(key, value, ref output, ref context);

        #endregion

        #region RENAME
        /// <inheritdoc />
        public GarnetStatus RENAME(ArgSlice oldKey, ArgSlice newKey, StoreType storeType = StoreType.All)
            => storageSession.RENAME(oldKey, newKey, storeType);
        #endregion

        #region EXISTS
        /// <inheritdoc />
        public GarnetStatus EXISTS(ArgSlice key, StoreType storeType = StoreType.All)
            => storageSession.EXISTS(key, storeType, ref context, ref objectContext);
        #endregion

        #region EXPIRE
        /// <inheritdoc />
        public unsafe GarnetStatus EXPIRE(ArgSlice key, ArgSlice expiryMs, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIRE(key, expiryMs, out timeoutSet, storeType, expireOption, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus EXPIRE(ArgSlice key, TimeSpan expiry, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIRE(key, expiry, out timeoutSet, storeType, expireOption, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus PEXPIRE(ArgSlice key, TimeSpan expiry, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
             => storageSession.EXPIRE(key, expiry, out timeoutSet, storeType, expireOption, ref context, ref objectContext, milliseconds: true);

        #endregion

        #region PERSIST
        /// <inheritdoc />
        public unsafe GarnetStatus PERSIST(ArgSlice key, StoreType storeType = StoreType.All)
            => storageSession.PERSIST(key, storeType, ref context, ref objectContext);
        #endregion

        #region Increment (INCR, INCRBY, DECR, DECRBY)
        /// <inheritdoc />
        public unsafe GarnetStatus Increment(ArgSlice key, ArgSlice input, ref ArgSlice output)
            => storageSession.Increment(key, input, ref output, ref context);
        #endregion

        #region DELETE
        /// <inheritdoc />
        public GarnetStatus DELETE(ArgSlice key, StoreType storeType = StoreType.All)
            => storageSession.DELETE(key, storeType, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus DELETE(ref SpanByte key, StoreType storeType = StoreType.All)
            => storageSession.DELETE(ref key, storeType, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus DELETE(byte[] key, StoreType storeType = StoreType.All)
            => storageSession.DELETE(key, storeType, ref context, ref objectContext);
        #endregion

        #region GETDEL
        /// <inheritdoc />
        public GarnetStatus GETDEL(ref SpanByte key, ref SpanByteAndMemory output)
            => storageSession.GETDEL(ref key, ref output, ref context);

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
        public GarnetStatus RMW_MainStore(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.RMW_MainStore(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus Read_MainStore(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.Read_MainStore(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus RMW_ObjectStore(ref byte[] key, ref SpanByte input, ref GarnetObjectStoreOutput output)
            => storageSession.RMW_ObjectStore(ref key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus Read_ObjectStore(ref byte[] key, ref SpanByte input, ref GarnetObjectStoreOutput output)
            => storageSession.Read_ObjectStore(ref key, ref input, ref output, ref objectContext);
        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public GarnetStatus StringSetBit(ArgSlice key, ArgSlice offset, bool bit, out bool previous)
           => storageSession.StringSetBit(key, offset, bit, out previous, ref context);

        /// <inheritdoc />
        public GarnetStatus StringSetBit(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
           => storageSession.StringSetBit(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringGetBit(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.StringGetBit(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringGetBit(ArgSlice key, ArgSlice offset, out bool bValue)
            => storageSession.StringGetBit(key, offset, out bValue, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitCount(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.StringBitCount(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitCount(ArgSlice key, long start, long end, out long result, bool useBitInterval = false)
             => storageSession.StringBitCount(key, start, end, useBitInterval, out result, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitOperation(ArgSlice[] keys, BitmapOperation bitop, out long result)
            => storageSession.StringBitOperation(keys, bitop, out result);

        /// <inheritdoc />
        public GarnetStatus StringBitOperation(BitmapOperation bitop, ArgSlice destinationKey, ArgSlice[] keys, out long result)
            => storageSession.StringBitOperation(bitop, destinationKey, keys, out result);

        /// <inheritdoc />
        public GarnetStatus StringBitPosition(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.StringBitPosition(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitField(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output)
            => storageSession.StringBitField(ref key, ref input, secondaryCommand, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitFieldReadOnly(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output)
            => storageSession.StringBitFieldReadOnly(ref key, ref input, secondaryCommand, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitField(ArgSlice key, List<BitFieldCmdArgs> commandArguments, out List<long?> result)
            => storageSession.StringBitField(key, commandArguments, out result, ref context);

        #endregion

        #region HyperLogLog Methods
        /// <inheritdoc />
        public GarnetStatus HyperLogLogAdd(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.HyperLogLogAdd(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogAdd(ArgSlice key, string[] elements, out bool updated)
            => storageSession.HyperLogLogAdd(key, elements, out updated, ref context);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(ArgSlice[] keys, ref SpanByte input, out long count, out bool error)
            => storageSession.HyperLogLogLength(keys, ref input, out count, out error, ref context);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogLength(ArgSlice[] keys, out long count)
            => storageSession.HyperLogLogLength(keys, out count, ref context);

        /// <inheritdoc />
        public GarnetStatus HyperLogLogMerge(ArgSlice[] keys, out bool error)
            => storageSession.HyperLogLogMerge(keys, out error);
        #endregion

        #region Server Methods

        /// <inheritdoc />
        public List<byte[]> GetDbKeys(ArgSlice pattern)
            => storageSession.DBKeys(pattern);

        /// <inheritdoc />
        public int GetDbSize()
            => storageSession.DbSize();

        /// <inheritdoc />
        public bool DbScan(ArgSlice patternB, bool allKeys, long cursor, out long storeCursor, out List<byte[]> Keys, long count = 10, Span<byte> type = default)
            => storageSession.DbScan(patternB, allKeys, cursor, out storeCursor, out Keys, count, type);

        /// <inheritdoc />
        public bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
            => storageSession.IterateMainStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public ITsavoriteScanIterator<SpanByte, SpanByte> IterateMainStore()
            => storageSession.IterateMainStore();

        /// <inheritdoc />
        public bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<byte[], IGarnetObject>
            => storageSession.IterateObjectStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public ITsavoriteScanIterator<byte[], IGarnetObject> IterateObjectStore()
            => storageSession.IterateObjectStore();

        #endregion

        #region Common Methods

        /// <inheritdoc />
        public GarnetStatus ObjectScan(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter)
         => storageSession.ObjectScan(key, input, ref outputFooter, ref objectContext);

        #endregion
    }
}