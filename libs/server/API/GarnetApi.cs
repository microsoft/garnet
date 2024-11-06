// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi<TKeyLocker, TEpochGuard> : IGarnetApi<TKeyLocker, TEpochGuard>, IGarnetWatchApi<TKeyLocker, TEpochGuard>
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
    {
        readonly StorageSession storageSession;

        internal GarnetApi(StorageSession storageSession)
        {
            this.storageSession = storageSession;
        }

        #region WATCH
        /// <inheritdoc />
        public readonly void WATCH(ArgSlice key, StoreType type)
            => storageSession.WATCH<TKeyLocker, TEpochGuard>(key, type);

        /// <inheritdoc />
        public readonly void WATCH(byte[] key, StoreType type)
            => storageSession.WATCH<TKeyLocker, TEpochGuard>(key, type);
        #endregion

        #region GET
        /// <inheritdoc />
        public readonly GarnetStatus GET(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.GET<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus GET_WithPending(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, long ctx, out bool pending)
            => storageSession.GET_WithPending<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ctx, out pending);

        /// <inheritdoc />
        public readonly bool GET_CompletePending((GarnetStatus, SpanByteAndMemory)[] outputArr, bool wait = false)
            => storageSession.GET_CompletePending<TKeyLocker, TEpochGuard>(outputArr, wait);

        /// <inheritdoc />
        public readonly bool GET_CompletePending(out CompletedOutputIterator<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long> completedOutputs, bool wait)
            => storageSession.GET_CompletePending<TKeyLocker>(out completedOutputs, wait);

        /// <inheritdoc />
        public readonly unsafe GarnetStatus GETForMemoryResult(ArgSlice key, out MemoryResult<byte> value)
            => storageSession.GET<TKeyLocker, TEpochGuard>(key, out value);

        /// <inheritdoc />
        public readonly unsafe GarnetStatus GET(ArgSlice key, out ArgSlice value)
            => storageSession.GET<TKeyLocker, TEpochGuard>(key, out value);

        /// <inheritdoc />
        public readonly GarnetStatus GET(byte[] key, out GarnetObjectStoreOutput value)
        {
            value = default;
            return storageSession.GET<TKeyLocker, TEpochGuard>(key, ref value);
        }
        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public readonly GarnetStatus GETRANGE(ref SpanByte key, int sliceStart, int sliceLength, ref SpanByteAndMemory output)
            => storageSession.GETRANGE<TKeyLocker, TEpochGuard>(ref key, sliceStart, sliceLength, ref output);
        #endregion

        #region TTL

        /// <inheritdoc />
        public readonly GarnetStatus TTL(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.TTL<TKeyLocker, TEpochGuard>(ref key, storeType, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus PTTL(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.TTL<TKeyLocker, TEpochGuard>(ref key, storeType, ref output, milliseconds: true);

        #endregion

        #region SET
        /// <inheritdoc />
        public readonly GarnetStatus SET(ref SpanByte key, ref SpanByte value)
            => storageSession.SET<TKeyLocker, TEpochGuard>(ref key, ref value);

        /// <inheritdoc />
        public readonly GarnetStatus SET_Conditional(ref SpanByte key, ref SpanByte input)
            => storageSession.SET_Conditional<TKeyLocker, TEpochGuard>(ref key, ref input);

        /// <inheritdoc />
        public readonly GarnetStatus SET_Conditional(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.SET_Conditional<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus SET(ArgSlice key, Memory<byte> value)
            => storageSession.SET<TKeyLocker, TEpochGuard>(key, value);

        /// <inheritdoc />
        public readonly GarnetStatus SET(ArgSlice key, ArgSlice value)
            => storageSession.SET<TKeyLocker, TEpochGuard>(key, value);

        /// <inheritdoc />
        public readonly GarnetStatus SET(byte[] key, IGarnetObject value)
            => storageSession.SET<TKeyLocker, TEpochGuard>(key, value);
        #endregion

        #region SETEX
        /// <inheritdoc />
        public readonly unsafe GarnetStatus SETEX(ArgSlice key, ArgSlice value, ArgSlice expiryMs)
            => storageSession.SETEX<TKeyLocker, TEpochGuard>(key, value, expiryMs);

        /// <inheritdoc />
        public readonly GarnetStatus SETEX(ArgSlice key, ArgSlice value, TimeSpan expiry)
            => storageSession.SETEX<TKeyLocker, TEpochGuard>(key, value, expiry);

        #endregion

        #region SETRANGE

        /// <inheritdoc />
        public readonly GarnetStatus SETRANGE(ArgSlice key, ArgSlice value, int offset, ref ArgSlice output)
            => storageSession.SETRANGE<TKeyLocker, TEpochGuard>(key, value, offset, ref output);

        #endregion

        #region APPEND

        /// <inheritdoc />
        public readonly GarnetStatus APPEND(ref SpanByte key, ref SpanByte value, ref SpanByteAndMemory output)
            => storageSession.APPEND<TKeyLocker, TEpochGuard>(ref key, ref value, ref output);

        /// <inheritdoc />    
        public readonly GarnetStatus APPEND(ArgSlice key, ArgSlice value, ref ArgSlice output)
            => storageSession.APPEND<TKeyLocker, TEpochGuard>(key, value, ref output);

        #endregion

        #region RENAME
        /// <inheritdoc />
        public readonly GarnetStatus RENAME(ArgSlice oldKey, ArgSlice newKey, StoreType storeType = StoreType.All)
            => storageSession.RENAME(oldKey, newKey, storeType);

        /// <inheritdoc />
        public readonly GarnetStatus RENAMENX(ArgSlice oldKey, ArgSlice newKey, out int result, StoreType storeType = StoreType.All)
            => storageSession.RENAMENX(oldKey, newKey, storeType, out result);
        #endregion

        #region EXISTS
        /// <inheritdoc />
        public readonly GarnetStatus EXISTS(ArgSlice key, StoreType storeType = StoreType.All)
            => storageSession.EXISTS<TKeyLocker, TEpochGuard>(key, storeType);
        #endregion

        #region EXPIRE
        /// <inheritdoc />
        public readonly unsafe GarnetStatus EXPIRE(ArgSlice key, ArgSlice expiryMs, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIRE<TKeyLocker, TEpochGuard>(key, expiryMs, out timeoutSet, storeType, expireOption);

        /// <inheritdoc />
        public readonly GarnetStatus EXPIRE(ArgSlice key, TimeSpan expiry, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIRE<TKeyLocker, TEpochGuard>(key, expiry, out timeoutSet, storeType, expireOption);

        /// <inheritdoc />
        public readonly GarnetStatus PEXPIRE(ArgSlice key, TimeSpan expiry, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
             => storageSession.EXPIRE<TKeyLocker, TEpochGuard>(key, expiry, out timeoutSet, storeType, expireOption, milliseconds: true);

        #endregion

        #region PERSIST
        /// <inheritdoc />
        public readonly unsafe GarnetStatus PERSIST(ArgSlice key, StoreType storeType = StoreType.All)
            => storageSession.PERSIST<TKeyLocker, TEpochGuard>(key, storeType);
        #endregion

        #region Increment (INCR, INCRBY, DECR, DECRBY)
        /// <inheritdoc />
        public readonly GarnetStatus Increment(ArgSlice key, ArgSlice input, ref ArgSlice output)
            => storageSession.Increment<TKeyLocker, TEpochGuard>(key, input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus Increment(ArgSlice key, out long output, long incrementCount = 1)
            => storageSession.Increment<TKeyLocker, TEpochGuard>(key, out output, incrementCount);

        /// <inheritdoc />
        public readonly GarnetStatus Decrement(ArgSlice key, out long output, long decrementCount = 1)
            => storageSession. Increment<TKeyLocker, TEpochGuard>(key, out output, -decrementCount);
        #endregion

        #region DELETE
        /// <inheritdoc />
        public readonly GarnetStatus DELETE(ArgSlice key, StoreType storeType = StoreType.All)
            => storageSession.DELETE<TKeyLocker, TEpochGuard>(key, storeType);

        /// <inheritdoc />
        public readonly GarnetStatus DELETE(ref SpanByte key, StoreType storeType = StoreType.All)
            => storageSession.DELETE<TKeyLocker, TEpochGuard>(ref key, storeType);

        /// <inheritdoc />
        public readonly GarnetStatus DELETE(byte[] key, StoreType storeType = StoreType.All)
            => storageSession.DELETE<TKeyLocker, TEpochGuard>(key, storeType);
        #endregion

        #region GETDEL
        /// <inheritdoc />
        public readonly GarnetStatus GETDEL(ref SpanByte key, ref SpanByteAndMemory output)
            => storageSession.GETDEL<TKeyLocker, TEpochGuard>(ref key, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus GETDEL(ArgSlice key, ref SpanByteAndMemory output)
            => storageSession.GETDEL<TKeyLocker, TEpochGuard>(key, ref output);
        #endregion

        #region TYPE

        /// <inheritdoc />
        public readonly GarnetStatus GetKeyType(ArgSlice key, out string typeName)
            => storageSession.GetKeyType<TKeyLocker, TEpochGuard>(key, out typeName);

        #endregion

        #region MEMORY

        /// <inheritdoc />
        public readonly GarnetStatus MemoryUsageForKey(ArgSlice key, out long memoryUsage, int samples = 0)
            => storageSession.MemoryUsageForKey<TKeyLocker, TEpochGuard>(key, out memoryUsage, samples);

        #endregion

        #region Advanced ops
        /// <inheritdoc />
        public readonly GarnetStatus RMW_MainStore(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.RMW_MainStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus Read_MainStore(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.Read_MainStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus RMW_ObjectStore(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            => storageSession.RMW_ObjectStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus Read_ObjectStore(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            => storageSession.Read_ObjectStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);
        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public readonly GarnetStatus StringSetBit(ArgSlice key, ArgSlice offset, bool bit, out bool previous)
           => storageSession.StringSetBit<TKeyLocker, TEpochGuard>(key, offset, bit, out previous);

        /// <inheritdoc />
        public readonly GarnetStatus StringSetBit(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
           => storageSession.StringSetBit<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus StringGetBit(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.StringGetBit<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus StringGetBit(ArgSlice key, ArgSlice offset, out bool bValue)
            => storageSession.StringGetBit<TKeyLocker, TEpochGuard>(key, offset, out bValue);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitCount(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.StringBitCount<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitCount(ArgSlice key, long start, long end, out long result, bool useBitInterval = false)
             => storageSession.StringBitCount<TKeyLocker, TEpochGuard>(key, start, end, useBitInterval, out result);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitOperation(Span<ArgSlice> keys, BitmapOperation bitop, out long result)
            => storageSession.StringBitOperation(keys, bitop, out result);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitOperation(BitmapOperation bitop, ArgSlice destinationKey, ArgSlice[] keys, out long result)
            => storageSession.StringBitOperation(bitop, destinationKey, keys, out result);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitPosition(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.StringBitPosition<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitField(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output)
            => storageSession.StringBitField<TKeyLocker, TEpochGuard>(ref key, ref input, secondaryCommand, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitFieldReadOnly(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output)
            => storageSession.StringBitFieldReadOnly<TKeyLocker, TEpochGuard>(ref key, ref input, secondaryCommand, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitField(ArgSlice key, List<BitFieldCmdArgs> commandArguments, out List<long?> result)
            => storageSession.StringBitField<TKeyLocker, TEpochGuard>(key, commandArguments, out result);

        #endregion

        #region HyperLogLog Methods
        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogAdd(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            => storageSession.HyperLogLogAdd<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogAdd(ArgSlice key, string[] elements, out bool updated)
            => storageSession.HyperLogLogAdd<TKeyLocker, TEpochGuard>(key, elements, out updated);

        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogLength(Span<ArgSlice> keys, ref SpanByte input, out long count, out bool error)
            => storageSession.HyperLogLogLength<TKeyLocker, TEpochGuard>(keys, ref input, out count, out error);

        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogLength(Span<ArgSlice> keys, out long count)
            => storageSession.HyperLogLogLength<TKeyLocker, TEpochGuard>(keys, out count);

        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogMerge(Span<ArgSlice> keys, out bool error)
            => storageSession.HyperLogLogMerge(keys, out error);
        #endregion

        #region Server Methods

        /// <inheritdoc />
        public readonly List<byte[]> GetDbKeys(ArgSlice pattern)
            => storageSession.DBKeys(pattern);

        /// <inheritdoc />
        public readonly int GetDbSize()
            => storageSession.DbSize();

        /// <inheritdoc />
        public readonly bool DbScan(ArgSlice patternB, bool allKeys, long cursor, out long storeCursor, out List<byte[]> Keys, long count = 10, ReadOnlySpan<byte> type = default)
            => storageSession.DbScan(patternB, allKeys, cursor, out storeCursor, out Keys, count, type);

        /// <inheritdoc />
        public readonly bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
            => storageSession.IterateMainStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public readonly ITsavoriteScanIterator<SpanByte, SpanByte> IterateMainStore()
            => storageSession.IterateMainStore();

        /// <inheritdoc />
        public readonly bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<byte[], IGarnetObject>
            => storageSession.IterateObjectStore(ref scanFunctions, untilAddress);

        /// <inheritdoc />
        public readonly ITsavoriteScanIterator<byte[], IGarnetObject> IterateObjectStore()
            => storageSession.IterateObjectStore();

        #endregion

        #region Common Methods

        /// <inheritdoc />
        public readonly GarnetStatus ObjectScan(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
         => storageSession.ObjectScan<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        #endregion
    }
}