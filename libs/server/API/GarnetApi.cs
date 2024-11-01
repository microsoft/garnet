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
    public partial struct GarnetApi : IGarnetApi, IGarnetWatchApi
    {
        readonly StorageSession storageSession;

        internal GarnetApi(StorageSession storageSession)
        {
            this.storageSession = storageSession;
        }

        #region WATCH
        /// <inheritdoc />
        public readonly void WATCH<TKeyLocker, TEpochGuard>(ArgSlice key, StoreType type)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.WATCH<TKeyLocker, TEpochGuard>(key, type);

        /// <inheritdoc />
        public readonly void WATCH<TKeyLocker, TEpochGuard>(byte[] key, StoreType type)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.WATCH<TKeyLocker, TEpochGuard>(key, type);
        #endregion

        #region GET
        /// <inheritdoc />
        public readonly GarnetStatus GET<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GET<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus GET_WithPending<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, long ctx, out bool pending)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GET_WithPending<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ctx, out pending);

        /// <inheritdoc />
        public readonly bool GET_CompletePending<TKeyLocker, TEpochGuard>((GarnetStatus, SpanByteAndMemory)[] outputArr, bool wait = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GET_CompletePending<TKeyLocker, TEpochGuard>(outputArr, wait);

        /// <inheritdoc />
        public readonly bool GET_CompletePending<TKeyLocker>(out CompletedOutputIterator<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long> completedOutputs, bool wait)
            where TKeyLocker : struct, ISessionLocker
            => storageSession.GET_CompletePending<TKeyLocker>(out completedOutputs, wait);

        /// <inheritdoc />
        public readonly unsafe GarnetStatus GETForMemoryResult<TKeyLocker, TEpochGuard>(ArgSlice key, out MemoryResult<byte> value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GET<TKeyLocker, TEpochGuard>(key, out value);

        /// <inheritdoc />
        public readonly unsafe GarnetStatus GET<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GET<TKeyLocker, TEpochGuard>(key, out value);

        /// <inheritdoc />
        public readonly GarnetStatus GET<TKeyLocker, TEpochGuard>(byte[] key, out GarnetObjectStoreOutput value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            value = default;
            return storageSession.GET<TKeyLocker, TEpochGuard>(key, ref value);
        }
        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public readonly GarnetStatus GETRANGE<TKeyLocker, TEpochGuard>(ref SpanByte key, int sliceStart, int sliceLength, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GETRANGE<TKeyLocker, TEpochGuard>(ref key, sliceStart, sliceLength, ref output);
        #endregion

        #region TTL

        /// <inheritdoc />
        public readonly GarnetStatus TTL<TKeyLocker, TEpochGuard>(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.TTL<TKeyLocker, TEpochGuard>(ref key, storeType, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus PTTL<TKeyLocker, TEpochGuard>(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.TTL<TKeyLocker, TEpochGuard>(ref key, storeType, ref output, milliseconds: true);

        #endregion

        #region SET
        /// <inheritdoc />
        public readonly GarnetStatus SET<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SET<TKeyLocker, TEpochGuard>(ref key, ref value);

        /// <inheritdoc />
        public readonly GarnetStatus SET_Conditional<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SET_Conditional<TKeyLocker, TEpochGuard>(ref key, ref input);

        /// <inheritdoc />
        public readonly GarnetStatus SET_Conditional<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SET_Conditional<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus SET<TKeyLocker, TEpochGuard>(ArgSlice key, Memory<byte> value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SET<TKeyLocker, TEpochGuard>(key, value);

        /// <inheritdoc />
        public readonly GarnetStatus SET<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SET<TKeyLocker, TEpochGuard>(key, value);

        /// <inheritdoc />
        public readonly GarnetStatus SET<TKeyLocker, TEpochGuard>(byte[] key, IGarnetObject value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SET<TKeyLocker, TEpochGuard>(key, value);
        #endregion

        #region SETEX
        /// <inheritdoc />
        public readonly unsafe GarnetStatus SETEX<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice value, ArgSlice expiryMs)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SETEX<TKeyLocker, TEpochGuard>(key, value, expiryMs);

        /// <inheritdoc />
        public readonly GarnetStatus SETEX<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice value, TimeSpan expiry)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SETEX<TKeyLocker, TEpochGuard>(key, value, expiry);

        #endregion

        #region SETRANGE

        /// <inheritdoc />
        public readonly GarnetStatus SETRANGE<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice value, int offset, ref ArgSlice output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.SETRANGE<TKeyLocker, TEpochGuard>(key, value, offset, ref output);

        #endregion

        #region APPEND

        /// <inheritdoc />
        public readonly GarnetStatus APPEND<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte value, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.APPEND<TKeyLocker, TEpochGuard>(ref key, ref value, ref output);

        /// <inheritdoc />    
        public readonly GarnetStatus APPEND<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice value, ref ArgSlice output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.APPEND<TKeyLocker, TEpochGuard>(key, value, ref output);

        #endregion

        #region RENAME
        /// <inheritdoc />
        public GarnetStatus RENAME(ArgSlice oldKey, ArgSlice newKey, StoreType storeType = StoreType.All)
            => storageSession.RENAME(oldKey, newKey, storeType);

        /// <inheritdoc />
        public GarnetStatus RENAMENX(ArgSlice oldKey, ArgSlice newKey, out int result, StoreType storeType = StoreType.All)
            => storageSession.RENAMENX(oldKey, newKey, storeType, out result);
        #endregion

        #region EXISTS
        /// <inheritdoc />
        public readonly GarnetStatus EXISTS<TKeyLocker, TEpochGuard>(ArgSlice key, StoreType storeType = StoreType.All)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.EXISTS<TKeyLocker, TEpochGuard>(key, storeType);
        #endregion

        #region EXPIRE
        /// <inheritdoc />
        public readonly unsafe GarnetStatus EXPIRE<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice expiryMs, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.EXPIRE<TKeyLocker, TEpochGuard>(key, expiryMs, out timeoutSet, storeType, expireOption);

        /// <inheritdoc />
        public readonly GarnetStatus EXPIRE<TKeyLocker, TEpochGuard>(ArgSlice key, TimeSpan expiry, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.EXPIRE<TKeyLocker, TEpochGuard>(key, expiry, out timeoutSet, storeType, expireOption);

        /// <inheritdoc />
        public readonly GarnetStatus PEXPIRE<TKeyLocker, TEpochGuard>(ArgSlice key, TimeSpan expiry, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => storageSession.EXPIRE<TKeyLocker, TEpochGuard>(key, expiry, out timeoutSet, storeType, expireOption, milliseconds: true);

        #endregion

        #region PERSIST
        /// <inheritdoc />
        public readonly unsafe GarnetStatus PERSIST<TKeyLocker, TEpochGuard>(ArgSlice key, StoreType storeType = StoreType.All)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.PERSIST<TKeyLocker, TEpochGuard>(key, storeType);
        #endregion

        #region Increment (INCR, INCRBY, DECR, DECRBY)
        /// <inheritdoc />
        public readonly GarnetStatus Increment<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice input, ref ArgSlice output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.Increment<TKeyLocker, TEpochGuard>(key, input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus Increment<TKeyLocker, TEpochGuard>(ArgSlice key, out long output, long incrementCount = 1)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.Increment<TKeyLocker, TEpochGuard>(key, out output, incrementCount);

        /// <inheritdoc />
        public readonly GarnetStatus Decrement<TKeyLocker, TEpochGuard>(ArgSlice key, out long output, long decrementCount = 1)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => Increment<TKeyLocker, TEpochGuard>(key, out output, -decrementCount);
        #endregion

        #region DELETE
        /// <inheritdoc />
        public readonly GarnetStatus DELETE<TKeyLocker, TEpochGuard>(ArgSlice key, StoreType storeType = StoreType.All)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.DELETE<TKeyLocker, TEpochGuard>(key, storeType);

        /// <inheritdoc />
        public readonly GarnetStatus DELETE<TKeyLocker, TEpochGuard>(ref SpanByte key, StoreType storeType = StoreType.All)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.DELETE<TKeyLocker, TEpochGuard>(ref key, storeType);

        /// <inheritdoc />
        public readonly GarnetStatus DELETE<TKeyLocker, TEpochGuard>(byte[] key, StoreType storeType = StoreType.All)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.DELETE<TKeyLocker, TEpochGuard>(key, storeType);
        #endregion

        #region GETDEL
        /// <inheritdoc />
        public readonly GarnetStatus GETDEL<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GETDEL<TKeyLocker, TEpochGuard>(ref key, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus GETDEL<TKeyLocker, TEpochGuard>(ArgSlice key, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GETDEL<TKeyLocker, TEpochGuard>(key, ref output);
        #endregion

        #region TYPE

        /// <inheritdoc />
        public readonly GarnetStatus GetKeyType<TKeyLocker, TEpochGuard>(ArgSlice key, out string typeName)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.GetKeyType<TKeyLocker, TEpochGuard>(key, out typeName);

        #endregion

        #region MEMORY

        /// <inheritdoc />
        public readonly GarnetStatus MemoryUsageForKey<TKeyLocker, TEpochGuard>(ArgSlice key, out long memoryUsage, int samples = 0)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.MemoryUsageForKey<TKeyLocker, TEpochGuard>(key, out memoryUsage, samples);

        #endregion

        #region Advanced ops
        /// <inheritdoc />
        public readonly GarnetStatus RMW_MainStore<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.RMW_MainStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus Read_MainStore<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.Read_MainStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus RMW_ObjectStore<TKeyLocker, TEpochGuard>(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.RMW_ObjectStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus Read_ObjectStore<TKeyLocker, TEpochGuard>(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.Read_ObjectStore<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);
        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public readonly GarnetStatus StringSetBit<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice offset, bool bit, out bool previous)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
           => storageSession.StringSetBit<TKeyLocker, TEpochGuard>(key, offset, bit, out previous);

        /// <inheritdoc />
        public readonly GarnetStatus StringSetBit<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
           => storageSession.StringSetBit<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus StringGetBit<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.StringGetBit<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus StringGetBit<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice offset, out bool bValue)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.StringGetBit<TKeyLocker, TEpochGuard>(key, offset, out bValue);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitCount<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.StringBitCount<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitCount<TKeyLocker, TEpochGuard>(ArgSlice key, long start, long end, out long result, bool useBitInterval = false)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => storageSession.StringBitCount<TKeyLocker, TEpochGuard>(key, start, end, useBitInterval, out result);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitOperation(Span<ArgSlice> keys, BitmapOperation bitop, out long result)
            => storageSession.StringBitOperation(keys, bitop, out result);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitOperation(BitmapOperation bitop, ArgSlice destinationKey, ArgSlice[] keys, out long result)
            => storageSession.StringBitOperation(bitop, destinationKey, keys, out result);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitPosition<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.StringBitPosition<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitField<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.StringBitField<TKeyLocker, TEpochGuard>(ref key, ref input, secondaryCommand, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitFieldReadOnly<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.StringBitFieldReadOnly<TKeyLocker, TEpochGuard>(ref key, ref input, secondaryCommand, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus StringBitField<TKeyLocker, TEpochGuard>(ArgSlice key, List<BitFieldCmdArgs> commandArguments, out List<long?> result)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.StringBitField<TKeyLocker, TEpochGuard>(key, commandArguments, out result);

        #endregion

        #region HyperLogLog Methods
        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogAdd<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HyperLogLogAdd<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogAdd<TKeyLocker, TEpochGuard>(ArgSlice key, string[] elements, out bool updated)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HyperLogLogAdd<TKeyLocker, TEpochGuard>(key, elements, out updated);

        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogLength<TKeyLocker, TEpochGuard>(Span<ArgSlice> keys, ref SpanByte input, out long count, out bool error)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => storageSession.HyperLogLogLength<TKeyLocker, TEpochGuard>(keys, ref input, out count, out error);

        /// <inheritdoc />
        public readonly GarnetStatus HyperLogLogLength<TKeyLocker, TEpochGuard>(Span<ArgSlice> keys, out long count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
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
        public readonly GarnetStatus ObjectScan<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
         => storageSession.ObjectScan<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        #endregion
    }
}