// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    // Example aliases:
    //   using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, SpanByteAllocator>, BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectAllocator>>;
    //   using LockableGarnetApi = GarnetApi<LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, SpanByteAllocator>, LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectAllocator>>;

    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi<TContext, TObjectContext, TVectorContext> : IGarnetApi, IGarnetWatchApi
        where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        where TVectorContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
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
        public GarnetStatus GET(ArgSlice key, ref RawStringInput input, ref SpanByteAndMemory output)
        {
            var asSpanByte = key.SpanByte;

            return storageSession.GET(ref asSpanByte, ref input, ref output, ref context);
        }

        /// <inheritdoc />
        public GarnetStatus GET_WithPending(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, long ctx, out bool pending)
            => storageSession.GET_WithPending(ref key, ref input, ref output, ctx, out pending, ref context);

        /// <inheritdoc />
        public bool GET_CompletePending((GarnetStatus, SpanByteAndMemory)[] outputArr, bool wait = false)
            => storageSession.GET_CompletePending(outputArr, wait, ref context);

        public bool GET_CompletePending(out CompletedOutputIterator<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long> completedOutputs, bool wait)
            => storageSession.GET_CompletePending(out completedOutputs, wait, ref context);

        /// <inheritdoc />
        public unsafe GarnetStatus GETForMemoryResult(ArgSlice key, out MemoryResult<byte> value)
            => storageSession.GET(key, out value, ref context);

        /// <inheritdoc />
        public unsafe GarnetStatus GET(ArgSlice key, out ArgSlice value)
        {
            return storageSession.GET(key, out value, ref context);
        }

        /// <inheritdoc />
        public GarnetStatus GET(byte[] key, out GarnetObjectStoreOutput value)
            => storageSession.GET(key, out value, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus LCS(ArgSlice key1, ArgSlice key2, ref SpanByteAndMemory output, bool lenOnly = false, bool withIndices = false, bool withMatchLen = false, int minMatchLen = 0)
            => storageSession.LCS(key1, key2, ref output, lenOnly, withIndices, withMatchLen, minMatchLen);
        #endregion

        #region GETEX

        /// <inheritdoc />
        public GarnetStatus GETEX(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.GETEX(ref key, ref input, ref output, ref context);

        #endregion

        #region GETRANGE
        /// <inheritdoc />
        public GarnetStatus GETRANGE(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.GETRANGE(ref key, ref input, ref output, ref context);
        #endregion

        #region TTL

        /// <inheritdoc />
        public GarnetStatus TTL(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.TTL(ref key, storeType, ref output, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus PTTL(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.TTL(ref key, storeType, ref output, ref context, ref objectContext, milliseconds: true);

        #endregion

        #region EXPIRETIME

        /// <inheritdoc />
        public GarnetStatus EXPIRETIME(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.EXPIRETIME(ref key, storeType, ref output, ref context, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus PEXPIRETIME(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output)
            => storageSession.EXPIRETIME(ref key, storeType, ref output, ref context, ref objectContext, milliseconds: true);

        #endregion

        #region SET

        public GarnetStatus SET(ref SpanByte key, ref SpanByte value)
           => storageSession.SET(ref key, ref value, ref context);

        /// <inheritdoc />
        public GarnetStatus SET(ArgSlice key, ref RawStringInput input, ref SpanByte value)
        {
            var asSpanByte = key.SpanByte;

            return storageSession.SET(ref asSpanByte, ref input, ref value, ref context);
        }

        /// <inheritdoc />
        public GarnetStatus DEL_Conditional(ref SpanByte key, ref RawStringInput input)
            => storageSession.DEL_Conditional(ref key, ref input, ref context);

        /// <inheritdoc />
        public GarnetStatus SET_Conditional(ArgSlice key, ref RawStringInput input, ref SpanByteAndMemory output)
        {
            var asSpanByte = key.SpanByte;

            return storageSession.SET_Conditional(ref asSpanByte, ref input, ref output, ref context);
        }

        /// <inheritdoc />
        public GarnetStatus SET_Conditional(ArgSlice key, ref RawStringInput input)
        {
            var asSpanByte = key.SpanByte;

            return storageSession.SET_Conditional(ref asSpanByte, ref input, ref context);
        }

        /// <inheritdoc />
        public GarnetStatus SET(ArgSlice key, Memory<byte> value)
        {
            return storageSession.SET(key, value, ref context);
        }

        /// <inheritdoc />
        public GarnetStatus SET(ArgSlice key, ArgSlice value)
        {
            var asSpanByte = key.SpanByte;
            var valSpanByte = value.SpanByte;

            return storageSession.SET(ref asSpanByte, ref valSpanByte, ref context);
        }

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
        public GarnetStatus APPEND(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.APPEND(ref key, ref input, ref output, ref context);

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

        /// <inheritdoc />
        public GarnetStatus IncrementByFloat(ArgSlice key, ref ArgSlice output, double val)
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
        public GarnetStatus IncrementByFloat(ArgSlice key, out double output, double val)
        {
            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatDoubleLength + 1];
            var _output = ArgSlice.FromPinnedSpan(outputBuffer);
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
        public GarnetStatus RMW_MainStore(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.RMW_MainStore(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus Read_MainStore(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.Read_MainStore(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus RMW_ObjectStore(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            => storageSession.RMW_ObjectStore(ref key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public GarnetStatus Read_ObjectStore(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            => storageSession.Read_ObjectStore(ref key, ref input, ref output, ref objectContext);

        public void ReadWithPrefetch<TBatch>(ref TBatch batch, long userContext = default)
            where TBatch : IReadArgBatch<SpanByte, RawStringInput, SpanByteAndMemory>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => storageSession.ReadWithPrefetch(ref batch, ref context, userContext);
        #endregion

        #region Bitmap Methods

        /// <inheritdoc />
        public GarnetStatus StringSetBit(ArgSlice key, ArgSlice offset, bool bit, out bool previous)
           => storageSession.StringSetBit(key, offset, bit, out previous, ref context);

        /// <inheritdoc />
        public GarnetStatus StringSetBit(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
           => storageSession.StringSetBit(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringGetBit(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.StringGetBit(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringGetBit(ArgSlice key, ArgSlice offset, out bool bValue)
            => storageSession.StringGetBit(key, offset, out bValue, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitCount(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.StringBitCount(ref key, ref input, ref output, ref context);

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
        public GarnetStatus StringBitPosition(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.StringBitPosition(ref key, ref input, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitField(ref SpanByte key, ref RawStringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output)
            => storageSession.StringBitField(ref key, ref input, secondaryCommand, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitFieldReadOnly(ref SpanByte key, ref RawStringInput input, RespCommand secondaryCommand, ref SpanByteAndMemory output)
            => storageSession.StringBitFieldReadOnly(ref key, ref input, secondaryCommand, ref output, ref context);

        /// <inheritdoc />
        public GarnetStatus StringBitField(ArgSlice key, List<BitFieldCmdArgs> commandArguments, out List<long?> result)
            => storageSession.StringBitField(key, commandArguments, out result, ref context);

        #endregion

        #region HyperLogLog Methods
        /// <inheritdoc />
        public GarnetStatus HyperLogLogAdd(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output)
            => storageSession.HyperLogLogAdd(ref key, ref input, ref output, ref context);

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
        public bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, ref long cursor, long untilAddress = -1, long maxAddress = long.MaxValue, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
            => storageSession.IterateMainStore(ref scanFunctions, ref cursor, untilAddress, maxAddress: maxAddress, includeTombstones: includeTombstones);

        /// <inheritdoc />
        public ITsavoriteScanIterator<SpanByte, SpanByte> IterateMainStore()
            => storageSession.IterateMainStore();

        /// <inheritdoc />
        public bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, ref long cursor, long untilAddress = -1, long maxAddress = long.MaxValue, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions<byte[], IGarnetObject>
            => storageSession.IterateObjectStore(ref scanFunctions, ref cursor, untilAddress, maxAddress: maxAddress, includeTombstones: includeTombstones);

        /// <inheritdoc />
        public ITsavoriteScanIterator<byte[], IGarnetObject> IterateObjectStore()
            => storageSession.IterateObjectStore();

        #endregion

        #region Common Methods

        /// <inheritdoc />
        public GarnetStatus ObjectScan(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
         => storageSession.ObjectScan(key, ref input, ref output, ref objectContext);

        /// <inheritdoc />
        public int GetScratchBufferOffset()
            => storageSession.scratchBufferBuilder.ScratchBufferOffset;

        /// <inheritdoc />
        public bool ResetScratchBuffer(int offset)
            => storageSession.scratchBufferBuilder.ResetScratchBuffer(offset);
        #endregion

        #region VectorSet commands

        /// <inheritdoc />
        public unsafe GarnetStatus VectorSetAdd(ArgSlice key, int reduceDims, VectorValueType valueType, ArgSlice values, ArgSlice element, VectorQuantType quantizer, int buildExplorationFactor, ArgSlice attributes, int numLinks, out VectorManagerResult result, out ReadOnlySpan<byte> errorMsg)
        => storageSession.VectorSetAdd(SpanByte.FromPinnedPointer(key.ptr, key.length), reduceDims, valueType, values, element, quantizer, buildExplorationFactor, attributes, numLinks, out result, out errorMsg);

        /// <inheritdoc />
        public unsafe GarnetStatus VectorSetRemove(ArgSlice key, ArgSlice element)
        => storageSession.VectorSetRemove(SpanByte.FromPinnedPointer(key.ptr, key.length), SpanByte.FromPinnedPointer(element.ptr, element.length));

        /// <inheritdoc />
        public unsafe GarnetStatus VectorSetValueSimilarity(ArgSlice key, VectorValueType valueType, ArgSlice values, int count, float delta, int searchExplorationFactor, ArgSlice filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result)
        => storageSession.VectorSetValueSimilarity(SpanByte.FromPinnedPointer(key.ptr, key.length), valueType, values, count, delta, searchExplorationFactor, filter.ReadOnlySpan, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes, out result);

        /// <inheritdoc />
        public unsafe GarnetStatus VectorSetElementSimilarity(ArgSlice key, ArgSlice element, int count, float delta, int searchExplorationFactor, ArgSlice filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result)
        => storageSession.VectorSetElementSimilarity(SpanByte.FromPinnedPointer(key.ptr, key.length), element.ReadOnlySpan, count, delta, searchExplorationFactor, filter.ReadOnlySpan, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes, out result);

        /// <inheritdoc/>
        public unsafe GarnetStatus VectorSetEmbedding(ArgSlice key, ArgSlice element, ref SpanByteAndMemory outputDistances)
        => storageSession.VectorSetEmbedding(SpanByte.FromPinnedPointer(key.ptr, key.length), element.ReadOnlySpan, ref outputDistances);

        /// <inheritdoc/>
        public unsafe GarnetStatus VectorSetDimensions(ArgSlice key, out int dimensions)
        => storageSession.VectorSetDimensions(SpanByte.FromPinnedPointer(key.ptr, key.length), out dimensions);

        #endregion
    }
}