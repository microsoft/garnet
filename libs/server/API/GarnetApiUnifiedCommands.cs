// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi<TContext, TObjectContext, TUnifiedContext> : IGarnetApi, IGarnetWatchApi
        where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
    {
        #region MEMORY

        /// <inheritdoc />
        public GarnetStatus MEMORYUSAGE(PinnedSpanByte key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output)
            => storageSession.Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        #endregion

        #region TYPE

        /// <inheritdoc />
        public GarnetStatus TYPE(PinnedSpanByte key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output)
            => storageSession.Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        #endregion

        #region TTL

        /// <inheritdoc />
        public GarnetStatus TTL(PinnedSpanByte key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output)
            => storageSession.Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        #endregion

        #region EXPIRETIME

        /// <inheritdoc />
        public GarnetStatus EXPIRETIME(PinnedSpanByte key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output)
            => storageSession.Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        #endregion

        #region EXISTS

        /// <inheritdoc />
        public GarnetStatus EXISTS(PinnedSpanByte key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output)
            => storageSession.Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        /// <inheritdoc />
        public GarnetStatus EXISTS(PinnedSpanByte key)
            => storageSession.EXISTS(key, ref unifiedContext);

        #endregion

        #region DELETE

        /// <inheritdoc />
        public GarnetStatus DELETE(PinnedSpanByte key)
            => storageSession.DELETE(key, ref unifiedContext);

        #endregion

        #region EXPIRE

        /// <inheritdoc />
        public unsafe GarnetStatus EXPIRE(PinnedSpanByte key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output)
            => storageSession.RMW_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        #endregion

        #region PERSIST

        /// <inheritdoc />
        public unsafe GarnetStatus PERSIST(PinnedSpanByte key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output)
            => storageSession.RMW_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        #endregion
    }
}