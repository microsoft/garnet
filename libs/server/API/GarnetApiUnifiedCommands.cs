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
    }
}
