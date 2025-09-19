// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        /// Checks if a key exists in the unified store context.
        /// </summary>
        /// <typeparam name="TUnifiedContext"></typeparam>
        /// <param name="key">The name of the key to use in the operation</param>
        /// <param name="unifiedContext">Basic unifiedContext for the unified store.</param>
        /// <returns></returns>
        public GarnetStatus EXISTS<TUnifiedContext>(PinnedSpanByte key, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            // Prepare input
            var input = new UnifiedStoreInput(RespCommand.EXISTS);

            // Prepare GarnetUnifiedStoreOutput output
            var output = new GarnetUnifiedStoreOutput();

            var status = Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

            return status;
        }

        /// <summary>
        /// Deletes a key from the unified store context.
        /// </summary>
        /// <param name="key">The name of the key to use in the operation</param>
        /// <param name="unifiedContext">Basic unifiedContext for the unified store.</param>
        /// <returns></returns>
        public GarnetStatus DELETE<TUnifiedContext>(PinnedSpanByte key, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = unifiedContext.Delete(key.ReadOnlySpan);
            Debug.Assert(!status.IsPending);
            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }
    }
}
