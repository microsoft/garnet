// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus Read_UnifiedStore<TUnifiedContext>(ReadOnlySpan<byte> key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = unifiedContext.Read(key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForUnifiedStoreSession(ref status, ref output, ref unifiedContext);

            if (status.Found)
                return GarnetStatus.OK;
            else
                return GarnetStatus.NOTFOUND;
        }
    }
}
