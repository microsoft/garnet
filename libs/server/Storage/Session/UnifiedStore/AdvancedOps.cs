// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus Read_UnifiedStore<TUnifiedContext>(ReadOnlySpan<byte> key, ref UnifiedInput input, ref UnifiedOutput output, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = unifiedContext.Read(key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForUnifiedStoreSession(ref status, ref output, ref unifiedContext);

            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public GarnetStatus RMW_UnifiedStore<TUnifiedContext>(ReadOnlySpan<byte> key, ref UnifiedInput input, ref UnifiedOutput output, ref TUnifiedContext context)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.RMW(key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForUnifiedStoreSession(ref status, ref output, ref context);

            return status.Found || status.Record.Created || status.Record.InPlaceUpdated
                ? GarnetStatus.OK
                : GarnetStatus.NOTFOUND;
        }
    }
}