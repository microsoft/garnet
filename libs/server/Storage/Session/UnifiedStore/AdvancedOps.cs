// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus Read_UnifiedStore<TUnifiedContext>(ReadOnlySpan<byte> key, ref UnifiedInput input, ref UnifiedOutput output, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = unifiedContext.Read((FixedSpanByteKey)key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForUnifiedStoreSession(ref status, ref output, ref unifiedContext);

            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// Updates a key through the unified store, returning <see cref="GarnetStatus.OK"/> if the update found the key.
        /// </summary>
        /// <remarks>
        /// Only a found record produces output: no unified-store update creates a record. A created or in-place-updated
        /// record without <see cref="Status.Found"/> is an expired record tombstoned by the update, which wrote no output.
        /// </remarks>
        public GarnetStatus RMW_UnifiedStore<TUnifiedContext>(ReadOnlySpan<byte> key, ref UnifiedInput input, ref UnifiedOutput output, ref TUnifiedContext context)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.RMW((FixedSpanByteKey)key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForUnifiedStoreSession(ref status, ref output, ref context);

            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }
    }
}