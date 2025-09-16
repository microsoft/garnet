// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = SpanByteAllocator<StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>;

    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus RMW_ObjectStore<TObjectContext>(ReadOnlySpan<byte> key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = objectStoreContext.RMW(key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref output, ref objectStoreContext);

            if (status.Found)
            {
                if (output.HasWrongType)
                    return GarnetStatus.WRONGTYPE;
                return GarnetStatus.OK;
            }

            return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus Read_ObjectStore<TObjectContext>(ReadOnlySpan<byte> key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = objectStoreContext.Read(key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref output, ref objectStoreContext);

            if (status.Found)
            {
                if (output.HasWrongType)
                    return GarnetStatus.WRONGTYPE;
                return GarnetStatus.OK;
            }

            return GarnetStatus.NOTFOUND;
        }
    }
}