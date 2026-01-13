// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus RMW_ObjectStore<TObjectContext>(ReadOnlySpan<byte> key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = objectContext.RMW(key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref output, ref objectContext);

            if (status.Found)
            {
                if (output.HasWrongType)
                    return GarnetStatus.WRONGTYPE;
                return GarnetStatus.OK;
            }

            return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus Read_ObjectStore<TObjectContext>(ReadOnlySpan<byte> key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
        where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = objectContext.Read(key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref output, ref objectContext);

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