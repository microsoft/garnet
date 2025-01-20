// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    using ObjectStoreAllocator = ObjectAllocator<IGarnetObject, StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>;

    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus RMW_ObjectStore<TObjectContext>(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var status = objectStoreContext.RMW(key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref output, ref objectStoreContext);

            if (status.Found)
            {
                if (output.spanByteAndMemory.Length == 0)
                    return GarnetStatus.WRONGTYPE;
                return GarnetStatus.OK;
            }

            return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus Read_ObjectStore<TObjectContext>(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var status = objectStoreContext.Read(key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForObjectStoreSession(ref status, ref output, ref objectStoreContext);

            if (status.Found)
            {
                if (output.spanByteAndMemory.Length == 0)
                    return GarnetStatus.WRONGTYPE;
                return GarnetStatus.OK;
            }

            return GarnetStatus.NOTFOUND;
        }
    }
}