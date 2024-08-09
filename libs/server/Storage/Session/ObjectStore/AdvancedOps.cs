// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus RMW_ObjectStore<TObjectContext>(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var status = objectStoreContext.RMW(ref key, ref input, ref output);

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

        public GarnetStatus Read_ObjectStore<TObjectContext>(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var status = objectStoreContext.Read(ref key, ref input, ref output);

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