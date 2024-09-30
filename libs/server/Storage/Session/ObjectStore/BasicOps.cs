// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Server session for RESP protocol - Basic Operations such as GET and SET that target the Object Store rather than Main Store
    /// </summary>
    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus GET<TObjectContext>(byte[] key, out GarnetObjectStoreOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            long ctx = default;
            var status = objectContext.Read(key, out output, ctx);

            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForObjectStoreSession(ref status, ref output, ref objectContext);
                StopPendingMetrics();
            }

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            else
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
        }

        public GarnetStatus SET<TObjectContext>(byte[] key, IGarnetObject value, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            objectContext.Upsert(key, value);
            return GarnetStatus.OK;
        }
    }
}