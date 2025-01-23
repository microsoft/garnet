// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server;
using Tsavorite.core;

namespace Garnet;

using ObjectStoreAllocator =
    GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer,
        DefaultRecordDisposer<byte[], IGarnetObject>>>;
using ObjectStoreFunctions =
    StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

public class ObjectStoreWrapper : IDisposable
{
    public readonly TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objectStore;
    readonly KVSettings<byte[], IGarnetObject> objKvSettings;
    public readonly CacheSizeTracker objectStoreSizeTracker;

    public ObjectStoreWrapper(
        TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objectStore,
        KVSettings<byte[], IGarnetObject> objKvSettings,
        CacheSizeTracker objectStoreSizeTracker)
    {
        this.objectStore = objectStore;
        this.objKvSettings = objKvSettings;
        this.objectStoreSizeTracker = objectStoreSizeTracker;
    }

    public void Dispose()
    {
        objectStore?.Dispose();
        objKvSettings?.LogDevice?.Dispose();
        objKvSettings?.ObjectLogDevice?.Dispose();
        objKvSettings?.Dispose();
    }
}