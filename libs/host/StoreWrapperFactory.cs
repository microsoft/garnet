// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.cluster;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tsavorite.core;

namespace Garnet;

using MainStoreAllocator =
    SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;
using ObjectStoreAllocator =
    GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer,
        DefaultRecordDisposer<byte[], IGarnetObject>>>;
using ObjectStoreFunctions =
    StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

public class StoreWrapperFactory : IDisposable
{
    /// <summary>
    /// Resp protocol version
    /// </summary>
    readonly string redisProtocolVersion = "7.2.5";
    
    readonly StoreFactory storeFactory;
    readonly GarnetServerOptions options;
    
    public StoreWrapperFactory(StoreFactory storeFactory, IOptions<GarnetServerOptions> options)
    {
        this.storeFactory = storeFactory;
        this.options = options.Value;
    }

    public StoreWrapper Create(
        string version,
        IGarnetServer server,
        TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> store,
        TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objectStore,
        CacheSizeTracker objectStoreSizeTracker,
        CustomCommandManager customCommandManager,
        TsavoriteLog appendOnlyFile,
        IClusterFactory clusterFactory,
        ILoggerFactory loggerFactory)
    {
        return new StoreWrapper(
            version, 
            redisProtocolVersion,
            server, 
            store,
            objectStore,
            objectStoreSizeTracker,
            customCommandManager, 
            appendOnlyFile,
            options,
            clusterFactory: clusterFactory, 
            loggerFactory: loggerFactory);
    }

    public void Dispose()
    {
        // TODO release managed resources here
    }
}