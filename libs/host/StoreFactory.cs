// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

public class StoreFactory
{
    private readonly IClusterFactory clusterFactory;
    private readonly GarnetServerOptions opts;
    private readonly ILoggerFactory loggerFactory;
    private readonly CustomCommandManager customCommandManager;

    public StoreFactory(
        IClusterFactory clusterFactory,
        IOptions<GarnetServerOptions> options,
        ILoggerFactory loggerFactory,
        CustomCommandManager customCommandManager)
    {
        this.clusterFactory = options.Value.EnableCluster ? clusterFactory : null;
        this.opts = options.Value;
        this.loggerFactory = loggerFactory;
        this.customCommandManager = customCommandManager;
    }

    public MainStoreWrapper CreateMainStore()
    {
        var kvSettings = opts.GetSettings(loggerFactory, out var logFactory);

        var checkpointDir = opts.CheckpointDir ?? opts.LogDir;

        // Run checkpoint on its own thread to control p99
        kvSettings.ThrottleCheckpointFlushDelayMs = opts.CheckpointThrottleFlushDelayMs;
        kvSettings.CheckpointVersionSwitchBarrier = opts.EnableCluster;

        var checkpointFactory = opts.DeviceFactoryCreator();
        if (opts.EnableCluster)
        {
            kvSettings.CheckpointManager = clusterFactory.CreateCheckpointManager(checkpointFactory,
                new DefaultCheckpointNamingScheme(checkpointDir + "/Store/checkpoints"), isMainStore: true);
        }
        else
        {
            kvSettings.CheckpointManager = new DeviceLogCommitCheckpointManager(checkpointFactory,
                new DefaultCheckpointNamingScheme(checkpointDir + "/Store/checkpoints"), removeOutdated: true);
        }

        return new(new(kvSettings
            , StoreFunctions<SpanByte, SpanByte>.Create()
            , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)), kvSettings);
    }

    public ObjectStoreWrapper CreateObjectStore()
    {
        TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objectStore = null;
        KVSettings<byte[], IGarnetObject> objKvSettings = null;
        CacheSizeTracker objectStoreSizeTracker = null;

        if (!opts.DisableObjects)
        {
            var checkpointDir = opts.CheckpointDir ?? opts.LogDir;

            objKvSettings = opts.GetObjectStoreSettings(loggerFactory?.CreateLogger("TsavoriteKV  [obj]"),
                out var objHeapMemorySize, out var objReadCacheHeapMemorySize);

            // Run checkpoint on its own thread to control p99
            objKvSettings.ThrottleCheckpointFlushDelayMs = opts.CheckpointThrottleFlushDelayMs;
            objKvSettings.CheckpointVersionSwitchBarrier = opts.EnableCluster;

            if (opts.EnableCluster)
                objKvSettings.CheckpointManager = clusterFactory.CreateCheckpointManager(
                    opts.DeviceFactoryCreator(),
                    new DefaultCheckpointNamingScheme(checkpointDir + "/ObjectStore/checkpoints"),
                    isMainStore: false);
            else
                objKvSettings.CheckpointManager = new DeviceLogCommitCheckpointManager(
                    opts.DeviceFactoryCreator(),
                    new DefaultCheckpointNamingScheme(checkpointDir + "/ObjectStore/checkpoints"),
                    removeOutdated: true);

            objectStore = new(objKvSettings
                , StoreFunctions<byte[], IGarnetObject>.Create(new ByteArrayKeyComparer(),
                    () => new ByteArrayBinaryObjectSerializer(),
                    () => new GarnetObjectSerializer(customCommandManager))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            if (objHeapMemorySize > 0 || objReadCacheHeapMemorySize > 0)
                objectStoreSizeTracker = new CacheSizeTracker(objectStore, objKvSettings, objHeapMemorySize,
                    objReadCacheHeapMemorySize,
                    loggerFactory);
        }

        return new(objectStore, objKvSettings, objectStoreSizeTracker);
    }
}