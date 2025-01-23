// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
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

public class StoreWrapperFactory
{
    /// <summary>
    /// Resp protocol version
    /// </summary>
    readonly string redisProtocolVersion = "7.2.5";

    readonly ILoggerFactory loggerFactory;
    readonly ILogger<StoreWrapperFactory> logger;
    readonly IGarnetServer garnetServer;
    readonly StoreFactory storeFactory;
    readonly GarnetServerOptions options;
    readonly CustomCommandManager customCommandManager;
    readonly IClusterFactory clusterFactory;
    readonly MainStoreWrapper mainStoreWrapper;
    readonly ObjectStoreWrapper objectStoreWrapper;
    readonly AppendOnlyFileWrapper appendOnlyFileWrapper;

    public StoreWrapperFactory(
        ILoggerFactory loggerFactory,
        ILogger<StoreWrapperFactory> logger,
        IGarnetServer garnetServer,
        StoreFactory storeFactory,
        IOptions<GarnetServerOptions> options,
        CustomCommandManager customCommandManager,
        IClusterFactory clusterFactory,
        MainStoreWrapper mainStoreWrapper,
        ObjectStoreWrapper objectStoreWrapper,
        AppendOnlyFileWrapper appendOnlyFileWrapper)
    {
        this.loggerFactory = loggerFactory;
        this.logger = logger;
        this.garnetServer = garnetServer;
        this.storeFactory = storeFactory;
        this.options = options.Value;
        this.customCommandManager = customCommandManager;
        this.clusterFactory = this.options.EnableCluster ? clusterFactory : null;
        this.mainStoreWrapper = mainStoreWrapper;
        this.objectStoreWrapper = objectStoreWrapper;
        this.appendOnlyFileWrapper = appendOnlyFileWrapper;
    }

    public StoreWrapper Create(string version)
    {
        var store = mainStoreWrapper.store;
        var objectStore = objectStoreWrapper.objectStore;
        var appendOnlyFile = appendOnlyFileWrapper.appendOnlyFile;

        var objectStoreSizeTracker = objectStoreWrapper.objectStoreSizeTracker;

        var configMemoryLimit = (store.IndexSize * 64) + store.Log.MaxMemorySizeBytes +
                                (store.ReadCache?.MaxMemorySizeBytes ?? 0) +
                                (appendOnlyFile?.MaxMemorySizeBytes ?? 0);
        if (objectStore != null)
        {

            configMemoryLimit += objectStore.IndexSize * 64 + objectStore.Log.MaxMemorySizeBytes +
                                 (objectStore.ReadCache?.MaxMemorySizeBytes ?? 0) +
                                 (objectStoreSizeTracker?.TargetSize ?? 0) +
                                 (objectStoreSizeTracker?.ReadCacheTargetSize ?? 0);
        }

        logger.LogInformation("Total configured memory limit: {configMemoryLimit}", configMemoryLimit);

        LoadModules();

        return new StoreWrapper(
            version,
            redisProtocolVersion,
            garnetServer,
            store,
            objectStore,
            objectStoreSizeTracker,
            customCommandManager,
            appendOnlyFile,
            options,
            clusterFactory: clusterFactory,
            loggerFactory: loggerFactory);
    }

    private void LoadModules()
    {
        if (options.LoadModuleCS == null)
            return;

        foreach (var moduleCS in options.LoadModuleCS)
        {
            var moduleCSData = moduleCS.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (moduleCSData.Length < 1)
                continue;

            var modulePath = moduleCSData[0];
            var moduleArgs = moduleCSData.Length > 1 ? moduleCSData.Skip(1).ToArray() : [];
            if (ModuleUtils.LoadAssemblies([modulePath], null, true, out var loadedAssemblies, out var errorMsg))
            {
                ModuleRegistrar.Instance.LoadModule(customCommandManager, loadedAssemblies.ToList()[0], moduleArgs,
                    logger, out errorMsg);
            }
            else
            {
                logger?.LogError("Module {0} failed to load with error {1}", modulePath,
                    Encoding.UTF8.GetString(errorMsg));
            }
        }
    }
}