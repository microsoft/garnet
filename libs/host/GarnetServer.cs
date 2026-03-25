// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Garnet.cluster;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Public interface for GarnetServer, allowing callers to interact with the server
    /// without knowing the TServerOptions type parameter used for JIT optimization.
    /// Use <see cref="GarnetServerFactory"/> to create instances.
    /// </summary>
    public interface IGarnetServerApp : IDisposable
    {
        /// <summary>
        /// Metrics API
        /// </summary>
        MetricsApi Metrics { get; }

        /// <summary>
        /// Command registration API
        /// </summary>
        RegisterApi Register { get; }

        /// <summary>
        /// Store API
        /// </summary>
        StoreApi Store { get; }

        /// <summary>
        /// Garnet provider
        /// </summary>
        GarnetProvider Provider { get; }

        /// <summary>
        /// Start server instance
        /// </summary>
        void Start();

        /// <summary>
        /// Dispose, optionally deleting logs and checkpoints
        /// </summary>
        void Dispose(bool deleteDir);

        /// <summary>
        /// Whether to dispose the logger factory when the server is disposed.
        /// Set by the factory when it creates the logger factory on behalf of the caller.
        /// </summary>
        bool DisposeLoggerFactory { set; }
    }

    /// <summary>
    /// Garnet server with JIT-optimized configuration.
    /// TServerOptions is a zero-size struct implementing IGarnetServerOptions whose static properties
    /// return constants. The JIT specializes this class per struct type, inlining all config checks
    /// and eliminating dead branches entirely.
    /// Use <see cref="GarnetServerFactory"/> to create instances from a <see cref="GarnetServerOptions"/>.
    /// </summary>
    public class GarnetServer<TServerOptions> : IGarnetServerApp
        where TServerOptions : struct, IGarnetServerOptions
    {
        /// <summary>
        /// Resp protocol version
        /// </summary>
        internal const string RedisProtocolVersion = "7.4.3";

        static readonly string version = GetVersion();
        static string GetVersion()
        {
            var Version = Assembly.GetExecutingAssembly().GetName().Version;
            return $"{Version.Major}.{Version.Minor}.{Version.Build}";
        }

        /// <inheritdoc />
        public GarnetProvider Provider { get; private set; }

        protected internal readonly GarnetServerOptions opts;
        private IGarnetServer[] servers;
        private SubscribeBroker subscribeBroker;
        private INamedDeviceFactory logFactory;
        private ILogger logger;
        private readonly ILoggerFactory loggerFactory;
        private readonly bool cleanupDir;
        /// <inheritdoc />
        public bool DisposeLoggerFactory { private get; set; }
        protected readonly LightEpoch storeEpoch, aofEpoch, pubSubEpoch;

        /// <summary>
        /// Store and associated information used by this Garnet server
        /// </summary>
        protected StoreWrapper storeWrapper;

        /// <inheritdoc />
        public MetricsApi Metrics { get; private set; }

        /// <inheritdoc />
        public RegisterApi Register { get; private set; }

        /// <inheritdoc />
        public StoreApi Store { get; private set; }

        /// <summary>
        /// Create Garnet Server instance using GarnetServerOptions instance; use Start to start the server.
        /// </summary>
        /// <param name="opts">Server options</param>
        /// <param name="loggerFactory">Logger factory</param>
        /// <param name="servers">The IGarnetServer to use. If none is provided, will use a GarnetServerTcp.</param>
        /// <param name="cleanupDir">Whether to clean up data folders on dispose</param>
        public GarnetServer(GarnetServerOptions opts, ILoggerFactory loggerFactory = null, IGarnetServer[] servers = null, bool cleanupDir = false)
        {
            this.servers = servers;
            this.opts = opts;
            this.loggerFactory = loggerFactory;
            this.cleanupDir = cleanupDir;
            this.storeEpoch = new LightEpoch();
            if (TServerOptions.EnableAOF)
                this.aofEpoch = new LightEpoch();
            if (!TServerOptions.DisablePubSub)
                this.pubSubEpoch = new LightEpoch();
            try
            {
                this.InitializeServer();
            }
            catch
            {
                storeEpoch?.Dispose();
                aofEpoch?.Dispose();
                pubSubEpoch?.Dispose();
                throw;
            }
        }

        private void InitializeServer()
        {
            Debug.Assert(opts != null);

            if (!TServerOptions.QuietMode)
            {
                var red = "\u001b[31m";
                var magenta = "\u001b[35m";
                var normal = "\u001b[0m";

                Console.WriteLine($"""
                    {red}    _________
                       /_||___||_\      {normal}Garnet {version} {(IntPtr.Size == 8 ? "64" : "32")} bit; {(TServerOptions.EnableCluster ? "cluster" : "standalone")} mode{red}
                       '. \   / .'      {normal}Listening on: {(opts.EndPoints.Length > 1 ? opts.EndPoints[0] + $" and {opts.EndPoints.Length - 1} more" : opts.EndPoints[0])}{red}
                         '.\ /.'        {magenta}https://aka.ms/GetGarnet{red}
                           '.'
                    {normal}
                    """);
            }

            var clusterFactory = TServerOptions.EnableCluster ? new ClusterFactory() : null;

            this.logger = this.loggerFactory?.CreateLogger("GarnetServer");
            logger?.LogInformation("Garnet {version} {bits} bit; {clusterMode} mode; Endpoint: [{endpoint}]",
                version, IntPtr.Size == 8 ? "64" : "32",
                TServerOptions.EnableCluster ? "cluster" : "standalone",
                string.Join(',', opts.EndPoints.Select(endpoint => endpoint.ToString())));
            logger?.LogInformation("Environment .NET {netVersion}; {osPlatform}; {processArch}", Environment.Version, Environment.OSVersion.Platform, RuntimeInformation.ProcessArchitecture);

            var customCommandManager = new CustomCommandManager();

            ThreadPool.GetMinThreads(out var minThreads, out var minCPThreads);
            ThreadPool.GetMaxThreads(out var maxThreads, out var maxCPThreads);

            bool minChanged = false, maxChanged = false;
            if (opts.ThreadPoolMinThreads > 0)
            {
                minThreads = opts.ThreadPoolMinThreads;
                minChanged = true;
            }
            if (opts.ThreadPoolMinIOCompletionThreads > 0)
            {
                minCPThreads = opts.ThreadPoolMinIOCompletionThreads;
                minChanged = true;
            }
            if (opts.ThreadPoolMaxThreads > 0)
            {
                maxThreads = opts.ThreadPoolMaxThreads;
                maxChanged = true;
            }
            if (opts.ThreadPoolMaxIOCompletionThreads > 0)
            {
                maxCPThreads = opts.ThreadPoolMaxIOCompletionThreads;
                maxChanged = true;
            }

            // First try to set the max threads
            var setMax = !maxChanged || ThreadPool.SetMaxThreads(maxThreads, maxCPThreads);

            // Set the min threads
            if (minChanged && !ThreadPool.SetMinThreads(minThreads, minCPThreads))
                throw new Exception($"Unable to call ThreadPool.SetMinThreads with {minThreads}, {minCPThreads}");

            // Retry to set max threads if it wasn't set in the earlier step
            if (!setMax && !ThreadPool.SetMaxThreads(maxThreads, maxCPThreads))
                throw new Exception($"Unable to call ThreadPool.SetMaxThreads with {maxThreads}, {maxCPThreads}");

            opts.Initialize(loggerFactory);
            StoreWrapper.DatabaseCreatorDelegate createDatabaseDelegate = (int dbId) =>
                CreateDatabase(dbId, opts, clusterFactory, customCommandManager);

            if (!TServerOptions.DisablePubSub)
                subscribeBroker = new SubscribeBroker(null, opts.PubSubPageSizeBytes(), opts.SubscriberRefreshFrequencyMs, pubSubEpoch, startFresh: true, logger);

            logger?.LogTrace("TLS is {tlsEnabled}", opts.TlsOptions == null ? "disabled" : "enabled");

            // Create Garnet TCP server if none was provided.
            if (servers == null)
            {
                servers = new IGarnetServer[opts.EndPoints.Length];
                for (var i = 0; i < servers.Length; i++)
                {
                    if (opts.EndPoints[i] is UnixDomainSocketEndPoint)
                    {
                        ArgumentException.ThrowIfNullOrWhiteSpace(opts.UnixSocketPath, nameof(opts.UnixSocketPath));

                        // Delete existing unix socket file, if it exists.
                        File.Delete(opts.UnixSocketPath);
                    }
                    servers[i] = new GarnetServerTcp(opts.EndPoints[i], 0, opts.TlsOptions, TServerOptions.NetworkSendThrottleMax, opts.NetworkConnectionLimit, opts.UnixSocketPath, opts.UnixSocketPermission, logger);
                }
            }

            storeWrapper = new StoreWrapper(version, RedisProtocolVersion, servers, customCommandManager, opts, subscribeBroker,
                createDatabaseDelegate: createDatabaseDelegate,
                clusterFactory: clusterFactory,
                loggerFactory: loggerFactory);

            if (logger != null)
            {
                var configMemoryLimit = (storeWrapper.store.IndexSize * 64) +
                                        storeWrapper.store.Log.MaxMemorySizeBytes +
                                        (storeWrapper.store.ReadCache?.MaxMemorySizeBytes ?? 0) +
                                        (storeWrapper.appendOnlyFile?.MaxMemorySizeBytes ?? 0) +
                                        (storeWrapper.sizeTracker?.TargetSize ?? 0) +
                                        (storeWrapper.sizeTracker?.ReadCacheTargetSize ?? 0);

                logger.LogInformation("Total configured memory limit: {configMemoryLimit}", configMemoryLimit);
            }

            var maxDatabases = TServerOptions.EnableCluster ? 1 : TServerOptions.MaxDatabases;
            logger?.LogInformation("Max number of logical databases allowed on server: {maxDatabases}", maxDatabases);

            if (opts.ExtensionBinPaths?.Length > 0)
            {
                logger?.LogTrace("Allowed binary paths for extension loading: {binPaths}", string.Join(",", opts.ExtensionBinPaths));
                logger?.LogTrace("Unsigned extension libraries {unsignedAllowed}allowed.", TServerOptions.ExtensionAllowUnsignedAssemblies ? string.Empty : "not ");
            }

            // Create session provider for Garnet
            Provider = new GarnetProvider(storeWrapper, subscribeBroker);

            // Create user facing API endpoints
            Metrics = new MetricsApi(Provider);
            Register = new RegisterApi(Provider);
            Store = new StoreApi(storeWrapper);

            for (var i = 0; i < servers.Length; i++)
                servers[i].Register(WireFormat.ASCII, Provider);

            LoadModules(customCommandManager);
        }

        private GarnetDatabase CreateDatabase(int dbId, GarnetServerOptions serverOptions, ClusterFactory clusterFactory,
            CustomCommandManager customCommandManager)
        {
            var store = CreateStore(dbId, clusterFactory, customCommandManager, storeEpoch, out var stateMachineDriver, out var sizeTracker, out var kvSettings);
            var (aofDevice, aof) = CreateAOF(dbId);

            return new GarnetDatabase(dbId, store, kvSettings, storeEpoch, stateMachineDriver, sizeTracker, aofDevice, aof, TServerOptions.AdjustedIndexMaxCacheLines == 0);
        }

        private void LoadModules(CustomCommandManager customCommandManager)
        {
            if (opts.LoadModuleCS == null)
                return;

            foreach (var moduleCS in opts.LoadModuleCS)
            {
                var moduleCSData = moduleCS.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (moduleCSData.Length < 1)
                    continue;

                var modulePath = moduleCSData[0];
                var moduleArgs = moduleCSData.Length > 1 ? moduleCSData.Skip(1).ToArray() : [];

                if (!ModuleUtils.LoadAssemblies([modulePath], null, TServerOptions.ExtensionAllowUnsignedAssemblies,
                        out var loadedAssemblies, out var errorMsg, ignorePathCheckWhenUndefined: true)
                    || !ModuleRegistrar.Instance.LoadModule(customCommandManager, loadedAssemblies.ToList()[0], moduleArgs, logger, out errorMsg))
                {
                    logger?.LogError("Module {0} failed to load with error {1}", modulePath, Encoding.UTF8.GetString(errorMsg));
                }
            }
        }

        private TsavoriteKV<StoreFunctions, StoreAllocator> CreateStore(int dbId, IClusterFactory clusterFactory, CustomCommandManager customCommandManager,
            LightEpoch epoch, out StateMachineDriver stateMachineDriver, out CacheSizeTracker sizeTracker, out KVSettings kvSettings)
        {
            sizeTracker = null;

            stateMachineDriver = new StateMachineDriver(epoch, loggerFactory?.CreateLogger($"StateMachineDriver"));

            kvSettings = opts.GetSettings(loggerFactory, epoch, stateMachineDriver, out logFactory);

            // Run checkpoint on its own thread to control p99
            kvSettings.ThrottleCheckpointFlushDelayMs = opts.CheckpointThrottleFlushDelayMs;

            var baseName = opts.GetStoreCheckpointDirectory(dbId);
            var defaultNamingScheme = new DefaultCheckpointNamingScheme(baseName);

            kvSettings.CheckpointManager = TServerOptions.EnableCluster ?
                clusterFactory.CreateCheckpointManager(opts.DeviceFactoryCreator, defaultNamingScheme, isMainStore: true, logger) :
                new GarnetCheckpointManager(opts.DeviceFactoryCreator, defaultNamingScheme, removeOutdated: true);

            var store = new TsavoriteKV<StoreFunctions, StoreAllocator>(kvSettings
                , Tsavorite.core.StoreFunctions.Create(new GarnetKeyComparer(),
                    () => new GarnetObjectSerializer(customCommandManager))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            if (kvSettings.LogMemorySize > 0 || kvSettings.ReadCacheMemorySize > 0)
                sizeTracker = new CacheSizeTracker(store, kvSettings.LogMemorySize, kvSettings.ReadCacheMemorySize, this.loggerFactory);
            return store;
        }

        private (IDevice, TsavoriteLog) CreateAOF(int dbId)
        {
            if (!TServerOptions.EnableAOF)
            {
                if (TServerOptions.CommitFrequencyMs != 0 || TServerOptions.WaitForCommit)
                    throw new Exception("Cannot use CommitFrequencyMs or CommitWait without EnableAOF");
                return (null, null);
            }

            if (TServerOptions.FastAofTruncate && TServerOptions.CommitFrequencyMs != -1)
                throw new Exception("Need to set CommitFrequencyMs to -1 (manual commits) with FastAofTruncate");

            opts.GetAofSettings(dbId, aofEpoch, out var aofSettings);
            var aofDevice = aofSettings.LogDevice;
            var appendOnlyFile = new TsavoriteLog(aofSettings, logger: this.loggerFactory?.CreateLogger("TsavoriteAof"));

            if (TServerOptions.CommitFrequencyMs < 0 && TServerOptions.WaitForCommit)
                throw new Exception("Cannot use CommitWait with manual commits");
            return (aofDevice, appendOnlyFile);
        }

        /// <inheritdoc />
        public void Start()
        {
            Provider.Recover();
            for (var i = 0; i < servers.Length; i++)
                servers[i].Start();
            Provider.Start();
            if (!TServerOptions.QuietMode)
                Console.WriteLine("* Ready to accept connections");
        }

        /// <summary>
        /// Dispose store (including log and checkpoint directory)
        /// </summary>
        public void Dispose()
        {
            Dispose(cleanupDir);
        }

        /// <summary>
        /// Dispose, optionally deleting logs and checkpoints
        /// </summary>
        /// <param name="deleteDir">Whether to delete logs and checkpoints</param>
        public void Dispose(bool deleteDir = true)
        {
            InternalDispose();
            if (deleteDir)
            {
                logFactory?.Delete(new FileDescriptor { directoryName = "" });
                if (opts.CheckpointDir != opts.LogDir && !string.IsNullOrEmpty(opts.CheckpointDir))
                {
                    var checkpointDeviceFactory = opts.DeviceFactoryCreator.Create(opts.CheckpointDir);
                    checkpointDeviceFactory.Delete(new FileDescriptor { directoryName = "" });
                }
            }
        }

        private void InternalDispose()
        {
            Provider?.Dispose();
            for (var i = 0; i < servers.Length; i++)
                servers[i]?.Dispose();
            subscribeBroker?.Dispose();
            storeEpoch?.Dispose();
            aofEpoch?.Dispose();
            pubSubEpoch?.Dispose();
            opts.AuthSettings?.Dispose();
            if (DisposeLoggerFactory)
                loggerFactory?.Dispose();
        }

        /// <summary>
        /// Flushes MemoryLogger entries into a destination logger.
        /// </summary>
        private static void FlushMemoryLogger(MemoryLogger memoryLogger, string categoryName, ILoggerFactory dstLoggerFactory = null)
        {
            if (memoryLogger == null) return;

            var disposeDstLoggerFactory = false;
            if (dstLoggerFactory == null)
            {
                dstLoggerFactory = LoggerFactory.Create(builder => builder.AddSimpleConsole(options =>
                {
                    options.SingleLine = true;
                    options.TimestampFormat = "hh::mm::ss ";
                }).SetMinimumLevel(LogLevel.Information));
                disposeDstLoggerFactory = true;
            }

            var dstLogger = dstLoggerFactory.CreateLogger(categoryName);
            memoryLogger.FlushLogger(dstLogger);

            if (disposeDstLoggerFactory)
            {
                dstLoggerFactory.Dispose();
            }
        }
    }
}