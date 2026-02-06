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
using Garnet.server.Auth.Settings;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Implementation Garnet server
    /// </summary>
    public class GarnetServer : IDisposable
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

        internal GarnetProvider Provider;

        private readonly GarnetServerOptions opts;
        private IGarnetServer[] servers;
        private SubscribeBroker subscribeBroker;
        private KVSettings<SpanByte, SpanByte> kvSettings;
        private KVSettings<byte[], IGarnetObject> objKvSettings;
        private INamedDeviceFactory logFactory;
        private MemoryLogger initLogger;
        private ILogger logger;
        private readonly ILoggerFactory loggerFactory;
        private readonly bool cleanupDir;
        private bool disposeLoggerFactory;

        /// <summary>
        /// Store and associated information used by this Garnet server
        /// </summary>
        protected StoreWrapper storeWrapper;

        /// <summary>
        /// Metrics API
        /// </summary>
        public MetricsApi Metrics;

        /// <summary>
        /// Command registration API
        /// </summary>
        public RegisterApi Register;

        /// <summary>
        /// Store API
        /// </summary>
        public StoreApi Store;

        /// <summary>
        /// Create Garnet Server instance using specified command line arguments; use Start to start the server.
        /// </summary>
        /// <param name="commandLineArgs">Command line arguments</param>
        /// <param name="loggerFactory">Logger factory</param>
        /// <param name="cleanupDir">Clean up directory.</param>
        /// <param name="authenticationSettingsOverride">Override for custom authentication settings.</param>
        public GarnetServer(string[] commandLineArgs, ILoggerFactory loggerFactory = null, bool cleanupDir = false, IAuthenticationSettings authenticationSettingsOverride = null)
        {
            Trace.Listeners.Add(new ConsoleTraceListener());

            // Set up an initial memory logger to log messages from configuration parser into memory.
            using (var memLogProvider = new MemoryLoggerProvider())
            {
                this.initLogger = (MemoryLogger)memLogProvider.CreateLogger("ArgParser");
            }

            if (!ServerSettingsManager.TryParseCommandLineArguments(commandLineArgs, out var serverSettings, out _, out _, out var exitGracefully, logger: this.initLogger))
            {
                if (exitGracefully)
                    Environment.Exit(0);

                // Flush logs from memory logger
                FlushMemoryLogger(this.initLogger, "ArgParser", loggerFactory);

                throw new GarnetException("Encountered an error when initializing Garnet server. Please see log messages above for more details.");
            }

            if (loggerFactory == null)
            {
                // If the main logger factory is created by GarnetServer, it should be disposed when GarnetServer is disposed
                disposeLoggerFactory = true;
            }
            else
            {
                this.initLogger.LogWarning(
                    $"Received an external ILoggerFactory object. The following configuration options are ignored: {nameof(serverSettings.FileLogger)}, {nameof(serverSettings.LogLevel)}, {nameof(serverSettings.DisableConsoleLogger)}.");
            }

            // If no logger factory is given, set up main logger factory based on parsed configuration values,
            // otherwise use given logger factory.
            this.loggerFactory = loggerFactory ?? LoggerFactory.Create(builder =>
            {
                if (!serverSettings.DisableConsoleLogger.GetValueOrDefault())
                {
                    builder.AddSimpleConsole(options =>
                    {
                        options.SingleLine = true;
                        options.TimestampFormat = "hh::mm::ss ";
                    });
                }

                // Optional: Flush log output to file.
                if (serverSettings.FileLogger != null)
                    builder.AddFile(serverSettings.FileLogger);
                builder.SetMinimumLevel(serverSettings.LogLevel);
            });

            // Assign values to GarnetServerOptions
            this.opts = serverSettings.GetServerOptions(this.loggerFactory.CreateLogger("Options"));
            this.opts.AuthSettings = authenticationSettingsOverride ?? this.opts.AuthSettings;
            this.cleanupDir = cleanupDir;
            this.InitializeServer();
        }

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
            this.InitializeServer();
        }

        private void InitializeServer()
        {
            Debug.Assert(opts != null);

            if (!opts.QuietMode)
            {
                var red = "\u001b[31m";
                var magenta = "\u001b[35m";
                var normal = "\u001b[0m";

                Console.WriteLine($"""
                    {red}    _________
                       /_||___||_\      {normal}Garnet {version} {(IntPtr.Size == 8 ? "64" : "32")} bit; {(opts.EnableCluster ? "cluster" : "standalone")} mode{red}
                       '. \   / .'      {normal}Listening on: {(opts.EndPoints.Length > 1 ? opts.EndPoints[0] + $" and {opts.EndPoints.Length - 1} more" : opts.EndPoints[0])}{red}
                         '.\ /.'        {magenta}https://aka.ms/GetGarnet{red}
                           '.'
                    {normal}
                    """);
            }

            var clusterFactory = opts.EnableCluster ? new ClusterFactory() : null;

            this.logger = this.loggerFactory?.CreateLogger("GarnetServer");
            logger?.LogInformation("Garnet {version} {bits} bit; {clusterMode} mode; Endpoint: [{endpoint}]",
                version, IntPtr.Size == 8 ? "64" : "32",
                opts.EnableCluster ? "cluster" : "standalone",
                string.Join(',', opts.EndPoints.Select(endpoint => endpoint.ToString())));
            logger?.LogInformation("Environment .NET {netVersion}; {osPlatform}; {processArch}", Environment.Version, Environment.OSVersion.Platform, RuntimeInformation.ProcessArchitecture);

            // Flush initialization logs from memory logger
            FlushMemoryLogger(this.initLogger, "ArgParser", this.loggerFactory);

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

            if (!opts.DisablePubSub)
                subscribeBroker = new SubscribeBroker(null, opts.PubSubPageSizeBytes(), opts.SubscriberRefreshFrequencyMs, true, logger);

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
                    servers[i] = new GarnetServerTcp(opts.EndPoints[i], 0, opts.TlsOptions, opts.NetworkSendThrottleMax, opts.NetworkConnectionLimit, opts.UnixSocketPath, opts.UnixSocketPermission, logger);
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
                                        (storeWrapper.appendOnlyFile?.MaxMemorySizeBytes ?? 0);
                if (storeWrapper.objectStore != null)
                    configMemoryLimit += (storeWrapper.objectStore.IndexSize * 64) +
                                         storeWrapper.objectStore.Log.MaxMemorySizeBytes +
                                         (storeWrapper.objectStore.ReadCache?.MaxMemorySizeBytes ?? 0) +
                                         (storeWrapper.objectStoreSizeTracker?.TargetSize ?? 0) +
                                         (storeWrapper.objectStoreSizeTracker?.ReadCacheTargetSize ?? 0);
                logger.LogInformation("Total configured memory limit: {configMemoryLimit}", configMemoryLimit);
            }

            var maxDatabases = opts.EnableCluster ? 1 : opts.MaxDatabases;
            logger?.LogInformation("Max number of logical databases allowed on server: {maxDatabases}", maxDatabases);

            if (opts.ExtensionBinPaths?.Length > 0)
            {
                logger?.LogTrace("Allowed binary paths for extension loading: {binPaths}", string.Join(",", opts.ExtensionBinPaths));
                logger?.LogTrace("Unsigned extension libraries {unsignedAllowed}allowed.", opts.ExtensionAllowUnsignedAssemblies ? string.Empty : "not ");
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
            var epoch = new LightEpoch();
            var store = CreateMainStore(dbId, clusterFactory, epoch, out var stateMachineDriver);
            var objectStore = CreateObjectStore(dbId, clusterFactory, customCommandManager, epoch, stateMachineDriver, out var objectStoreSizeTracker);
            var (aofDevice, aof) = CreateAOF(dbId);
            return new GarnetDatabase(dbId, store, objectStore, epoch, stateMachineDriver, objectStoreSizeTracker,
                aofDevice, aof, serverOptions.AdjustedIndexMaxCacheLines == 0,
                serverOptions.AdjustedObjectStoreIndexMaxCacheLines == 0);
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

                if (!ModuleUtils.LoadAssemblies([modulePath], null, opts.ExtensionAllowUnsignedAssemblies,
                        out var loadedAssemblies, out var errorMsg, ignorePathCheckWhenUndefined: true)
                    || !ModuleRegistrar.Instance.LoadModule(customCommandManager, loadedAssemblies.ToList()[0], moduleArgs, logger, out errorMsg))
                {
                    logger?.LogError("Module {0} failed to load with error {1}", modulePath, Encoding.UTF8.GetString(errorMsg));
                }
            }
        }

        private TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> CreateMainStore(int dbId, IClusterFactory clusterFactory,
            LightEpoch epoch, out StateMachineDriver stateMachineDriver)
        {
            stateMachineDriver = new StateMachineDriver(epoch, loggerFactory?.CreateLogger($"StateMachineDriver"));

            kvSettings = opts.GetSettings(loggerFactory, epoch, stateMachineDriver, out logFactory);

            // Run checkpoint on its own thread to control p99
            kvSettings.ThrottleCheckpointFlushDelayMs = opts.CheckpointThrottleFlushDelayMs;

            var baseName = opts.GetMainStoreCheckpointDirectory(dbId);
            var defaultNamingScheme = new DefaultCheckpointNamingScheme(baseName);

            kvSettings.CheckpointManager = opts.EnableCluster ?
                clusterFactory.CreateCheckpointManager(opts.DeviceFactoryCreator, defaultNamingScheme, isMainStore: true, logger) :
                new GarnetCheckpointManager(opts.DeviceFactoryCreator, defaultNamingScheme, removeOutdated: true);

            return new TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator>(kvSettings
                , StoreFunctions<SpanByte, SpanByte>.Create()
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        private TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> CreateObjectStore(int dbId, IClusterFactory clusterFactory, CustomCommandManager customCommandManager,
            LightEpoch epoch, StateMachineDriver stateMachineDriver, out CacheSizeTracker objectStoreSizeTracker)
        {
            objectStoreSizeTracker = null;
            if (opts.DisableObjects)
                return null;

            objKvSettings = opts.GetObjectStoreSettings(loggerFactory, epoch, stateMachineDriver,
                out var objHeapMemorySize, out var objReadCacheHeapMemorySize);

            // Run checkpoint on its own thread to control p99
            objKvSettings.ThrottleCheckpointFlushDelayMs = opts.CheckpointThrottleFlushDelayMs;

            var baseName = opts.GetObjectStoreCheckpointDirectory(dbId);
            var defaultNamingScheme = new DefaultCheckpointNamingScheme(baseName);

            objKvSettings.CheckpointManager = opts.EnableCluster ?
                clusterFactory.CreateCheckpointManager(opts.DeviceFactoryCreator, defaultNamingScheme, isMainStore: false, logger) :
                new GarnetCheckpointManager(opts.DeviceFactoryCreator, defaultNamingScheme, removeOutdated: true);

            var objStore = new TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator>(
                objKvSettings,
                StoreFunctions<byte[], IGarnetObject>.Create(new ByteArrayKeyComparer(),
                    () => new ByteArrayBinaryObjectSerializer(),
                    () => new GarnetObjectSerializer(customCommandManager)),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            if (objHeapMemorySize > 0 || objReadCacheHeapMemorySize > 0)
                objectStoreSizeTracker = new CacheSizeTracker(objStore, objKvSettings, objHeapMemorySize, objReadCacheHeapMemorySize,
                    this.loggerFactory);

            return objStore;

        }

        private (IDevice, TsavoriteLog) CreateAOF(int dbId)
        {
            if (!opts.EnableAOF)
            {
                if (opts.CommitFrequencyMs != 0 || opts.WaitForCommit)
                    throw new Exception("Cannot use CommitFrequencyMs or CommitWait without EnableAOF");
                return (null, null);
            }

            if (opts.FastAofTruncate && opts.CommitFrequencyMs != -1)
                throw new Exception("Need to set CommitFrequencyMs to -1 (manual commits) with FastAofTruncate");

            opts.GetAofSettings(dbId, out var aofSettings);
            var aofDevice = aofSettings.LogDevice;
            var appendOnlyFile = new TsavoriteLog(aofSettings, logger: this.loggerFactory?.CreateLogger("TsavoriteLog [aof]"));
            if (opts.CommitFrequencyMs < 0 && opts.WaitForCommit)
                throw new Exception("Cannot use CommitWait with manual commits");
            return (aofDevice, appendOnlyFile);
        }

        /// <summary>
        /// Start server instance
        /// </summary>
        public void Start()
        {
            Provider.Recover();
            for (var i = 0; i < servers.Length; i++)
                servers[i].Start();
            Provider.Start();
            if (!opts.QuietMode)
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
            kvSettings.LogDevice?.Dispose();
            if (!opts.DisableObjects)
            {
                objKvSettings.LogDevice?.Dispose();
                objKvSettings.ObjectLogDevice?.Dispose();
            }
            opts.AuthSettings?.Dispose();
            if (disposeLoggerFactory)
                loggerFactory?.Dispose();
        }

        private static void DeleteDirectory(string path)
        {
            if (path == null) return;

            // Exceptions may happen due to a handle briefly remaining held after Dispose().
            try
            {
                foreach (string directory in Directory.GetDirectories(path))
                {
                    DeleteDirectory(directory);
                }

                Directory.Delete(path, true);
            }
            catch (Exception ex) when (ex is IOException ||
                                       ex is UnauthorizedAccessException)
            {
                try
                {
                    Directory.Delete(path, true);
                }
                catch { }
            }
        }

        /// <summary>
        /// Flushes MemoryLogger entries into a destination logger.
        /// Destination logger is either created from ILoggerFactory parameter or from a default console logger.
        /// </summary>
        /// <param name="memoryLogger">The memory logger</param>
        /// <param name="categoryName">The category name of the destination logger</param>
        /// <param name="dstLoggerFactory">Optional logger factory for creating the destination logger</param>
        private static void FlushMemoryLogger(MemoryLogger memoryLogger, string categoryName, ILoggerFactory dstLoggerFactory = null)
        {
            if (memoryLogger == null) return;

            // If no logger factory supplied, create a default console logger
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

            // Create the destination logger
            var dstLogger = dstLoggerFactory.CreateLogger(categoryName);

            // Flush all entries from the memory logger into the destination logger
            memoryLogger.FlushLogger(dstLogger);

            // If a default console logger factory was created, it is no longer needed
            if (disposeDstLoggerFactory)
            {
                dstLoggerFactory.Dispose();
            }
        }
    }
}