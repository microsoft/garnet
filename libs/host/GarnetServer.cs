// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
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
    /// Implementation Garnet server
    /// </summary>
    public class GarnetServer : IDisposable
    {
        internal GarnetProvider Provider;

        private readonly GarnetServerOptions opts;
        private IGarnetServer server;
        private TsavoriteKV<SpanByte, SpanByte> store;
        private TsavoriteKV<byte[], IGarnetObject> objectStore;
        private IDevice aofDevice;
        private TsavoriteLog appendOnlyFile;
        private SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> broker;
        private LogSettings logSettings, objLogSettings;
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

        // IMPORTANT: Keep the version in sync with .azure\pipelines\azure-pipelines-external-release.yml line ~6.
        readonly string version = "1.0.9";

        /// <summary>
        /// Resp protocol version
        /// </summary>
        readonly string redisProtocolVersion = "6.2.11";

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
        public GarnetServer(string[] commandLineArgs, ILoggerFactory loggerFactory = null, bool cleanupDir = false)
        {
            Trace.Listeners.Add(new ConsoleTraceListener());

            // Set up an initial memory logger to log messages from configuration parser into memory.
            using (var memLogProvider = new MemoryLoggerProvider())
            {
                this.initLogger = (MemoryLogger)memLogProvider.CreateLogger("ArgParser");
            }

            if (!ServerSettingsManager.TryParseCommandLineArguments(commandLineArgs, out var serverSettings, out _, this.initLogger))
            {
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
            this.cleanupDir = cleanupDir;
            this.InitializeServer();
        }

        /// <summary>
        /// Create Garnet Server instance using GarnetServerOptions instance; use Start to start the server.
        /// </summary>
        /// <param name="opts">Server options</param>
        /// <param name="loggerFactory">Logger factory</param>
        /// <param name="server">The IGarnetServer to use. If none is provided, will use a GarnetServerTcp.</param>
        /// <param name="cleanupDir">Whether to clean up data folders on dispose</param>
        public GarnetServer(GarnetServerOptions opts, ILoggerFactory loggerFactory = null, IGarnetServer server = null, bool cleanupDir = false)
        {
            this.server = server;
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

                Console.WriteLine($@"{red}    _________
   /_||___||_\      {normal}Garnet {version} {(IntPtr.Size == 8 ? "64" : "32")} bit; {(opts.EnableCluster ? "cluster" : "standalone")} mode{red}
   '. \   / .'      {normal}Port: {opts.Port}{red}
     '.\ /.'        {magenta}https://aka.ms/GetGarnet{red}
       '.'
{normal}");
            }

            IClusterFactory clusterFactory = null;
            if (opts.EnableCluster) clusterFactory = new ClusterFactory();

            this.logger = this.loggerFactory?.CreateLogger("GarnetServer");
            logger?.LogInformation("Garnet {version} {bits} bit; {clusterMode} mode; Port: {port}", version, IntPtr.Size == 8 ? "64" : "32", opts.EnableCluster ? "cluster" : "standalone", opts.Port);

            // Flush initialization logs from memory logger
            FlushMemoryLogger(this.initLogger, "ArgParser", this.loggerFactory);

            var customCommandManager = new CustomCommandManager();

            bool setMax = true;
            if (opts.ThreadPoolMaxThreads > 0)
                setMax = ThreadPool.SetMaxThreads(opts.ThreadPoolMaxThreads, opts.ThreadPoolMaxThreads);

            if (opts.ThreadPoolMinThreads > 0)
            {
                if (!ThreadPool.SetMinThreads(opts.ThreadPoolMinThreads, opts.ThreadPoolMinThreads))
                    throw new Exception($"Unable to call ThreadPool.SetMinThreads with {opts.ThreadPoolMinThreads}");
            }

            // Retry to set max threads if it wasn't set in the previous step
            if (!setMax && !ThreadPool.SetMaxThreads(opts.ThreadPoolMaxThreads, opts.ThreadPoolMaxThreads))
                throw new Exception($"Unable to call ThreadPool.SetMaxThreads with {opts.ThreadPoolMaxThreads}");

            opts.GetSettings(out logSettings, out var indexSize, out var revivSettings, out logFactory);

            var CheckpointDir = opts.CheckpointDir;
            if (CheckpointDir == null) CheckpointDir = opts.LogDir;

            var checkpointSettings = new CheckpointSettings
            {
                // Run checkpoint on its own thread to control p99
                ThrottleCheckpointFlushDelayMs = opts.CheckpointThrottleFlushDelayMs,
                CheckpointVersionSwitchBarrier = opts.EnableCluster
            };
            var checkpointFactory = opts.DeviceFactoryCreator();
            if (opts.EnableCluster)
            {
                checkpointSettings.CheckpointManager = clusterFactory.CreateCheckpointManager(checkpointFactory, new DefaultCheckpointNamingScheme(CheckpointDir + "/Store/checkpoints"), true, logger);
            }
            else
            {
                checkpointSettings.CheckpointManager = new DeviceLogCommitCheckpointManager(checkpointFactory,
                    new DefaultCheckpointNamingScheme(CheckpointDir + "/Store/checkpoints"),
                    removeOutdated: true);
            }


            store = new TsavoriteKV<SpanByte, SpanByte>(indexSize, logSettings, checkpointSettings, revivificationSettings: revivSettings, logger: this.loggerFactory?.CreateLogger("TsavoriteKV [main]"));

            CacheSizeTracker objectStoreSizeTracker = null;
            if (!opts.DisableObjects)
            {
                opts.GetObjectStoreSettings(out objLogSettings, out var objRevivSettings, out var objIndexSize, out var objTotalMemorySize);

                var objCheckpointSettings = new CheckpointSettings
                {
                    ThrottleCheckpointFlushDelayMs = opts.CheckpointThrottleFlushDelayMs,
                    CheckpointVersionSwitchBarrier = opts.EnableCluster
                };
                var objectCheckpointFactory = opts.DeviceFactoryCreator();
                if (opts.EnableCluster)
                {
                    objCheckpointSettings.CheckpointManager = clusterFactory.CreateCheckpointManager(opts.DeviceFactoryCreator(), new DefaultCheckpointNamingScheme(CheckpointDir + "/ObjectStore/checkpoints"), false, logger);
                }
                else
                {
                    objCheckpointSettings.CheckpointManager = new DeviceLogCommitCheckpointManager(opts.DeviceFactoryCreator(),
                        new DefaultCheckpointNamingScheme(CheckpointDir + "/ObjectStore/checkpoints"),
                        removeOutdated: true);
                }

                objectStore = new TsavoriteKV<byte[], IGarnetObject>(objIndexSize, objLogSettings, objCheckpointSettings,
                        new SerializerSettings<byte[], IGarnetObject> { keySerializer = () => new ByteArrayBinaryObjectSerializer(), valueSerializer = () => new GarnetObjectSerializer(customCommandManager) },
                        revivificationSettings: objRevivSettings, logger: this.loggerFactory?.CreateLogger("TsavoriteKV  [obj]"));
                if (objTotalMemorySize > 0)
                    objectStoreSizeTracker = new CacheSizeTracker(objectStore, objLogSettings, objTotalMemorySize, this.loggerFactory);
            }

            if (!opts.DisablePubSub)
            {
                broker = new SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>>(new SpanByteKeySerializer(), null, opts.PubSubPageSizeBytes(), true);
            }

            if (opts.EnableAOF)
            {
                if (opts.MainMemoryReplication)
                {
                    if (opts.CommitFrequencyMs != -1)
                        throw new Exception("Need to set CommitFrequencyMs to -1 (manual commits) with MainMemoryReplication");
                }

                opts.GetAofSettings(out var aofSettings);
                aofDevice = aofSettings.LogDevice;
                appendOnlyFile = new TsavoriteLog(aofSettings, logger: this.loggerFactory?.CreateLogger("TsavoriteLog [aof]"));

                if (opts.CommitFrequencyMs < 0 && opts.WaitForCommit)
                {
                    throw new Exception("Cannot use CommitWait with manual commits");
                }
            }
            else
            {
                if (opts.CommitFrequencyMs != 0 || opts.WaitForCommit)
                {
                    throw new Exception("Cannot use CommitFrequencyMs or CommitWait without EnableAOF");
                }
            }


            logger?.LogTrace("TLS is {tlsEnabled}", (opts.TlsOptions == null ? "disabled" : "enabled"));


            // Create Garnet TCP server if none was provided.
            if (this.server == null)
            {
                server = new GarnetServerTcp(opts.Address, opts.Port, 0, opts.TlsOptions, opts.NetworkSendThrottleMax, logger);
            }

            storeWrapper = new StoreWrapper(version, redisProtocolVersion, server, store, objectStore, objectStoreSizeTracker, customCommandManager, appendOnlyFile, opts, clusterFactory: clusterFactory, loggerFactory: loggerFactory);

            // Create session provider for Garnet
            Provider = new GarnetProvider(storeWrapper, broker);

            // Create user facing API endpoints
            Metrics = new MetricsApi(Provider);
            Register = new RegisterApi(Provider);
            Store = new StoreApi(storeWrapper);

            server.Register(WireFormat.ASCII, Provider);
        }

        /// <summary>
        /// Start server instance
        /// </summary>
        public void Start()
        {
            Provider.Recover();
            server.Start();
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
                    var ckptdir = opts.DeviceFactoryCreator();
                    ckptdir.Initialize(opts.CheckpointDir);
                    ckptdir.Delete(new FileDescriptor { directoryName = "" });
                }
            }
        }

        private void InternalDispose()
        {
            Provider?.Dispose();
            server.Dispose();
            broker?.Dispose();
            store.Dispose();
            appendOnlyFile?.Dispose();
            aofDevice?.Dispose();
            logSettings.LogDevice?.Dispose();
            if (!opts.DisableObjects)
            {
                objectStore.Dispose();
                objLogSettings.LogDevice?.Dispose();
                objLogSettings.ObjectLogDevice?.Dispose();
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