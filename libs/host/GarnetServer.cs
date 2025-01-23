// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using Garnet.cluster;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using Garnet.server.Auth.Settings;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tsavorite.core;

namespace Garnet
{
    using MainStoreAllocator =
        SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;
    using ObjectStoreAllocator =
        GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer,
            DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions =
        StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Implementation Garnet server
    /// </summary>
    public class GarnetServer : IDisposable
    {
        static readonly string version = GetVersion();

        static string GetVersion()
        {
            var Version = Assembly.GetExecutingAssembly().GetName().Version;
            return $"{Version.Major}.{Version.Minor}.{Version.Build}";
        }

        internal GarnetProvider Provider;

        private readonly GarnetServerOptions opts;
        private IGarnetServer server;
        private ILogger logger;
        private readonly ILoggerFactory loggerFactory;
        private readonly bool cleanupDir;

        /// <summary>
        /// Store and associated information used by this Garnet server
        /// </summary>
        protected StoreWrapper storeWrapper;

        /// <summary>
        /// Resp protocol version
        /// </summary>
        readonly string redisProtocolVersion = "7.2.5";

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
        /// Create Garnet Server instance using GarnetServerOptions instance; use Start to start the server.
        /// </summary>
        /// <param name="options">Server options</param>
        /// <param name="logger">Logger</param>
        /// <param name="loggerFactory">Logger factory</param>
        /// <param name="server">The IGarnetServer to use. If none is provided, will use a GarnetServerTcp.</param>
        /// <param name="garnetProvider"></param>
        /// <param name="metricsApi"></param>
        /// <param name="registerApi"></param>
        /// <param name="storeApi"></param>
        public GarnetServer(
            IOptions<GarnetServerOptions> options,
            ILogger<GarnetServer> logger,
            ILoggerFactory loggerFactory,
            IGarnetServer server,
            GarnetProvider garnetProvider,
            MetricsApi metricsApi,
            RegisterApi registerApi,
            StoreApi storeApi)
        {
            this.server = server;
            this.opts = options.Value;
            this.logger = logger;
            this.loggerFactory = loggerFactory;
            this.Provider = garnetProvider;
            this.Metrics = metricsApi;
            this.Register = registerApi;
            this.Store = storeApi;

            this.cleanupDir = false;
            this.InitializeServerUpdated();
        }

        private void InitializeServerUpdated()
        {
            Debug.Assert(opts != null);

            if (!opts.QuietMode)
            {
                var red = "\u001b[31m";
                var magenta = "\u001b[35m";
                var normal = "\u001b[0m";

                Console.WriteLine($@"{red}            _________
           /_||___||_\      {normal}Garnet {version} {(IntPtr.Size == 8 ? "64" : "32")} bit; {(opts.EnableCluster ? "cluster" : "standalone")} mode{red}
           '. \   / .'      {normal}Port: {opts.Port}{red}
             '.\ /.'        {magenta}https://aka.ms/GetGarnet{red}
               '.'
        {normal}");
            }

            logger?.LogInformation("Garnet {version} {bits} bit; {clusterMode} mode; Port: {port}", version,
                IntPtr.Size == 8 ? "64" : "32", opts.EnableCluster ? "cluster" : "standalone", opts.Port);

            // Flush initialization logs from memory logger
            FlushMemoryLogger(null, "ArgParser", this.loggerFactory);

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

            logger?.LogTrace("TLS is {tlsEnabled}", opts.TlsOptions == null ? "disabled" : "enabled");

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
        }

        /// <summary>
        /// Flushes MemoryLogger entries into a destination logger.
        /// Destination logger is either created from ILoggerFactory parameter or from a default console logger.
        /// </summary>
        /// <param name="memoryLogger">The memory logger</param>
        /// <param name="categoryName">The category name of the destination logger</param>
        /// <param name="dstLoggerFactory">Optional logger factory for creating the destination logger</param>
        private static void FlushMemoryLogger(MemoryLogger memoryLogger, string categoryName,
            ILoggerFactory dstLoggerFactory = null)
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