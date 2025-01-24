// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Garnet.networking;
using Garnet.server;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tsavorite.core;

namespace Garnet;

internal class GarnetServerHostedService : BackgroundService
{
    private readonly GarnetProvider provider;
    private readonly GarnetServerOptions opts;
    private readonly IGarnetServer server;
    private readonly ILogger<GarnetServerHostedService> logger;
    private readonly ILoggerFactory loggerFactory;
    private readonly bool cleanupDir;
    
    public GarnetServerHostedService(
        IOptions<GarnetServerOptions> options,
        ILogger<GarnetServerHostedService> logger,
        ILoggerFactory loggerFactory,
        IGarnetServer server,
        GarnetProvider garnetProvider)
    {
        this.server = server;
        this.opts = options.Value;
        this.logger = logger;
        this.loggerFactory = loggerFactory;
        this.provider = garnetProvider;

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

            Console.WriteLine($@"{red}        _________
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

        server.Register(WireFormat.ASCII, provider);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        provider.Recover();
        server.Start();
        provider.Start();
        
        if (!opts.QuietMode)
        {
            logger.LogInformation("* Ready to accept connections");
        }
        
        logger.LogInformation("Garnet server running at: {time}", DateTimeOffset.Now);

        await Task.CompletedTask;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (cleanupDir)
        {
            if (opts.CheckpointDir != opts.LogDir && !string.IsNullOrEmpty(opts.CheckpointDir))
            {
                var ckptdir = opts.DeviceFactoryCreator();
                ckptdir.Initialize(opts.CheckpointDir);
                ckptdir.Delete(new FileDescriptor { directoryName = "" });
            }
        }
        
        await base.StopAsync(cancellationToken);
    }
    
    static readonly string version = GetVersion();

    static string GetVersion()
    {
        var Version = Assembly.GetExecutingAssembly().GetName().Version;
        return $"{Version.Major}.{Version.Minor}.{Version.Build}";
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