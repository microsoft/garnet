// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Garnet;

public class GarnetApplication : IHost
{
    public MetricsApi Metrics
    {
        get
        {
            return host.Services.GetRequiredService<MetricsApi>();
        }
    }

    public RegisterApi Register
    {
        get
        {
            return host.Services.GetRequiredService<RegisterApi>();
        }
    }

    public StoreApi Store
    {
        get
        {
            return host.Services.GetRequiredService<StoreApi>();
        }
    }

    readonly IHost host;

    public GarnetApplication(IHost host)
    {
        this.host = host;
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
        => host.StartAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken = default)
        => host.StopAsync(cancellationToken);

    public IServiceProvider Services { get => host.Services; }

    public void Dispose()
    {
        host.Dispose();
    }

    public void Run()
    {
        HostingAbstractionsHostExtensions.Run(this);
    }

    public Task RunAsync(CancellationToken cancellationToken = default)
    {
        HostingAbstractionsHostExtensions.RunAsync(this, cancellationToken);
        return Task.CompletedTask;
    }

    public static GarnetApplicationBuilder CreateHostBuilder(string[] args)
    {
        MemoryLogger initLogger;

        // Set up an initial memory logger to log messages from configuration parser into memory.
        using (var memLogProvider = new MemoryLoggerProvider())
        {
            initLogger = (MemoryLogger)memLogProvider.CreateLogger("ArgParser");
        }

        if (!ServerSettingsManager.TryParseCommandLineArguments(args, out var serverSettings, out _,
                out var exitGracefully, initLogger))
        {
            if (exitGracefully)
                System.Environment.Exit(0);

            // Flush logs from memory logger
            //initLogger.FlushLogger(logger);

            throw new GarnetException(
                "Encountered an error when initializing Garnet server. Please see log messages above for more details.");
        }

        var garnetServerOptions = serverSettings.GetServerOptions(null);

        return new(new GarnetApplicationOptions { Args = args }, garnetServerOptions);
    }

    public static GarnetApplicationBuilder CreateHostBuilder(string[] args, GarnetServerOptions options)
    {
        return new(new GarnetApplicationOptions
        {
            Args = args,
        }, options);
    }
}