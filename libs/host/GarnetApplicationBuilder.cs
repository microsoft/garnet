// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Reflection;
using Garnet.cluster;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tsavorite.core;

namespace Garnet;

public class GarnetApplicationBuilder : IHostApplicationBuilder
{
    readonly HostApplicationBuilder hostApplicationBuilder;

    internal GarnetApplicationBuilder(GarnetApplicationOptions options, GarnetServerOptions garnetServerOptions)
    {
        var configuration = new ConfigurationManager();

        configuration.AddEnvironmentVariables(prefix: "GARNET_");

        hostApplicationBuilder = new HostApplicationBuilder(new HostApplicationBuilderSettings
        {
            Args = options.Args,
            ApplicationName = options.ApplicationName,
            EnvironmentName = options.EnvironmentName,
            Configuration = configuration
        });

        hostApplicationBuilder.Services.AddOptions();
        
        var garnetServerOptionsWrapped = Microsoft.Extensions.Options.Options.Create(garnetServerOptions);
        hostApplicationBuilder.Services.AddSingleton(garnetServerOptionsWrapped);

        hostApplicationBuilder.Logging.ClearProviders();
        hostApplicationBuilder.Logging.AddSimpleConsole(simpleConsoleFormatterOptions =>
        {
            simpleConsoleFormatterOptions.SingleLine = true;
            simpleConsoleFormatterOptions.TimestampFormat = "hh::mm::ss ";
        });
        hostApplicationBuilder.Logging.SetMinimumLevel(garnetServerOptions.LogLevel);
        
        hostApplicationBuilder.Services.AddTransient<IClusterFactory, ClusterFactory>();
        hostApplicationBuilder.Services.AddTransient<StoreFactory>();
        hostApplicationBuilder.Services.AddTransient<StoreWrapperFactory>();

        hostApplicationBuilder.Services.AddSingleton<CustomCommandManager>();

        hostApplicationBuilder.Services.AddSingleton<AppendOnlyFileWrapper>(sp =>
        {
            var loggerFactory = sp.GetService<ILoggerFactory>();

            var options = sp.GetRequiredService<IOptions<GarnetServerOptions>>();
            var opts = options.Value;

            var appendOnlyFileWrapper = new AppendOnlyFileWrapper(null, null);

            if (opts.EnableAOF)
            {
                if (opts.MainMemoryReplication && opts.CommitFrequencyMs != -1)
                    throw new Exception(
                        "Need to set CommitFrequencyMs to -1 (manual commits) with MainMemoryReplication");

                opts.GetAofSettings(out var aofSettings);

                var aofDevice = aofSettings.LogDevice;
                var appendOnlyFile = new TsavoriteLog(aofSettings,
                    logger: loggerFactory?.CreateLogger("TsavoriteLog [aof]"));

                appendOnlyFileWrapper = new AppendOnlyFileWrapper(aofDevice, appendOnlyFile);

                if (opts.CommitFrequencyMs < 0 && opts.WaitForCommit)
                    throw new Exception("Cannot use CommitWait with manual commits");

                return appendOnlyFileWrapper;
            }

            if (opts.CommitFrequencyMs != 0 || opts.WaitForCommit)
                throw new Exception("Cannot use CommitFrequencyMs or CommitWait without EnableAOF");

            return appendOnlyFileWrapper;
        });
        
        hostApplicationBuilder.Services.AddSingleton<MainStoreWrapper>(sp =>
        {
            var storeFactory = sp.GetRequiredService<StoreFactory>();

            return storeFactory.CreateMainStore();
        });
        
        hostApplicationBuilder.Services.AddSingleton<ObjectStoreWrapper>(sp =>
        {
            var storeFactory = sp.GetRequiredService<StoreFactory>();

            return storeFactory.CreateObjectStore();
        });
        
        hostApplicationBuilder.Services.AddSingleton<IGarnetServer, GarnetServerTcp>();

        hostApplicationBuilder.Services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<GarnetServerOptions>>();
            var opts = options.Value;

            return new SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>>(
                new SpanByteKeySerializer(), null, opts.PubSubPageSizeBytes(), opts.SubscriberRefreshFrequencyMs, true);
        });

        hostApplicationBuilder.Services.AddSingleton<StoreWrapper>(sp =>
        {
            var storeWrapperFactory = sp.GetRequiredService<StoreWrapperFactory>();

            var version = GetVersion();

            return storeWrapperFactory.Create(version);
        });

        hostApplicationBuilder.Services.AddSingleton<GarnetProviderFactory>();

        hostApplicationBuilder.Services.AddSingleton<GarnetProvider>(sp =>
        {
            var garnetProviderFactory = sp.GetRequiredService<GarnetProviderFactory>();

            return garnetProviderFactory.Create();
        });
        
        hostApplicationBuilder.Services.AddSingleton<MetricsApi>();
        hostApplicationBuilder.Services.AddSingleton<RegisterApi>();
        hostApplicationBuilder.Services.AddSingleton<StoreApi>();

        hostApplicationBuilder.Services.AddTransient<GarnetServer>();
        
        /*
        hostApplicationBuilder.Services.AddTransient<GarnetServer>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<GarnetServerOptions>>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var server = sp.GetRequiredService<IGarnetServer>();
            var clusterFactory = sp.GetRequiredService<IClusterFactory>();
            var customCommandManager = sp.GetRequiredService<CustomCommandManager>();

            return new GarnetServer(options, loggerFactory, server, clusterFactory, customCommandManager);
        });
        */

        hostApplicationBuilder.Services.AddHostedService<GarnetServerHostedService>();

        /*
        hostApplicationBuilder.Services.AddHostedService<GarnetServerHostedService>(sp =>
        {
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var logger = loggerFactory.CreateLogger<GarnetServerHostedService>();

            var server =  new GarnetServer(garnetServerOptions, loggerFactory);

            return new GarnetServerHostedService(server, logger);
        });
        */
    }

    public GarnetApplication Build()
    {
        var host = hostApplicationBuilder.Build();
        return new GarnetApplication(host);
    }

    public void ConfigureContainer<TContainerBuilder>(IServiceProviderFactory<TContainerBuilder> factory,
        Action<TContainerBuilder> configure = null)
        where TContainerBuilder : notnull
    {
        hostApplicationBuilder.ConfigureContainer(factory, configure);
    }

    public IDictionary<object, object> Properties { get; }

    public IConfigurationManager Configuration
        => hostApplicationBuilder.Configuration;

    public IHostEnvironment Environment
        => hostApplicationBuilder.Environment;

    public ILoggingBuilder Logging
        => hostApplicationBuilder.Logging;

    public IMetricsBuilder Metrics
        => hostApplicationBuilder.Metrics;

    public IServiceCollection Services
        => hostApplicationBuilder.Services;
    
    
    static string GetVersion()
    {
        var Version = Assembly.GetExecutingAssembly().GetName().Version;
        return $"{Version.Major}.{Version.Minor}.{Version.Build}";
    }
}