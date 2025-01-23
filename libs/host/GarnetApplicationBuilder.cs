// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.cluster;
using Garnet.server;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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
        
        hostApplicationBuilder.Services.AddTransient<IClusterFactory, ClusterFactory>();
        hostApplicationBuilder.Services.AddTransient<CustomCommandManager>();

        hostApplicationBuilder.Services.AddTransient<IGarnetServer, GarnetServerTcp>();

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
}