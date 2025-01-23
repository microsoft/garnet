// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.server;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

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
        
        hostApplicationBuilder.Logging.ClearProviders();
        hostApplicationBuilder.Logging.AddSimpleConsole(simpleConsoleFormatterOptions =>
        {
            simpleConsoleFormatterOptions.SingleLine = true;
            simpleConsoleFormatterOptions.TimestampFormat = "hh::mm::ss ";
        });
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