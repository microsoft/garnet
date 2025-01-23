// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Garnet;

public class GarnetServerHostedService : BackgroundService, IDisposable
{
    readonly GarnetServer server;
    readonly ILogger<GarnetServerHostedService> logger;
    
    public GarnetServerHostedService(
        GarnetServerOptions garnetServerOptions, 
        ILogger<GarnetServerHostedService> logger,
        ILoggerFactory loggerFactory)
    {
        this.server = new GarnetServer(garnetServerOptions, loggerFactory);
        this.logger = logger;
    }

    public GarnetServerHostedService(GarnetServer server, ILogger<GarnetServerHostedService> logger)
    {
        this.server = server;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        server.Start();
        
        logger.LogInformation("Garnet server running at: {time}", DateTimeOffset.Now);
        
        await Task.CompletedTask;
    }

    public void Dispose()
    {
        server.Dispose();
        base.Dispose();
    }
}