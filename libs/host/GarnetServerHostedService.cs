// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Garnet;

internal class GarnetServerHostedService : BackgroundService
{
    readonly GarnetServer server;
    readonly ILogger<GarnetServerHostedService> logger;

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
}