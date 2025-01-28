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
    private readonly ILogger<GarnetServerHostedService> logger;
    private readonly GarnetServer server;

    public GarnetServerHostedService(
        ILogger<GarnetServerHostedService> logger,
        GarnetServer server)
    {
        this.logger = logger;
        this.server = server;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        server.Start();

        logger.LogInformation("Garnet server running at: {time}", DateTimeOffset.Now);

        await Task.CompletedTask;
    }
}