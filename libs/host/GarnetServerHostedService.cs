// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Garnet;

public class GarnetServerHostedService : IHostedService
{
    readonly GarnetServer server;
    
    public GarnetServerHostedService(GarnetServer server)
    {
        this.server = server;
    }
    
    public Task StartAsync(CancellationToken cancellationToken)
    {
        server.Start();
        
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        server.Dispose();
        
        return Task.CompletedTask;
    }
}