// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

class Program
{
    static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Services.AddHostedService(_ => new Worker(args));

        // Configure Host shutdown timeout
        builder.Services.Configure<HostOptions>(options =>
        {
            // Set graceful shutdown timeout to 5 seconds
            options.ShutdownTimeout = TimeSpan.FromSeconds(5);
        });

        builder.Services.AddWindowsService(options =>
        {
            options.ServiceName = "Microsoft Garnet Server";
        });

        var host = builder.Build();
        host.Run();
    }
}