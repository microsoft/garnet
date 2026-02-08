// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

class Program
{
    static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Services.AddHostedService(_ => new Worker(args));

        builder.Services.AddWindowsService(options =>
        {
            options.ServiceName = "Microsoft Garnet Server";
        });

        var host = builder.Build();
        host.Run();
    }
}