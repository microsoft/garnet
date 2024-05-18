// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Garnet
{
    /// <summary>
    /// Garnet server entry point
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            var builder = Host.CreateEmptyApplicationBuilder(null);
            builder.Services.AddHostedService(sp => new GarnetService(args));

            // Use OperatingSystem.IsWindows to determine if the server is running on Windows
            // The branch can be removed by the trimmer if the target OS is not Windows
            if (OperatingSystem.IsWindows())
            {
                builder.Services.AddWindowsService(options =>
                {
                    options.ServiceName = "Microsoft Garnet Server";
                });
            }

            var host = builder.Build();
            host.Run();
        }
    }
}