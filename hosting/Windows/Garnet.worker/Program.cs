// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

class Program
{
    // Data finalization (AOF commit / checkpoint) uses up to 15 seconds internally (see GarnetServer.FinalizeDataAsync).
    // Add this buffer on top of the connection-drain timeout so the host shutdown budget covers the full shutdown sequence.
    private const int DataFinalizationBufferSeconds = 20;

    static void Main(string[] args)
    {
        // Pre-parse only the shutdown-timeout argument so we can configure both
        // the host shutdown budget and the Worker's connection-drain timeout from a single value.
        var shutdownTimeoutSeconds = ParseShutdownTimeoutSeconds(args, defaultSeconds: 5);
        var shutdownTimeout = TimeSpan.FromSeconds(shutdownTimeoutSeconds);

        var builder = Host.CreateApplicationBuilder(args);

        // Tell the .NET host (and the Windows SCM via WindowsServiceLifetime) how long to wait
        // before forcibly killing the process. We add DataFinalizationBufferSeconds so that AOF
        // commit / checkpoint can complete after connection draining finishes.
        builder.Services.Configure<HostOptions>(opts =>
        {
            opts.ShutdownTimeout = shutdownTimeout + TimeSpan.FromSeconds(DataFinalizationBufferSeconds);
        });

        builder.Services.AddHostedService(_ => new Worker(args, shutdownTimeout));

        builder.Services.AddWindowsService(options =>
        {
            options.ServiceName = "Microsoft Garnet Server";
        });

        var host = builder.Build();
        host.Run();
    }

    /// <summary>
    /// Scans <paramref name="args"/> for <c>--shutdown-timeout &lt;value&gt;</c> and returns
    /// the parsed integer, or <paramref name="defaultSeconds"/> if the argument is absent or invalid.
    /// This lightweight pre-parse avoids a full CommandLineParser pass before the host is built.
    /// </summary>
    private static int ParseShutdownTimeoutSeconds(string[] args, int defaultSeconds)
    {
        for (var i = 0; i < args.Length - 1; i++)
        {
            if (args[i] is "--shutdown-timeout" or "-shutdown-timeout" &&
                int.TryParse(args[i + 1], out var value) && value > 0)
            {
                return value;
            }
        }
        return defaultSeconds;
    }
}