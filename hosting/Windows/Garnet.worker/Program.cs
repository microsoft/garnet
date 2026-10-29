// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet;
using Garnet.server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

class Program
{
    // Extra host budget beyond connection drain + data finalization (quiesce, stop listening, etc.).
    private const int HostShutdownMarginSeconds = 5;

    static void Main(string[] args)
    {
        // Pre-parse shutdown-related arguments so we can configure both the host shutdown budget
        // and the Worker's connection-drain timeout before the generic host is built.
        var shutdownTimeoutSeconds = ParseIntOption(args, "--shutdown-timeout", "-shutdown-timeout", GarnetServerOptions.DefaultShutdownTimeoutSeconds);
        var dataFinalizationTimeoutSeconds = ParseIntOption(args, "--data-finalization-timeout", "-data-finalization-timeout", GarnetServerOptions.DefaultDataFinalizationTimeoutSeconds);
        var shutdownTimeout = TimeSpan.FromSeconds(shutdownTimeoutSeconds);

        var builder = Host.CreateApplicationBuilder(args);

        // Tell the .NET host (and the Windows SCM via WindowsServiceLifetime) how long to wait
        // before forcibly killing the process: drain + data finalization + margin.
        builder.Services.Configure<HostOptions>(opts =>
        {
            opts.ShutdownTimeout = shutdownTimeout
                + TimeSpan.FromSeconds(dataFinalizationTimeoutSeconds)
                + TimeSpan.FromSeconds(HostShutdownMarginSeconds);
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
    /// Scans <paramref name="args"/> for <paramref name="longName"/> (or <paramref name="shortName"/>) and returns
    /// the parsed positive integer, or <paramref name="defaultSeconds"/> if the argument is absent or invalid.
    /// </summary>
    private static int ParseIntOption(string[] args, string longName, string shortName, int defaultSeconds)
    {
        for (var i = 0; i < args.Length - 1; i++)
        {
            if (args[i] is var name && (name == longName || name == shortName) &&
                int.TryParse(args[i + 1], out var value) && value > 0)
            {
                return value;
            }
        }
        return defaultSeconds;
    }
}