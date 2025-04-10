// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Perfolizer.Metrology;

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly)
#if DEBUG
    .Run(args, new DebugInProcessConfig());
#else
    .Run(args, new BaseConfig());
#endif

public class BaseConfig : ManualConfig
{
    public Job Net8BaseJob { get; }
    public Job Net9BaseJob { get; }

    public BaseConfig()
    {
        _ = AddLogger(ConsoleLogger.Default);
        _ = AddExporter(DefaultExporters.Markdown);
        _ = AddColumnProvider(DefaultColumnProviders.Instance);
        _ = WithSummaryStyle(SummaryStyle.Default.WithSizeUnit(SizeUnit.B));

        var baseJob = Job.Default.WithGcServer(true);

        Net8BaseJob = baseJob
            .WithRuntime(CoreRuntime.Core80)
            .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"));
        Net9BaseJob = baseJob
            .WithRuntime(CoreRuntime.Core90)
            .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"));

        // Get value of environment variable BDNRUNPARAM - determines if running net8.0, net9.0 or both (if env var is not set or invalid)
        var bdnRunParam = Environment.GetEnvironmentVariable("BDNRUNPARAM");

        switch (bdnRunParam)
        {
            case "net8.0":
                _ = AddJob(Net8BaseJob.WithId(".NET 8"));
                break;
            case "net9.0":
                _ = AddJob(Net9BaseJob.WithId(".NET 9"));
                break;
            default:
                _ = AddJob(
                    Net8BaseJob.WithId(".NET 8"),
                    Net9BaseJob.WithId(".NET 9")
                    );
                break;
        }
    }
}