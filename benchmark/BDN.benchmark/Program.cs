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
    public Job Net10BaseJob { get; }

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
        Net10BaseJob = baseJob
            .WithRuntime(CoreRuntime.CreateForNewVersion("net10.0", ".NET 10.0"))
            .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"));

        // Get value of environment variable BDNRUNPARAM - determines if running net8.0, net10.0 or both (if env var is not set or invalid)
        var bdnRunParam = Environment.GetEnvironmentVariable("BDNRUNPARAM");

        switch (bdnRunParam)
        {
            case "net8.0":
                _ = AddJob(Net8BaseJob.WithId(".NET 8"));
                break;
            case "net10.0":
                _ = AddJob(Net10BaseJob.WithId(".NET 10"));
                break;
            default:
                _ = AddJob(
                    Net8BaseJob.WithId(".NET 8"),
                    Net10BaseJob.WithId(".NET 10")
                    );
                break;
        }
    }
}