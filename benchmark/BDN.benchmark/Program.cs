// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly)
#if DEBUG
    .Run(args, new DebugInProcessConfig());
#else
    .Run(args, new BaseConfig());
#endif

public class BaseConfig : ManualConfig
{
    public Job Net6BaseJob { get; }
    public Job Net8BaseJob { get; }

    public BaseConfig()
    {
        AddLogger(ConsoleLogger.Default);
        AddExporter(DefaultExporters.Markdown);
        AddColumnProvider(DefaultColumnProviders.Instance);

        var baseJob = Job.Default.WithGcServer(true);

        Net6BaseJob = baseJob.WithRuntime(CoreRuntime.Core60);
        Net8BaseJob = baseJob.WithRuntime(CoreRuntime.Core80)
            .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"));

        AddJob(
            Net6BaseJob.WithId(".NET 6"),
            Net8BaseJob.WithId(".NET 8")
            );
    }
}